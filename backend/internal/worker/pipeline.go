package worker

import (
	"context"
	"fmt"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// pipeline is the shared reliable-consumption sequence every event handler runs.
//
// It exists so the ordering rules are implemented once instead of being re-derived per
// handler, because getting them wrong is silent: an event is either applied twice or lost,
// and neither shows up as an error at the call site.
//
// The order is significant:
//
//  1. Begin on the consumption store. This is the authoritative duplicate check (contract
//     4.2 and 4.3), because PostgreSQL keeps the record and Redis may not. The result
//     distinguishes three states, and only one of them may apply the event:
//     reserved - this delivery owns the attempt; already consumed - acknowledge without
//     applying; in progress - do NOT acknowledge, because if that lease holder died this
//     delivery is the only thing that can still apply the event.
//  2. Claim on the duplicate guard. This is the cheap short circuit in front of the record,
//     and it is what rejects the common race where two consumers receive the same event
//     after a rebalance. A nil guard disables it, which is safe.
//  3. Apply. Only now does the event reach the domain.
//  4. Complete the record, or Fail it and release the guard claim so a redelivery can
//     retry. Releasing on failure is essential: keeping the claim would make the redelivery
//     look like a duplicate and the event would be lost.
type pipeline struct {
	store event.ConsumptionStore
	guard DuplicateGuard
	scope string
}

func newPipeline(store event.ConsumptionStore, guard DuplicateGuard, scope string) (pipeline, error) {
	if store == nil {
		return pipeline{}, fmt.Errorf("consumption store is required")
	}
	if scope == "" {
		return pipeline{}, fmt.Errorf("idempotency scope is required")
	}
	return pipeline{store: store, guard: guard, scope: scope}, nil
}

// run executes one consumption attempt.
//
// apply must be safe to call at most once per reservation: the pipeline guarantees it is not
// called for an event that is already consumed or currently held by a live lease, but it
// cannot undo it if it fails halfway, which is why the domain side must itself be idempotent
// or transactional.
func (p pipeline) run(ctx context.Context, e event.Event, delivery DeliveryInfo, apply func(context.Context, int) error) error {
	record := event.ConsumptionRecord{
		EventID:       e.EventID,
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		TraceID:       e.TraceID,
		Stream:        delivery.Stream,
		StreamID:      delivery.StreamID,
		Consumer:      delivery.Consumer,
		Attempt:       attemptOf(delivery),
	}

	reservation, err := p.store.Begin(ctx, record)
	if err != nil {
		// A store failure is infrastructure, so it is retryable rather than terminal.
		return fmt.Errorf("begin consumption of %s: %w", e.EventID, err)
	}
	switch reservation.State {
	case event.ReservationAlreadyConsumed:
		return ErrDuplicate
	case event.ReservationInProgress:
		// Another live lease owns this event. Returning an error rather than success keeps
		// the transport entry pending, so nothing is acknowledged and nothing is lost if
		// that lease holder turns out to be dead.
		return fmt.Errorf("%w: %s (lease expires %s)", ErrLeaseHeld, e.EventID, reservation.LeaseExpiresAt.Format("2006-01-02T15:04:05Z07:00"))
	case event.ReservationReserved:
		// Proceed below.
	default:
		return fmt.Errorf("begin consumption of %s: unknown reservation state %q", e.EventID, reservation.State)
	}

	claimToken := ""
	if p.guard != nil {
		token, err := p.guard.Claim(ctx, p.scope, e.EventID)
		if err != nil {
			// The guard's own policy decides whether an outage is fatal; when it is, the
			// reservation must be released so the retry is not blocked.
			//
			// Released rather than failed: the handler never ran, so this was not an attempt and
			// must not be charged to the retry budget. See ConsumptionStore.Release.
			_ = p.store.Release(ctx, reservation, "duplicate guard claim failed: "+err.Error())
			return fmt.Errorf("claim duplicate guard for %s: %w", e.EventID, err)
		}
		if token == "" {
			// The guard is held elsewhere, but the authoritative store has already granted this
			// delivery the reservation. That combination means the store found no live reservation,
			// so whoever holds the guard is not processing this event right now: it is a stale claim
			// from an attempt that died, or another process whose own store cannot see ours.
			//
			// It must NOT be reported as a duplicate. ErrDuplicate tells the worker to acknowledge,
			// and acknowledging here would discard an event the store just decided nobody had
			// consumed. Reporting it as held leaves the entry pending, so the work resumes when the
			// claim expires instead of being lost. The Redis layer is an optimisation and does not
			// get to overrule the store's verdict.
			//
			// Released rather than failed, for the same reason as above: waiting for another
			// consumer's claim is not an attempt, and charging it would let a burst of contention
			// spend the budget before the first real failure.
			_ = p.store.Release(ctx, reservation, "duplicate guard held after the reservation was granted")
			return fmt.Errorf("%w: %s (duplicate guard is held by another claim)", ErrLeaseHeld, e.EventID)
		}
		claimToken = token
	}

	if err := apply(ctx, reservation.Attempt); err != nil {
		// Preserve the error as-is so the worker can still tell a permanent failure from a
		// transient one and choose dead-letter or retry. The store's attempt number travels with
		// it, because the transport's delivery count also rises for reclaims that were never
		// attempts and is therefore not a retry budget.
		if p.guard != nil {
			// Released with the token this claim minted, so a claim another consumer has
			// since acquired is left alone.
			_ = p.guard.Release(ctx, p.scope, e.EventID, claimToken)
		}
		_ = p.store.Fail(ctx, reservation, err.Error())
		// The reservation travels with the failure, not only the attempt number: the dead-letter
		// path uses it to prove this delivery is still the current attempt before it writes or
		// acknowledges anything. Without it every permanent failure would reach the store with an
		// empty reservation, and the ownership check - the thing that stops a stale delivery
		// overwriting a newer attempt's outcome - would be skipped entirely.
		return &attemptError{attempt: reservation.Attempt, reservation: reservation, err: err}
	}

	if err := p.store.Complete(ctx, reservation, event.OutcomeSucceeded, ""); err != nil {
		// The attempt number travels with this error for the same reason it does above: the
		// transport's delivery count is not a retry budget. Without it the worker falls back to
		// that count, which reclaims have inflated, and a bookkeeping failure on an event that was
		// in fact applied would be misread as an exhausted retry budget and dead-lettered.
		return &attemptError{
			attempt:     reservation.Attempt,
			reservation: reservation,
			err:         fmt.Errorf("complete consumption of %s: %w", e.EventID, err),
		}
	}
	return nil
}

func attemptOf(delivery DeliveryInfo) int {
	if delivery.Attempt < 1 {
		return 1
	}
	return delivery.Attempt
}
