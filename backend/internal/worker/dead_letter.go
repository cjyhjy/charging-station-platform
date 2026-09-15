package worker

import (
	"context"
	"fmt"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// This file holds the dead-letter path, which is the one place in the worker where a
// partial failure can silently destroy an event.
//
// The dead-letter path has three effects, and every ordering of them can fail somewhere:
//
//	DLQ   - the parked entry an operator works through
//	REC   - the consumption record showing the event is terminally dead-lettered
//	ACK   - the acknowledgement that removes the entry from the source stream
//
// The order chosen here is guard -> DLQ -> REC -> ACK, and each step is chosen for what
// happens when the following step fails:
//
//   - The guard is claimed first, keyed on the event id, and is never released. A crash
//     after DLQ and before ACK leaves the source entry pending, so the recovery pass runs
//     this path again; the guard is already held, so no second dead letter is written. This
//     is what makes the DLQ write idempotent, which plain XADD cannot be.
//   - REC follows DLQ, so a crash between them leaves a parked entry with a stale record
//     rather than a record of a dead letter that does not exist. The retry repairs the
//     record.
//   - ACK is last, because acknowledging is the only irreversible step. Once the source
//     entry is gone nothing can rediscover it, so everything that must happen first has
//     already happened.
//
// The previous order was DLQ -> ACK -> REC, which acknowledged before the terminal fact was
// durable: a failure to record left the event acknowledged, parked, and recorded as merely
// failed, so an operator querying for lost events would find nothing.

// The dead-letter claim's key namespaces now live with the claim store itself
// (internal/repository/redis), which is where the two markers and their lifetimes are defined.
// The worker only names the states they produce.

// NewDeadLetterRecorder narrows a ConsumptionStore to a DeadLetterRecorder.
//
// It is the constructor a process uses to attach dead-letter recording to its workers, so
// the store stays the single place consumption outcomes are written. It is not optional: a
// worker without one refuses to serve, because it could not tell whether it is still the
// current attempt before parking an event.
func NewDeadLetterRecorder(store event.ConsumptionStore) (DeadLetterRecorder, error) {
	if store == nil {
		return nil, fmt.Errorf("consumption store is required")
	}
	return deadLetterRecorder{store: store}, nil
}

type deadLetterRecorder struct {
	store event.ConsumptionStore
}

func (r deadLetterRecorder) BeginDeadLettered(ctx context.Context, reservation event.Reservation, reason string) (event.DeadLetterClaim, error) {
	return r.store.BeginDeadLetter(ctx, reservation, reason)
}

func (r deadLetterRecorder) FinalizeDeadLettered(ctx context.Context, reservation event.Reservation, record event.ConsumptionRecord, reason string) (bool, error) {
	return r.store.FinalizeDeadLetter(ctx, reservation, record, reason)
}

func (r deadLetterRecorder) AbortDeadLettered(ctx context.Context, reservation event.Reservation, reason string) (bool, error) {
	return r.store.AbortDeadLetter(ctx, reservation, reason)
}

// DeadLetterClaimStore is the storage side of the dead-letter guard.
//
// It is the narrow view of the Redis claim store that the guard needs: who is writing an
// event's parked entry right now, and whether a write for it is already on record.
type DeadLetterClaimStore interface {
	// Claim either takes the write for this delivery or reports what is already known about it.
	Claim(ctx context.Context, eventID string) (token string, state redisrepo.DeadLetterClaimState, err error)
	// MarkWritten publishes that the parked entry exists.
	MarkWritten(ctx context.Context, eventID string, token string) error
	// Release gives the write claim back after a failed write.
	Release(ctx context.Context, eventID string, token string) error
}

// NewDeadLetterGuard adapts the stateful claim store into the guard the worker uses.
//
// The store reports its own state type because it lives in the Redis layer and cannot import
// this package; the mapping below is the only place the two vocabularies meet.
func NewDeadLetterGuard(store DeadLetterClaimStore) (DeadLetterGuard, error) {
	if store == nil {
		return nil, fmt.Errorf("dead-letter claim store is required")
	}
	return deadLetterGuard{store: store}, nil
}

type deadLetterGuard struct {
	store DeadLetterClaimStore
}

func (g deadLetterGuard) ClaimDeadLetter(ctx context.Context, eventID string) (string, DeadLetterClaimState, error) {
	token, state, err := g.store.Claim(ctx, eventID)
	if err != nil {
		return "", "", err
	}
	switch state {
	case redisrepo.DeadLetterClaimTaken:
		return token, DeadLetterClaimTaken, nil
	case redisrepo.DeadLetterClaimWritten:
		return "", DeadLetterClaimWritten, nil
	case redisrepo.DeadLetterClaimWriting:
		return "", DeadLetterClaimWriting, nil
	default:
		// An unknown state is refused rather than guessed at: the difference between the three
		// states is the difference between parking an event, skipping a duplicate write and
		// leaving the entry pending, and guessing wrong loses the event.
		return "", "", fmt.Errorf("claim dead letter for %s: unknown claim state %q", eventID, string(state))
	}
}

func (g deadLetterGuard) MarkDeadLetterWritten(ctx context.Context, eventID string, token string) error {
	return g.store.MarkWritten(ctx, eventID, token)
}

func (g deadLetterGuard) ReleaseDeadLetter(ctx context.Context, eventID string, token string) error {
	return g.store.Release(ctx, eventID, token)
}

// consumptionRecordFor builds the record the dead-letter path persists.
//
// It carries the stream coordinates so an operator can move from the record back to the
// entry, which is what makes a dead letter investigable. An undecodable entry has no event
// id, and the caller uses an empty record for it.
func consumptionRecordFor(e event.Event, delivery DeliveryInfo, attempt int) event.ConsumptionRecord {
	if e.EventID == "" {
		return event.ConsumptionRecord{}
	}
	return event.ConsumptionRecord{
		EventID:       e.EventID,
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		TraceID:       e.TraceID,
		Stream:        delivery.Stream,
		StreamID:      delivery.StreamID,
		Consumer:      delivery.Consumer,
		Attempt:       attempt,
	}
}
