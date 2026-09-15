package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// fakeGuard records the guard calls so a test can assert the claim/release lifecycle,
// which is what stops an at-least-once redelivery from being mistaken for a duplicate.
type fakeGuard struct {
	claimable  bool
	claimErr   error
	releaseErr error

	claims    int
	releases  int
	lastKey   string
	lastToken string
}

func (g *fakeGuard) Claim(_ context.Context, _, key string) (string, error) {
	g.claims++
	g.lastKey = key
	if g.claimErr != nil {
		return "", g.claimErr
	}
	if !g.claimable {
		return "", nil
	}
	return "owner-token-1", nil
}

func (g *fakeGuard) Release(_ context.Context, _, key, token string) error {
	g.releases++
	g.lastKey = key
	g.lastToken = token
	return g.releaseErr
}

func newPipelineForTest(t *testing.T, store event.ConsumptionStore, guard DuplicateGuard) pipeline {
	t.Helper()
	pipe, err := newPipeline(store, guard, "test-scope")
	if err != nil {
		t.Fatalf("new pipeline: %v", err)
	}
	return pipe
}

func TestNewPipelineValidatesInputs(t *testing.T) {
	if _, err := newPipeline(nil, nil, "scope"); err == nil {
		t.Fatal("expected a nil store to be rejected")
	}
	if _, err := newPipeline(event.NewMemoryConsumptionStore(), nil, ""); err == nil {
		t.Fatal("expected an empty scope to be rejected")
	}
}

func TestPipelineAppliesAndCompletesOnSuccess(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, &fakeGuard{claimable: true})
	e := newChargeStartedEvent(t, "evt_ok")

	applied := 0
	err := pipe.run(context.Background(), e, DeliveryInfo{Stream: "s", StreamID: "1-1", Consumer: "c", Attempt: 1},
		func(context.Context, int) error { applied++; return nil })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected 1 apply, got %d", applied)
	}

	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected outcome %s, got %s", event.OutcomeSucceeded, entry.Outcome)
	}
	if entry.Stream != "s" || entry.StreamID != "1-1" || entry.Consumer != "c" {
		t.Fatalf("expected the stream coordinates recorded, got %+v", entry)
	}
}

// The consumption record is the authoritative duplicate check, so an event it already
// accepted must never reach the domain again.
func TestPipelineSkipsAnAlreadyConsumedEvent(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, &fakeGuard{claimable: true})
	e := newChargeStartedEvent(t, "evt_done")

	reservation, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID})
	if err != nil {
		t.Fatalf("seed begin: %v", err)
	}
	if err := store.Complete(context.Background(), reservation, event.OutcomeSucceeded, ""); err != nil {
		t.Fatalf("seed complete: %v", err)
	}

	applied := 0
	runErr := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error {
		applied++
		return nil
	})
	if !errors.Is(runErr, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", runErr)
	}
	if applied != 0 {
		t.Fatalf("the domain must not see a consumed event again, applied %d times", applied)
	}
}

// A guard contention after the store granted the reservation must never be reported as a
// duplicate. ErrDuplicate tells the worker to acknowledge, and acknowledging here would discard
// an event the authoritative store just decided nobody had consumed.
//
// The situation is a stale claim: the store found no live reservation, so whoever holds the guard
// is not processing this event right now. Reporting it as held leaves the entry pending, so the
// work resumes when the claim expires instead of being lost.
func TestPipelineTreatsAGuardHeldAfterAGrantedReservationAsHeldNotDuplicate(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	guard := &fakeGuard{claimable: false}
	pipe := newPipelineForTest(t, store, guard)
	e := newChargeStartedEvent(t, "evt_raced")

	applied := 0
	err := pipe.run(context.Background(), e, DeliveryInfo{Attempt: 1}, func(context.Context, int) error {
		applied++
		return nil
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
	if errors.Is(err, ErrDuplicate) {
		t.Fatal("a guard contention must not be reported as a duplicate, because the worker acknowledges duplicates")
	}
	if applied != 0 {
		t.Fatalf("a guard-held event must not be applied, applied %d times", applied)
	}
	if guard.claims != 1 {
		t.Fatalf("expected 1 claim attempt, got %d", guard.claims)
	}
	if guard.releases != 0 {
		t.Fatalf("the loser must not release another consumer's claim, got %d releases", guard.releases)
	}

	// The reservation is given back so the redelivery after the claim expires can take it again -
	// and it is abandoned, not failed, because the handler never ran: waiting for another
	// consumer's claim must not be charged to this event's retry budget.
	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome != event.OutcomeAbandoned {
		t.Fatalf("expected the reservation to be abandoned, got %s", entry.Outcome)
	}
	// The decisive assertion for the budget: the next delivery is granted the SAME attempt.
	next, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID})
	if err != nil {
		t.Fatalf("begin after the abandonment: %v", err)
	}
	if next.State != event.ReservationReserved {
		t.Fatalf("expected the redelivery to be granted the reservation, got %s", next.State)
	}
	if next.Attempt != 1 {
		t.Fatalf("expected waiting to leave the attempt count at 1, got %d", next.Attempt)
	}
}

// The Redis guard must not be able to overrule the store: when the store has already decided the
// event was consumed, that verdict stands and the delivery is acknowledged.
func TestPipelineTrustsTheStoreOverTheGuardForAnAlreadyConsumedEvent(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	// The guard would happily grant a claim, but the store says the event is finished.
	guard := &fakeGuard{claimable: true}
	pipe := newPipelineForTest(t, store, guard)
	e := newChargeStartedEvent(t, "evt_store_wins")

	reservation, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID})
	if err != nil {
		t.Fatalf("seed begin: %v", err)
	}
	if err := store.Complete(context.Background(), reservation, event.OutcomeSucceeded, ""); err != nil {
		t.Fatalf("seed complete: %v", err)
	}

	applied := 0
	err = pipe.run(context.Background(), e, DeliveryInfo{Attempt: 2}, func(context.Context, int) error {
		applied++
		return nil
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate from the store's verdict, got %v", err)
	}
	if applied != 0 {
		t.Fatalf("the domain must not see a consumed event again, applied %d times", applied)
	}
	if guard.claims != 0 {
		t.Fatalf("the guard must not be consulted once the store has decided, got %d claims", guard.claims)
	}
}

// Releasing on failure is essential: keeping the claim would make the redelivery look
// like a duplicate and the event would be lost.
func TestPipelineReleasesTheGuardAndReservationOnFailure(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	guard := &fakeGuard{claimable: true}
	pipe := newPipelineForTest(t, store, guard)
	e := newChargeStartedEvent(t, "evt_failed")

	applyErr := errors.New("gateway timeout")
	err := pipe.run(context.Background(), e, DeliveryInfo{Attempt: 1}, func(context.Context, int) error {
		return applyErr
	})
	if !errors.Is(err, applyErr) {
		t.Fatalf("expected the apply error to be returned unchanged, got %v", err)
	}
	if guard.releases != 1 {
		t.Fatalf("expected the guard claim to be released, got %d releases", guard.releases)
	}

	// The event must be re-processable after the failure.
	applied := 0
	if err := pipe.run(context.Background(), e, DeliveryInfo{Attempt: 2}, func(context.Context, int) error {
		applied++
		return nil
	}); err != nil {
		t.Fatalf("expected the retry to succeed, got %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected the retry to apply, got %d", applied)
	}
	entry, _ := store.Entry(e.EventID)
	if entry.Attempts != 2 {
		t.Fatalf("expected 2 recorded attempts, got %d", entry.Attempts)
	}
}

// The permanence marker must survive the pipeline, or the worker would retry a failure
// it should dead-letter immediately.
func TestPipelinePreservesPermanentClassification(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_perm")

	err := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error {
		return Permanent(errors.New("unknown charger"))
	})
	if !IsPermanent(err) {
		t.Fatalf("expected a permanent error to stay permanent, got %v", err)
	}
}

func TestPipelineWorksWithoutAGuard(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_noguard")

	if err := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
	if entry, _ := store.Entry(e.EventID); entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected success, got %s", entry.Outcome)
	}
}

// A guard outage is infrastructure: the reservation must be given back so a retry is not
// blocked by a stale reservation, and abandoning it must not spend the retry budget either -
// the handler never ran, so no attempt happened.
func TestPipelineReleasesTheReservationWhenTheGuardFails(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	guard := &fakeGuard{claimErr: errors.New("redis unavailable")}
	pipe := newPipelineForTest(t, store, guard)
	e := newChargeStartedEvent(t, "evt_guardfail")

	err := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error { return nil })
	if err == nil {
		t.Fatal("expected the guard failure to surface")
	}
	entry, _ := store.Entry(e.EventID)
	if entry.Outcome != event.OutcomeAbandoned {
		t.Fatalf("expected the reservation abandoned, got %s", entry.Outcome)
	}
	next, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID})
	if err != nil {
		t.Fatalf("begin after the abandonment: %v", err)
	}
	if next.Attempt != 1 {
		t.Fatalf("expected the retry to reuse attempt 1, got %d", next.Attempt)
	}
}

// The attempt handed to the handler comes from the consumption store, not from the transport's
// delivery count. Redis increments its delivery counter for every reclaim, including the reclaims
// that happen while another live lease holds the reservation and no attempt is made at all, so a
// budget measured against it would be spent by a lease wait.
func TestPipelinePassesTheStoresAttemptCountToApply(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_attempt")

	var got int
	// The transport claims this is delivery 40, which is what a lease wait plus retries would
	// produce. The store has never granted an attempt for this event, so the first attempt is 1.
	if err := pipe.run(context.Background(), e, DeliveryInfo{Attempt: 40}, func(_ context.Context, attempt int) error {
		got = attempt
		return nil
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected the store's first attempt (1), got %d", got)
	}

	// A redelivery after a failure advances the store's counter by exactly one, whatever the
	// transport reports.
	if err := pipe.run(context.Background(), newChargeStartedEvent(t, "evt_attempt_two"), DeliveryInfo{Attempt: 3}, func(_ context.Context, attempt int) error {
		return errors.New("transient")
	}); err == nil {
		t.Fatal("expected the failure to surface")
	}

	second := newChargeStartedEvent(t, "evt_attempt_two")
	if err := pipe.run(context.Background(), second, DeliveryInfo{Attempt: 400}, func(_ context.Context, attempt int) error {
		got = attempt
		return nil
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected the store's second attempt (2), got %d", got)
	}
}
