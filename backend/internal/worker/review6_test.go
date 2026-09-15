package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the sixth review of BE-B-04: both findings were about the *real* execution path,
// because the previous round's tests built the pieces by hand and therefore never ran it.
//
// The first is that the pipeline did not hand the reservation to the worker at all, so the
// ownership check added for the stale-generation fix was skipped on every permanent failure. The
// second is that the dead-letter claim held no lease, so it excluded nobody.

// The finding: pipeline.go built the attemptError with the attempt number only, so a permanent
// failure or an exhausted budget reached the dead-letter path with an empty reservation. The
// ownership check compares the reservation's owner with the record's, and an empty reservation
// cannot match anything - which is why the check refuses it rather than trusting it, and why
// handing it over is what makes the check work at all.
//
// This goes through the real pipeline: a handler, a store and a delivery, with nothing built by
// hand.
func TestAPermanentFailureThroughThePipelineCarriesItsReservation(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	handler, err := NewChargeHandler(&stubApplier{err: Permanent(errors.New("bad payload"))}, store, nil,
		ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	e := newChargeStartedEvent(t, "evt_pipeline_reservation")
	err = handler.HandleDelivery(ctx, e, DeliveryInfo{Stream: "ncs:stream:charge-event", StreamID: "1-1", Consumer: "c1"})
	if err == nil {
		t.Fatal("expected the permanent failure to surface")
	}
	if !IsPermanent(err) {
		t.Fatalf("expected the permanent marker to survive, got %v", err)
	}

	reservation, ok := reservationOfError(err)
	if !ok {
		t.Fatal("a failed attempt must carry its reservation, or the dead-letter path cannot check ownership")
	}
	if reservation.EventID != e.EventID {
		t.Fatalf("expected the reservation for %s, got %q", e.EventID, reservation.EventID)
	}
	if reservation.Owner == "" {
		t.Fatal("expected the reservation to name the owner that was granted the attempt")
	}
	if reservation.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", reservation.Attempt)
	}
	// It must be the reservation the store still holds: the check is only meaningful when the
	// owner matches what the record says.
	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Owner != reservation.Owner {
		t.Fatalf("expected the reservation owner %q to match the record's %q", reservation.Owner, entry.Owner)
	}
}

// takeoverApplier models what happens when a handler outlives its own lease: the lease expires
// while the handler is still running, and another consumer takes the event over.
//
// It is how the stale-generation case actually happens, so the tests below need no hand-built error
// at all - the permanent failure comes from this applier, through the pipeline.
type takeoverApplier struct {
	clock *localClock
	store event.ConsumptionStore
	// newer is the handler of the consumer that takes over. When it is set, that consumer applies
	// the event and completes it, which is the case that used to overwrite a success.
	newer Handler
	// takeOverOnly makes the second consumer reserve the event without finishing it, which models
	// a newer attempt that is still working when the old one fails.
	takeOverOnly bool

	calls   int
	attempt int
}

func (a *takeoverApplier) Apply(ctx context.Context, e event.Event, attempt int) error {
	a.calls++
	a.attempt = attempt
	// The handler outlives the attempt's lease.
	a.clock.advance(2 * time.Minute)
	if a.takeOverOnly {
		if _, err := a.store.Begin(ctx, event.ConsumptionRecord{EventID: e.EventID}); err != nil {
			return err
		}
	}
	if a.newer != nil {
		if err := a.newer.Handle(ctx, e); err != nil {
			return err
		}
	}
	return Permanent(errors.New("bad payload"))
}

// The end-to-end version of the previous round's finding: the old consumer's handler fails
// permanently *after* a newer consumer has taken the event over and applied it. Everything here is
// the real path - the pipeline grants the reservation, the takeover happens inside the handler, and
// the failure travels back with that reservation.
func TestAStaleDeliveryThroughThePipelineStaysSilentAfterTheEventWasApplied(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	newerApplier := &stubApplier{}
	newer, err := NewChargeHandler(newerApplier, store, nil, ChargeHandlerConfig{Consumer: "c2", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	applier := &takeoverApplier{clock: clock, newer: newer}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	fixture := fixtureWithStream(t, stream, stream, handler, func(config *Config) { config.MaxAttempts = 3 })
	requireRecorder(t, fixture.worker, store)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())

	e := newChargeStartedEvent(t, "evt_pipeline_stale")
	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(ctx, delivery); err != nil {
		t.Fatalf("a superseded delivery is not a failure of this worker: %v", err)
	}
	if applier.calls != 1 || newerApplier.calls != 1 {
		t.Fatalf("expected one attempt each, got %d and %d", applier.calls, newerApplier.calls)
	}

	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected the newer attempt's success to survive, got %s", entry.Outcome)
	}
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("applied work must not be parked, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("a superseded delivery must not acknowledge, got %d pending", len(pending))
	}
}

// The same path while the newer attempt is still working: the event belongs to it, so this delivery
// may not write, record or acknowledge anything.
func TestAStaleDeliveryThroughThePipelineStaysSilentWhileANewerAttemptWorks(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	applier := &takeoverApplier{clock: clock, store: store, takeOverOnly: true}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	fixture := fixtureWithStream(t, stream, stream, handler, func(config *Config) { config.MaxAttempts = 3 })
	requireRecorder(t, fixture.worker, store)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())

	e := newChargeStartedEvent(t, "evt_pipeline_held")
	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(ctx, delivery); err != nil {
		t.Fatalf("process: %v", err)
	}

	// The newer attempt holds the event now, and this delivery may not touch it.
	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome == event.OutcomeDeadLettered || entry.Outcome == event.OutcomeDeadLettering {
		t.Fatalf("a superseded delivery must leave the record alone, got %s", entry.Outcome)
	}
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("expected no dead letter, got %d", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}
}

// The finding: the dead-letter claim held no lease, and Fail had already cleared the one it
// inherited. Begin only treated an empty outcome with a live lease as in-progress, so a second
// consumer could reserve the event immediately and run the handler again while the first delivery
// was still writing and finalising the parked entry.
func TestTheDeadLetterClaimHoldsTheEventWhileTheParkIsWritten(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := event.ConsumptionRecord{EventID: "evt_claim_holds"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// The handler failed, which is what clears the reservation's lease before the claim is taken.
	if err := store.Fail(ctx, reservation, "bad payload"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	claim, err := store.BeginDeadLetter(ctx, reservation, "permanent_failure")
	if err != nil || claim != event.DeadLetterClaimGranted {
		t.Fatalf("expected the claim to be granted, got %s and %v", claim, err)
	}

	// A second delivery must be told the event is being handled, not handed the reservation.
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != event.ReservationInProgress {
		t.Fatalf("expected the claim to hold the event, got %s", second.State)
	}
	if second.Attempt != reservation.Attempt {
		t.Fatalf("expected no new attempt while the claim is held, got %d", second.Attempt)
	}

	// The other release paths must not clear the claim's lease either: Fail and Release are what a
	// pipeline calls for a reservation, and either of them reaching a claim would hand the event to
	// another consumer while the parked entry is still being written.
	if err := store.Fail(ctx, reservation, "late failure"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := store.Release(ctx, reservation, "late release"); err != nil {
		t.Fatalf("release: %v", err)
	}
	stillHeld, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if stillHeld.State != event.ReservationInProgress {
		t.Fatalf("Fail and Release must not release a dead-letter claim, got %s", stillHeld.State)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != event.OutcomeDeadLettering {
		t.Fatalf("expected the claim to survive, got %s", entry.Outcome)
	}

	// Once the claim's lease expires, a park that died mid-write is recoverable: the event can be
	// taken over and the handler runs again.
	clock.advance(2 * time.Minute)
	third, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if third.State != event.ReservationReserved {
		t.Fatalf("expected an expired claim to be recoverable, got %s", third.State)
	}
	if third.Attempt != reservation.Attempt+1 {
		t.Fatalf("expected the takeover to be a new attempt, got %d", third.Attempt)
	}
}

// A write that fails must give the claim back, so the retry parks the event immediately instead of
// waiting for the claim to expire - and the record must not be left claiming a park that is not
// happening.
func TestAFailedDeadLetterWriteGivesTheClaimBack(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := event.ConsumptionRecord{EventID: "evt_claim_abort"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := store.BeginDeadLetter(ctx, reservation, "permanent_failure"); err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	gave, err := store.AbortDeadLetter(ctx, reservation, "write failed")
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if !gave {
		t.Fatal("expected the claim holder to be able to give the claim back")
	}

	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != event.OutcomeFailed {
		t.Fatalf("expected the reservation released as failed, got %s", entry.Outcome)
	}
	// Nothing holds the event any more, so the retry may reserve and claim again immediately.
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != event.ReservationReserved {
		t.Fatalf("expected the retry to be granted the event, got %s", second.State)
	}
	claim, err := store.BeginDeadLetter(ctx, second, "permanent_failure")
	if err != nil || claim != event.DeadLetterClaimGranted {
		t.Fatalf("expected the retry to claim the park, got %s and %v", claim, err)
	}
}

// A claim can only be given up by the reservation that holds it, so a stale delivery cannot release
// the attempt that took the event over.
func TestAbortingADeadLetterClaimRequiresTheClaimHolder(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := event.ConsumptionRecord{EventID: "evt_claim_abort_owner"}

	stale, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := store.BeginDeadLetter(ctx, stale, "permanent_failure"); err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if current.Owner == stale.Owner {
		t.Fatal("expected the takeover to mint a new owner")
	}

	gave, err := store.AbortDeadLetter(ctx, stale, "stale holder")
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if gave {
		t.Fatal("a stale holder must not release the claim of the attempt that took the event over")
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Owner != current.Owner {
		t.Fatal("a stale abort must not disturb the attempt that owns the event")
	}
}

// Through the worker: the dead-letter write fails, so the claim must be given back and the event
// must stay retryable - and a later attempt must park it once the stream accepts writes again.
func TestWorkerGivesTheClaimBackWhenTheDeadLetterWriteFails(t *testing.T) {
	stream := &addFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failing: true}
	t.Cleanup(func() { _ = stream.Close() })

	// A real pipeline, so the failure carries the reservation this test is about.
	store := event.NewMemoryConsumptionStore()
	handler, err := NewChargeHandler(&stubApplier{err: Permanent(errors.New("bad payload"))}, store, nil,
		ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, handler, func(config *Config) { config.MaxAttempts = 5 })
	requireRecorder(t, fixture.worker, store)
	guard := newFakeDeadLetterGuard()
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_write_failed"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err == nil {
		t.Fatal("expected the failed write to surface")
	}

	// The claim was given back: the record must not claim a park that is not happening.
	entry, ok := store.Entry("evt_write_failed")
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome == event.OutcomeDeadLettering {
		t.Fatal("a failed write must not leave the event marked as being parked")
	}
	if guard.isWritten("evt_write_failed") {
		t.Fatal("a failed write must not be published as a completed one")
	}

	// Once the stream accepts writes, the retry parks the event and acknowledges it.
	stream.setFailing(false)
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the retry to park the event, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged, got %d pending", len(pending))
	}
}
