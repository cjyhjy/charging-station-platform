package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the third review of BE-B-04: failures on the crossing paths between the
// consumption store, the Redis guard and the retry budget.
//
// The first two could each leave an event in neither the source stream nor the dead-letter
// stream, which is worse than either a duplicate or a delay.

// Finding 1: if the dead-letter guard is claimed and the write then fails, the guard must be
// released. Leaving it held made the retry treat "a write is in progress" as "an entry exists",
// so it skipped the write, recorded a terminal outcome and acknowledged the source entry: the
// event would be in neither stream.
func TestDeadLetterReleasesItsGuardWhenTheParkFails(t *testing.T) {
	stream := &addFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failing: true}
	t.Cleanup(func() { _ = stream.Close() })

	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("bad payload"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	guard := newFakeDeadLetterGuard()
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_park_failed"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err == nil {
		t.Fatal("expected the parking failure to surface")
	}

	// The claim must not survive a write that did not happen.
	if guard.isWriting("evt_park_failed") {
		t.Fatal("expected the dead-letter claim to be released after the write failed")
	}
	// And nothing may be published as written, or the retry would skip the write it still owes.
	if guard.isWritten("evt_park_failed") {
		t.Fatal("a failed write must not be published as a completed one")
	}
	// Nothing was parked and nothing may be acknowledged, so the event is still recoverable.
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("expected no dead letter, got %d", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}

	// The decisive assertion: once the write can succeed, the retry parks the event. Before the
	// fix the released claim is what made this possible; without it the retry would have
	// acknowledged an event it never parked.
	stream.setFailing(false)
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the retry to park the event, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged after parking, got %d pending", len(pending))
	}
}

// The release must use the token the claim minted, so a claim acquired by another consumer in the
// meantime is left alone.
func TestDeadLetterGuardReleaseRequiresTheOwningToken(t *testing.T) {
	guard := newFakeDeadLetterGuard()
	ctx := context.Background()

	token, state, err := guard.ClaimDeadLetter(ctx, "evt_owner")
	if err != nil || state != DeadLetterClaimTaken {
		t.Fatalf("expected a claim, got %v and %v", state, err)
	}
	// A stale holder releasing with a token that does not own the claim changes nothing.
	if err := guard.ReleaseDeadLetter(ctx, "evt_owner", "someone-elses-token"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if !guard.isWriting("evt_owner") {
		t.Fatal("a foreign token must not release the claim")
	}
	if err := guard.ReleaseDeadLetter(ctx, "evt_owner", token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if guard.isWriting("evt_owner") {
		t.Fatal("the owning token must release the claim")
	}
}

// The Redis adapter must expose the same token discipline through the worker-facing interface.
func TestNewDeadLetterGuardRequiresAGuard(t *testing.T) {
	if _, err := NewDeadLetterGuard(nil); err == nil {
		t.Fatal("expected a nil guard to be rejected")
	}
}

// Finding 2: a stale Redis guard must not be able to overrule the consumption store. The store
// uses a five minute lease and the guard's TTL is longer, so a process that dies holding both
// leaves a guard that outlives the lease. The next delivery legitimately takes the lease over,
// and the guard then reports contention; treating that as a duplicate would acknowledge an event
// nobody applied.
func TestWorkerDoesNotAcknowledgeWhenAStaleGuardOutlivesTheLease(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	// The dead-letter/duplicate guard is held by the attempt that died, and its TTL is longer than
	// the lease, so it is still held when the lease is taken over.
	guard := &fakeGuard{claimable: false}

	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, store, guard, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	fixture := newWorkerFixture(t, handler, nil)

	e := newChargeStartedEvent(t, "evt_stale_guard")
	// The dead attempt reserved the event and then stopped.
	if _, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID}); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}

	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	// Inside the lease the delivery is refused, which is correct.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process inside the lease: %v", err)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending inside the lease, got %d", len(pending))
	}

	// Past the lease the delivery takes the reservation over, but the guard is still held.
	clock.advance(2 * time.Minute)
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process after the lease: %v", err)
	}

	// The decisive assertion: the entry must NOT be acknowledged. Acknowledging here would discard
	// an event the store had just granted this delivery, because the guard alone does not know
	// whether the event was applied.
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("a stale guard must leave the entry pending, got %d pending", len(pending))
	}
	if applier.calls != 0 {
		t.Fatalf("the handler must not run while the guard is held, ran %d times", applier.calls)
	}
	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("a guard contention must not dead-letter the event")
	}
}

// The worker must treat a guard contention as held rather than as a duplicate, because the two
// lead to opposite transport actions.
func TestWorkerLeavesAnEntryPendingWhenTheGuardIsHeld(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return ErrLeaseHeld
	}), nil)

	fixture.publish(t, newChargeStartedEvent(t, "evt_held_action"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}
	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("a held event must not be dead-lettered")
	}
}

// Finding 3: the retry budget must not be spent by the reclaims that happen while a lease is
// held. Those reclaims increment the Redis delivery count without any attempt being made, so
// measuring the budget against it means the first real transient failure after a lease wait is
// dead-lettered immediately.
func TestWorkerRetryBudgetIgnoresTransportReclaims(t *testing.T) {
	store := event.NewMemoryConsumptionStore()

	// The applier records the attempt the pipeline hands it, which is the store's count, and fails
	// transiently every time.
	var attempts []int
	applier := &failingApplier{attempts: &attempts, err: errors.New("gateway timeout")}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	fixture := newWorkerFixture(t, handler, func(config *Config) { config.MaxAttempts = 3 })
	// The recorder is what makes a dead-lettered event terminal in the store. Production always
	// wires it; see TestDeadLetteringIsOnlyTerminalWhenRecorded for what happens without it.
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)

	fixture.publish(t, newChargeStartedEvent(t, "evt_budget"))
	delivery := fixture.delivery(t)

	// Simulate a long lease wait: the transport count is already far beyond MaxAttempts because
	// every reclaim during the wait incremented it.
	delivery.DeliveryCount = 500

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process: %v", err)
	}

	// The first real failure must be retried, not dead-lettered, because the store has granted
	// exactly one attempt.
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("a transport count of 500 must not exhaust a 3 attempt budget, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending for a retry, got %d", len(pending))
	}
	if len(attempts) != 1 || attempts[0] != 1 {
		t.Fatalf("expected the handler to be told attempt 1, got %v", attempts)
	}

	// Two more attempts: the budget is three, so the third failure is terminal. The transport count
	// keeps climbing and must not change the decision.
	for i := 0; i < 2; i++ {
		delivery.DeliveryCount += 100
		if err := fixture.worker.process(context.Background(), delivery); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the third attempt to be dead-lettered, got %d", got)
	}
	if len(attempts) != 3 {
		t.Fatalf("expected 3 attempts, got %v", attempts)
	}
	// The dead letter records the store's attempt, not the inflated transport count.
	if got := fixture.deadLetters(t)[0].Values["dead_letter_attempts"]; got != "3" {
		t.Fatalf("expected the dead letter to record attempt 3, got %q", got)
	}

	// A fourth process call must not run the handler: the event is terminal and the store refuses
	// the reservation.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process after dead-lettering: %v", err)
	}
	if len(attempts) != 3 {
		t.Fatalf("a dead-lettered event must not be attempted again, attempts are %v", attempts)
	}
}

// failingApplier records the attempt number the pipeline passes and always fails.
type failingApplier struct {
	attempts *[]int
	err      error
}

func (a *failingApplier) Apply(_ context.Context, _ event.Event, attempt int) error {
	*a.attempts = append(*a.attempts, attempt)
	return a.err
}

// A recorded dead letter is terminal: the event must not be reserved again, or the handler would
// run a second time for an event that has already been given up on.
//
// The recorder is no longer optional, so the "without a recorder" half of this test is gone: a
// worker without one refuses to consume, which is asserted in review5_test.go. Leaving it in
// would have documented a configuration that can no longer run.
func TestADeadLetteredEventIsNotAttemptedAgain(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	var attempts []int
	applier := &failingApplier{attempts: &attempts, err: Permanent(errors.New("bad payload"))}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	fixture := newWorkerFixture(t, handler, func(config *Config) { config.MaxAttempts = 5 })
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())

	fixture.publish(t, newChargeStartedEvent(t, "evt_terminal"))
	delivery := fixture.delivery(t)

	for i := 0; i < 2; i++ {
		if err := fixture.worker.process(context.Background(), delivery); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}

	if len(attempts) != 1 {
		t.Fatalf("a recorded dead letter must be terminal, attempts are %v", attempts)
	}
	// The guard keeps the parked backlog free of duplicates.
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected exactly one dead letter, got %d", got)
	}
}

// The attempt carried by a handler failure must survive wrapping, so the worker can read it while
// errors.Is and errors.As still classify the failure.
func TestAttemptOfErrorReadsThroughWrapping(t *testing.T) {
	base := errors.New("transient")
	wrapped := &attemptError{attempt: 4, err: Permanent(base)}

	attempt, ok := attemptOfError(wrapped)
	if !ok || attempt != 4 {
		t.Fatalf("expected attempt 4, got %d ok=%v", attempt, ok)
	}
	if !IsPermanent(wrapped) {
		t.Fatal("the permanent marker must survive the attempt wrapper")
	}
	if !errors.Is(wrapped, base) {
		t.Fatal("the underlying error must remain reachable through the attempt wrapper")
	}
	if _, ok := attemptOfError(base); ok {
		t.Fatal("an error without an attempt must report that it has none")
	}
}
