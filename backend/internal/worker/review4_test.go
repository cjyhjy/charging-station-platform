package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the fourth review of BE-B-04: the crossing paths between the dead-letter
// claim, the consumption store's attempt counter and the stream coordinates a worker is
// started with.
//
// The first one could destroy an event outright, and the other three each misreport what
// happened.

// Finding 1: a claimed dead-letter write is not a completed one. When another consumer holds
// the claim and is still writing - blocked in XADD, or about to fail - this delivery used to
// conclude that a parked entry already existed, record the terminal outcome and acknowledge the
// source entry. If that other write then failed, the event was in neither stream: gone from the
// source stream because it was acknowledged, and absent from the dead-letter stream because the
// only write of it failed.
func TestWorkerLeavesTheEntryPendingWhileAnotherConsumerIsWritingTheDeadLetter(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("bad payload"))
	}), nil)

	guard := newFakeDeadLetterGuard()
	// Another consumer took the claim and has not published a completed write.
	guard.holdWrite("evt_inflight")
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_inflight"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("an in-flight write is not a failure of this delivery: %v", err)
	}

	// The decisive assertion: nothing was acknowledged, so the event is still recoverable.
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d pending", len(pending))
	}
	// And this delivery wrote nothing of its own, since another consumer owns the write.
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("expected no dead letter from this delivery, got %d", got)
	}
	// No completed write is on record either, so a retry must not skip the write.
	if guard.isWritten("evt_inflight") {
		t.Fatal("an in-flight write must never be recorded as a completed one")
	}
}

// The same scenario taken to its end: the other consumer's write fails and gives the claim
// back, and the retry then parks the event instead of having lost it.
func TestWorkerParksTheEventAfterTheOtherConsumerGivesUpTheWrite(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("bad payload"))
	}), nil)

	guard := newFakeDeadLetterGuard()
	guard.holdWrite("evt_inflight_gives_up")
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_inflight_gives_up"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process: %v", err)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}

	// The other consumer's XADD failed, so it releases the claim - the same token discipline the
	// Redis store enforces.
	if err := guard.ReleaseDeadLetter(context.Background(), "evt_inflight_gives_up", "another-consumer"); err != nil {
		t.Fatalf("release: %v", err)
	}

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry: %v", err)
	}
	// The event is now parked exactly once and the source entry is acknowledged.
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the retry to park the event, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged, got %d pending", len(pending))
	}
	if !guard.isWritten("evt_inflight_gives_up") {
		t.Fatal("expected the completed write to be published")
	}
}

// A retry that finds a completed write still finishes the remaining steps without writing a
// second copy - the behaviour the guard exists for, and the one the state model must not lose.
func TestWorkerSkipsTheWriteWhenACompletedOneIsOnRecord(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("bad payload"))
	}), nil)

	guard := newFakeDeadLetterGuard()
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_already_written"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("first process: %v", err)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected one dead letter, got %d", got)
	}

	// The retry sees the published write and must not park a second copy, while still finishing
	// the acknowledgement.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("second process: %v", err)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the parked copy to stay single, got %d", got)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged, got %d pending", len(pending))
	}
}

// Finding 2: waiting for another consumer's duplicate-guard claim is not an attempt. The wait
// used to be recorded with store.Fail, so every refusal advanced the reservation's attempt
// counter, and an event that queued behind a burst of contention arrived at its first real
// failure with the retry budget already spent.
func TestWaitingForTheDuplicateGuardDoesNotSpendTheRetryBudget(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	guard := &fakeGuard{claimable: false}
	applier := &stubApplier{}

	handler, err := NewChargeHandler(applier, store, guard, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	fixture := newWorkerFixture(t, handler, func(config *Config) { config.MaxAttempts = 3 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_waited"))
	delivery := fixture.delivery(t)

	// A burst of contention: every delivery is refused because another consumer holds the claim.
	for i := 0; i < 20; i++ {
		if err := fixture.worker.process(context.Background(), delivery); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	if applier.calls != 0 {
		t.Fatalf("a guard-held event must not reach the handler, ran %d times", applier.calls)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending through the contention, got %d", len(pending))
	}

	// The contention ends and the first real attempt fails transiently. It must still be attempt
	// one: 20 waits must not have consumed a three-attempt budget.
	guard.claimable = true
	applier.err = errors.New("device unavailable")

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("first real attempt: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("expected the handler to run once, ran %d times", applier.calls)
	}
	if applier.attempt != 1 {
		t.Fatalf("expected the first real attempt to be attempt 1, got %d", applier.attempt)
	}
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("a transient failure on attempt 1 must not be dead-lettered, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending for the retry, got %d", len(pending))
	}
}

// completeFailingStore fails the first N terminal writes, which is how a bookkeeping write that
// cannot reach the database behaves after the event itself was applied.
type completeFailingStore struct {
	*event.MemoryConsumptionStore
	failuresLeft int
}

func (s *completeFailingStore) Complete(ctx context.Context, reservation event.Reservation, outcome event.ConsumptionOutcome, detail string) error {
	if s.failuresLeft > 0 {
		s.failuresLeft--
		return errors.New("consumption store unavailable")
	}
	return s.MemoryConsumptionStore.Complete(ctx, reservation, outcome, detail)
}

// Finding 3: a failure to write the terminal record is infrastructure, not the event's fault,
// and it now travels with the store's attempt number. Without it the worker fell back to the
// transport's delivery count - which reclaims inflate without any attempt being made - and an
// event that had in fact been applied was dead-lettered because a bookkeeping write failed.
func TestAFailedTerminalWriteIsNotChargedToTheTransportDeliveryCount(t *testing.T) {
	const inflatedDeliveryCount = 50

	clock := newLocalClock()
	store := &completeFailingStore{MemoryConsumptionStore: event.NewMemoryConsumptionStore(), failuresLeft: 1}
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	fixture := newWorkerFixture(t, handler, func(config *Config) { config.MaxAttempts = 3 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_complete_failed"))
	delivery := fixture.delivery(t)
	// Redis would report this many deliveries after a long lease wait, with no attempt made.
	delivery.DeliveryCount = inflatedDeliveryCount

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("a bookkeeping failure is retryable and must not surface as a delivery failure: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("expected the event to be applied once, got %d", applier.calls)
	}
	// The decisive assertion: a transport count of 50 must not be read as an exhausted
	// three-attempt budget, or an applied event would be parked and counted twice.
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("an infrastructure write failure must not be dead-lettered, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}
	entry, ok := store.Entry("evt_complete_failed")
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome == event.OutcomeDeadLettered {
		t.Fatal("the event must not be recorded as terminally dead-lettered")
	}

	// The reservation is still live, because the terminal write is what would have finished it, so
	// the immediate retry is refused as held and must not re-apply an event that was applied.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry while the reservation is live: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("the event must not be applied again while its reservation is live, applied %d times", applier.calls)
	}

	// Once the lease expires the reservation is taken over, and the budget continues from the
	// store's own count: attempt 2 and 3 of 3, not attempt 51 of 3.
	clock.advance(2 * time.Minute)
	applier.err = errors.New("device unavailable")
	for i := 0; i < 2; i++ {
		if err := fixture.worker.process(context.Background(), delivery); err != nil {
			t.Fatalf("attempt %d: %v", i+2, err)
		}
	}
	if applier.attempt != 3 {
		t.Fatalf("expected the last attempt to be attempt 3, got %d", applier.attempt)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the exhausted budget to park the event once, got %d dead letters", got)
	}
}

// recordingStartClient records the start position each consumer group was created with, so the
// runner's propagation can be asserted against the call the worker actually makes.
type recordingStartClient struct {
	*redisrepo.MemoryStream

	mu       sync.Mutex
	startIDs map[string]string
}

func newRecordingStartClient() *recordingStartClient {
	return &recordingStartClient{MemoryStream: redisrepo.NewMemoryStream(), startIDs: map[string]string{}}
}

func (c *recordingStartClient) EnsureGroup(ctx context.Context, stream, group, startID string) error {
	if err := c.MemoryStream.EnsureGroup(ctx, stream, group, startID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startIDs[stream+"/"+group] = startID
	return nil
}

// startPosition reports the position the group was created with, or "" while it does not exist
// yet. It is synchronised because the runner creates the group on its own goroutine.
func (c *recordingStartClient) startPosition(stream, group string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startIDs[stream+"/"+group]
}

// Finding 4: StartID was not carried through StreamConfig, so a process that started its workers
// through the runner could only ever get the default. The documented way to ignore pre-existing
// history - an explicit "$" - was unusable there, and a deployment that asked for it silently
// replayed the backlog instead.
func TestRunnerPassesTheConfiguredStartPositionToTheWorker(t *testing.T) {
	client := newRecordingStartClient()
	t.Cleanup(func() { _ = client.Close() })

	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), []StreamConfig{{
		Stream:          "ncs:stream:charge-event",
		Group:           "charge",
		Consumer:        "c1",
		Count:           1,
		Block:           time.Millisecond,
		PendingInterval: time.Hour,
		RetryAfter:      time.Hour,
		MaxAttempts:     3,
		StartID:         "$",
	}})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for client.startPosition("ncs:stream:charge-event", "charge") == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runner: %v", err)
	}

	if got := client.startPosition("ncs:stream:charge-event", "charge"); got != "$" {
		t.Fatalf("expected the group to be created at %q, got %q", "$", got)
	}
}

// An unset start position keeps meaning "the beginning of the stream", which is what makes the
// backlog published before the worker first started recoverable.
func TestRunnerDefaultsTheStartPositionToTheBeginningOfTheStream(t *testing.T) {
	client := newRecordingStartClient()
	t.Cleanup(func() { _ = client.Close() })

	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), []StreamConfig{{
		Stream:          "ncs:stream:charge-event",
		Group:           "charge",
		Consumer:        "c1",
		Count:           1,
		Block:           time.Millisecond,
		PendingInterval: time.Hour,
		RetryAfter:      time.Hour,
		MaxAttempts:     3,
	}})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for client.startPosition("ncs:stream:charge-event", "charge") == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runner: %v", err)
	}

	if got := client.startPosition("ncs:stream:charge-event", "charge"); got != defaultGroupStartID {
		t.Fatalf("expected the group to be created at %q, got %q", defaultGroupStartID, got)
	}
}
