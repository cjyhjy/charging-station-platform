package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the fifth review of BE-B-04: what a delivery that has outlived its own lease is
// allowed to do, and what a worker without a dead-letter recorder is allowed to do.
//
// The first could destroy a success, and the second could corrupt the consumption record without
// anyone noticing.

// The finding: an old consumer can overwrite a new consumer's success.
//
// The timeline is ordinary under a slow handler: this delivery reserves the event, its handler runs
// past the lease, another consumer takes the event over and applies it, and only then does this
// delivery fail - permanently, so it goes to the dead-letter path. Parking and recording it there
// would turn applied work into a dead letter, and acknowledging it would delete the only copy of
// work that consumer may still be doing.
func TestWorkerStaysSilentWhenANewerAttemptAlreadyAppliedTheEvent(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	// The delivery this worker is processing. The handler is wired through a variable so the test
	// can hand the worker the stale failure once the newer attempt has finished.
	var respond func(context.Context, event.Event) error
	e := newChargeStartedEvent(t, "evt_stale_success")
	fixture := fixtureWithStream(t, stream, stream, HandlerFunc(func(ctx context.Context, e event.Event) error {
		return respond(ctx, e)
	}), func(config *Config) { config.MaxAttempts = 3 })
	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	// The old generation reserves the event...
	stale, err := store.Begin(ctx, event.ConsumptionRecord{EventID: e.EventID, Stream: fixture.config.Stream, StreamID: delivery.ID})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// ...its lease expires, and a newer generation takes the event over and applies it.
	clock.advance(2 * time.Minute)
	applier := &stubApplier{}
	newer, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c2", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	if err := newer.HandleDelivery(ctx, e, DeliveryInfo{Stream: fixture.config.Stream, StreamID: delivery.ID, Consumer: "c2"}); err != nil {
		t.Fatalf("the newer attempt must succeed: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("expected the newer attempt to apply the event once, got %d", applier.calls)
	}

	// The old generation now fails permanently and reaches the dead-letter path with the
	// reservation it was granted - the one the event has since moved past.
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())
	respond = func(context.Context, event.Event) error {
		return &attemptError{attempt: stale.Attempt, reservation: stale, err: Permanent(errors.New("bad payload"))}
	}

	if err := fixture.worker.process(ctx, delivery); err != nil {
		t.Fatalf("a superseded delivery is not a failure of this worker: %v", err)
	}

	// The decisive assertions: the success survives, nothing was parked, and nothing was
	// acknowledged.
	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected the applied outcome to survive, got %s", entry.Outcome)
	}
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("applied work must not be parked, got %d dead letters", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("a superseded delivery must not acknowledge, got %d pending", len(pending))
	}
}

// The same refusal while the newer attempt is still working: the event belongs to somebody else,
// so nothing may be written, recorded or acknowledged.
func TestWorkerStaysSilentWhenANewerAttemptIsStillProcessing(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	var respond func(context.Context, event.Event) error
	e := newChargeStartedEvent(t, "evt_stale_in_progress")
	fixture := fixtureWithStream(t, stream, stream, HandlerFunc(func(ctx context.Context, e event.Event) error {
		return respond(ctx, e)
	}), func(config *Config) { config.MaxAttempts = 3 })
	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	stale, err := store.Begin(ctx, event.ConsumptionRecord{EventID: e.EventID, Stream: fixture.config.Stream, StreamID: delivery.ID})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, event.ConsumptionRecord{EventID: e.EventID, Stream: fixture.config.Stream, StreamID: delivery.ID})
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if current.Owner == stale.Owner {
		t.Fatal("expected the takeover to mint a new owner")
	}

	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())
	respond = func(context.Context, event.Event) error {
		return &attemptError{attempt: stale.Attempt, reservation: stale, err: Permanent(errors.New("bad payload"))}
	}

	if err := fixture.worker.process(ctx, delivery); err != nil {
		t.Fatalf("process: %v", err)
	}

	entry, _ := store.Entry(e.EventID)
	if entry.Outcome == event.OutcomeDeadLettered || entry.Outcome == event.OutcomeDeadLettering {
		t.Fatalf("a superseded delivery must leave the record alone, got %s", entry.Outcome)
	}
	if entry.Owner != current.Owner {
		t.Fatal("a superseded delivery must not disturb the attempt that owns the event")
	}
	if got := len(fixture.deadLetters(t)); got != 0 {
		t.Fatalf("expected no dead letter, got %d", got)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}
}

// The store's two phases are what make the refusal above possible, so they are asserted directly
// as well: a claim from an attempt that still owns the event is granted, and the terminal write
// happens only for the claim holder.
func TestDeadLetterRecordingRequiresTheCurrentAttempt(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}

	stale, err := store.Begin(ctx, event.ConsumptionRecord{EventID: "evt_claim_owner"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	claim, err := recorder.BeginDeadLettered(ctx, stale, "bad payload")
	if err != nil || claim != event.DeadLetterClaimGranted {
		t.Fatalf("expected the current attempt to be granted the claim, got %s and %v", claim, err)
	}

	// The lease expires; a newer attempt takes over and succeeds.
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, event.ConsumptionRecord{EventID: "evt_claim_owner"})
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if err := store.Complete(ctx, current, event.OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	finalized, err := recorder.FinalizeDeadLettered(ctx, stale, event.ConsumptionRecord{EventID: "evt_claim_owner"}, "bad payload")
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if finalized {
		t.Fatal("expected the terminal write to be refused after the event moved on")
	}
}

// The recorder is required, not optional. Without it a worker cannot ask whether it is still the
// current attempt, so it cannot park or acknowledge safely: the guarantee the P0 fix rests on would
// simply be absent, and the deployment would look healthy while corrupting records.
func TestWorkerRefusesToConsumeWithoutADeadLetterRecorder(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	worker, err := New(stream, HandlerFunc(func(context.Context, event.Event) error { return nil }), workerConfig())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = worker.Run(ctx)
	if err == nil {
		t.Fatal("expected a worker without a dead-letter recorder to refuse to consume")
	}
	if !strings.Contains(err.Error(), "dead-letter recorder is required") {
		t.Fatalf("expected the refusal to name the missing recorder, got %v", err)
	}
}

// pingCountingClient records whether a connection was used, so the runner's refusal can be
// distinguished from the per-worker one: the runner must refuse before it connects anything.
type pingCountingClient struct {
	*redisrepo.MemoryStream
	pings atomic.Int32
}

func (c *pingCountingClient) Ping(ctx context.Context) error {
	c.pings.Add(1)
	return c.MemoryStream.Ping(ctx)
}

// The runner surfaces the refusal at startup: a deployment that forgot the recorder fails to run
// rather than running without the guarantee. The enforcement lives in the worker, so this asserts
// the observable outcome - an error, and no connection used to consume anything.
func TestRunnerRefusesToStartWithoutADeadLetterRecorder(t *testing.T) {
	client := &pingCountingClient{MemoryStream: redisrepo.NewMemoryStream()}
	t.Cleanup(func() { _ = client.Close() })

	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), []StreamConfig{{
		Stream: "ncs:stream:charge-event", Group: "g", Consumer: "c", Block: time.Millisecond, PendingInterval: time.Hour,
	}})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	err = runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected the runner to refuse to start without a dead-letter recorder")
	}
	if !strings.Contains(err.Error(), "dead-letter recorder is required") {
		t.Fatalf("expected the refusal to name the missing recorder, got %v", err)
	}
	if pings := client.pings.Load(); pings != 0 {
		t.Fatalf("expected the refusal before any connection was used, got %d pings", pings)
	}
}

// A handler outside the pipeline reports no reservation, so there is no attempt to check - but the
// record is still checked, which is what stops a worker whose routing changed from parking an event
// another worker has already applied.
func TestDeadLetterWithoutAReservationStillRespectsAFinishedEvent(t *testing.T) {
	ctx := context.Background()
	store := event.NewMemoryConsumptionStore()
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}

	// A newer worker applied the event through the pipeline.
	reservation, err := store.Begin(ctx, event.ConsumptionRecord{EventID: "evt_unreserved"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Complete(ctx, reservation, event.OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// A worker that does not know the event type parks it without a reservation.
	claim, err := recorder.BeginDeadLettered(ctx, event.Reservation{}, "permanent_failure")
	if err != nil || claim != event.DeadLetterClaimGranted {
		t.Fatalf("expected the unkeyed claim to be granted, got %s and %v", claim, err)
	}
	finalized, err := recorder.FinalizeDeadLettered(ctx, event.Reservation{}, event.ConsumptionRecord{EventID: "evt_unreserved"}, "permanent_failure")
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if finalized {
		t.Fatal("expected the park to be refused for an event that already finished")
	}
	entry, _ := store.Entry("evt_unreserved")
	if entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected the success to survive, got %s", entry.Outcome)
	}
}
