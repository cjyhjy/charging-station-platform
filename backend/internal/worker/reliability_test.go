package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// workerFixture builds a worker over the in-memory Streams adapter, so the retry and
// dead-letter decisions can be asserted against real pending, ack and dead-letter
// state instead of a mock.
type workerFixture struct {
	worker *Worker
	stream *redisrepo.MemoryStream
	config Config
}

const (
	fixtureStream  = "ncs:stream:charge-event"
	fixtureGroup   = "charge-event-workers"
	fixtureConsume = "worker-test-1"
)

// requireRecorder attaches a dead-letter recorder to a worker, which every worker now needs in
// order to serve: without it the worker cannot tell whether a delivery is still the current
// attempt, so it refuses to consume rather than risk overwriting a newer attempt's outcome.
func requireRecorder(t *testing.T, worker *Worker, store event.ConsumptionStore) {
	t.Helper()
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new dead-letter recorder: %v", err)
	}
	worker.SetDeadLetterRecorder(recorder)
}

// requireRunnerRecorder is the same for a runner, which forwards the recorder to every stream.
func requireRunnerRecorder(t *testing.T, runner *Runner, store event.ConsumptionStore) {
	t.Helper()
	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new dead-letter recorder: %v", err)
	}
	runner.SetDeadLetterRecorder(recorder)
}

func newWorkerFixture(t *testing.T, handler Handler, mutate func(*Config)) *workerFixture {
	t.Helper()
	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	config := Config{
		Stream:          fixtureStream,
		Group:           fixtureGroup,
		Consumer:        fixtureConsume,
		Count:           10,
		Block:           time.Millisecond,
		PendingInterval: time.Millisecond,
		RetryAfter:      0,
		MaxAttempts:     3,
	}
	if mutate != nil {
		mutate(&config)
	}
	worker, err := New(stream, handler, config)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return &workerFixture{worker: worker, stream: stream, config: config}
}

// publish writes an event to the fixture stream.
func (f *workerFixture) publish(t *testing.T, e event.Event) {
	t.Helper()
	fields, err := e.Fields()
	if err != nil {
		t.Fatalf("event fields: %v", err)
	}
	if _, err := f.stream.Add(context.Background(), f.config.Stream, fields); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// delivery reads the next pending entry for the fixture consumer. It uses the real
// read path so the returned Delivery carries a genuine delivery count.
func (f *workerFixture) delivery(t *testing.T) redisrepo.Delivery {
	t.Helper()
	ctx := context.Background()
	if err := f.stream.EnsureGroup(ctx, f.config.Stream, f.config.Group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	deliveries, err := f.stream.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: f.config.Stream, Group: f.config.Group, Consumer: f.config.Consumer, Count: 1,
	})
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	return deliveries[0]
}

// reclaim bumps the delivery count, simulating the pending-recovery pass that runs
// after RetryAfter.
func (f *workerFixture) reclaim(t *testing.T, id string) redisrepo.Delivery {
	t.Helper()
	claimed, err := f.stream.Claim(context.Background(), f.config.Stream, f.config.Group, f.config.Consumer, []string{id}, 0)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed delivery, got %d", len(claimed))
	}
	return claimed[0]
}

func (f *workerFixture) pending(t *testing.T) []redisrepo.PendingMessage {
	t.Helper()
	pending, err := f.stream.Pending(context.Background(), f.config.Stream, f.config.Group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	return pending
}

func (f *workerFixture) deadLetters(t *testing.T) []redisrepo.Record {
	t.Helper()
	records, err := f.stream.Records(context.Background(), event.StreamDeadLetter)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	return records
}

func newChargeStartedEvent(t *testing.T, id string) event.Event {
	t.Helper()
	e, err := event.New(event.ChargeStarted, "order", "order_01", "trace_01", map[string]string{"charger_id": "ch_01"})
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	e.EventID = id
	return e
}

// A permanent failure must not consume the retry budget: retrying cannot fix it, and
// spending the budget delays the operator seeing the bad event.
func TestWorkerDeadLettersAPermanentFailureImmediately(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("unsupported charger model"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_permanent"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	deadLetters := fixture.deadLetters(t)
	if len(deadLetters) != 1 {
		t.Fatalf("expected the event to be dead-lettered on the first attempt, got %d", len(deadLetters))
	}
	if deadLetters[0].Values["dead_letter_reason"] == "" {
		t.Fatal("expected a dead-letter reason")
	}
	if deadLetters[0].Values["event_id"] != "evt_permanent" {
		t.Fatalf("expected the original event fields to be preserved, got %#v", deadLetters[0].Values)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged, got %d pending", len(pending))
	}
}

// A transient failure must keep the entry pending so the recovery pass can retry it,
// and must only reach the dead-letter stream once the budget is exhausted.
func TestWorkerRetriesATransientFailureUntilTheBudgetIsExhausted(t *testing.T) {
	attempts := 0
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		attempts++
		return errors.New("charger gateway timeout")
	}), func(config *Config) { config.MaxAttempts = 2 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_transient"))
	first := fixture.delivery(t)
	if err := fixture.worker.process(context.Background(), first); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("the first transient failure must not be dead-lettered")
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending for a retry, got %d", len(pending))
	}

	// The recovery pass claims the entry, which advances the delivery count to the
	// budget, so the next failure is terminal.
	second := fixture.reclaim(t, first.ID)
	if second.DeliveryCount != 2 {
		t.Fatalf("expected delivery count 2 after a reclaim, got %d", second.DeliveryCount)
	}
	if err := fixture.worker.process(context.Background(), second); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(fixture.deadLetters(t)) != 1 {
		t.Fatalf("expected the exhausted retry to be dead-lettered, got %d", len(fixture.deadLetters(t)))
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

// A duplicate is normal under at-least-once delivery, so it must be acknowledged
// without a failure. Treating it as a failure would burn the budget and eventually
// dead-letter a perfectly good event.
func TestWorkerAcknowledgesDuplicateWithoutCountingAFailure(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return fmt.Errorf("wrapped: %w", ErrDuplicate)
	}), func(config *Config) { config.MaxAttempts = 1 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_duplicate"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("a duplicate must never be dead-lettered")
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the duplicate to be acknowledged, got %d pending", len(pending))
	}
}

// An entry that cannot be decoded has no retry that could help it.
func TestWorkerDeadLettersAnUndecodableEntry(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		t.Fatal("the handler must not be called for an undecodable entry")
		return nil
	}), func(config *Config) { config.MaxAttempts = 5 })

	ctx := context.Background()
	if err := fixture.stream.EnsureGroup(ctx, fixture.config.Stream, fixture.config.Group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if _, err := fixture.stream.Add(ctx, fixture.config.Stream, map[string]string{"event_id": "evt_broken"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	deliveries, err := fixture.stream.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: fixture.config.Stream, Group: fixture.config.Group, Consumer: fixture.config.Consumer, Count: 1,
	})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d and %v", len(deliveries), err)
	}
	if err := fixture.worker.process(ctx, deliveries[0]); err != nil {
		t.Fatalf("process: %v", err)
	}

	deadLetters := fixture.deadLetters(t)
	if len(deadLetters) != 1 {
		t.Fatalf("expected the broken entry to be dead-lettered, got %d", len(deadLetters))
	}
	// The reason is the bounded label an operator groups by, and the free text is kept separately so
	// a distinct error message cannot become a new metric series.
	if reason := deadLetters[0].Values["dead_letter_reason"]; reason != string(deadLetterReasonInvalidEvent) {
		t.Fatalf("expected the invalid_event reason label, got %q", reason)
	}
	if detail := deadLetters[0].Values["dead_letter_detail"]; detail == "" {
		t.Fatal("expected the free-text detail to be preserved on the dead letter")
	}
}

// Every outcome must reach the consumption record, not only the successful ones:
// otherwise an operator querying for lost events would find nothing.
func TestWorkerRecordsTheDeadLetterOutcome(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("malformed payload"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)

	fixture.publish(t, newChargeStartedEvent(t, "evt_recorded"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	entry, ok := store.Entry("evt_recorded")
	if !ok {
		t.Fatal("expected a consumption record for the dead-lettered event")
	}
	if entry.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("expected outcome %s, got %s", event.OutcomeDeadLettered, entry.Outcome)
	}
	if entry.Detail == "" {
		t.Fatal("expected the dead-letter reason to be recorded")
	}
}

type observed struct {
	delivery DeliveryInfo
}

// Handlers must see the transport metadata, because the attempt count is what tells a
// retry from a first delivery.
func TestWorkerPassesDeliveryMetadataToHandlers(t *testing.T) {
	seen := make(chan observed, 2)

	fixture := newWorkerFixture(t, recordingHandler{seen: seen}, func(config *Config) { config.MaxAttempts = 3 })

	fixture.publish(t, newChargeStartedEvent(t, "evt_meta"))
	first := fixture.delivery(t)
	if err := fixture.worker.process(context.Background(), first); err != nil {
		t.Fatalf("process: %v", err)
	}
	got := (<-seen).delivery
	if got.Stream != fixture.config.Stream {
		t.Errorf("expected stream %s, got %s", fixture.config.Stream, got.Stream)
	}
	if got.StreamID != first.ID {
		t.Errorf("expected stream id %s, got %s", first.ID, got.StreamID)
	}
	if got.Consumer != fixture.config.Consumer {
		t.Errorf("expected consumer %s, got %s", fixture.config.Consumer, got.Consumer)
	}
	if got.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", got.Attempt)
	}
}

type recordingHandler struct {
	seen chan observed
}

func (h recordingHandler) Handle(ctx context.Context, e event.Event) error {
	return h.HandleDelivery(ctx, e, DeliveryInfo{})
}

func (h recordingHandler) HandleDelivery(_ context.Context, _ event.Event, delivery DeliveryInfo) error {
	h.seen <- observed{delivery: delivery}
	return nil
}
