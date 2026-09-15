package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the review findings for BE-B-05, plus the discovery the fixes came with: a metric
// is a claim about what happened, so it may only be reported once the thing it claims has really
// happened, and no label may be able to grow with traffic.

// Finding: an outcome was reported before the transport action that made it true. A failed
// acknowledgement sends the entry back for another attempt, so reporting first counted the same
// delivery twice and the success total grew without extra work having been done.
//
// The handler runs through the real consumption pipeline here, so the redelivery is a genuine
// duplicate of an event that was applied once - which is the case where a premature "succeeded"
// is most misleading, because it is reported for a delivery that will be processed again.
func TestWorkerDoesNotReportASuccessItFailedToAcknowledge(t *testing.T) {
	stream := &ackFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failuresLeft: 1}
	t.Cleanup(func() { _ = stream.Close() })

	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, event.NewMemoryConsumptionStore(), nil, ChargeHandlerConfig{
		Consumer: "c1",
		Scope:    "charge-event",
	})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}

	observer := &recordingObserver{}
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, handler, nil)
	fixture.worker.SetObserver(observer)

	fixture.publish(t, newChargeStartedEvent(t, "evt_ack_retry"))
	first := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), first); err == nil {
		t.Fatal("expected the failed acknowledgement to be reported")
	}
	if outcomes := observer.outcomes(); len(outcomes) != 0 {
		t.Fatalf("nothing may be reported before the acknowledgement succeeds, got %v", outcomes)
	}
	// The entry is still pending, which is what makes the retry possible at all.
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d", len(pending))
	}

	// The redelivery acknowledges successfully. The event was already applied, so the pipeline
	// reports a duplicate and the applier does not run a second time.
	redelivery := fixture.reclaim(t, first.ID)
	if err := fixture.worker.process(context.Background(), redelivery); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	outcomes := observer.outcomes()
	if len(outcomes) != 1 {
		t.Fatalf("expected exactly one reported outcome across both deliveries, got %v", outcomes)
	}
	if outcomes[0] != observability.OutcomeDuplicate {
		t.Fatalf("expected %s, got %s", observability.OutcomeDuplicate, outcomes[0])
	}
	if applier.calls != 1 {
		t.Fatalf("expected the event to be applied once, got %d", applier.calls)
	}
}

// The same rule for the other direction: a parked event must not be counted until it is really in
// the dead-letter stream AND the source entry is really acknowledged.
func TestWorkerDoesNotCountADeadLetterItFailedToAcknowledge(t *testing.T) {
	stream := &ackFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failuresLeft: 1}
	t.Cleanup(func() { _ = stream.Close() })

	observer := &recordingObserver{}
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("unsupported charger model"))
	}), nil)
	fixture.worker.SetObserver(observer)
	// The guard is what makes the parking write idempotent, so the failed acknowledgement is what is
	// under test rather than a duplicate park.
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())

	fixture.publish(t, newChargeStartedEvent(t, "evt_dlq_ack_retry"))
	first := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), first); err == nil {
		t.Fatal("expected the failed acknowledgement to be reported")
	}
	observer.mu.Lock()
	deadLetteredAfterFailure := len(observer.deadLettered)
	handledAfterFailure := len(observer.handled)
	observer.mu.Unlock()
	if deadLetteredAfterFailure != 0 || handledAfterFailure != 0 {
		t.Fatalf("expected nothing reported while the acknowledgement failed, got dead_lettered=%d handled=%d",
			deadLetteredAfterFailure, handledAfterFailure)
	}

	// The event is parked exactly once, even though the parking attempt failed after the write.
	if records := fixture.deadLetters(t); len(records) != 1 {
		t.Fatalf("expected exactly one parked copy, got %d", len(records))
	}

	redelivery := fixture.reclaim(t, first.ID)
	if err := fixture.worker.process(context.Background(), redelivery); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	// The retry found the dead letter already written, so it is reported as a suppression and NOT as
	// a write. Counting both would make the parked backlog look twice as large as it is.
	if len(observer.deadLettered) != 0 {
		t.Fatalf("a skipped write must not be counted as a write: %#v", observer.deadLettered)
	}
	if len(observer.suppressed) != 1 {
		t.Fatalf("expected one suppression, got %d", len(observer.suppressed))
	}
	// The terminal outcome is still reported once the entry is finally acknowledged.
	if len(observer.handled) != 1 || observer.handled[0].outcome != observability.OutcomeDeadLettered {
		t.Fatalf("expected one terminal outcome, got %#v", observer.handled)
	}
}

// Finding: event_type comes from the envelope and anything can put an arbitrary value there, so
// using it as a metric label let traffic grow the series set without bound - a memory-exhaustion
// path needing no privilege at all. Anything outside the frozen event list is reported as "unknown".
//
// The invented types go through the router, which is what really happens to them: no handler accepts
// them, so they are parked. That is the path where an unbounded label would be most expensive,
// because a publisher can produce one series per invented type without ever getting an event applied.
func TestWorkerBoundsTheEventTypeLabel(t *testing.T) {
	handler, err := NewChargeHandler(&stubApplier{}, event.NewMemoryConsumptionStore(), nil, ChargeHandlerConfig{
		Consumer: "c1",
		Scope:    "charge-event",
	})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	router := NewRouter()
	if err := router.Register(event.ChargeStarted, handler); err != nil {
		t.Fatalf("register: %v", err)
	}

	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, router, nil)
	fixture.worker.SetObserver(observer)

	ctx := context.Background()
	if err := fixture.stream.EnsureGroup(ctx, fixture.config.Stream, fixture.config.Group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	eventTypes := []string{string(event.ChargeStarted), "invented.type", "another.invented.type"}
	for index, eventType := range eventTypes {
		fields := map[string]string{
			"event_id":       fmt.Sprintf("evt_label_%d", index),
			"event_type":     eventType,
			"aggregate_type": "order",
			"aggregate_id":   "order_01",
			"occurred_at":    time.Now().UTC().Format(time.RFC3339Nano),
			"trace_id":       "trace_01",
			"payload":        "{}",
		}
		if _, err := fixture.stream.Add(ctx, fixture.config.Stream, fields); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	deliveries, err := fixture.stream.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: fixture.config.Stream, Group: fixture.config.Group, Consumer: fixture.config.Consumer, Count: 3,
	})
	if err != nil || len(deliveries) != 3 {
		t.Fatalf("expected 3 deliveries, got %d and %v", len(deliveries), err)
	}
	for _, delivery := range deliveries {
		if err := fixture.worker.process(ctx, delivery); err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	labels := map[string]int{}
	for _, item := range observer.handled {
		labels[item.eventType]++
	}
	if labels[string(event.ChargeStarted)] != 1 {
		t.Fatalf("expected the known type to keep its own series, got %v", labels)
	}
	if labels[unknownEventTypeLabel] != 2 {
		t.Fatalf("expected the invented types to collapse into %q, got %v", unknownEventTypeLabel, labels)
	}
	if len(labels) != 2 {
		t.Fatalf("expected at most two series, got %v", labels)
	}
	// The parked entries carry the same bounded label, and the reason stays a closed value.
	if len(observer.deadLettered) != 2 {
		t.Fatalf("expected both invented types to be parked, got %d", len(observer.deadLettered))
	}
	for _, item := range observer.deadLettered {
		if item.eventType != unknownEventTypeLabel {
			t.Fatalf("expected the %q label on a parked entry, got %q", unknownEventTypeLabel, item.eventType)
		}
		if item.reason != string(deadLetterReasonPermanent) {
			t.Fatalf("expected the closed permanent_failure reason, got %q", item.reason)
		}
	}
}

// The dead-letter reason is a label too, so free text must never reach it: any publisher can vary an
// error message. The message travels as the park record's detail instead.
func TestWorkerKeepsFreeTextOutOfTheDeadLetterReason(t *testing.T) {
	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return errors.New("charger ch_01 replied with firmware 1.2.3 at 12:04:05")
	}), func(config *Config) { config.MaxAttempts = 1 })
	fixture.worker.SetObserver(observer)

	fixture.publish(t, newChargeStartedEvent(t, "evt_reason_detail"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.deadLettered) != 1 {
		t.Fatalf("expected one parked event, got %d", len(observer.deadLettered))
	}
	if observer.deadLettered[0].reason != string(deadLetterReasonExhausted) {
		t.Fatalf("expected the closed reason label, got %q", observer.deadLettered[0].reason)
	}

	records := fixture.deadLetters(t)
	if len(records) != 1 {
		t.Fatalf("expected one parked copy, got %d", len(records))
	}
	if records[0].Values["dead_letter_reason"] != string(deadLetterReasonExhausted) {
		t.Fatalf("expected the closed reason on the parked entry, got %q", records[0].Values["dead_letter_reason"])
	}
	if records[0].Values["dead_letter_detail"] == "" {
		t.Fatal("expected the error text to be preserved as the detail")
	}
}

// recordingClient wraps a memory stream and records how far the runner had got, so the readiness
// callback can be checked against real preparation rather than against a flag the test sets.
//
// The counters are synchronised because every worker reads the stream from its own goroutine.
type recordingClient struct {
	*redisrepo.MemoryStream

	pingErr error

	mu      sync.Mutex
	ensured map[string]bool
	reads   atomic.Int32
}

func newRecordingClient() *recordingClient {
	return &recordingClient{MemoryStream: redisrepo.NewMemoryStream(), ensured: map[string]bool{}}
}

func (c *recordingClient) Ping(ctx context.Context) error {
	if c.pingErr != nil {
		return c.pingErr
	}
	return c.MemoryStream.Ping(ctx)
}

func (c *recordingClient) EnsureGroup(ctx context.Context, stream, group, startID string) error {
	if err := c.MemoryStream.EnsureGroup(ctx, stream, group, startID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensured[stream+"/"+group] = true
	return nil
}

func (c *recordingClient) groupExists(stream, group string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ensured[stream+"/"+group]
}

func (c *recordingClient) ReadGroup(ctx context.Context, options redisrepo.ReadGroupOptions) ([]redisrepo.Delivery, error) {
	c.reads.Add(1)
	return c.MemoryStream.ReadGroup(ctx, options)
}

func (c *recordingClient) readCalls() int { return int(c.reads.Load()) }

// Finding: the startup snapshot was taken before the consumer groups existed, where lag is not a
// number at all. The runner now reports readiness once every group exists and before any worker
// starts reading, so a snapshot taken there describes a state that can actually be interpreted.
func TestRunnerReportsReadinessOnlyAfterEveryGroupExists(t *testing.T) {
	client := newRecordingClient()
	t.Cleanup(func() { _ = client.Close() })

	streams := []StreamConfig{
		{Stream: "ncs:stream:charge-event", Group: "charge", Consumer: "c1", Count: 1, Block: time.Millisecond, PendingInterval: time.Hour, RetryAfter: time.Hour, MaxAttempts: 3},
		{Stream: "ncs:stream:charger-command", Group: "command", Consumer: "c1", Count: 1, Block: time.Millisecond, PendingInterval: time.Hour, RetryAfter: time.Hour, MaxAttempts: 3},
	}
	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), streams)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	// Every worker refuses to consume without a dead-letter recorder, and the runner prepares them
	// all before it reports readiness, so this test needs one.
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())

	var readyCalls atomic.Int32
	runner.SetOnReady(func() {
		readyCalls.Add(1)
		for _, stream := range streams {
			if !client.groupExists(stream.Stream, stream.Group) {
				t.Errorf("group %s/%s does not exist yet", stream.Stream, stream.Group)
			}
		}
		if reads := client.readCalls(); reads != 0 {
			t.Errorf("expected no consumption before readiness, got %d reads", reads)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Wait until the callback has run, then stop the runner.
	deadline := time.Now().Add(5 * time.Second)
	for readyCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runner: %v", err)
	}
	if readyCalls.Load() != 1 {
		t.Fatalf("expected readiness to be reported exactly once, got %d", readyCalls.Load())
	}
}

// Readiness must not be reported when a worker could not be prepared: a caller waiting on it would
// otherwise sample a stream whose group was never created, which is the state the callback exists to
// avoid.
func TestRunnerDoesNotReportReadinessWhenPreparationFails(t *testing.T) {
	client := newRecordingClient()
	client.pingErr = errors.New("redis unreachable")
	t.Cleanup(func() { _ = client.Close() })

	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), []StreamConfig{
		{Stream: "ncs:stream:charge-event", Group: "charge", Consumer: "c1", Count: 1, Block: time.Millisecond, PendingInterval: time.Hour, RetryAfter: time.Hour, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	readyCalls := 0
	runner.SetOnReady(func() { readyCalls++ })

	if err := runner.Run(context.Background()); err == nil {
		t.Fatal("expected the failed preparation to be reported")
	}
	if readyCalls != 0 {
		t.Fatalf("readiness must not be reported after a failed preparation, got %d calls", readyCalls)
	}
	if reads := client.readCalls(); reads != 0 {
		t.Fatalf("no consumption may start after a failed preparation, got %d reads", reads)
	}
}

// A shutdown that interrupts preparation is not a startup failure: the process was already stopping.
func TestRunnerTreatsACancelledPreparationAsACleanStop(t *testing.T) {
	client := newRecordingClient()
	t.Cleanup(func() { _ = client.Close() })

	runner, err := NewRunner(client, HandlerFunc(func(context.Context, event.Event) error { return nil }), []StreamConfig{
		{Stream: "ncs:stream:charge-event", Group: "charge", Consumer: "c1", Count: 1, Block: time.Millisecond, PendingInterval: time.Hour, RetryAfter: time.Hour, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("expected a clean stop, got %v", err)
	}
}

// A delivery superseded by a newer attempt is reported as its own outcome and counted as nothing
// else. This is the observability half of the B-04 fix that stops a stale delivery from parking an
// event a newer attempt has already applied: the silence it must observe is about the transport and
// the record, not about the metrics, and a handler that outlived its lease is worth alerting on
// because it is invisible in every other series.
func TestWorkerReportsASupersededDeliveryWithoutCountingADeadLetter(t *testing.T) {
	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		// A reservation the event has since moved past: this is what the pipeline reports when a
		// handler outlives its own lease.
		return &attemptError{
			attempt:     1,
			reservation: event.Reservation{EventID: "evt_superseded_metric", Owner: "stale-owner", Attempt: 1},
			err:         errors.New("device unavailable"),
		}
	}), func(config *Config) { config.MaxAttempts = 1 })
	fixture.worker.SetObserver(observer)
	requireRecorder(t, fixture.worker, event.NewMemoryConsumptionStore())

	fixture.publish(t, newChargeStartedEvent(t, "evt_superseded_metric"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.handled) != 1 {
		t.Fatalf("expected exactly one reported outcome, got %#v", observer.handled)
	}
	if observer.handled[0].outcome != observability.OutcomeSuperseded {
		t.Fatalf("expected %s, got %s", observability.OutcomeSuperseded, observer.handled[0].outcome)
	}
	// The event was not parked, so no counter may say it was.
	if len(observer.deadLettered) != 0 {
		t.Fatalf("a superseded delivery must not be counted as parked: %#v", observer.deadLettered)
	}
	if len(observer.suppressed) != 0 {
		t.Fatalf("a superseded delivery wrote nothing, so nothing was suppressed either: %#v", observer.suppressed)
	}
}
