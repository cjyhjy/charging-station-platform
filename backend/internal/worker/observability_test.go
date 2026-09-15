package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// recordingObserver captures the facts a worker reports so they can be asserted without
// reading metric series.
type recordingObserver struct {
	mu             sync.Mutex
	handled        []observedOutcome
	deadLettered   []observedDeadLetter
	suppressed     []observedDeadLetter
	recoveredTotal int
	recoveredCalls int
}

type observedOutcome struct {
	stream    string
	eventType string
	outcome   observability.Outcome
	attempt   int
}

type observedDeadLetter struct {
	stream    string
	eventType string
	reason    string
	attempt   int
}

func (o *recordingObserver) EventHandled(stream, eventType string, outcome observability.Outcome, attempt int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handled = append(o.handled, observedOutcome{stream: stream, eventType: eventType, outcome: outcome, attempt: attempt})
}

func (o *recordingObserver) EventDeadLettered(stream, eventType, reason string, attempt int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deadLettered = append(o.deadLettered, observedDeadLetter{stream: stream, eventType: eventType, reason: reason, attempt: attempt})
}

func (o *recordingObserver) DeadLetterSuppressed(stream, eventType string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.suppressed = append(o.suppressed, observedDeadLetter{stream: stream, eventType: eventType})
}

func (o *recordingObserver) PendingRecovered(stream string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recoveredCalls++
	o.recoveredTotal += count
}

func (o *recordingObserver) outcomes() []observability.Outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]observability.Outcome, 0, len(o.handled))
	for _, item := range o.handled {
		out = append(out, item.outcome)
	}
	return out
}

func (o *recordingObserver) lastOutcome() (observedOutcome, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.handled) == 0 {
		return observedOutcome{}, false
	}
	return o.handled[len(o.handled)-1], true
}

func TestWorkerReportsSucceededOutcome(t *testing.T) {
	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.worker.SetObserver(observer)

	fixture.publish(t, newChargeStartedEvent(t, "evt_obs_ok"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	outcome, ok := observer.lastOutcome()
	if !ok {
		t.Fatal("expected an observer call")
	}
	if outcome.outcome != observability.OutcomeSucceeded {
		t.Fatalf("expected %s, got %s", observability.OutcomeSucceeded, outcome.outcome)
	}
	if outcome.stream != fixture.config.Stream {
		t.Fatalf("expected stream %s, got %s", fixture.config.Stream, outcome.stream)
	}
	if outcome.eventType != string(event.ChargeStarted) {
		t.Fatalf("expected the event type, got %s", outcome.eventType)
	}
	if outcome.attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", outcome.attempt)
	}
}

func TestWorkerReportsDuplicateOutcomeWithoutDeadLettering(t *testing.T) {
	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		return ErrDuplicate
	}), nil)
	fixture.worker.SetObserver(observer)

	fixture.publish(t, newChargeStartedEvent(t, "evt_obs_dup"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	outcome, _ := observer.lastOutcome()
	if outcome.outcome != observability.OutcomeDuplicate {
		t.Fatalf("expected %s, got %s", observability.OutcomeDuplicate, outcome.outcome)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.deadLettered) != 0 {
		t.Fatalf("a duplicate must not be reported as dead-lettered, got %+v", observer.deadLettered)
	}
}

func TestWorkerReportsRetryAndPermanentOutcomes(t *testing.T) {
	t.Run("transient failure is reported as a retry", func(t *testing.T) {
		observer := &recordingObserver{}
		fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
			return errors.New("gateway timeout")
		}), func(config *Config) { config.MaxAttempts = 3 })
		fixture.worker.SetObserver(observer)

		fixture.publish(t, newChargeStartedEvent(t, "evt_obs_retry"))
		if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
			t.Fatalf("process: %v", err)
		}

		outcome, _ := observer.lastOutcome()
		if outcome.outcome != observability.OutcomeRetried {
			t.Fatalf("expected %s, got %s", observability.OutcomeRetried, outcome.outcome)
		}
	})

	t.Run("permanent failure is reported as dead-lettered with a reason", func(t *testing.T) {
		observer := &recordingObserver{}
		fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
			return Permanent(errors.New("unknown charger"))
		}), func(config *Config) { config.MaxAttempts = 5 })
		fixture.worker.SetObserver(observer)

		fixture.publish(t, newChargeStartedEvent(t, "evt_obs_perm"))
		if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
			t.Fatalf("process: %v", err)
		}

		outcome, _ := observer.lastOutcome()
		if outcome.outcome != observability.OutcomeDeadLettered {
			t.Fatalf("expected %s, got %s", observability.OutcomeDeadLettered, outcome.outcome)
		}
		observer.mu.Lock()
		defer observer.mu.Unlock()
		if len(observer.deadLettered) != 1 {
			t.Fatalf("expected one dead-letter report, got %d", len(observer.deadLettered))
		}
		// The reason label is what lets an operator separate a bad payload from an
		// exhausted retry budget.
		if observer.deadLettered[0].reason != "permanent_failure" {
			t.Fatalf("expected the permanent_failure reason, got %q", observer.deadLettered[0].reason)
		}
	})

	t.Run("an exhausted budget is reported as dead-lettered", func(t *testing.T) {
		observer := &recordingObserver{}
		fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
			return errors.New("gateway timeout")
		}), func(config *Config) { config.MaxAttempts = 1 })
		fixture.worker.SetObserver(observer)

		fixture.publish(t, newChargeStartedEvent(t, "evt_obs_exhausted"))
		if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
			t.Fatalf("process: %v", err)
		}

		observer.mu.Lock()
		defer observer.mu.Unlock()
		if len(observer.deadLettered) != 1 {
			t.Fatalf("expected one dead-letter report, got %d", len(observer.deadLettered))
		}
		if observer.deadLettered[0].reason != "retry_exhausted" {
			t.Fatalf("expected the retry_exhausted reason, got %q", observer.deadLettered[0].reason)
		}
	})
}

// An undecodable entry is dead-lettered with its own reason and no event type, because there
// is no envelope to read a type from.
func TestWorkerReportsInvalidEventsWithTheirOwnReason(t *testing.T) {
	observer := &recordingObserver{}
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.worker.SetObserver(observer)

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

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.deadLettered) != 1 {
		t.Fatalf("expected one dead-letter report, got %d", len(observer.deadLettered))
	}
	if observer.deadLettered[0].reason != string(deadLetterReasonInvalidEvent) {
		t.Fatalf("expected the invalid_event reason, got %q", observer.deadLettered[0].reason)
	}
	// An undecodable entry has no usable event type, so it must be labelled with the bounded
	// placeholder rather than with whatever bytes the payload happened to contain.
	if observer.deadLettered[0].eventType != unknownEventTypeLabel {
		t.Fatalf("expected the %q label, got %q", unknownEventTypeLabel, observer.deadLettered[0].eventType)
	}
}

// The recovery pass is what makes a restart safe, so the count of reclaimed entries is
// reported: it is the evidence that a crashed consumer's work was picked back up.
func TestWorkerReportsRecoveredPendingEntries(t *testing.T) {
	observer := &recordingObserver{}
	handlerStarted := make(chan struct{}, 1)

	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error {
		select {
		case handlerStarted <- struct{}{}:
		default:
		}
		return nil
	}), func(config *Config) {
		config.MaxAttempts = 3
		config.PendingInterval = 10 * time.Millisecond
		config.Block = 10 * time.Millisecond
	})
	fixture.worker.SetObserver(observer)
	// A worker refuses to consume without a dead-letter recorder, so every test that runs one
	// wires it - see TestWorkerRefusesToConsumeWithoutADeadLetterRecorder.
	requireRecorder(t, fixture.worker, event.NewMemoryConsumptionStore())

	// Two entries delivered to a consumer that never acknowledged them, which is exactly the
	// state a crashed process leaves behind.
	ctx := context.Background()
	if err := fixture.stream.EnsureGroup(ctx, fixture.config.Stream, fixture.config.Group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	for i := 0; i < 2; i++ {
		fields, err := newChargeStartedEvent(t, "evt_recover_"+string(rune('a'+i))).Fields()
		if err != nil {
			t.Fatalf("fields: %v", err)
		}
		if _, err := fixture.stream.Add(ctx, fixture.config.Stream, fields); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if _, err := fixture.stream.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: fixture.config.Stream, Group: fixture.config.Group, Consumer: "crashed-worker", Count: 2,
	}); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fixture.worker.Run(runCtx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		observer.mu.Lock()
		recovered := observer.recoveredTotal
		observer.mu.Unlock()
		if recovered >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.recoveredTotal != 2 {
		t.Fatalf("expected 2 recovered entries reported, got %d", observer.recoveredTotal)
	}
}

// Every worker line about an event must carry the trace id, which is what makes an incident
// searchable across the async hop.
func TestWorkerLogsCarryTheTraceID(t *testing.T) {
	var buffer strings.Builder
	var mu sync.Mutex
	safeBuffer := &lockedWriter{mu: &mu, builder: &buffer}

	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.worker.SetLogger(slog.New(observability.NewContextHandler(slog.NewJSONHandler(safeBuffer, nil))))

	e := newChargeStartedEvent(t, "evt_trace_log")
	e.TraceID = "trace_log_01"
	fixture.publish(t, e)
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	line := buffer.String()
	if !strings.Contains(line, `"trace_id":"trace_log_01"`) {
		t.Fatalf("expected the trace id on the log line, got %s", line)
	}
	if !strings.Contains(line, `"event_id":"evt_trace_log"`) {
		t.Fatalf("expected the event id on the log line, got %s", line)
	}
	if !strings.Contains(line, `"outcome":"succeeded"`) {
		t.Fatalf("expected the outcome on the log line, got %s", line)
	}
}

// A handler must see the event's trace id in its context, so its own logging and downstream
// calls inherit it without the id being threaded through every signature.
func TestWorkerPropagatesTheTraceIDToTheHandlerContext(t *testing.T) {
	var seenTraceID string
	fixture := newWorkerFixture(t, HandlerFunc(func(ctx context.Context, _ event.Event) error {
		seenTraceID = observability.TraceIDFromContext(ctx)
		return nil
	}), nil)

	e := newChargeStartedEvent(t, "evt_trace_ctx")
	e.TraceID = "trace_context_01"
	fixture.publish(t, e)
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}
	if seenTraceID != "trace_context_01" {
		t.Fatalf("expected the handler to see the trace id, got %q", seenTraceID)
	}
}

func TestWorkerLoggerIsOptional(t *testing.T) {
	// A worker without a logger must not panic, so a caller can omit one in tests.
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.publish(t, newChargeStartedEvent(t, "evt_nolog"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}
}

func TestWorkerDefaultObserverDiscardsFacts(t *testing.T) {
	// A worker constructed without an observer must still process events.
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.publish(t, newChargeStartedEvent(t, "evt_default_observer"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("expected no dead letters")
	}
}

func TestSetObserverTreatsNilAsNoop(t *testing.T) {
	fixture := newWorkerFixture(t, HandlerFunc(func(context.Context, event.Event) error { return nil }), nil)
	fixture.worker.SetObserver(nil)
	if fixture.worker.observer == nil {
		t.Fatal("expected a non-nil observer after setting nil")
	}
}

// A skipped dead-letter write is reported as its own fact, not as a written one: an operator
// reading the parked backlog must not see a duplicate that was never written.
func TestWorkerReportsSuppressedDeadLetterWrites(t *testing.T) {
	// The first acknowledgement fails, so the retry re-enters the dead-letter path with the
	// guard already held.
	stream := &ackFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failuresLeft: 1}
	t.Cleanup(func() { _ = stream.Close() })

	observer := &recordingObserver{}
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("unknown charger"))
	}), func(config *Config) { config.MaxAttempts = 5 })
	fixture.worker.SetObserver(observer)
	fixture.worker.SetDeadLetterGuard(newFakeDeadLetterGuard())

	fixture.publish(t, newChargeStartedEvent(t, "evt_suppressed"))
	delivery := fixture.delivery(t)

	if err := fixture.worker.process(context.Background(), delivery); err == nil {
		t.Fatal("expected the acknowledgement failure to surface")
	}
	observer.mu.Lock()
	if len(observer.suppressed) != 0 {
		observer.mu.Unlock()
		t.Fatal("the first attempt wrote the dead letter, so nothing was suppressed yet")
	}
	observer.mu.Unlock()

	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry: %v", err)
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.suppressed) != 1 {
		t.Fatalf("expected one suppression reported on the retry, got %d", len(observer.suppressed))
	}
	if observer.suppressed[0].stream != fixture.config.Stream {
		t.Fatalf("expected the stream on the report, got %q", observer.suppressed[0].stream)
	}
	// And exactly one dead letter exists, which is what makes the suppression correct rather than
	// a lost write.
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected exactly one dead letter, got %d", got)
	}
}

// The startup log must name the group's start position, because it decides whether a backlog
// published before this process first ran is consumed or skipped.
func TestWorkerLogsTheConsumerGroupStartPosition(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = stream.Close() })

	var buffer strings.Builder
	var mu sync.Mutex
	writer := &lockedWriter{mu: &mu, builder: &buffer}

	handler := HandlerFunc(func(context.Context, event.Event) error { return nil })
	fixture := fixtureWithStream(t, stream, stream, handler, func(config *Config) {
		config.Consumer = "worker-log"
		config.Block = 10 * time.Millisecond
		config.PendingInterval = 10 * time.Millisecond
	})
	fixture.worker.SetLogger(slog.New(slog.NewJSONHandler(writer, nil)))
	requireRecorder(t, fixture.worker, event.NewMemoryConsumptionStore())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fixture.worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		seen := strings.Contains(buffer.String(), "consumer group ready")
		mu.Unlock()
		if seen {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	line := buffer.String()
	if !strings.Contains(line, "consumer group ready") {
		t.Fatalf("expected a group-ready line, got %s", line)
	}
	if !strings.Contains(line, `"start_id":"0-0"`) {
		t.Fatalf("expected the default start position on the line, got %s", line)
	}
}

type lockedWriter struct {
	mu      *sync.Mutex
	builder *strings.Builder
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.builder.Write(p)
}
