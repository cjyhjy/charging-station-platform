package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestRegistryAddsCountersAndSetsGauges(t *testing.T) {
	registry := NewRegistry()

	registry.AddCounter("ncs_test_total", map[string]string{"outcome": "ok"}, 1)
	registry.AddCounter("ncs_test_total", map[string]string{"outcome": "ok"}, 2)
	registry.SetGauge("ncs_test_gauge", nil, 7)

	if value, ok := registry.Counter("ncs_test_total", map[string]string{"outcome": "ok"}); !ok || value != 3 {
		t.Fatalf("expected counter 3, got %v ok=%v", value, ok)
	}
	if value, ok := registry.Gauge("ncs_test_gauge", nil); !ok || value != 7 {
		t.Fatalf("expected gauge 7, got %v ok=%v", value, ok)
	}
}

// Labels must be part of the series identity, or two outcomes would silently share one
// counter and the breakdown an operator needs would be lost.
func TestRegistrySeparatesSeriesByLabels(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter("ncs_test_total", map[string]string{"outcome": "succeeded"}, 5)
	registry.AddCounter("ncs_test_total", map[string]string{"outcome": "retried"}, 2)

	succeeded, _ := registry.Counter("ncs_test_total", map[string]string{"outcome": "succeeded"})
	retried, _ := registry.Counter("ncs_test_total", map[string]string{"outcome": "retried"})
	if succeeded != 5 || retried != 2 {
		t.Fatalf("expected 5 and 2, got %v and %v", succeeded, retried)
	}

	// Label order must not create a second series for the same logical labels.
	registry.AddCounter("ncs_test_total", map[string]string{"a": "1", "b": "2"}, 1)
	registry.AddCounter("ncs_test_total", map[string]string{"b": "2", "a": "1"}, 1)
	if value, _ := registry.Counter("ncs_test_total", map[string]string{"a": "1", "b": "2"}); value != 2 {
		t.Fatalf("expected label order to be irrelevant, got %v", value)
	}
}

// A counter that can fall would make a rate calculation nonsense, so a negative delta is
// clamped rather than allowed to rewind the series.
func TestRegistryClampsCountersAtZero(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter("ncs_test_total", nil, 1)
	registry.AddCounter("ncs_test_total", nil, -5)

	if value, _ := registry.Counter("ncs_test_total", nil); value != 0 {
		t.Fatalf("expected the counter clamped to 0, got %v", value)
	}
}

func TestRegistryIgnoresAnEmptyName(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter("", nil, 1)
	registry.SetGauge("", nil, 1)
	if registry.Len() != 0 {
		t.Fatalf("expected an empty name to be ignored, got %d series", registry.Len())
	}
}

// Snapshots must be ordered so a log line and a test assertion are comparable between
// runs.
func TestRegistrySnapshotIsOrdered(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter("b_total", map[string]string{"x": "2"}, 1)
	registry.AddCounter("b_total", map[string]string{"x": "1"}, 1)
	registry.SetGauge("a_gauge", nil, 3)

	samples := registry.Snapshot()
	if len(samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(samples))
	}
	if samples[0].Name != "a_gauge" || samples[0].Kind != KindGauge {
		t.Fatalf("expected the gauge first, got %+v", samples[0])
	}
	if samples[1].Labels["x"] != "1" || samples[2].Labels["x"] != "2" {
		t.Fatalf("expected label ordering, got %+v and %+v", samples[1], samples[2])
	}
}

func TestSampleStringRendersStableLabels(t *testing.T) {
	sample := Sample{
		Name:   "ncs_stream_lag",
		Kind:   KindGauge,
		Value:  12,
		Labels: map[string]string{"stream": "ncs:stream:charge-event", "group": "g"},
	}
	rendered := sample.String()
	want := `ncs_stream_lag{group="g",stream="ncs:stream:charge-event"}=12`
	if rendered != want {
		t.Fatalf("expected %q, got %q", want, rendered)
	}

	// A sample without labels must not render an empty label block.
	bare := Sample{Name: "ncs_test_total", Kind: KindCounter, Value: 1}.String()
	if bare != "ncs_test_total=1" {
		t.Fatalf("expected a bare rendering, got %q", bare)
	}
}

func TestRegistrySnapshotDoesNotAliasInternalLabels(t *testing.T) {
	registry := NewRegistry()
	labels := map[string]string{"stream": "s"}
	registry.AddCounter("ncs_test_total", labels, 1)

	// A caller mutating its own map must not corrupt the stored series.
	labels["stream"] = "mutated"
	samples := registry.Snapshot()
	if samples[0].Labels["stream"] != "s" {
		t.Fatalf("expected the stored label, got %q", samples[0].Labels["stream"])
	}
}

func TestRegistryReset(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter("ncs_test_total", nil, 1)
	registry.SetGauge("ncs_test_gauge", nil, 1)
	registry.Reset()

	if registry.Len() != 0 {
		t.Fatalf("expected an empty registry, got %d", registry.Len())
	}
	if _, ok := registry.Counter("ncs_test_total", nil); ok {
		t.Fatal("expected the counter to be gone")
	}
}

// The registry is written from several stream workers at once, so it must be safe under
// concurrency. Run with -race.
func TestRegistryIsConcurrencySafe(t *testing.T) {
	registry := NewRegistry()
	const workers = 16
	const perWorker = 100

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				registry.EventHandled("s", "CHARGE_STARTED", OutcomeSucceeded, 1)
				registry.EventHandled("s", "CHARGE_STARTED", OutcomeRetried, 2)
				registry.SetGauge("ncs_stream_pending", map[string]string{"stream": "s"}, float64(j))
				_ = registry.Snapshot()
			}
		}()
	}
	wg.Wait()

	want := float64(workers * perWorker)
	if value, _ := registry.Counter(MetricEventsTotal, map[string]string{
		"stream": "s", "event_type": "CHARGE_STARTED", "outcome": string(OutcomeSucceeded),
	}); value != want {
		t.Fatalf("expected %v succeeded events, got %v", want, value)
	}
	if value, _ := registry.Counter(MetricRetriesTotal, map[string]string{
		"stream": "s", "event_type": "CHARGE_STARTED", "attempt": "2",
	}); value != want {
		t.Fatalf("expected %v retries, got %v", want, value)
	}
}

func TestRegistryObserverRecordsOutcomesAndReasons(t *testing.T) {
	registry := NewRegistry()

	registry.EventHandled("charge", "CHARGE_STARTED", OutcomeSucceeded, 1)
	registry.EventHandled("charge", "CHARGE_STARTED", OutcomeDuplicate, 3)
	registry.EventHandled("charge", "CHARGE_STARTED", OutcomeRetried, 2)
	registry.EventHandled("charge", "CHARGE_STARTED", OutcomeDeadLettered, 3)
	registry.EventDeadLettered("charge", "CHARGE_STARTED", "retry_exhausted", 3)
	registry.PendingRecovered("charge", 4)
	// A zero or negative recovery count must not create a series.
	registry.PendingRecovered("charge", 0)

	if value, _ := registry.Counter(MetricEventsTotal, map[string]string{
		"stream": "charge", "event_type": "CHARGE_STARTED", "outcome": string(OutcomeDuplicate),
	}); value != 1 {
		t.Fatalf("expected 1 duplicate, got %v", value)
	}
	if value, _ := registry.Counter(MetricDeadLetteredTotal, map[string]string{
		"stream": "charge", "event_type": "CHARGE_STARTED", "reason": "retry_exhausted", "attempt": "3",
	}); value != 1 {
		t.Fatalf("expected 1 dead letter, got %v", value)
	}
	if value, _ := registry.Counter(MetricPendingRecoveredTotal, map[string]string{"stream": "charge"}); value != 4 {
		t.Fatalf("expected 4 recovered, got %v", value)
	}
}

func TestAttemptLabelNormalisesInvalidAttempts(t *testing.T) {
	if got := attemptLabel(0); got != "1" {
		t.Fatalf("expected an invalid attempt to normalise to 1, got %q", got)
	}
	if got := attemptLabel(-3); got != "1" {
		t.Fatalf("expected a negative attempt to normalise to 1, got %q", got)
	}
	if got := attemptLabel(5); got != "5" {
		t.Fatalf("expected 5, got %q", got)
	}
}

func TestNoopObserverDiscardsFacts(t *testing.T) {
	// The noop observer exists so the consumer loop has no nil check to forget.
	observer := NoopObserver()
	observer.EventHandled("s", "t", OutcomeSucceeded, 1)
	observer.EventDeadLettered("s", "t", "r", 1)
	observer.PendingRecovered("s", 1)
}

// --- trace propagation ---

func TestTraceIDRoundTripsThroughContext(t *testing.T) {
	ctx := WithTraceID(context.Background(), "trace_01")
	if got := TraceIDFromContext(ctx); got != "trace_01" {
		t.Fatalf("expected trace_01, got %q", got)
	}
	if got := TraceIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected no trace id, got %q", got)
	}
}

func TestWithTraceIDIgnoresAnEmptyID(t *testing.T) {
	// Storing an empty id would make logging print a populated-looking blank field.
	ctx := WithTraceID(context.Background(), "")
	if got := TraceIDFromContext(ctx); got != "" {
		t.Fatalf("expected no trace id, got %q", got)
	}
}

func TestWithTraceIDToleratesANilContext(t *testing.T) {
	//nolint:staticcheck // a nil context is exactly the mistake being defended against.
	ctx := WithTraceID(nil, "trace_01")
	if got := TraceIDFromContext(ctx); got != "trace_01" {
		t.Fatalf("expected trace_01, got %q", got)
	}
}

func TestContextHandlerAddsTraceIDToRecords(t *testing.T) {
	var buffer strings.Builder
	logger := slog.New(NewContextHandler(slog.NewJSONHandler(&buffer, nil)))

	ctx := WithTraceID(context.Background(), "trace_abc")
	logger.InfoContext(ctx, "event consumed", "event_id", "evt_01")

	line := buffer.String()
	if !strings.Contains(line, `"trace_id":"trace_abc"`) {
		t.Fatalf("expected the trace id on the line, got %s", line)
	}
	if !strings.Contains(line, `"event_id":"evt_01"`) {
		t.Fatalf("expected the original attributes preserved, got %s", line)
	}
}

func TestContextHandlerOmitsAnAbsentTraceID(t *testing.T) {
	var buffer strings.Builder
	logger := slog.New(NewContextHandler(slog.NewJSONHandler(&buffer, nil)))

	logger.InfoContext(context.Background(), "no trace here")

	if strings.Contains(buffer.String(), "trace_id") {
		t.Fatalf("expected no trace id field, got %s", buffer.String())
	}
}

func TestContextHandlerPreservesStructuredFields(t *testing.T) {
	// The wrapper must be transparent apart from the added attribute, or a log pipeline
	// would lose the fields it depends on.
	var buffer strings.Builder
	logger := slog.New(NewContextHandler(slog.NewJSONHandler(&buffer, nil))).
		With("service", "ncs-worker").
		WithGroup("event")

	logger.InfoContext(WithTraceID(context.Background(), "trace_grouped"), "consumed", "id", "evt_02")

	var decoded map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buffer.String())), &decoded); err != nil {
		t.Fatalf("expected valid JSON, got %s: %v", buffer.String(), err)
	}
	if decoded["service"] != "ncs-worker" {
		t.Fatalf("expected the service attribute preserved, got %v", decoded["service"])
	}
	event, ok := decoded["event"].(map[string]any)
	if !ok {
		t.Fatalf("expected the event group, got %v", decoded["event"])
	}
	if event["id"] != "evt_02" {
		t.Fatalf("expected the grouped id preserved, got %v", event["id"])
	}
	// The added attribute lands in the group, which is a known consequence of adding to a
	// record that already carries grouped attributes.
	if _, ok := event["trace_id"]; !ok {
		t.Fatalf("expected the trace id inside the group, got %v", event)
	}
}

// A gauge that only ever rose would be a counter with a misleading name: in-flight requests rise and
// fall, and a gauge must follow both directions without being clamped.
func TestIncGaugeMovesBothWays(t *testing.T) {
	registry := NewRegistry()
	registry.IncGauge("ncs_test_in_flight", nil, 1)
	registry.IncGauge("ncs_test_in_flight", nil, 1)
	if value, ok := registry.Gauge("ncs_test_in_flight", nil); !ok || value != 2 {
		t.Fatalf("gauge = %v (present %v), want 2", value, ok)
	}
	registry.IncGauge("ncs_test_in_flight", nil, -1)
	if value, _ := registry.Gauge("ncs_test_in_flight", nil); value != 1 {
		t.Fatalf("gauge = %v, want 1", value)
	}
	// No clamp: the caller's own numbers are reported as they are, so a leak is visible instead of
	// hidden behind a zero.
	registry.IncGauge("ncs_test_in_flight", nil, -5)
	if value, _ := registry.Gauge("ncs_test_in_flight", nil); value != -4 {
		t.Fatalf("gauge = %v, want -4: a gauge is not clamped and a leak must be visible", value)
	}
}
