package observability

import (
	"context"
	"errors"
	"testing"

	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the fourth review of BE-B-05: metrics that must not lie, and labels that must not
// grow without bound.

// Finding 3: lag must not be reported while no consumer group exists. A first deployment whose
// stream is already full would otherwise look healthy, because a missing group has no backlog to be
// behind.
func TestCollectorReportsAMissingGroupInsteadOfZeroLag(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	// The stream already holds a backlog produced by the outbox before any worker ran.
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 500, EntriesAdded: 500}
	// Its group does not exist yet, which the stub reports as ErrGroupMissing.

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	if _, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); ok {
		t.Fatal("lag must not be reported as zero while the group does not exist")
	}
	if value, ok := registry.Gauge(MetricStreamGroupMissing, map[string]string{"stream": stream}); !ok || value != 1 {
		t.Fatalf("expected the missing group to be reported, got %v ok=%v", value, ok)
	}
	// The stream length is still reported, so an operator can see the backlog that exists.
	if value, ok := registry.Gauge(MetricStreamLength, map[string]string{"stream": stream}); !ok || value != 500 {
		t.Fatalf("expected the stream length to be reported, got %v ok=%v", value, ok)
	}
}

// Once the group exists the same stream must report a real backlog, so the series above is a
// deliberate absence rather than a broken metric.
func TestCollectorReportsLagOnceTheGroupExists(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 500, EntriesAdded: 500}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 500, EntriesRead: 0}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); !ok || value != 500 {
		t.Fatalf("expected a backlog of 500, got %v ok=%v", value, ok)
	}
	if value, ok := registry.Gauge(MetricStreamGroupMissing, map[string]string{"stream": stream}); !ok || value != 0 {
		t.Fatalf("expected the missing group flag to clear, got %v ok=%v", value, ok)
	}
}

// Finding 4: a lag gauge that can no longer be computed must disappear. Leaving the previous value
// would repeat a stale backlog as if it were current, which is worse than reporting nothing.
func TestCollectorClearsALagGaugeThatCanNoLongerBeComputed(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 10, EntriesAdded: 100}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 42, EntriesRead: 58}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); !ok || value != 42 {
		t.Fatalf("expected lag 42, got %v ok=%v", value, ok)
	}

	// Redis can no longer report lag, and there is no delivered count to derive it from.
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: -1, EntriesRead: -1}
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); ok {
		t.Fatalf("expected the stale lag gauge to be cleared, got %v", value)
	}
}

// A stream whose group disappears must also lose its lag series rather than keep reporting the last
// number it had.
func TestCollectorClearsLagWhenTheGroupDisappears(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 10, EntriesAdded: 100}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 7, EntriesRead: 93}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	delete(inspector.groups, stream+"/g")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); ok {
		t.Fatalf("expected the lag gauge to be cleared, got %v", value)
	}
}

// A sampling failure is different from an unknowable value: the series keeps its last reading, which
// is the normal behaviour for a scrape that failed, so this is asserted rather than left implicit.
func TestCollectorKeepsTheLastSampleOnAFailure(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 10, EntriesAdded: 100}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 3, EntriesRead: 97}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	inspector.groupErr[stream+"/g"] = errors.New("connection refused")
	if err := collector.Collect(context.Background()); err == nil {
		t.Fatal("expected the sampling failure to be reported")
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); !ok || value != 3 {
		t.Fatalf("expected the last reading to survive a failed scrape, got %v ok=%v", value, ok)
	}
}

func TestRegistryDeleteGaugeRemovesOnlyTheNamedSeries(t *testing.T) {
	registry := NewRegistry()
	registry.SetGauge(MetricStreamLag, map[string]string{"stream": "a"}, 1)
	registry.SetGauge(MetricStreamLag, map[string]string{"stream": "b"}, 2)

	registry.DeleteGauge(MetricStreamLag, map[string]string{"stream": "a"})
	if _, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": "a"}); ok {
		t.Fatal("expected the series to be gone")
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": "b"}); !ok || value != 2 {
		t.Fatalf("expected the other series to survive, got %v ok=%v", value, ok)
	}
	// Deleting something absent is a no-op rather than a panic.
	registry.DeleteGauge(MetricStreamLag, map[string]string{"stream": "absent"})
	registry.DeleteGauge("", nil)
}

// Finding 5: the attempt label must be bounded by a constant rather than by configuration, so a
// deployment that sets a large MaxAttempts cannot create an unbounded series set.
func TestAttemptLabelIsBoundedByAConstant(t *testing.T) {
	if got := attemptLabel(maxAttemptLabel + 5000); got != "20+" {
		t.Fatalf("expected the capped label, got %q", got)
	}
	if got := attemptLabel(maxAttemptLabel); got != "20+" {
		t.Fatalf("expected the cap to apply at the boundary, got %q", got)
	}
	if got := attemptLabel(maxAttemptLabel - 1); got != "19" {
		t.Fatalf("expected 19, got %q", got)
	}
}
