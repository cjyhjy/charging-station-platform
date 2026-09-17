package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// stubInspector returns canned stream state so the collector's behaviour can be asserted
// without a server. The real sampling path is covered by the Redis integration tests.
type stubInspector struct {
	streams map[string]redisrepo.StreamInfo
	groups  map[string]redisrepo.GroupInfo
	lengths map[string]int64

	streamErr map[string]error
	groupErr  map[string]error
	lengthErr map[string]error

	streamCalls int
}

func newStubInspector() *stubInspector {
	return &stubInspector{
		streams:   map[string]redisrepo.StreamInfo{},
		groups:    map[string]redisrepo.GroupInfo{},
		lengths:   map[string]int64{},
		streamErr: map[string]error{},
		groupErr:  map[string]error{},
		lengthErr: map[string]error{},
	}
}

func (s *stubInspector) StreamInfo(_ context.Context, stream string) (redisrepo.StreamInfo, error) {
	s.streamCalls++
	if err := s.streamErr[stream]; err != nil {
		return redisrepo.StreamInfo{Stream: stream}, err
	}
	info, ok := s.streams[stream]
	if !ok {
		return redisrepo.StreamInfo{Stream: stream}, fmt.Errorf("stream info %s: %w", stream, redisrepo.ErrNotFound)
	}
	return info, nil
}

func (s *stubInspector) GroupInfo(_ context.Context, stream, group string) (redisrepo.GroupInfo, error) {
	key := stream + "/" + group
	if err := s.groupErr[key]; err != nil {
		return redisrepo.GroupInfo{Stream: stream, Group: group}, err
	}
	info, ok := s.groups[key]
	if !ok {
		return redisrepo.GroupInfo{}, fmt.Errorf("group info %s: %w", key, redisrepo.ErrGroupMissing)
	}
	return info, nil
}

func (s *stubInspector) StreamLen(_ context.Context, stream string) (int64, error) {
	if err := s.lengthErr[stream]; err != nil {
		return 0, err
	}
	return s.lengths[stream], nil
}

// syncBuffer serialises writes from the collector's reporting goroutine and reads from the
// test goroutine.
//
// strings.Builder is not safe for concurrent use, and slog writes to it from Run while the
// test polls for a line, so a plain builder races under -race.
type syncBuffer struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.String()
}

func newCollectorFixture(t *testing.T, inspector StreamInspector, targets []StreamTarget, deadLetter string) (*Collector, *Registry) {
	t.Helper()
	registry := NewRegistry()
	collector, err := NewCollector(inspector, registry, targets, deadLetter)
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	return collector, registry
}

func TestNewCollectorValidatesInputs(t *testing.T) {
	registry := NewRegistry()
	if _, err := NewCollector(nil, registry, nil, ""); err == nil {
		t.Fatal("expected a nil inspector to be rejected")
	}
	if _, err := NewCollector(newStubInspector(), nil, nil, ""); err == nil {
		t.Fatal("expected a nil registry to be rejected")
	}
	if _, err := NewCollector(newStubInspector(), registry, []StreamTarget{{Stream: "s"}}, ""); err == nil {
		t.Fatal("expected a target without a group to be rejected")
	}
	if _, err := NewCollector(newStubInspector(), registry, []StreamTarget{{Group: "g"}}, ""); err == nil {
		t.Fatal("expected a target without a stream to be rejected")
	}
}

func TestCollectorSamplesLagPendingLengthAndDeadLetters(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	group := "charge-event-workers"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 120, EntriesAdded: 400, LastGeneratedID: "9-9", GroupCount: 1}
	inspector.groups[stream+"/"+group] = redisrepo.GroupInfo{Stream: stream, Group: group, Pending: 7, Consumers: 2, EntriesRead: 350, Lag: 50}
	inspector.lengths["ncs:stream:dead-letter"] = 3

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: group}}, "ncs:stream:dead-letter")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	checks := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{MetricStreamLag, map[string]string{"stream": stream}, 50},
		{MetricStreamPending, map[string]string{"stream": stream}, 7},
		{MetricStreamLength, map[string]string{"stream": stream}, 120},
		{MetricDeadLetterLength, nil, 3},
	}
	for _, check := range checks {
		value, ok := registry.Gauge(check.name, check.labels)
		if !ok {
			t.Fatalf("expected the %s gauge to be set", check.name)
		}
		if value != check.want {
			t.Fatalf("expected %s=%v, got %v", check.name, check.want, value)
		}
	}
}

// Redis reports lag as nil when it cannot compute it. Deriving the backlog keeps the metric
// meaningful instead of reporting a healthy-looking zero.
func TestCollectorDerivesLagWhenTheServerCannotReportIt(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	group := "g"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, EntriesAdded: 100, Length: 100}
	inspector.groups[stream+"/"+group] = redisrepo.GroupInfo{Stream: stream, Group: group, EntriesRead: 60, Lag: -1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: group}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}

	value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream})
	if !ok {
		t.Fatal("expected a lag gauge")
	}
	if value != 40 {
		t.Fatalf("expected a derived lag of 40, got %v", value)
	}
}

// When neither value is available the gauge is left unset, so a stale number is never
// mistaken for a fresh one.
func TestCollectorLeavesLagUnsetWhenItCannotBeKnown(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, EntriesAdded: 100}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", EntriesRead: -1, Lag: -1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if _, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); ok {
		t.Fatal("expected no lag gauge when the backlog cannot be determined")
	}
}

// A freshly deployed stream does not exist as a key and its group may not exist yet. That is not a
// failure, so it is tolerated rather than counted as a sampling error.
//
// Lag is deliberately NOT reported as zero. The entries already in the stream have not been served
// to any group, and whether they ever will depends on the start position that group is created with,
// so a zero would show a first deployment with a backlog as a healthy stream with nothing behind.
// The missing group is reported on its own series instead.
func TestCollectorToleratesAMissingStreamAndGroupWithoutClaimingZeroLag(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "ncs:stream:dead-letter")
	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("expected a missing stream to be tolerated, got %v", err)
	}

	for _, name := range []string{MetricStreamPending, MetricStreamLength} {
		value, ok := registry.Gauge(name, map[string]string{"stream": stream})
		if !ok {
			t.Fatalf("expected the %s gauge to be set to zero", name)
		}
		if value != 0 {
			t.Fatalf("expected %s=0, got %v", name, value)
		}
	}
	if _, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); ok {
		t.Fatal("lag must not be reported while no consumer group exists")
	}
	if value, ok := registry.Gauge(MetricStreamGroupMissing, map[string]string{"stream": stream}); !ok || value != 1 {
		t.Fatalf("expected the missing group to be reported, got %v ok=%v", value, ok)
	}
	if value, _ := registry.Counter(MetricCollectorErrorsTotal, map[string]string{"stream": stream}); value != 0 {
		t.Fatalf("a missing stream must not be counted as a failure, got %v", value)
	}
}

// One unresolvable stream must not stop the others from being reported, and the failure must
// be counted so a broken collector is visible instead of quietly serving stale gauges.
func TestCollectorReportsFailuresWithoutStoppingOtherStreams(t *testing.T) {
	inspector := newStubInspector()
	broken := "ncs:stream:order-event"
	healthy := "ncs:stream:charge-event"
	inspector.streamErr[broken] = errors.New("connection refused")
	inspector.streams[healthy] = redisrepo.StreamInfo{Stream: healthy, Length: 5}
	inspector.groups[healthy+"/g"] = redisrepo.GroupInfo{Stream: healthy, Group: "g", Lag: 1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{
		{Stream: broken, Group: "g"},
		{Stream: healthy, Group: "g"},
	}, "")

	err := collector.Collect(context.Background())
	if err == nil {
		t.Fatal("expected the sampling failure to be reported")
	}
	if !strings.Contains(err.Error(), broken) {
		t.Fatalf("expected the failing stream to be named, got %v", err)
	}
	if value, ok := registry.Gauge(MetricStreamLength, map[string]string{"stream": healthy}); !ok || value != 5 {
		t.Fatalf("expected the healthy stream to be sampled, got %v ok=%v", value, ok)
	}
	if value, _ := registry.Counter(MetricCollectorErrorsTotal, map[string]string{"stream": broken}); value != 1 {
		t.Fatalf("expected the failure to be counted, got %v", value)
	}
}

func TestCollectorCountsAGroupInfoFailure(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 5}
	inspector.groupErr[stream+"/g"] = errors.New("timeout")

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	if err := collector.Collect(context.Background()); err == nil {
		t.Fatal("expected the group failure to be reported")
	}
	if value, _ := registry.Counter(MetricCollectorErrorsTotal, map[string]string{"stream": stream}); value != 1 {
		t.Fatalf("expected the failure to be counted, got %v", value)
	}
}

func TestCollectorCountsADeadLetterLengthFailure(t *testing.T) {
	inspector := newStubInspector()
	inspector.lengthErr["ncs:stream:dead-letter"] = errors.New("timeout")

	collector, registry := newCollectorFixture(t, inspector, nil, "ncs:stream:dead-letter")
	if err := collector.Collect(context.Background()); err == nil {
		t.Fatal("expected the dead-letter failure to be reported")
	}
	if value, _ := registry.Counter(MetricCollectorErrorsTotal, map[string]string{"stream": "ncs:stream:dead-letter"}); value != 1 {
		t.Fatalf("expected the failure to be counted, got %v", value)
	}
}

func TestCollectorSummaryIncludesWorkerAndStreamSeries(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 9}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 2, Pending: 1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")
	registry.EventHandled(stream, "CHARGE_STARTED", OutcomeRetried, 2)

	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	summary := collector.Summary()
	for _, fragment := range []string{
		`ncs_stream_lag{stream="ncs:stream:charge-event"}=2`,
		`ncs_stream_pending{stream="ncs:stream:charge-event"}=1`,
		`ncs_stream_length{stream="ncs:stream:charge-event"}=9`,
		MetricRetriesTotal,
	} {
		if !strings.Contains(summary, fragment) {
			t.Errorf("expected the summary to contain %q, got %s", fragment, summary)
		}
	}
}

func TestCollectorRunSamplesUntilCancelled(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 1}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")

	buffer := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buffer, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, 10*time.Millisecond, logger) }()

	// Wait until at least one sampled line has been logged.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buffer.String(), "stream state") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected a clean stop, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the collector did not stop on cancellation")
	}

	if !strings.Contains(buffer.String(), "stream state") {
		t.Fatal("expected the collector to log a snapshot")
	}
	if value, ok := registry.Gauge(MetricStreamLag, map[string]string{"stream": stream}); !ok || value != 1 {
		t.Fatalf("expected the sampled lag, got %v ok=%v", value, ok)
	}
}

func TestCollectorRunRequiresAnInterval(t *testing.T) {
	collector, _ := newCollectorFixture(t, newStubInspector(), nil, "")
	if err := collector.Run(context.Background(), 0, nil); err == nil {
		t.Fatal("expected a zero interval to be rejected")
	}
}

// A sampling failure must not stop the reporting loop: the next pass may succeed, and the
// failure is already counted.
func TestCollectorRunSurvivesASamplingFailure(t *testing.T) {
	inspector := newStubInspector()
	inspector.streamErr["s"] = errors.New("connection refused")

	collector, _ := newCollectorFixture(t, inspector, []StreamTarget{{Stream: "s", Group: "g"}}, "")

	buffer := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buffer, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, 10*time.Millisecond, logger) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buffer.String(), "stream sampling failed") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if !strings.Contains(buffer.String(), "stream sampling failed") {
		t.Fatal("expected the sampling failure to be logged")
	}
	// The loop must still have logged snapshots after the failure.
	if !strings.Contains(buffer.String(), "stream state") {
		t.Fatal("expected the loop to keep reporting after a failure")
	}
}
