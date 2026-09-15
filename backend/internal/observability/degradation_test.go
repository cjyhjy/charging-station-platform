package observability

import (
	"context"
	"strings"
	"sync"
	"testing"

	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// fakeDegradationSource serves cumulative capability counts, so the sampler's delta handling can
// be asserted without a Redis outage.
type fakeDegradationSource struct {
	mu    sync.Mutex
	stats []redisrepo.DegradationStat
}

func (f *fakeDegradationSource) Snapshot() []redisrepo.DegradationStat {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]redisrepo.DegradationStat, len(f.stats))
	copy(out, f.stats)
	return out
}

func (f *fakeDegradationSource) set(stats ...redisrepo.DegradationStat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = stats
}

func newSamplerFixture(t *testing.T) (*DegradationSampler, *fakeDegradationSource, *Registry) {
	t.Helper()
	source := &fakeDegradationSource{}
	registry := NewRegistry()
	sampler, err := NewDegradationSampler(source, registry)
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}
	return sampler, source, registry
}

func TestNewDegradationSamplerValidatesInputs(t *testing.T) {
	if _, err := NewDegradationSampler(nil, NewRegistry()); err == nil {
		t.Fatal("expected a nil source to be rejected")
	}
	if _, err := NewDegradationSampler(&fakeDegradationSource{}, nil); err == nil {
		t.Fatal("expected a nil registry to be rejected")
	}
}

// The first sample publishes the whole accumulated count, because dropping it would hide every
// failure that happened before the sampler started.
func TestDegradationSamplerPublishesTheInitialCount(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 5, FailOpen: 5})

	if published := sampler.Sample(); published != 5 {
		t.Fatalf("expected 5 published on the first sample, got %d", published)
	}
	value, ok := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	})
	if !ok || value != 5 {
		t.Fatalf("expected a fail-open counter of 5, got %v ok=%v", value, ok)
	}
}

// The source is cumulative while a counter expects increments, so a second sample must publish
// only the difference. Publishing the total again would make any rate calculation nonsense.
func TestDegradationSamplerPublishesOnlyDeltas(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 3, FailOpen: 3})
	sampler.Sample()

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 8, FailOpen: 8})
	if published := sampler.Sample(); published != 5 {
		t.Fatalf("expected 5 new failures, got %d", published)
	}

	value, _ := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	})
	if value != 8 {
		t.Fatalf("expected the counter to equal the accumulated total 8, got %v", value)
	}

	// A sample with no change must publish nothing.
	if published := sampler.Sample(); published != 0 {
		t.Fatalf("expected no new failures, got %d", published)
	}
	if value, _ := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	}); value != 8 {
		t.Fatalf("expected the counter to stay at 8, got %v", value)
	}
}

// The two modes mean opposite things to an operator, so they are published as separate series and
// their sum must equal the capability total.
func TestDegradationSamplerSplitsFailOpenFromFailClosed(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{
		Capability: redisrepo.CapabilitySession,
		Failures:   7,
		FailOpen:   2,
		FailClosed: 5,
	})
	sampler.Sample()

	open, ok := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilitySession),
		"fail_mode":  string(FailOpenLabel),
	})
	if !ok || open != 2 {
		t.Fatalf("expected 2 fail-open, got %v ok=%v", open, ok)
	}
	closed, ok := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilitySession),
		"fail_mode":  string(FailClosedLabel),
	})
	if !ok || closed != 5 {
		t.Fatalf("expected 5 fail-closed, got %v ok=%v", closed, ok)
	}
	if open+closed != 7 {
		t.Fatalf("expected the labels to sum to the capability total 7, got %v", open+closed)
	}
}

// A capability that switches mode between samples must not double count: the previous revision
// derived the split from the total, which would have republished failures under the new label.
func TestDegradationSamplerHandlesAModeSwitch(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 4, FailOpen: 4})
	sampler.Sample()

	// The same 4 failures, now attributed to fail-closed.
	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 4, FailOpen: 0, FailClosed: 4})
	if published := sampler.Sample(); published != 4 {
		t.Fatalf("expected the new mode to be published once, got %d", published)
	}

	open, _ := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	})
	closed, _ := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailClosedLabel),
	})
	if open != 4 || closed != 4 {
		t.Fatalf("expected 4 per mode, got open=%v closed=%v", open, closed)
	}

	// A further sample with no change publishes nothing.
	if published := sampler.Sample(); published != 0 {
		t.Fatalf("expected no new failures after the switch settled, got %d", published)
	}
}

// A reset must re-baseline rather than publish a negative delta, which AddCounter would clamp and
// silently lose.
func TestDegradationSamplerRebaselinesAfterAReset(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 10, FailOpen: 10})
	sampler.Sample()

	// Degradation.Reset clears the counters.
	source.set()
	if published := sampler.Sample(); published != 0 {
		t.Fatalf("expected nothing published after a reset, got %d", published)
	}

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 2, FailOpen: 2})
	if published := sampler.Sample(); published != 2 {
		t.Fatalf("expected the post-reset count to be published, got %d", published)
	}
	value, _ := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	})
	// The published counters are monotonic even though the source reset, which is the property a
	// counter must have.
	if value != 12 {
		t.Fatalf("expected the monotonic counter to reach 12, got %v", value)
	}
}

func TestDegradationSamplerIgnoresZeroModes(t *testing.T) {
	sampler, source, registry := newSamplerFixture(t)

	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityLock, Failures: 3, FailClosed: 3})
	sampler.Sample()

	if _, ok := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityLock),
		"fail_mode":  string(FailOpenLabel),
	}); ok {
		t.Fatal("expected no fail-open series for a capability that never failed open")
	}
}

// The collector must publish the bridge as part of a normal sampling pass, or the policy stays
// invisible in the snapshot an operator reads.
func TestCollectorSamplesDegradationWhenConfigured(t *testing.T) {
	inspector := newStubInspector()
	stream := "ncs:stream:charge-event"
	inspector.streams[stream] = redisrepo.StreamInfo{Stream: stream, Length: 1}
	inspector.groups[stream+"/g"] = redisrepo.GroupInfo{Stream: stream, Group: "g", Lag: 1}

	collector, registry := newCollectorFixture(t, inspector, []StreamTarget{{Stream: stream, Group: "g"}}, "")

	source := &fakeDegradationSource{}
	source.set(redisrepo.DegradationStat{Capability: redisrepo.CapabilityCache, Failures: 2, FailOpen: 2})
	sampler, err := NewDegradationSampler(source, registry)
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}
	collector.SetDegradationSampler(sampler)

	if err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if value, ok := registry.Counter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(redisrepo.CapabilityCache),
		"fail_mode":  string(FailOpenLabel),
	}); !ok || value != 2 {
		t.Fatalf("expected the degradation counter in the snapshot, got %v ok=%v", value, ok)
	}
	if !strings.Contains(collector.Summary(), MetricRedisCapabilityFailuresTotal) {
		t.Fatalf("expected the summary to include the degradation counter, got %s", collector.Summary())
	}
}

// The worker's suppression report is a distinct fact from a written dead letter, so it must land
// in its own series.
func TestRegistryRecordsSuppressedDeadLettersSeparately(t *testing.T) {
	registry := NewRegistry()
	registry.EventDeadLettered("charge", "CHARGE_STARTED", "retry_exhausted", 3)
	registry.DeadLetterSuppressed("charge", "CHARGE_STARTED")

	written, _ := registry.Counter(MetricDeadLetteredTotal, map[string]string{
		"stream": "charge", "event_type": "CHARGE_STARTED", "reason": "retry_exhausted", "attempt": "3",
	})
	if written != 1 {
		t.Fatalf("expected 1 written dead letter, got %v", written)
	}
	suppressed, ok := registry.Counter(MetricDeadLetterSuppressedTotal, map[string]string{
		"stream": "charge", "event_type": "CHARGE_STARTED",
	})
	if !ok || suppressed != 1 {
		t.Fatalf("expected 1 suppressed dead letter, got %v ok=%v", suppressed, ok)
	}
	// Folding the suppression into the written counter would make the parked backlog look larger
	// than it is.
	if written != 1 {
		t.Fatalf("the suppression must not increment the written counter, got %v", written)
	}
}

func TestOutcomeLeaseHeldIsDistinctFromRetried(t *testing.T) {
	if OutcomeLeaseHeld == OutcomeRetried {
		t.Fatal("a held lease is not a retry: nothing failed")
	}
	registry := NewRegistry()
	registry.EventHandled("s", "CHARGE_STARTED", OutcomeLeaseHeld, 2)

	if value, ok := registry.Counter(MetricEventsTotal, map[string]string{
		"stream": "s", "event_type": "CHARGE_STARTED", "outcome": string(OutcomeLeaseHeld),
	}); !ok || value != 1 {
		t.Fatalf("expected a lease_held series, got %v ok=%v", value, ok)
	}
	// A held lease must not be counted as a retry, because it does not consume the retry budget.
	if _, ok := registry.Counter(MetricRetriesTotal, map[string]string{
		"stream": "s", "event_type": "CHARGE_STARTED", "attempt": "2",
	}); ok {
		t.Fatal("a held lease must not be counted as a retry")
	}
}
