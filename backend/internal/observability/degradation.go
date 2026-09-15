package observability

import (
	"fmt"
	"sync"

	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// FailModeLabel names the degradation decision a failure was handled by.
type FailModeLabel string

const (
	// FailOpenLabel means the failure was hidden from the caller behind the capability's
	// fallback.
	FailOpenLabel FailModeLabel = "fail_open"
	// FailClosedLabel means the failure was surfaced, so the caller rejected the work.
	FailClosedLabel FailModeLabel = "fail_closed"
)

// DegradationSource reports the cumulative Redis capability failures the B-01 degradation
// policy has handled.
//
// The policy is fail-open or fail-closed per capability, which means a failing Redis can be
// almost invisible from the outside: the cache stops caching, the duplicate guard stops
// guarding, and every other metric still looks healthy because the failures were deliberately
// absorbed. Bridging those counters is what makes the policy observable instead of merely
// implemented.
type DegradationSource interface {
	Snapshot() []redisrepo.DegradationStat
}

// modeKey identifies one published series.
type modeKey struct {
	capability redisrepo.Capability
	mode       FailModeLabel
}

// DegradationSampler publishes Redis capability failures as counters.
//
// The source reports cumulative totals while a counter expects increments, so the sampler
// subtracts what it published last time and adds only the difference. Publishing the totals
// directly would make a rate calculation nonsense, and publishing a gauge would lose the
// monotonic property a counter is supposed to have.
//
// Each mode is tracked separately rather than deriving the split from the total, because the
// two labels mean opposite things to an operator: fail-open hid a failure, fail-closed rejected
// traffic. Deriving them would also double-count as soon as a capability switched mode.
type DegradationSampler struct {
	source   DegradationSource
	registry *Registry

	mu   sync.Mutex
	last map[modeKey]int64
}

// NewDegradationSampler validates its inputs.
func NewDegradationSampler(source DegradationSource, registry *Registry) (*DegradationSampler, error) {
	if source == nil {
		return nil, fmt.Errorf("degradation sampler: source is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("degradation sampler: registry is required")
	}
	return &DegradationSampler{
		source:   source,
		registry: registry,
		last:     map[modeKey]int64{},
	}, nil
}

// Sample publishes the failures observed since the previous call and returns how many were
// published, so a caller can log or alert on activity without diffing the registry itself.
func (s *DegradationSampler) Sample() int {
	stats := s.source.Snapshot()

	s.mu.Lock()
	defer s.mu.Unlock()

	published := 0
	seen := make(map[modeKey]struct{}, len(stats)*2)
	for _, stat := range stats {
		for _, mode := range []struct {
			label FailModeLabel
			count int64
		}{
			{FailOpenLabel, stat.FailOpen},
			{FailClosedLabel, stat.FailClosed},
		} {
			key := modeKey{capability: stat.Capability, mode: mode.label}
			seen[key] = struct{}{}

			previous, known := s.last[key]
			if !known {
				// First observation: publish the whole accumulated count, because there is no
				// earlier baseline to subtract from and dropping it would lose every failure
				// that happened before the sampler started.
				if mode.count <= 0 {
					s.last[key] = 0
					continue
				}
				s.last[key] = mode.count
				s.add(stat.Capability, mode.label, mode.count)
				published += int(mode.count)
				continue
			}
			if mode.count <= previous {
				// A reset re-baselines instead of publishing a negative delta, which AddCounter
				// would clamp and silently lose.
				if mode.count < previous {
					s.last[key] = mode.count
				}
				continue
			}
			delta := mode.count - previous
			s.last[key] = mode.count
			s.add(stat.Capability, mode.label, delta)
			published += int(delta)
		}
	}

	// A series that disappeared from the snapshot is forgotten, so a later reappearance starts
	// from zero rather than publishing a stale delta.
	for key := range s.last {
		if _, ok := seen[key]; !ok {
			delete(s.last, key)
		}
	}
	return published
}

func (s *DegradationSampler) add(capability redisrepo.Capability, mode FailModeLabel, delta int64) {
	s.registry.AddCounter(MetricRedisCapabilityFailuresTotal, map[string]string{
		"capability": string(capability),
		"fail_mode":  string(mode),
	}, float64(delta))
}
