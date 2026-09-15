package redis

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Capability names one Redis-backed guarantee so a degradation decision and its
// counters are per-capability instead of global. Redis going down must not mean
// the same thing for a cache entry and for a login session.
type Capability string

const (
	// CapabilityCache covers station and charger read-through caching.
	CapabilityCache Capability = "cache"
	// CapabilitySession covers login-state storage.
	CapabilitySession Capability = "session"
	// CapabilityRateLimit covers the abuse guard on unauthenticated endpoints.
	CapabilityRateLimit Capability = "rate-limit"
	// CapabilityLock covers distributed order locking.
	CapabilityLock Capability = "lock"
	// CapabilityHealth covers the reachability probe.
	CapabilityHealth Capability = "health"
	// CapabilityIdempotency covers the duplicate-event short circuit in front of
	// the authoritative consumption record.
	CapabilityIdempotency Capability = "idempotency"
)

// FailMode states what a caller observes when Redis is unavailable. It decides
// whether the dependency failure is surfaced as an error or swallowed and
// replaced by the capability's safe fallback.
type FailMode int

const (
	// FailOpen hides an availability failure behind the documented fallback:
	// a cache read reports a miss, a dropped write is ignored, and a rate-limit
	// check allows the request while flagging itself degraded. The caller keeps
	// working against PostgreSQL, which stays the source of truth.
	FailOpen FailMode = iota
	// FailClosed surfaces the failure so the caller can answer 503 instead of
	// proceeding without the guarantee Redis provides. Used where continuing
	// unprotected would be worse than failing the request.
	FailClosed
)

func (m FailMode) String() string {
	if m == FailClosed {
		return "fail-closed"
	}
	return "fail-open"
}

// Policy is the per-capability degradation baseline.
//
// This is configuration, not business logic. It exists because each choice
// trades availability against safety, and that trade belongs to the owning
// module rather than to this package. The defaults below are chosen so that the
// P0 business loop stays recoverable without Redis, which is an explicit
// approval criterion in docs/migration/backend-parallel-development.md
// section 8, while no check that protects money or identity is weakened:
//
//	cache      FailOpen   PostgreSQL remains the source of truth, so a cache
//	                      miss costs latency only.
//	session    FailClosed an unverifiable login must never be accepted, so the
//	                      request fails instead of silently becoming anonymous.
//	rate-limit FailOpen   refusing login and order traffic because the abuse
//	                      guard is down would break the main flow. The result
//	                      is flagged Degraded so the caller can alert.
//	lock       FailClosed a lock that cannot be taken must not be assumed to be
//	                      held, because the lock is what prevents two
//	                      charge-start requests from both succeeding.
//	idempotency FailOpen  the guard only suppresses rapid duplicates. The
//	                      authoritative duplicate protection is the consumption
//	                      record in PostgreSQL, so refusing to process an event
//	                      because the short circuit is down would lose work for
//	                      no safety gain.
//
// The lock default is the one that can deny business traffic while Redis is
// down. Callers that must degrade instead can switch it to FailOpen, in which
// case Acquire reports Degraded and the caller decides whether to continue
// unprotected.
type Policy struct {
	Cache       FailMode
	Session     FailMode
	RateLimit   FailMode
	Lock        FailMode
	Idempotency FailMode
}

// DefaultPolicy returns the baseline described on Policy.
func DefaultPolicy() Policy {
	return Policy{
		Cache:       FailOpen,
		Session:     FailClosed,
		RateLimit:   FailOpen,
		Lock:        FailClosed,
		Idempotency: FailOpen,
	}
}

// modeFor returns the configured mode for a capability. A capability without a
// field (health) is always FailClosed: a probe reports the truth.
func (p Policy) modeFor(capability Capability) FailMode {
	switch capability {
	case CapabilityCache:
		return p.Cache
	case CapabilitySession:
		return p.Session
	case CapabilityRateLimit:
		return p.RateLimit
	case CapabilityLock:
		return p.Lock
	case CapabilityIdempotency:
		return p.Idempotency
	default:
		return FailClosed
	}
}

// Validate rejects an out-of-range mode so a typo cannot silently widen a
// policy.
func (p Policy) Validate() error {
	for _, item := range []struct {
		name string
		mode FailMode
	}{
		{"cache", p.Cache},
		{"session", p.Session},
		{"rate-limit", p.RateLimit},
		{"lock", p.Lock},
		{"idempotency", p.Idempotency},
	} {
		if item.mode != FailOpen && item.mode != FailClosed {
			return errors.New("invalid fail mode for " + item.name)
		}
	}
	return nil
}

// DegradationStat is the per-capability failure snapshot.
type DegradationStat struct {
	Capability Capability
	// Failures counts every availability failure observed for the capability,
	// including the ones that were hidden from the caller.
	Failures int64
	// FailOpen and FailClosed split Failures by the decision that was taken, so
	// an operator can tell "Redis is down and we degraded" apart from "Redis is
	// down and we are rejecting traffic".
	FailOpen   int64
	FailClosed int64
	// LastError is a truncated message, never a credential. Adapters must build
	// error text from ConnConfig.String(), which redacts the password.
	LastError string
	LastAt    time.Time
}

const maxLastErrorLength = 256

// Degradation is a concurrency-safe counter of dependency failures per
// capability. It is deliberately not a logger: the repository layer records,
// and the observability module (B-05) decides how to surface it.
type Degradation struct {
	mu    sync.Mutex
	stats map[Capability]*DegradationStat
	clock func() time.Time
}

// NewDegradation returns an empty failure registry using the wall clock.
func NewDegradation() *Degradation {
	return &Degradation{
		stats: make(map[Capability]*DegradationStat),
		clock: func() time.Time { return time.Now().UTC() },
	}
}

// Record notes one availability failure for a capability together with the
// decision that was applied. A nil error is ignored so callers can record
// unconditionally.
func (d *Degradation) Record(capability Capability, mode FailMode, err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	stat, ok := d.stats[capability]
	if !ok {
		stat = &DegradationStat{Capability: capability}
		d.stats[capability] = stat
	}
	stat.Failures++
	if mode == FailOpen {
		stat.FailOpen++
	} else {
		stat.FailClosed++
	}
	stat.LastError = truncate(err.Error(), maxLastErrorLength)
	stat.LastAt = d.clock()
}

// Failures reports the total failure count for one capability.
func (d *Degradation) Failures(capability Capability) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if stat, ok := d.stats[capability]; ok {
		return stat.Failures
	}
	return 0
}

// Snapshot returns a copy of every stat, ordered by capability so an ops
// endpoint produces stable output and tests do not depend on map iteration.
func (d *Degradation) Snapshot() []DegradationStat {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]DegradationStat, 0, len(d.stats))
	for _, stat := range d.stats {
		out = append(out, *stat)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Capability < out[j].Capability })
	return out
}

// Reset clears the counters. It exists for tests and for a manual ops reset.
func (d *Degradation) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats = make(map[Capability]*DegradationStat)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

// isUnavailable reports whether err means "Redis could not be reached". Only
// this class of failure is subject to a degradation policy.
func isUnavailable(err error) bool { return errors.Is(err, ErrUnavailable) }

// observer carries the policy and counters shared by every capability in this
// package, so each one applies the same rules instead of reimplementing them.
type observer struct {
	policy Policy
	stats  *Degradation
}

func newObserver(policy Policy, stats *Degradation) observer {
	if stats == nil {
		stats = NewDegradation()
	}
	return observer{policy: policy, stats: stats}
}

// run executes one dependency call under the capability's policy.
//
// The rules are deliberately narrow:
//
//   - a caller cancellation is returned as-is and is not counted as a
//     degradation, because that is our own request ending, not Redis failing;
//   - any error that is not an availability failure (for example ErrNotFound)
//     is a normal result and is never swallowed;
//   - only an availability failure is counted, and only that failure may be
//     replaced by fallback when the mode is FailOpen.
func run[T any](
	ctx context.Context,
	obs observer,
	capability Capability,
	fallback T,
	op func() (T, error),
) (T, error) {
	value, err := op()
	if err == nil {
		return value, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return value, ctxErr
	}
	if !isUnavailable(err) {
		return value, err
	}

	mode := obs.policy.modeFor(capability)
	obs.stats.Record(capability, mode, err)
	if mode == FailOpen {
		return fallback, nil
	}
	return value, err
}
