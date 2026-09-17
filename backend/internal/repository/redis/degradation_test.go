package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDefaultPolicyProtectsIdentityAndMoney(t *testing.T) {
	policy := DefaultPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy must be valid: %v", err)
	}

	// The cache may degrade: PostgreSQL stays the source of truth.
	if policy.modeFor(CapabilityCache) != FailOpen {
		t.Error("cache must fail open, because PostgreSQL remains authoritative")
	}
	// Login state may not degrade: an unverifiable session must never be accepted.
	if policy.modeFor(CapabilitySession) != FailClosed {
		t.Error("session must fail closed, because an unverifiable login is not a login")
	}
	// Order locking may not degrade silently: the lock is what prevents double
	// charge starts.
	if policy.modeFor(CapabilityLock) != FailClosed {
		t.Error("lock must fail closed, because an untaken lock must not be assumed")
	}
	// The abuse guard degrades to keep login and order traffic flowing.
	if policy.modeFor(CapabilityRateLimit) != FailOpen {
		t.Error("rate limit must fail open, so a guard outage cannot block the main flow")
	}
}

func TestPolicyModeForUnknownCapabilityIsFailClosed(t *testing.T) {
	// A capability added without a policy field must default to the safe mode.
	if got := DefaultPolicy().modeFor(CapabilityHealth); got != FailClosed {
		t.Fatalf("expected FailClosed for an unmapped capability, got %s", got)
	}
}

func TestPolicyValidateRejectsUnknownMode(t *testing.T) {
	policy := DefaultPolicy()
	policy.Cache = FailMode(42)
	if err := policy.Validate(); err == nil {
		t.Fatal("expected an out-of-range fail mode to be rejected")
	}
}

func TestFailModeString(t *testing.T) {
	if FailOpen.String() != "fail-open" {
		t.Errorf("unexpected label %q", FailOpen.String())
	}
	if FailClosed.String() != "fail-closed" {
		t.Errorf("unexpected label %q", FailClosed.String())
	}
}

func TestDegradationRecordsFailuresPerCapability(t *testing.T) {
	stats := NewDegradation()
	stats.Record(CapabilityCache, FailOpen, unavailable("connection refused"))
	stats.Record(CapabilityCache, FailOpen, unavailable("connection refused"))
	stats.Record(CapabilityLock, FailClosed, unavailable("timeout"))

	if got := stats.Failures(CapabilityCache); got != 2 {
		t.Fatalf("expected 2 cache failures, got %d", got)
	}
	if got := stats.Failures(CapabilityLock); got != 1 {
		t.Fatalf("expected 1 lock failure, got %d", got)
	}
	if got := stats.Failures(CapabilitySession); got != 0 {
		t.Fatalf("expected 0 session failures, got %d", got)
	}

	snapshot := stats.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(snapshot))
	}
	// Ordering is by capability so an ops endpoint is stable.
	if snapshot[0].Capability != CapabilityCache || snapshot[1].Capability != CapabilityLock {
		t.Fatalf("expected cache then lock, got %s then %s", snapshot[0].Capability, snapshot[1].Capability)
	}
	if snapshot[0].FailOpen != 2 || snapshot[0].FailClosed != 0 {
		t.Fatalf("expected both cache failures counted as fail-open, got %+v", snapshot[0])
	}
	if snapshot[1].FailClosed != 1 || snapshot[1].FailOpen != 0 {
		t.Fatalf("expected the lock failure counted as fail-closed, got %+v", snapshot[1])
	}
}

func TestDegradationIgnoresNilError(t *testing.T) {
	stats := NewDegradation()
	stats.Record(CapabilityCache, FailOpen, nil)
	if got := stats.Failures(CapabilityCache); got != 0 {
		t.Fatalf("expected no failure recorded, got %d", got)
	}
}

func TestDegradationTruncatesLastError(t *testing.T) {
	stats := NewDegradation()
	long := strings.Repeat("x", maxLastErrorLength*2)
	stats.Record(CapabilityCache, FailOpen, errors.New(long))

	snapshot := stats.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 stat, got %d", len(snapshot))
	}
	if len(snapshot[0].LastError) != maxLastErrorLength {
		t.Fatalf("expected the message truncated to %d bytes, got %d", maxLastErrorLength, len(snapshot[0].LastError))
	}
	if snapshot[0].LastAt.IsZero() {
		t.Fatal("expected the failure to be timestamped")
	}
}

func TestDegradationResetClearsCounters(t *testing.T) {
	stats := NewDegradation()
	stats.Record(CapabilityCache, FailOpen, unavailable("down"))
	stats.Reset()
	if got := stats.Failures(CapabilityCache); got != 0 {
		t.Fatalf("expected counters cleared, got %d", got)
	}
	if len(stats.Snapshot()) != 0 {
		t.Fatal("expected an empty snapshot after reset")
	}
}

func TestNewObserverReplacesNilStats(t *testing.T) {
	obs := newObserver(DefaultPolicy(), nil)
	obs.stats.Record(CapabilityCache, FailOpen, unavailable("down"))
	if got := obs.stats.Failures(CapabilityCache); got != 1 {
		t.Fatalf("expected an internal registry, got %d", got)
	}
}

// run is the single place the degradation rules are implemented, so its
// boundaries are tested directly rather than only through one caller.
func TestRunReturnsValueWhenOperationSucceeds(t *testing.T) {
	obs := newObserver(DefaultPolicy(), NewDegradation())
	value, err := run(context.Background(), obs, CapabilityCache, -1, func() (int, error) {
		return 7, nil
	})
	if err != nil || value != 7 {
		t.Fatalf("expected 7 and no error, got %d and %v", value, err)
	}
}

func TestRunNeverSwallowsAnApplicationError(t *testing.T) {
	// ErrNotFound is a normal result. A FailOpen policy must not turn it into a
	// silent fallback, or a cache miss and a real absence would blur together.
	stats := NewDegradation()
	policy := DefaultPolicy()
	policy.Cache = FailOpen
	obs := newObserver(policy, stats)

	_, err := run(context.Background(), obs, CapabilityCache, 0, func() (int, error) {
		return 0, errors.New("some application failure")
	})
	if err == nil {
		t.Fatal("expected the application error to surface")
	}
	if got := stats.Failures(CapabilityCache); got != 0 {
		t.Fatalf("an application error must not be counted as a degradation, got %d", got)
	}
}

func TestRunFailOpenReplacesAvailabilityFailureWithFallback(t *testing.T) {
	stats := NewDegradation()
	policy := DefaultPolicy()
	policy.Cache = FailOpen
	obs := newObserver(policy, stats)

	value, err := run(context.Background(), obs, CapabilityCache, -1, func() (int, error) {
		return 0, unavailable("connection refused")
	})
	if err != nil {
		t.Fatalf("expected the failure to be hidden, got %v", err)
	}
	if value != -1 {
		t.Fatalf("expected the fallback, got %d", value)
	}
	if got := stats.Failures(CapabilityCache); got != 1 {
		t.Fatalf("expected the failure counted even though it was hidden, got %d", got)
	}
}

func TestRunFailClosedSurfacesAvailabilityFailure(t *testing.T) {
	stats := NewDegradation()
	policy := DefaultPolicy()
	policy.Session = FailClosed
	obs := newObserver(policy, stats)

	_, err := run(context.Background(), obs, CapabilitySession, "", func() (string, error) {
		return "", unavailable("connection refused")
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	if got := stats.Failures(CapabilitySession); got != 1 {
		t.Fatalf("expected the failure counted, got %d", got)
	}
}

// A caller cancellation is our own request ending, not Redis failing. Counting
// it would make a deploying service look like it has a Redis outage.
func TestRunTreatsCallerCancellationAsNotADegradation(t *testing.T) {
	stats := NewDegradation()
	obs := newObserver(DefaultPolicy(), stats)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := run(ctx, obs, CapabilityCache, -1, func() (int, error) {
		return 0, unavailable("connection refused")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := stats.Failures(CapabilityCache); got != 0 {
		t.Fatalf("a caller cancellation must not be counted as a degradation, got %d", got)
	}
}

func TestRunHonoursDeadlineExceededFromCaller(t *testing.T) {
	stats := NewDegradation()
	obs := newObserver(DefaultPolicy(), stats)

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	_, err := run(ctx, obs, CapabilityCache, -1, func() (int, error) {
		return 0, unavailable("connection refused")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if got := stats.Failures(CapabilityCache); got != 0 {
		t.Fatalf("expected no degradation recorded for a deadline, got %d", got)
	}
}
