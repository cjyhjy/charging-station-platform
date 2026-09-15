package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newTestLimiter builds a limiter over an explicitly supplied store, used when a
// test injects a stub that must fail a specific command.
func newTestLimiter(t *testing.T, commands Commands, policy Policy, stats *Degradation, clock *testClock) *Limiter {
	t.Helper()
	limiter, err := NewLimiter(commands, policy, stats, clock.Now)
	if err != nil {
		t.Fatalf("new limiter: %v", err)
	}
	return limiter
}

// newLimiterFixture shares one clock between the store and the limiter. Both
// halves must read the same clock or advancing time would move the limiter's
// view of the window while the stored counter kept its own deadline.
func newLimiterFixture(t *testing.T, policy Policy, stats *Degradation) (*MemoryCommands, *Limiter, *testClock) {
	t.Helper()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	return commands, newTestLimiter(t, commands, policy, stats, clock), clock
}

func TestNewLimiterValidatesInputs(t *testing.T) {
	if _, err := NewLimiter(nil, DefaultPolicy(), nil, nil); err == nil {
		t.Fatal("expected nil commands to be rejected")
	}
	if _, err := NewLimiter(NewMemoryCommands(), Policy{RateLimit: FailMode(9)}, nil, nil); err == nil {
		t.Fatal("expected an invalid policy to be rejected")
	}
}

func TestLimiterAllowsUpToTheLimitThenRefuses(t *testing.T) {
	ctx := context.Background()
	_, limiter, _ := newLimiterFixture(t, DefaultPolicy(), nil)
	limit := Limit{Requests: 3, Window: time.Minute}

	for attempt := 1; attempt <= 3; attempt++ {
		result, err := limiter.Allow(ctx, "13800000000", limit)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if !result.Allowed {
			t.Fatalf("attempt %d should be allowed", attempt)
		}
		if result.Remaining != 3-attempt {
			t.Errorf("attempt %d: expected %d remaining, got %d", attempt, 3-attempt, result.Remaining)
		}
		if result.Degraded {
			t.Errorf("attempt %d must not be marked degraded", attempt)
		}
	}

	result, err := limiter.Allow(ctx, "13800000000", limit)
	if err != nil {
		t.Fatalf("refusal: %v", err)
	}
	if result.Allowed {
		t.Fatal("the fourth attempt must be refused")
	}
	if result.Remaining != 0 {
		t.Fatalf("expected 0 remaining, got %d", result.Remaining)
	}
}

func TestLimiterWindowResetsAfterItElapses(t *testing.T) {
	ctx := context.Background()
	_, limiter, clock := newLimiterFixture(t, DefaultPolicy(), nil)
	limit := Limit{Requests: 1, Window: time.Minute}

	if result, err := limiter.Allow(ctx, "id_01", limit); err != nil || !result.Allowed {
		t.Fatalf("expected the first attempt to be allowed, got %+v and %v", result, err)
	}
	if result, err := limiter.Allow(ctx, "id_01", limit); err != nil || result.Allowed {
		t.Fatalf("expected the second attempt to be refused, got %+v and %v", result, err)
	}

	clock.Advance(time.Minute)
	if result, err := limiter.Allow(ctx, "id_01", limit); err != nil || !result.Allowed {
		t.Fatalf("expected a fresh window after the ttl, got %+v and %v", result, err)
	}
}

func TestLimiterIdentitiesAreIndependent(t *testing.T) {
	ctx := context.Background()
	_, limiter, _ := newLimiterFixture(t, DefaultPolicy(), nil)
	limit := Limit{Requests: 1, Window: time.Minute}

	if result, err := limiter.Allow(ctx, "id_01", limit); err != nil || !result.Allowed {
		t.Fatalf("id_01: %+v and %v", result, err)
	}
	// One identity exhausting its window must not affect another.
	if result, err := limiter.Allow(ctx, "id_02", limit); err != nil || !result.Allowed {
		t.Fatalf("id_02 must have its own window, got %+v and %v", result, err)
	}
}

func TestLimiterReportsResetAtFromTheWindow(t *testing.T) {
	ctx := context.Background()
	_, limiter, clock := newLimiterFixture(t, DefaultPolicy(), nil)

	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 5, Window: time.Minute})
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	want := clock.Now().Add(time.Minute)
	if !result.ResetAt.Equal(want) {
		t.Fatalf("expected reset at %s, got %s", want, result.ResetAt)
	}
}

func TestLimiterValidatesLimitAndIdentity(t *testing.T) {
	ctx := context.Background()
	_, limiter, _ := newLimiterFixture(t, DefaultPolicy(), nil)

	if _, err := limiter.Allow(ctx, "", Limit{Requests: 1, Window: time.Minute}); err == nil {
		t.Fatal("expected an empty identity to be rejected")
	}
	if _, err := limiter.Allow(ctx, "id:01", Limit{Requests: 1, Window: time.Minute}); err == nil {
		t.Fatal("expected an identity with a colon to be rejected")
	}
	if _, err := limiter.Allow(ctx, "id_01", Limit{Requests: 0, Window: time.Minute}); err == nil {
		t.Fatal("expected a zero request limit to be rejected")
	}
	if _, err := limiter.Allow(ctx, "id_01", Limit{Requests: 1, Window: 0}); err == nil {
		t.Fatal("expected a zero window to be rejected")
	}
}

// FailOpen is the default for the abuse guard: refusing login and order traffic
// because the limiter is down would break the main flow. The result is flagged
// so the caller can alert instead of mistaking it for a verified check.
func TestLimiterFailOpenAllowsButFlagsDegraded(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands, limiter, _ := newLimiterFixture(t, DefaultPolicy(), stats)

	commands.SetDown(errors.New("connection refused"))
	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 1, Window: time.Minute})
	if err != nil {
		t.Fatalf("expected the outage to be hidden, got %v", err)
	}
	if !result.Allowed {
		t.Fatal("expected the request to be allowed while degraded")
	}
	if !result.Degraded {
		t.Fatal("expected the result to be flagged degraded")
	}
	if got := stats.Failures(CapabilityRateLimit); got != 1 {
		t.Fatalf("expected the outage to be counted, got %d", got)
	}
}

func TestLimiterFailClosedSurfacesOutage(t *testing.T) {
	ctx := context.Background()
	policy := DefaultPolicy()
	policy.RateLimit = FailClosed
	commands, limiter, _ := newLimiterFixture(t, policy, nil)

	commands.SetDown(errors.New("connection refused"))
	if _, err := limiter.Allow(ctx, "id_01", Limit{Requests: 1, Window: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable under FailClosed, got %v", err)
	}
}

// The rate limiter must count and set the window in one atomic operation. Using a
// separate INCR and EXPIRE leaves a permanent counter when the process dies, the
// reply is lost, or the cleanup fails, and that counter would block an identity
// forever. This test pins the atomic form.
func TestLimiterUsesOneAtomicWindowedIncrement(t *testing.T) {
	ctx := context.Background()
	commands := newStubCommands()
	limiter := newTestLimiter(t, commands, DefaultPolicy(), nil, newTestClock())
	limit := Limit{Requests: 3, Window: 45 * time.Second}

	if _, err := limiter.Allow(ctx, "id_01", limit); err != nil {
		t.Fatalf("allow: %v", err)
	}

	incr, incrWindow, expire, _ := commands.calls()
	if incrWindow != 1 {
		t.Fatalf("expected exactly 1 windowed increment, got %d", incrWindow)
	}
	if incr != 0 {
		t.Fatalf("the limiter must not use the non-windowed INCR, got %d call(s)", incr)
	}
	if expire != 0 {
		t.Fatalf("the limiter must not issue a separate EXPIRE, got %d call(s)", expire)
	}

	key, window := commands.window()
	wantKey, _ := AuthRateLimitKey("id_01")
	if key != wantKey {
		t.Fatalf("expected the window applied to %s, got %s", wantKey, key)
	}
	if window != limit.Window {
		t.Fatalf("expected the window %s, got %s", limit.Window, window)
	}
}

// The window must be attached on the very first increment, so an interrupted
// process cannot leave a counter with no expiration.
func TestLimiterAttachesTheWindowOnTheFirstIncrement(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	limiter := newTestLimiter(t, commands, DefaultPolicy(), nil, clock)

	if _, err := limiter.Allow(ctx, "id_01", Limit{Requests: 5, Window: time.Minute}); err != nil {
		t.Fatalf("allow: %v", err)
	}

	key, _ := AuthRateLimitKey("id_01")
	ttl, hasExpiry, err := commands.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the counter to have a window immediately after the first increment")
	}
	if ttl != time.Minute {
		t.Fatalf("expected a full one minute window, got %s", ttl)
	}
}

// A counter left without a window by an older client must heal instead of
// blocking the identity forever.
func TestLimiterHealsACounterWithoutAWindow(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	key, _ := AuthRateLimitKey("id_01")
	// A pre-existing counter with no expiration, as an older two-step client
	// would have left behind after a crash.
	if err := commands.Set(ctx, key, "7", 0); err != nil {
		t.Fatalf("set: %v", err)
	}

	limiter := newTestLimiter(t, commands, DefaultPolicy(), nil, clock)
	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 10, Window: time.Minute})
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	// The pre-existing 7 must be carried forward, not reset: 10 - (7+1) = 2.
	if !result.Allowed {
		t.Fatal("expected the call to be allowed inside the limit")
	}
	if result.Remaining != 2 {
		t.Fatalf("expected the pre-existing count to be honoured (2 remaining), got %d", result.Remaining)
	}

	_, hasExpiry, err := commands.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the windowless counter to be healed with a window")
	}
}

func TestLimiterRefusesAHealedCounterBeyondTheLimit(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	key, _ := AuthRateLimitKey("id_01")
	if err := commands.Set(ctx, key, "7", 0); err != nil {
		t.Fatalf("set: %v", err)
	}

	limiter := newTestLimiter(t, commands, DefaultPolicy(), nil, clock)
	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 5, Window: time.Minute})
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if result.Allowed {
		t.Fatal("a carried-forward count above the limit must be refused")
	}
	if result.Remaining != 0 {
		t.Fatalf("expected 0 remaining, got %d", result.Remaining)
	}
}

func TestLimiterFailClosedSurfacesWindowedIncrementFailure(t *testing.T) {
	ctx := context.Background()
	policy := DefaultPolicy()
	policy.RateLimit = FailClosed
	commands := newStubCommands()
	commands.incrWindowErr = unavailable("connection refused")
	limiter := newTestLimiter(t, commands, policy, nil, newTestClock())

	if _, err := limiter.Allow(ctx, "id_01", Limit{Requests: 1, Window: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestLimiterToleratesTTLFailure(t *testing.T) {
	ctx := context.Background()
	commands := newStubCommands()
	// ResetAt is informational; losing it must not refuse a request.
	commands.ttlErr = errors.New("ttl unsupported")
	limiter := newTestLimiter(t, commands, DefaultPolicy(), nil, newTestClock())

	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 2, Window: time.Minute})
	if err != nil {
		t.Fatalf("expected the request to be allowed, got %v", err)
	}
	if !result.Allowed {
		t.Fatal("expected the first attempt to be allowed")
	}
}

func TestLimiterGrantsEveryAttemptInsideTheLimitUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	_, limiter, _ := newLimiterFixture(t, DefaultPolicy(), nil)
	limit := Limit{Requests: 100, Window: time.Minute}

	const workers = 50
	results := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go func() {
			result, err := limiter.Allow(ctx, "shared", limit)
			if err != nil {
				results <- false
				return
			}
			results <- result.Allowed
		}()
	}

	allowed := 0
	for i := 0; i < workers; i++ {
		if <-results {
			allowed++
		}
	}
	// Every attempt is inside the limit, so a lost increment would show up here
	// as an over-count of refusals under -race.
	if allowed != workers {
		t.Fatalf("expected all %d attempts allowed, got %d", workers, allowed)
	}
}

func TestLimiterRefusesExactlyBeyondTheLimitUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	_, limiter, _ := newLimiterFixture(t, DefaultPolicy(), nil)
	const limitValue = 10
	limit := Limit{Requests: limitValue, Window: time.Minute}

	const workers = 40
	results := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go func() {
			result, err := limiter.Allow(ctx, "shared", limit)
			if err != nil {
				t.Errorf("allow: %v", err)
				results <- false
				return
			}
			results <- result.Allowed
		}()
	}

	allowed := 0
	for i := 0; i < workers; i++ {
		if <-results {
			allowed++
		}
	}
	// A fixed-window counter with an atomic INCR must admit exactly the limit
	// when all attempts land in one window.
	if allowed != limitValue {
		t.Fatalf("expected exactly %d admissions, got %d", limitValue, allowed)
	}
}
