package redis

import (
	"context"
	"sync"
	"time"
)

// testClock is a mutable clock the tests own. Injecting it through SetClock and
// the clock parameters keeps TTL and window assertions deterministic instead of
// making tests sleep and hope.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return newTestClockAt(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
}

// newTestClockAt returns a clock anchored at an explicit instant, used when a
// test needs a deadline far away from the wall clock.
func newTestClockAt(instant time.Time) *testClock {
	return &testClock{now: instant}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stubCommands delegates to a real MemoryCommands and overrides only the
// operations a test needs to fail or observe. Embedding keeps the stub honest:
// every method it does not override behaves exactly as production would.
type stubCommands struct {
	*MemoryCommands

	getErr    error
	setErr    error
	incrErr   error
	expireErr error
	setNXErr  error
	ttlErr    error

	incrWindowErr error

	// Call counters let a test assert which commands a capability uses, which is
	// how the "one atomic windowed increment" invariant is pinned.
	mu                 sync.Mutex
	incrCalls          int
	incrWindowCalls    int
	expireCalls        int
	compareDeleteCalls int
	observedWindow     time.Duration
	observedWindowKey  string
}

func newStubCommands() *stubCommands {
	return &stubCommands{MemoryCommands: NewMemoryCommands()}
}

// calls returns the recorded command counts.
func (s *stubCommands) calls() (incr, incrWindow, expire, compareDelete int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.incrCalls, s.incrWindowCalls, s.expireCalls, s.compareDeleteCalls
}

// window records the arguments of the last windowed increment.
func (s *stubCommands) window() (string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observedWindowKey, s.observedWindow
}

func (s *stubCommands) Get(ctx context.Context, key string) (string, error) {
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.MemoryCommands.Get(ctx, key)
}

func (s *stubCommands) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if s.setErr != nil {
		return s.setErr
	}
	return s.MemoryCommands.Set(ctx, key, value, ttl)
}

func (s *stubCommands) Incr(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	s.incrCalls++
	s.mu.Unlock()
	if s.incrErr != nil {
		return 0, s.incrErr
	}
	return s.MemoryCommands.Incr(ctx, key)
}

func (s *stubCommands) IncrWithWindow(ctx context.Context, key string, window time.Duration) (int64, time.Duration, error) {
	s.mu.Lock()
	s.incrWindowCalls++
	s.observedWindowKey = key
	s.observedWindow = window
	s.mu.Unlock()
	if s.incrWindowErr != nil {
		return 0, 0, s.incrWindowErr
	}
	return s.MemoryCommands.IncrWithWindow(ctx, key, window)
}

func (s *stubCommands) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	s.expireCalls++
	s.mu.Unlock()
	if s.expireErr != nil {
		return false, s.expireErr
	}
	return s.MemoryCommands.Expire(ctx, key, ttl)
}

func (s *stubCommands) CompareAndDelete(ctx context.Context, key, expected string) (bool, error) {
	s.mu.Lock()
	s.compareDeleteCalls++
	s.mu.Unlock()
	return s.MemoryCommands.CompareAndDelete(ctx, key, expected)
}

func (s *stubCommands) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if s.setNXErr != nil {
		return false, s.setNXErr
	}
	return s.MemoryCommands.SetNX(ctx, key, value, ttl)
}

func (s *stubCommands) TTL(ctx context.Context, key string) (time.Duration, bool, error) {
	if s.ttlErr != nil {
		return 0, false, s.ttlErr
	}
	return s.MemoryCommands.TTL(ctx, key)
}

// unavailable is the failure a production adapter must produce when Redis cannot
// be reached. Tests use it to drive every degradation path.
func unavailable(reason string) error {
	return &availabilityError{reason: reason}
}

type availabilityError struct{ reason string }

func (e *availabilityError) Error() string { return "redis unreachable: " + e.reason }
func (e *availabilityError) Unwrap() error { return ErrUnavailable }
