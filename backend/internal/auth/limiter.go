package auth

import (
	"context"
	"sync"
	"time"
)

// LoginRateLimiter throttles login attempts per account. The A line owns the
// policy (limit + window) and depends on this interface; the B line owns the
// Redis limiter that production deployments will wire in.
type LoginRateLimiter interface {
	// Allow reports whether one more attempt is permitted for the key within
	// the current window. When denied, retryAfter is a safe waiting hint.
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration)
}

// FixedWindowLimiter is an in-memory counter keyed by string with a shared
// window length. Counters expire with the window, so memory stays bounded by
// the number of accounts attacked per window.
type FixedWindowLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	counts map[string]*windowCount
	clock  func() time.Time
}

type windowCount struct {
	bucketStart time.Time
	attempts    int
}

// NewFixedWindowLimiter builds a limiter allowing limit attempts per window.
func NewFixedWindowLimiter(limit int, window time.Duration, clock func() time.Time) *FixedWindowLimiter {
	if clock == nil {
		clock = time.Now
	}
	return &FixedWindowLimiter{
		limit:  limit,
		window: window,
		counts: make(map[string]*windowCount),
		clock:  clock,
	}
}

func (l *FixedWindowLimiter) Allow(_ context.Context, key string) (bool, time.Duration) {
	now := l.clock()

	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok := l.counts[key]
	if !ok || now.Sub(current.bucketStart) >= l.window {
		l.counts[key] = &windowCount{bucketStart: now, attempts: 0}
		current = l.counts[key]
		// Opportunistic cleanup keeps the map from growing without bound.
		if len(l.counts) > 1024 {
			l.evictExpiredLocked(now)
		}
	}

	if current.attempts >= l.limit {
		retryAfter := l.window - now.Sub(current.bucketStart)
		if retryAfter < 0 {
			retryAfter = 0
		}
		return false, retryAfter
	}

	current.attempts++
	return true, 0
}

func (l *FixedWindowLimiter) evictExpiredLocked(now time.Time) {
	for key, counter := range l.counts {
		if now.Sub(counter.bucketStart) >= l.window {
			delete(l.counts, key)
		}
	}
}
