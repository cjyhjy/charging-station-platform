package redis

import (
	"context"
	"fmt"
	"time"
)

// Limit is one fixed-window allowance.
type Limit struct {
	// Requests is the maximum number of calls permitted inside Window.
	Requests int
	Window   time.Duration
}

func (l Limit) validate() error {
	if l.Requests <= 0 {
		return fmt.Errorf("rate limit requests must be greater than zero")
	}
	if l.Window <= 0 {
		return fmt.Errorf("rate limit window must be greater than zero")
	}
	return nil
}

// LimitResult is the outcome of one allowance check.
type LimitResult struct {
	Allowed   bool
	Remaining int
	// ResetAt is when the current window ends and the counter starts again.
	ResetAt time.Time
	// Degraded is true when Redis was unreachable and the FailOpen policy let
	// the request through unchecked. Callers should alert on it rather than
	// treat the request as verified.
	Degraded bool
}

// Limiter is a fixed-window counter limiter under
// ncs:auth:rate-limit:{identity}.
//
// A fixed window is deliberate. Counting and attaching the window is one
// indivisible operation (IncrWithWindow, a single server-side script), which is
// the property that matters: a counter must never exist without an expiration, or
// the identity would be blocked forever. The cost of a fixed window is that up to
// twice the limit can pass across a window boundary, which is acceptable because
// this is a coarse abuse guard on unauthenticated endpoints such as login and SMS
// sending, not a billing meter. A sliding window or token bucket would need extra
// state for a guarantee this guard does not owe.
//
// The counter lives in Redis only, so losing Redis resets every window. That is
// a deliberate trade: a limiter is not business state, and section 4.3 of the
// contract forbids treating Redis as the source of truth.
type Limiter struct {
	commands Commands
	observer observer
	clock    func() time.Time
}

// NewLimiter validates its inputs. A nil clock uses the wall clock.
func NewLimiter(commands Commands, policy Policy, stats *Degradation, clock func() time.Time) (*Limiter, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Limiter{commands: commands, observer: newObserver(policy, stats), clock: clock}, nil
}

// Allow consumes one unit of the allowance for identity and reports whether the
// call may proceed.
//
// The counter is created without a TTL and the window is attached immediately
// afterwards. If the EXPIRE fails the key is deleted and the call errors, so a
// limiter that cannot set its window fails loudly instead of leaving a
// permanent counter that would lock the identity out forever.
func (l *Limiter) Allow(ctx context.Context, identity string, limit Limit) (LimitResult, error) {
	key, err := AuthRateLimitKey(identity)
	if err != nil {
		return LimitResult{}, err
	}
	if err := limit.validate(); err != nil {
		return LimitResult{}, err
	}

	// Under FailOpen the fallback allows the request but marks it degraded, so
	// the caller can distinguish "checked and allowed" from "not checked".
	fallback := LimitResult{
		Allowed:   true,
		Remaining: limit.Requests,
		ResetAt:   l.clock().Add(limit.Window),
		Degraded:  true,
	}

	return run(ctx, l.observer, CapabilityRateLimit, fallback, func() (LimitResult, error) {
		// One atomic step increments the counter and guarantees its window. A
		// separate INCR then EXPIRE cannot be made crash safe: a process that
		// dies between them, a lost reply, or a failed cleanup all leave a
		// permanent counter that would block the identity forever.
		count, remaining, err := l.commands.IncrWithWindow(ctx, key, limit.Window)
		if err != nil {
			return LimitResult{}, fmt.Errorf("count rate limit window for %s: %w", key, err)
		}

		resetAt := l.clock().Add(limit.Window)
		if remaining > 0 {
			resetAt = l.clock().Add(remaining)
		}

		left := limit.Requests - int(count)
		if left < 0 {
			left = 0
		}
		return LimitResult{
			Allowed:   count <= int64(limit.Requests),
			Remaining: left,
			ResetAt:   resetAt,
		}, nil
	})
}
