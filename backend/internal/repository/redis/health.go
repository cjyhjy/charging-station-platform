package redis

import (
	"context"
	"fmt"
	"time"
)

// HealthChecker is the reachability contract consumed by a readiness probe. It
// returns nil only when Redis actually answered.
//
// The A-line /readyz handler does not call this yet: wiring it in touches
// backend/internal/httpapi/, which belongs to the A line. This module defines
// and tests the contract, and the wiring is a separate, approved change.
type HealthChecker interface {
	Check(context.Context) error
}

var _ HealthChecker = (*Health)(nil)

// HealthStatus is the snapshot an ops endpoint renders.
type HealthStatus struct {
	OK        bool
	CheckedAt time.Time
	Latency   time.Duration
	// Error is a truncated, credential-free message. It is empty when OK.
	Error string
}

// Health probes Redis with a bounded timeout.
//
// Unlike the capability wrappers, a probe never applies a degradation policy: a
// health check that hid a failure would defeat its purpose. Failures are still
// counted under CapabilityHealth so an operator can see probe failures next to
// the capability counters.
type Health struct {
	commands Commands
	observer observer
	timeout  time.Duration
	clock    func() time.Time
}

// DefaultHealthTimeout bounds a probe so a hung Redis cannot stall a readiness
// endpoint.
const DefaultHealthTimeout = 2 * time.Second

// NewHealth validates its inputs. A non-positive timeout uses the default.
func NewHealth(commands Commands, stats *Degradation, timeout time.Duration, clock func() time.Time) (*Health, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if timeout <= 0 {
		timeout = DefaultHealthTimeout
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	// Health ignores the capability policy on purpose; FailClosed describes the
	// counter semantics, not a decision the probe is allowed to make.
	return &Health{
		commands: commands,
		observer: newObserver(DefaultPolicy(), stats),
		timeout:  timeout,
		clock:    clock,
	}, nil
}

// Check pings Redis. It returns nil when Redis answered, and an error wrapping
// ErrUnavailable otherwise, so a caller can map the result onto 503 through
// errors.Is.
func (h *Health) Check(ctx context.Context) error {
	_, err := h.probe(ctx)
	return err
}

// Status returns a snapshot for an ops endpoint. A probe that cannot reach
// Redis still produces a status; only the caller's own cancellation is not
// recorded as a dependency failure.
func (h *Health) Status(ctx context.Context) HealthStatus {
	status, _ := h.probe(ctx)
	return status
}

func (h *Health) probe(ctx context.Context) (HealthStatus, error) {
	probeCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	startedAt := h.clock()
	err := h.commands.Ping(probeCtx)
	latency := h.clock().Sub(startedAt)

	status := HealthStatus{OK: err == nil, CheckedAt: startedAt, Latency: latency}
	if err == nil {
		return status, nil
	}

	// A caller cancellation means our own request ended, not that Redis failed.
	if ctxErr := ctx.Err(); ctxErr != nil {
		status.Error = ctxErr.Error()
		return status, ctxErr
	}

	if !isUnavailable(err) {
		err = fmt.Errorf("ping redis: %w: %w", ErrUnavailable, err)
	}
	status.Error = truncate(err.Error(), maxLastErrorLength)

	// A probe failure is a real dependency failure even though the probe does
	// not degrade, so it is recorded under FailClosed semantics.
	h.observer.stats.Record(CapabilityHealth, FailClosed, err)
	return status, err
}
