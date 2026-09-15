package observability

import (
	"context"
	"log/slog"
	"time"
)

// Dependency probing, shared by the three processes.
//
// The API already reported whether PostgreSQL and Redis answered; the worker and the publisher need
// the same answer, because a worker that cannot reach PostgreSQL is not doing its job even while it
// is connected to Redis, and a publisher that cannot reach either is not publishing. Rather than
// three copies of the probe, it lives here and each process decides what to do with the verdict: the
// API also flips its readiness, the other two publish the gauges.
//
// Every external call is a function field so a caller can drive any outcome - and any failure - in a
// test without a database.

// ProbeConfig carries the probe's dependencies, its sampling interval and what to do with the
// verdict.
type ProbeConfig struct {
	// PostgresUp reports whether PostgreSQL answered.
	PostgresUp func(context.Context) bool
	// RedisUp reports whether Redis answered.
	RedisUp func(context.Context) bool
	// SchemaVersion reads the applied migration version. Optional: it is a gauge, not a verdict.
	SchemaVersion func(context.Context) (int, error)
	// OutboxBacklog counts unpublished outbox rows. Optional, same reason.
	OutboxBacklog func(context.Context) (int64, error)
	// OutboxOldestAge reports how long the oldest unpublished row has waited, in seconds. Optional;
	// the approved alert table alerts on it, so a deployment that wants those rules needs it wired.
	OutboxOldestAge func(context.Context) (float64, error)
	// Registry receives the gauges and counters. Required.
	Registry *Registry
	// Logger receives one line per failed dependency. Required.
	Logger *slog.Logger
	// Interval is the sampling period. Zero samples once and returns, which is what a test wants.
	Interval time.Duration
	// Ready, when set, receives the combined verdict. The API uses it to drive /readyz; the worker
	// and the publisher have no readiness endpoint, so they pass nil.
	Ready func(bool)
}

// ProbeDependencies samples the dependencies and publishes the gauges until the context is
// cancelled.
//
// A failed gauge query publishes nothing: reporting the previous backlog while PostgreSQL is
// unreachable would be a fabricated number, and the series that says "the dependency is down" is
// already there to alert on.
func ProbeDependencies(ctx context.Context, cfg ProbeConfig) {
	if cfg.Registry == nil || cfg.Logger == nil {
		return
	}

	sample := func() {
		postgresUp := cfg.PostgresUp == nil || cfg.PostgresUp(ctx)
		cfg.Registry.SetGauge(MetricDependencyUp, map[string]string{"dependency": DependencyPostgres}, boolGauge(postgresUp))
		if postgresUp {
			if cfg.SchemaVersion != nil {
				if version, err := cfg.SchemaVersion(ctx); err != nil {
					cfg.Logger.Error("schema version probe failed", "error", err)
				} else {
					cfg.Registry.SetGauge(MetricMigrationsVersion, nil, float64(version))
				}
			}
			if cfg.OutboxBacklog != nil {
				if backlog, err := cfg.OutboxBacklog(ctx); err != nil {
					cfg.Logger.Error("outbox backlog probe failed", "error", err)
				} else {
					cfg.Registry.SetGauge(MetricOutboxUnpublished, nil, float64(backlog))
				}
			}
			if cfg.OutboxOldestAge != nil {
				if age, err := cfg.OutboxOldestAge(ctx); err != nil {
					cfg.Logger.Error("oldest outbox age probe failed", "error", err)
				} else {
					cfg.Registry.SetGauge(MetricOutboxOldestUnpublished, nil, age)
				}
			}
		} else {
			cfg.Registry.IncCounter(MetricProbeFailuresTotal, map[string]string{"dependency": DependencyPostgres}, 1)
			cfg.Logger.Error("postgres probe failed", "detail", "the process reports the dependency as down")
		}

		redisUp := cfg.RedisUp == nil || cfg.RedisUp(ctx)
		cfg.Registry.SetGauge(MetricDependencyUp, map[string]string{"dependency": DependencyRedis}, boolGauge(redisUp))
		if !redisUp {
			cfg.Registry.IncCounter(MetricProbeFailuresTotal, map[string]string{"dependency": DependencyRedis}, 1)
			cfg.Logger.Error("redis probe failed", "detail", "the process reports the dependency as down")
		}

		if cfg.Ready != nil {
			cfg.Ready(postgresUp && redisUp)
		}
	}

	sample()
	if cfg.Interval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample()
		}
	}
}

// SuccessClock records when a process last did its job successfully.
//
// It exists because liveness, connection state and "is it actually working" are three different
// questions: a worker can hold open connections, report both dependencies up and consume nothing at
// all. The timestamp is what a dashboard alerts on, and it is set from the process's own success
// path rather than sampled from a queue depth.
type SuccessClock struct {
	registry *Registry
	metric   string
	// now is a field so a test can pin the clock.
	now func() time.Time
}

// NewSuccessClock returns a clock writing to metric.
func NewSuccessClock(registry *Registry, metric string) *SuccessClock {
	return &SuccessClock{registry: registry, metric: metric, now: time.Now}
}

// SetNow replaces the clock the marker reads, so a test can pin the timestamp it asserts.
func (c *SuccessClock) SetNow(now func() time.Time) {
	if c == nil || now == nil {
		return
	}
	c.now = now
}

// Mark records the current time.
func (c *SuccessClock) Mark() {
	if c == nil || c.registry == nil || c.metric == "" {
		return
	}
	clock := c.now
	if clock == nil {
		clock = time.Now
	}
	c.registry.SetGauge(c.metric, nil, float64(clock().UTC().Unix()))
}

// MarkObserver wraps an observer so a successful delivery also moves the timestamp.
//
// The worker's runner reports every outcome through one observer; wrapping keeps the timestamp on
// the same path as the counters, so the series cannot drift from the events it summarizes.
func (c *SuccessClock) MarkObserver(inner Observer) Observer {
	if c == nil {
		return inner
	}
	return successClockObserver{inner: inner, clock: c}
}

type successClockObserver struct {
	inner Observer
	clock *SuccessClock
}

func (o successClockObserver) EventHandled(stream, eventType string, outcome Outcome, attempt int) {
	o.inner.EventHandled(stream, eventType, outcome, attempt)
	if outcome == OutcomeSucceeded {
		o.clock.Mark()
	}
}

func (o successClockObserver) EventDeadLettered(stream, eventType, reason string, attempt int) {
	o.inner.EventDeadLettered(stream, eventType, reason, attempt)
}

func (o successClockObserver) DeadLetterSuppressed(stream, eventType string) {
	o.inner.DeadLetterSuppressed(stream, eventType)
}

func (o successClockObserver) PendingRecovered(stream string, count int) {
	o.inner.PendingRecovered(stream, count)
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
