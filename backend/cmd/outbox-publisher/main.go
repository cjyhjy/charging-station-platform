// Command outbox-publisher moves PostgreSQL outbox rows onto Redis Streams.
//
// It is a separate process rather than a loop inside the worker (BE-I-01 decision 2), because
// the two scale differently: the publisher is a single writer by construction, while workers
// scale out with the backlog. Keeping them apart also means a worker restart does not stop
// publishing.
//
// The publish order is fixed: append to the stream, then mark the row published. A crash in
// between publishes the same event again, which the consumer removes by event id; marking
// first would lose the event instead.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/config"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	"github.com/heguangV/charging-station-platform/backend/internal/repository/postgres"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
	"github.com/heguangV/charging-station-platform/backend/internal/worker"
	"github.com/heguangV/charging-station-platform/backend/migrations"
)

// defaultInterval is how long the loop waits between passes when the outbox is empty. A short
// interval keeps the end-to-end latency low; the cost of an empty pass is one indexed query.
const defaultInterval = 500 * time.Millisecond

// defaultBatch bounds how many rows one pass publishes, so a large backlog cannot turn one
// pass into an unbounded amount of work.
const defaultBatch = 100

// publisherLock is a lock this process holds.
//
// Held() is part of the contract rather than an implementation detail: a lock that was acquired
// once is not a lock that is still held, and the difference decides whether this process may keep
// publishing. Alive() answers the same question for the session behind it.
type publisherLock interface {
	// Alive reports whether the lock is still held right now.
	Alive(ctx context.Context) bool
	// Release gives the lock back.
	Release(ctx context.Context) error
}

// lockAcquirer is the single-active guarantee, narrowed so the loop can be tested without a
// database. A nil lock with no error means another publisher owns it.
type lockAcquirer interface {
	Acquire(ctx context.Context) (publisherLock, error)
}

// loopDeps is everything one loop needs, so the process wiring and the loop itself stay
// separate concerns.
type loopDeps struct {
	source   worker.OutboxSource
	writer   worker.StreamWriter
	locker   lockAcquirer
	logger   *slog.Logger
	interval time.Duration
	batch    int
	// observer receives the facts only the publishing loop knows: whether this process holds the
	// lock, whether it lost it, and when it last managed to publish. It is optional so the loop
	// stays usable without a metrics endpoint (tests, one-off runs).
	observer loopObserver
}

// loopObserver is the loop's view of the process metrics.
type loopObserver interface {
	LockAcquired()
	LockLost()
	LocksHeld(bool)
	Standby()
	Published(int)
	PassFailed()
}

// registryObserver writes the loop's facts into the process registry.
type registryObserver struct {
	registry *observability.Registry
	clock    *observability.SuccessClock
}

func (o *registryObserver) LockAcquired() {
	o.registry.SetGauge(observability.MetricPublisherStandby, nil, 0)
}

func (o *registryObserver) LockLost() {
	o.registry.IncCounter(observability.MetricPublisherLockLossesTotal, nil, 1)
}

func (o *registryObserver) LocksHeld(held bool) {
	if !held {
		o.registry.SetGauge(observability.MetricPublisherStandby, nil, 1)
	}
}

func (o *registryObserver) Standby() {
	o.registry.SetGauge(observability.MetricPublisherStandby, nil, 1)
}

func (o *registryObserver) Published(count int) {
	o.registry.IncCounter(observability.MetricPublisherPublishedTotal, nil, float64(count))
	o.clock.Mark()
}

func (o *registryObserver) PassFailed() {
	o.registry.IncCounter(observability.MetricPublisherPassFailuresTotal, nil, 1)
}

func main() {
	var (
		intervalFlag = flag.Duration("interval", durationOr(os.Getenv("NCS_OUTBOX_INTERVAL"), defaultInterval), "pause between publishing passes")
		batchFlag    = flag.Int("batch", intOr(os.Getenv("NCS_OUTBOX_BATCH"), defaultBatch), "maximum rows published per pass")
	)
	flag.Parse()

	logger := slog.New(observability.NewContextHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	settings, err := redisrepo.SettingsFromEnv(nil)
	if err != nil {
		logger.Error("load redis configuration", "error", err)
		os.Exit(1)
	}
	appConfig, err := config.Load()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}
	if strings.TrimSpace(appConfig.PostgresDSN) == "" {
		logger.Error("NCS_POSTGRES_DSN is required: the publisher reads the outbox from PostgreSQL")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStartup()

	db, err := postgres.Open(startupCtx, appConfig.PostgresDSN, 4)
	if err != nil {
		logger.Error("connect to postgres", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// Migration gate: this process writes tables a migration introduces, and it does not run
	// migrations itself. Starting against an older schema would fail later, in the middle of a
	// device flow, instead of here where the fix is one command.
	expectedSchema, err := postgres.HighestMigrationVersion(migrations.FS)
	if err != nil {
		logger.Error("read migration set", "error", err)
		os.Exit(1)
	}
	if err := postgres.AssertSchemaVersion(ctx, db, expectedSchema); err != nil {
		logger.Error("schema is out of date; run the migration gate first", "error", err,
			"expected_schema_version", expectedSchema)
		os.Exit(1)
	}
	if version, err := postgres.SchemaVersion(ctx, db); err == nil {
		logger.Info("schema version verified", "schema_version", version, "expected", expectedSchema)
	}

	source, err := postgres.NewOutboxSource(db)
	if err != nil {
		logger.Error("create outbox source", "error", err)
		os.Exit(1)
	}

	capabilities, err := redisrepo.NewCapabilities(settings)
	if err != nil {
		logger.Error("create redis foundation", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := capabilities.Close(); err != nil {
			logger.Error("close redis foundation", "error", err)
		}
	}()

	// The publisher's own facts - lock held or lost, standby, last successful publish - are invisible
	// from the API, so this process serves its own endpoint. Loopback by default; a public bind has to
	// be requested explicitly and is refused otherwise (B-06 ruling on ops endpoints).
	registry := observability.NewRegistry()
	successClock := observability.NewSuccessClock(registry, observability.MetricPublisherLastPublish)
	// Fixed series are created at startup so a scrape always finds the metrics this process promises.
	observability.RegisterProcessMetrics(registry, observability.ProcessMetricsConfig{Publisher: true})
	metrics, err := observability.StartMetricsServer(observability.MetricsServerConfig{
		Addr:            envOr(metricsAddrEnv, defaultMetricsAddr),
		Registry:        registry,
		Logger:          logger,
		AllowPublicBind: truthyEnv(metricsAllowPublicEnv),
	})
	if err != nil {
		logger.Error("start metrics endpoint", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metrics.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown metrics endpoint", "error", err)
		}
	}()

	// Dependency and backlog sampling: a publisher that cannot reach PostgreSQL cannot publish, and
	// the backlog gauge is the number an operator watches while that is true.
	probeCtx, stopProbe := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		observability.ProbeDependencies(probeCtx, observability.ProbeConfig{
			PostgresUp:      func(ctx context.Context) bool { return db.PingContext(ctx) == nil },
			RedisUp:         func(ctx context.Context) bool { return capabilities.Ready(ctx) == nil },
			SchemaVersion:   func(ctx context.Context) (int, error) { return postgres.SchemaVersion(ctx, db) },
			OutboxBacklog:   func(ctx context.Context) (int64, error) { return postgres.OutboxBacklog(ctx, db) },
			OutboxOldestAge: func(ctx context.Context) (float64, error) { return postgres.OldestUnpublishedOutboxAge(ctx, db) },
			Registry:        registry,
			Logger:          logger,
			Interval:        dependencyProbeInterval,
		})
	}()
	defer func() {
		stopProbe()
		<-probeDone
	}()

	logger.Info("outbox publisher starting",
		"interval", intervalFlag.String(),
		"batch", *batchFlag,
		"redis", settings.Conn.String(),
	)

	deps := loopDeps{
		observer: &registryObserver{registry: registry, clock: successClock},
		source:   source,
		writer:   capabilities.Streams,
		locker:   advisoryLocker{db: db},
		logger:   logger,
		interval: *intervalFlag,
		batch:    *batchFlag,
	}
	if err := runLoop(ctx, deps); err != nil {
		logger.Error("outbox publisher stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("outbox publisher stopped")
}

// runLoop publishes until the context is cancelled.
//
// It holds the advisory lock for as long as it publishes, and stands by while another publisher
// holds it. Standing by rather than exiting matters for a supervisor that does not restart on a
// clean exit: a standby must be able to take over the moment the active publisher stops, and it
// must never publish while it does not hold the lock.
func runLoop(ctx context.Context, deps loopDeps) error {
	if deps.source == nil || deps.writer == nil || deps.locker == nil {
		return errors.New("outbox publisher: source, writer and lock are required")
	}
	if deps.interval <= 0 {
		deps.interval = defaultInterval
	}
	if deps.batch <= 0 {
		deps.batch = defaultBatch
	}
	if deps.logger == nil {
		deps.logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	publisher, err := worker.NewPublisher(deps.source, deps.writer, worker.PublisherConfig{BatchSize: deps.batch})
	if err != nil {
		return err
	}

	ticker := time.NewTicker(deps.interval)
	defer ticker.Stop()

	var (
		lock      publisherLock
		standby   = true
		published int
		passes    int
	)
	defer func() {
		if lock != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := lock.Release(shutdownCtx); err != nil {
				deps.logger.Error("release publisher lock", "error", err)
			}
		}
	}()

	for {
		// A lock that was acquired earlier is not necessarily a lock this process still holds:
		// the dedicated connection can drop, at which point the server releases the session lock
		// and another publisher may take it. Publishing then would break the single-active
		// guarantee this module is approved on, so the liveness of the lock is checked before
		// every pass, and losing it stops publication until it can be acquired again.
		if lock != nil && !lock.Alive(ctx) {
			if deps.observer != nil {
				deps.observer.LockLost()
			}
			deps.logger.Error("publisher lock is no longer held; stopping publication until it can be re-acquired",
				"reason", "the lock session ended, so another publisher may now be active")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := lock.Release(shutdownCtx)
			cancel()
			if err != nil {
				deps.logger.Error("release lost publisher lock", "error", err)
			}
			lock = nil
			standby = true
		}

		if lock == nil {
			acquired, err := deps.locker.Acquire(ctx)
			if err != nil {
				return fmt.Errorf("acquire publisher lock: %w", err)
			}
			if acquired == nil {
				if deps.observer != nil {
					deps.observer.Standby()
				}
				if standby {
					deps.logger.Warn("another outbox publisher holds the lock; standing by without publishing")
					standby = false
				}
			} else {
				lock = acquired
				standby = true
				if deps.observer != nil {
					deps.observer.LockAcquired()
				}
				deps.logger.Info("outbox publisher lock acquired; this process is publishing")
			}
		}

		if lock != nil {
			result, err := publisher.PublishOnce(ctx)
			passes++
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// A failed pass is not fatal: the row stays unpublished and the next pass
				// retries it. The counter and the log keep a persistent failure visible.
				if deps.observer != nil {
					deps.observer.PassFailed()
				}
				deps.logger.Error("publish pass failed", "error", err, "published", result.Published)
			} else if result.Published > 0 {
				published += result.Published
				if deps.observer != nil {
					deps.observer.Published(result.Published)
				}
				deps.logger.Info("published outbox rows", "pass_published", result.Published, "total_published", published)
			}
		}

		select {
		case <-ctx.Done():
			deps.logger.Info("outbox publisher stopping", "passes", passes, "published", published)
			return nil
		case <-ticker.C:
		}
	}
}

// advisoryLocker adapts the PostgreSQL advisory lock to the narrow interface the loop needs.
//
// A nil lock with no error means another publisher owns it, which is a normal situation rather
// than a failure.
type advisoryLocker struct {
	db *sql.DB
}

func (l advisoryLocker) Acquire(ctx context.Context) (publisherLock, error) {
	lock, err := postgres.AcquireOutboxPublisherLock(ctx, l.db)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, nil
	}
	return advisoryLockHandle{lock: lock}, nil
}

// advisoryLockHandle exposes the PostgreSQL lock through the loop's interface.
type advisoryLockHandle struct {
	lock *postgres.OutboxPublisherLock
}

func (h advisoryLockHandle) Alive(ctx context.Context) bool { return h.lock.Alive(ctx) }

func (h advisoryLockHandle) Release(ctx context.Context) error { return h.lock.Release(ctx) }

// The ops endpoint defaults (B-06 ruling): API 9090, worker 9091, publisher 9092, all loopback,
// all overridable through the environment, none hardcoded to a public interface.
const (
	metricsAddrEnv          = "NCS_METRICS_ADDR"
	metricsAllowPublicEnv   = "NCS_METRICS_ALLOW_PUBLIC_BIND"
	defaultMetricsAddr      = "127.0.0.1:9092"
	dependencyProbeInterval = 5 * time.Second
)

func truthyEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// envOr reads a trimmed environment variable, falling back when it is unset or blank.
func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationOr(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func intOr(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
