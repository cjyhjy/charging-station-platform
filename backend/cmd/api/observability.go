package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/httpapi"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
)

// Process-level observability for the API: what it serves, how slow it is, and whether the things
// it depends on are answering.
//
// This wiring lives in cmd/api rather than in the API package because it is infrastructure, not
// domain: the ruling on the two-track route puts cmd/api and internal/observability on the B line,
// and the business handlers must not have to know that a metrics registry exists. Nothing here
// changes a route, a status code or a response body - it observes them.

// dependencyProbeInterval is how often the API re-checks PostgreSQL and Redis.
//
// Readiness is driven by this probe instead of by a fixed "ready" flag set at startup, because a
// process that keeps accepting sessions while its Redis is gone answers with 500s that a load
// balancer would happily keep sending to it.
const dependencyProbeInterval = 5 * time.Second

// registerWithMetrics wraps the module registrars so every registered route is instrumented.
//
// The route *pattern* is the label, not the request path: a path label would create one series per
// order number and grow without bound, which is the classic way a metrics endpoint becomes an
// outage of its own.
type registerWithMetrics struct {
	inner    registrar
	registry *observability.Registry
}

// registrar is the registration surface the module handlers expect.
type registrar interface {
	Register(pattern string, handler http.HandlerFunc)
}

func (r registerWithMetrics) Register(pattern string, handler http.HandlerFunc) {
	r.inner.Register(pattern, r.observe(pattern, handler))
}

func (r registerWithMetrics) observe(pattern string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		started := time.Now()
		r.registry.IncGauge(observability.MetricRequestsInFlight, nil, 1)
		recorder := &metricsRecorder{ResponseWriter: w, status: http.StatusOK}
		next(recorder, request)
		r.registry.IncGauge(observability.MetricRequestsInFlight, nil, -1)

		labels := map[string]string{
			"method": request.Method,
			"route":  pattern,
			"status": strconv.Itoa(recorder.status),
		}
		r.registry.IncCounter(observability.MetricRequestsTotal, labels, 1)
		r.registry.ObserveHistogram(observability.MetricRequestDuration, labels, time.Since(started).Seconds())
	}
}

// metricsRecorder remembers the status a handler wrote, defaulting to 200 because a handler that
// writes a body without an explicit WriteHeader has answered 200.
type metricsRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *metricsRecorder) WriteHeader(status int) {
	if r.wrote {
		// A second WriteHeader on the same response is a no-op at the transport level; passing it
		// through would only make the server log a superfluous-call warning.
		return
	}
	r.status = status
	r.wrote = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *metricsRecorder) Write(body []byte) (int, error) {
	if !r.wrote {
		r.wrote = true
	}
	return r.ResponseWriter.Write(body)
}

// metricsHandler serves the registry in the Prometheus text format.
//
// The endpoint is deliberately not registered in OpenAPI: it is an internal operational endpoint
// for Nginx and the monitoring system, and the ruling keeps it out of the client contract. Nginx
// restricts it to the ops range, and the API binds to the internal interface, so it is not
// reachable from the internet even before that.
func metricsHandler(registry *observability.Registry, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			httpapi.WriteError(w, request, http.StatusMethodNotAllowed, httpapi.CodeMethodNotAllowed, "method not allowed", nil)
			return
		}
		w.Header().Set("Content-Type", observability.PrometheusContentType)
		if _, err := registry.WritePrometheus(w); err != nil {
			// The body may already be partially written; log and stop rather than write an error
			// envelope into the middle of an exposition body.
			logger.Error("metrics exposition failed", "error", err)
		}
	}
}

// probeDependencies samples PostgreSQL and Redis and drives readiness from the result.
//
// Refusing to serve while a dependency is down is the frozen policy for this platform (sessions and
// locks fail closed, and PostgreSQL is the source of truth), so the probe turns that policy into
// something the deployment can act on: /readyz reports 503, and the load balancer stops sending
// traffic to a process that would only fail it.
//
// Every external call is a function field rather than a connection, so the probe's behaviour -
// including what it does when a dependency fails - is testable without a database.
func probeDependencies(ctx context.Context, cfg probeConfig) {
	probe := func() {
		postgresUp := cfg.postgresUp(ctx)
		cfg.registry.SetGauge(observability.MetricDependencyUp, map[string]string{"dependency": observability.DependencyPostgres}, boolGauge(postgresUp))
		if postgresUp {
			if version, err := cfg.schemaVersion(ctx); err != nil {
				cfg.logger.Error("schema version probe failed", "error", err)
			} else {
				cfg.registry.SetGauge(observability.MetricMigrationsVersion, nil, float64(version))
			}
			if backlog, err := cfg.outboxBacklog(ctx); err != nil {
				cfg.logger.Error("outbox backlog probe failed", "error", err)
			} else {
				cfg.registry.SetGauge(observability.MetricOutboxUnpublished, nil, float64(backlog))
			}
		} else {
			cfg.registry.IncCounter(observability.MetricProbeFailuresTotal, map[string]string{"dependency": observability.DependencyPostgres}, 1)
			cfg.logger.Error("postgres probe failed; the API reports not ready")
		}

		redisUp := cfg.redisUp(ctx)
		cfg.registry.SetGauge(observability.MetricDependencyUp, map[string]string{"dependency": observability.DependencyRedis}, boolGauge(redisUp))
		if !redisUp {
			cfg.registry.IncCounter(observability.MetricProbeFailuresTotal, map[string]string{"dependency": observability.DependencyRedis}, 1)
			cfg.logger.Error("redis probe failed; the API reports not ready")
		}

		cfg.setReady(postgresUp && redisUp)
	}

	probe()
	if cfg.interval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe()
		}
	}
}

// probeConfig carries the probe's dependencies and the interval between samples. The functions are
// fields so a test can drive every outcome without a PostgreSQL or a Redis.
type probeConfig struct {
	postgresUp    func(context.Context) bool
	redisUp       func(context.Context) bool
	schemaVersion func(context.Context) (int, error)
	outboxBacklog func(context.Context) (int64, error)
	registry      *observability.Registry
	logger        *slog.Logger
	interval      time.Duration
	setReady      func(bool)
}

// redisPinger is the part of the Redis client the probe needs.
func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// migrationGateEnv documents the flag and environment variable that apply migrations and exit.
//
// A deploy needs a step that brings the schema up to date before the worker and the publisher
// start; without it the only way to migrate is to start the whole API, which also binds a port and
// starts background sweeps that are pointless in a migration step.
const (
	migrateOnlyEnv = "NCS_MIGRATE_ONLY"
)

// migrateOnlyRequested reads the migration-only switch from the environment or the command line.
func migrateOnlyRequested() bool {
	if truthy(os.Getenv(migrateOnlyEnv)) {
		return true
	}
	for _, arg := range os.Args[1:] {
		if arg == "-migrate-only" || arg == "--migrate-only" {
			return true
		}
	}
	return false
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
