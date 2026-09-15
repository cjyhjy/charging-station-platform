package main

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

// metricsAddress returns the address the process serves its metrics on.
//
// The default is loopback on the port the ruling assigned to each process, and the address is
// validated by the observability package before it is bound: an empty or public bind is refused
// unless NCS_METRICS_ALLOW_PUBLIC_BIND says otherwise.
func metricsAddress(envName, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
		return value
	}
	return fallback
}

// metricsServer starts the process metrics endpoint and returns it for shutdown.
func metricsServer(cfg metricsConfig, registry *observability.Registry, logger *slog.Logger) (*observability.MetricsServer, error) {
	server, err := observability.StartMetricsServer(observability.MetricsServerConfig{
		Addr:            metricsAddress(cfg.envName, cfg.fallback),
		Registry:        registry,
		Logger:          logger,
		AllowPublicBind: truthy(os.Getenv(cfg.allowPublicEnv)),
	})
	if err != nil {
		return nil, err
	}
	return server, nil
}

type metricsConfig struct {
	envName        string
	allowPublicEnv string
	fallback       string
}

// The ruling assigned each process its own ops address, all on loopback: 9090 for the API, 9091 for
// the worker, 9092 for the publisher. They are defaults, not constants - a deployment can move them
// with the environment variable, and the address is validated before it is bound.
const (
	metricsAddrEnv        = "NCS_METRICS_ADDR"
	metricsAllowPublicEnv = "NCS_METRICS_ALLOW_PUBLIC_BIND"
	defaultMetricsAddr    = "127.0.0.1:9090"
)

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
