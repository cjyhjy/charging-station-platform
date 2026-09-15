package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/observability"
)

// The API's own observability has two jobs an operator depends on: the metrics endpoint must label
// traffic by route pattern (bounded) and never by path (unbounded), and readiness must follow the
// dependencies rather than a flag set once at startup. Both are tested here without a database.

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeRegistrar records the patterns a module registers, so the wrapper can be exercised the way
// the real server uses it.
type fakeRegistrar struct {
	registered map[string]http.HandlerFunc
}

func newFakeRegistrar() *fakeRegistrar {
	return &fakeRegistrar{registered: map[string]http.HandlerFunc{}}
}

func (f *fakeRegistrar) Register(pattern string, handler http.HandlerFunc) {
	f.registered[pattern] = handler
}

func TestInstrumentationLabelsByRoutePatternNotByPath(t *testing.T) {
	registry := observability.NewRegistry()
	inner := newFakeRegistrar()
	wrapped := registerWithMetrics{inner: inner, registry: registry}
	wrapped.Register("/api/v1/orders/{orderNo}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := inner.registered["/api/v1/orders/{orderNo}"]
	for _, orderNo := range []string{"ORD-1", "ORD-2", "ORD-3"} {
		recorder := httptest.NewRecorder()
		handler(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/orders/"+orderNo, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", recorder.Code)
		}
	}

	labels := map[string]string{"method": "GET", "route": "/api/v1/orders/{orderNo}", "status": "200"}
	count, ok := registry.Counter(observability.MetricRequestsTotal, labels)
	if !ok || count != 3 {
		t.Fatalf("requests total = %v (present %v), want 3 for the route pattern", count, ok)
	}
	// Three different order numbers must not create three series: the pattern is the label.
	for _, sample := range registry.Snapshot() {
		if sample.Name != observability.MetricRequestsTotal {
			continue
		}
		if strings.Contains(sample.Labels["route"], "ORD-") {
			t.Fatalf("a request path leaked into a metric label: %v", sample.Labels)
		}
	}
	// One counter for the route, one in-flight gauge, and one histogram family - not one series
	// per order number.
	if series := len(registry.Snapshot()); series != 2 {
		t.Fatalf("expected 2 scalar series for one route, got %d: %v", series, registry.Snapshot())
	}
	if series := len(registry.HistogramSnapshot()); series != 1 {
		t.Fatalf("expected one histogram series, got %d", series)
	}
}

func TestInstrumentationRecordsStatusAndLatency(t *testing.T) {
	registry := observability.NewRegistry()
	registry.RegisterHistogram(observability.MetricRequestDuration, observability.DefaultDurationBuckets)
	inner := newFakeRegistrar()
	wrapped := registerWithMetrics{inner: inner, registry: registry}

	wrapped.Register("/api/v1/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	wrapped.Register("/api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		// A handler that writes a body without WriteHeader has answered 200; the default matters
		// because otherwise every such route would be reported as status 0.
		_, _ = w.Write([]byte("ok"))
	})

	recorder := httptest.NewRecorder()
	inner.registered["/api/v1/orders"](recorder, httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil))
	if count, ok := registry.Counter(observability.MetricRequestsTotal, map[string]string{
		"method": "POST", "route": "/api/v1/orders", "status": "409",
	}); !ok || count != 1 {
		t.Fatalf("conflict response not recorded: %v (present %v)", count, ok)
	}

	recorder = httptest.NewRecorder()
	inner.registered["/api/v1/health"](recorder, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if count, ok := registry.Counter(observability.MetricRequestsTotal, map[string]string{
		"method": "GET", "route": "/api/v1/health", "status": "200",
	}); !ok || count != 1 {
		t.Fatalf("implicit 200 not recorded: %v (present %v)", count, ok)
	}

	histograms := registry.HistogramSnapshot()
	if len(histograms) != 2 {
		t.Fatalf("expected one histogram series per route, got %d", len(histograms))
	}
	for _, histogram := range histograms {
		if histogram.Count != 1 {
			t.Fatalf("histogram observations = %d, want 1", histogram.Count)
		}
		if histogram.Counts[len(histogram.Counts)-1] != 1 {
			t.Fatalf("the +Inf bucket must count every observation: %v", histogram.Counts)
		}
	}
	// In-flight returns to zero after the request finishes: a leaked in-flight gauge would show a
	// permanently busy process.
	if gauge, ok := registry.Gauge(observability.MetricRequestsInFlight, nil); !ok || gauge != 0 {
		t.Fatalf("in-flight gauge = %v (present %v), want 0", gauge, ok)
	}
}

func TestMetricsHandlerAnswersPrometheusTextAndRejectsOtherMethods(t *testing.T) {
	registry := observability.NewRegistry()
	registry.AddCounter(observability.MetricRequestsTotal, map[string]string{
		"method": "GET", "route": "/api/v1/orders", "status": "200",
	}, 5)
	handler := metricsHandler(registry, discardLogger())

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != observability.PrometheusContentType {
		t.Fatalf("content type = %q, want %q", got, observability.PrometheusContentType)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "# TYPE ncs_api_requests_total counter") || !strings.Contains(body, "ncs_api_requests_total") {
		t.Fatalf("body is not a Prometheus exposition:\n%s", body)
	}

	// A metrics endpoint that accepts writes is a needless surface; the API answers 405 in the
	// shared error envelope like every other route.
	recorder = httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
}

// Readiness must follow the dependencies. This drives every combination, including the one that
// matters most: PostgreSQL up and Redis down still means not ready, because sessions and locks fail
// closed and the process would only answer failures.
func TestDependencyProbeDrivesReadiness(t *testing.T) {
	cases := []struct {
		name         string
		postgresUp   bool
		redisUp      bool
		wantReady    bool
		wantFailures map[string]float64
	}{
		{name: "both up", postgresUp: true, redisUp: true, wantReady: true},
		{name: "postgres down", postgresUp: false, redisUp: true, wantReady: false, wantFailures: map[string]float64{"postgres": 1}},
		{name: "redis down", postgresUp: true, redisUp: false, wantReady: false, wantFailures: map[string]float64{"redis": 1}},
		{name: "both down", postgresUp: false, redisUp: false, wantReady: false, wantFailures: map[string]float64{"postgres": 1, "redis": 1}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := observability.NewRegistry()
			var ready bool
			var readyCalls int
			probeDependencies(context.Background(), probeConfig{
				postgresUp:    func(context.Context) bool { return testCase.postgresUp },
				redisUp:       func(context.Context) bool { return testCase.redisUp },
				schemaVersion: func(context.Context) (int, error) { return 7, nil },
				outboxBacklog: func(context.Context) (int64, error) { return 12, nil },
				registry:      registry,
				logger:        discardLogger(),
				interval:      0,
				setReady:      func(value bool) { ready = value; readyCalls++ },
			})

			if ready != testCase.wantReady {
				t.Fatalf("ready = %v, want %v", ready, testCase.wantReady)
			}
			if readyCalls != 1 {
				t.Fatalf("readiness set %d times, want once per probe", readyCalls)
			}
			postgresGauge, _ := registry.Gauge(observability.MetricDependencyUp, map[string]string{"dependency": "postgres"})
			if want := boolGauge(testCase.postgresUp); postgresGauge != want {
				t.Fatalf("postgres up gauge = %v, want %v", postgresGauge, want)
			}
			redisGauge, _ := registry.Gauge(observability.MetricDependencyUp, map[string]string{"dependency": "redis"})
			if want := boolGauge(testCase.redisUp); redisGauge != want {
				t.Fatalf("redis up gauge = %v, want %v", redisGauge, want)
			}
			for dependency, want := range testCase.wantFailures {
				got, ok := registry.Counter(observability.MetricProbeFailuresTotal, map[string]string{"dependency": dependency})
				if !ok || got != want {
					t.Fatalf("probe failures for %s = %v (present %v), want %v", dependency, got, ok, want)
				}
			}
			// The gauges that only make sense with a live database are set when it is up and left
			// alone when it is not: reporting a stale backlog while PostgreSQL is unreachable would
			// be a fabricated number.
			backlog, backlogPresent := registry.Gauge(observability.MetricOutboxUnpublished, nil)
			version, versionPresent := registry.Gauge(observability.MetricMigrationsVersion, nil)
			if testCase.postgresUp {
				if !backlogPresent || backlog != 12 || !versionPresent || version != 7 {
					t.Fatalf("backlog = %v (present %v), version = %v (present %v), want 12 and 7", backlog, backlogPresent, version, versionPresent)
				}
			} else if backlogPresent || versionPresent {
				t.Fatalf("a probe failure must not publish a backlog or a version: %v / %v", backlog, version)
			}
		})
	}
}

// A probe that raises a broken query must not take the process down: the failure is logged and the
// dependency gauges stay honest.
func TestDependencyProbeSurvivesATemplateFailure(t *testing.T) {
	registry := observability.NewRegistry()
	var ready bool
	probeDependencies(context.Background(), probeConfig{
		postgresUp:    func(context.Context) bool { return true },
		redisUp:       func(context.Context) bool { return true },
		schemaVersion: func(context.Context) (int, error) { return 0, errors.New("relation does not exist") },
		outboxBacklog: func(context.Context) (int64, error) { return 0, errors.New("connection reset") },
		registry:      registry,
		logger:        discardLogger(),
		interval:      0,
		setReady:      func(value bool) { ready = value },
	})
	if !ready {
		t.Fatal("a failing gauge query must not make the process unready: the database answered")
	}
	if _, present := registry.Gauge(observability.MetricOutboxUnpublished, nil); present {
		t.Fatal("a failed backlog query must not publish a value")
	}
}

// The probe runs until its context is cancelled, which is what makes shutdown deterministic.
func TestDependencyProbeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	samples := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		probeDependencies(ctx, probeConfig{
			postgresUp:    func(context.Context) bool { return true },
			redisUp:       func(context.Context) bool { return true },
			schemaVersion: func(context.Context) (int, error) { return 7, nil },
			outboxBacklog: func(context.Context) (int64, error) { return 0, nil },
			registry:      observability.NewRegistry(),
			logger:        discardLogger(),
			interval:      5 * time.Millisecond,
			setReady: func(bool) {
				mu.Lock()
				samples++
				mu.Unlock()
			},
		})
	}()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		enough := samples >= 2
		mu.Unlock()
		if enough {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the probe did not sample repeatedly")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the probe did not stop when its context was cancelled")
	}
}

func TestMigrateOnlySwitch(t *testing.T) {
	t.Setenv(migrateOnlyEnv, "")
	original := os.Args
	defer func() { os.Args = original }()

	os.Args = []string{"ncs-api"}
	if migrateOnlyRequested() {
		t.Fatal("no switch means the API serves traffic")
	}
	os.Args = []string{"ncs-api", "-migrate-only"}
	if !migrateOnlyRequested() {
		t.Fatal("the flag is not recognised")
	}
	os.Args = []string{"ncs-api"}
	t.Setenv(migrateOnlyEnv, "true")
	if !migrateOnlyRequested() {
		t.Fatal("the environment variable is not recognised")
	}
	t.Setenv(migrateOnlyEnv, "no")
	if migrateOnlyRequested() {
		t.Fatal("a value that is not a truthy string must not enable it")
	}
}
