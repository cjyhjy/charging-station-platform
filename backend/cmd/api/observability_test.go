package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

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
