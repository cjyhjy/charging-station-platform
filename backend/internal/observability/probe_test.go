package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The metrics endpoint is internal, so its two properties are that it binds somewhere private and
// that it reports the process's real numbers. Both are checked here, including the bind rule, which
// is a rule in code rather than a paragraph in a README because the consequence of getting it wrong
// is an internal endpoint reachable from the internet.

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestValidateMetricsAddressRefusesAnythingPublic(t *testing.T) {
	allowed := []string{"127.0.0.1:9091", "127.0.0.5:9091", "localhost:9091", "[::1]:9091", "10.20.0.7:9091", "192.168.1.9:9091", "172.16.5.4:9091"}
	for _, addr := range allowed {
		if err := ValidateMetricsAddress(addr, false); err != nil {
			t.Fatalf("ValidateMetricsAddress(%q) = %v, want nil", addr, err)
		}
	}

	refused := []string{"", ":9091", "0.0.0.0:9091", "[::]:9091", "8.8.8.8:9091", "metrics.example.com:9091", "127.0.0.1"}
	for _, addr := range refused {
		err := ValidateMetricsAddress(addr, false)
		if err == nil {
			t.Fatalf("ValidateMetricsAddress(%q) = nil, want an error", addr)
		}
		if addr != "127.0.0.1" && !errors.Is(err, ErrPublicMetricsBind) {
			t.Fatalf("ValidateMetricsAddress(%q) error = %v, want ErrPublicMetricsBind", addr, err)
		}
	}

	// An explicit opt-in is the only way to bind publicly, and it has to be explicit: the default is
	// the safe one because metrics describe the deployment's internals.
	if err := ValidateMetricsAddress("0.0.0.0:9091", true); err != nil {
		t.Fatalf("the explicit opt-in must allow a public bind, got %v", err)
	}
	// Even with the opt-in, a malformed address stays malformed.
	if err := ValidateMetricsAddress("not-an-address", true); err == nil {
		t.Fatal("a malformed address must be refused even with the opt-in")
	}
}

func TestMetricsHandlerServesTheExpositionAndRejectsWrites(t *testing.T) {
	registry := NewRegistry()
	registry.IncCounter(MetricEventsTotal, map[string]string{"outcome": "succeeded"}, 3)
	handler := MetricsHandler(registry, quietLogger())

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != PrometheusContentType {
		t.Fatalf("content type = %q, want %q", got, PrometheusContentType)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "ncs_worker_events_total{outcome=\"succeeded\"} 3") {
		t.Fatalf("body does not carry the series:\n%s", body)
	}

	recorder = httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", recorder.Code)
	}
	if recorder.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("Allow header = %q", recorder.Header().Get("Allow"))
	}
}

// A running endpoint is what the worker and the publisher actually need, so the server is started
// and scraped once rather than only unit tested.
func TestStartMetricsServerServesAndStops(t *testing.T) {
	registry := NewRegistry()
	registry.SetGauge(MetricPublisherStandby, nil, 1)
	// Port 0 lets the kernel pick a free port, which keeps the test from fighting the default 9091.
	server, err := StartMetricsServer(MetricsServerConfig{Addr: "127.0.0.1:0", Registry: registry, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("StartMetricsServer() error = %v", err)
	}
	address := server.Addr()
	if !strings.HasPrefix(address, "127.0.0.1:") {
		t.Fatalf("bound address = %q, want loopback", address)
	}

	response, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "ncs_publisher_standby 1") {
		t.Fatalf("unexpected scrape result %d:\n%s", response.StatusCode, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if _, err := http.Get("http://" + address + "/metrics"); err == nil {
		t.Fatal("the endpoint still answers after shutdown")
	}

	// A public bind is refused before anything is opened, so a deployment cannot accidentally expose
	// the process's internals.
	if _, err := StartMetricsServer(MetricsServerConfig{Addr: "0.0.0.0:0", Registry: registry, Logger: quietLogger()}); !errors.Is(err, ErrPublicMetricsBind) {
		t.Fatalf("StartMetricsServer(0.0.0.0) error = %v, want ErrPublicMetricsBind", err)
	}
	if _, err := StartMetricsServer(MetricsServerConfig{Addr: "127.0.0.1:0"}); err == nil {
		t.Fatal("a metrics server without a registry must be refused")
	}
}

// The success clock answers "is it still working", which connection state cannot: a worker can hold
// open connections and consume nothing at all.
func TestSuccessClockMarksSuccessfulDeliveries(t *testing.T) {
	registry := NewRegistry()
	clock := NewSuccessClock(registry, MetricWorkerLastSuccess)
	pinned := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	clock.now = func() time.Time { return pinned }

	observer := clock.MarkObserver(NoopObserver())
	observer.EventHandled("ncs:stream:charge-event", "CHARGE_START_REQUESTED", OutcomeRetried, 1)
	if value, present := registry.Gauge(MetricWorkerLastSuccess, nil); present {
		t.Fatalf("a retry must not move the success timestamp, got %v", value)
	}
	observer.EventHandled("ncs:stream:charge-event", "CHARGE_START_REQUESTED", OutcomeSucceeded, 1)
	value, present := registry.Gauge(MetricWorkerLastSuccess, nil)
	if !present || value != float64(pinned.Unix()) {
		t.Fatalf("success timestamp = %v (present %v), want %d", value, present, pinned.Unix())
	}
	// Other observer callbacks must still reach the wrapped observer.
	counting := &countingObserver{}
	wrapped := clock.MarkObserver(counting)
	wrapped.EventDeadLettered("ncs:stream:charge-event", "ORDER_CREATED", "retry exhausted", 3)
	wrapped.PendingRecovered("ncs:stream:charge-event", 2)
	wrapped.DeadLetterSuppressed("ncs:stream:charge-event", "ORDER_CREATED")
	if counting.deadLettered != 1 || counting.recovered != 2 || counting.suppressed != 1 {
		t.Fatalf("the wrapper swallowed observer calls: %+v", counting)
	}
	// A nil clock is a safe no-op, so a caller can wire it unconditionally.
	var missing *SuccessClock
	missing.Mark()
}

type countingObserver struct {
	deadLettered int
	recovered    int
	suppressed   int
}

func (c *countingObserver) EventHandled(string, string, Outcome, int) {}

func (c *countingObserver) EventDeadLettered(string, string, string, int) { c.deadLettered++ }

func (c *countingObserver) DeadLetterSuppressed(string, string) { c.suppressed++ }

func (c *countingObserver) PendingRecovered(_ string, count int) { c.recovered += count }

// The probe is shared by all three processes, so its failure behaviour is pinned once: a failing
// gauge query publishes nothing, a down dependency publishes 0 and counts once, and a caller that
// passes no readiness callback still gets the gauges.
func TestProbePublishesGaugesAndSurvivesFailures(t *testing.T) {
	registry := NewRegistry()
	ProbeDependencies(context.Background(), ProbeConfig{
		PostgresUp:    func(context.Context) bool { return true },
		RedisUp:       func(context.Context) bool { return false },
		SchemaVersion: func(context.Context) (int, error) { return 0, errors.New("relation does not exist") },
		OutboxBacklog: func(context.Context) (int64, error) { return 0, errors.New("connection reset") },
		Registry:      registry,
		Logger:        quietLogger(),
	})

	if _, present := registry.Gauge(MetricMigrationsVersion, nil); present {
		t.Fatal("a failed schema query must not publish a version")
	}
	if _, present := registry.Gauge(MetricOutboxUnpublished, nil); present {
		t.Fatal("a failed backlog query must not publish a value")
	}
	if value, _ := registry.Gauge(MetricDependencyUp, map[string]string{"dependency": DependencyPostgres}); value != 1 {
		t.Fatalf("postgres gauge = %v, want 1", value)
	}
	if value, _ := registry.Gauge(MetricDependencyUp, map[string]string{"dependency": DependencyRedis}); value != 0 {
		t.Fatalf("redis gauge = %v, want 0", value)
	}
	if count, _ := registry.Counter(MetricProbeFailuresTotal, map[string]string{"dependency": DependencyRedis}); count != 1 {
		t.Fatalf("redis failure counter = %v, want 1", count)
	}
	// The publisher and the worker have no readiness endpoint; passing no callback must be fine.
	ProbeDependencies(context.Background(), ProbeConfig{
		PostgresUp: func(context.Context) bool { return true },
		RedisUp:    func(context.Context) bool { return true },
		Registry:   registry,
		Logger:     quietLogger(),
	})
	if value, _ := registry.Gauge(MetricDependencyUp, map[string]string{"dependency": DependencyRedis}); value != 1 {
		t.Fatalf("redis gauge = %v, want 1 after recovery", value)
	}
}

// A monitoring team reads a missing series as "this build does not export it", so the fixed metrics
// the ruling requires are present from startup. Series whose labels come from traffic are not
// invented, because a fabricated label value can never move.
func TestRegisterProcessMetricsPreCreatesTheRequiredSeries(t *testing.T) {
	registry := NewRegistry()
	RegisterProcessMetrics(registry, ProcessMetricsConfig{
		Worker:    true,
		Publisher: true,
		Streams:   []string{"ncs:stream:order-event"},
	})

	required := []struct {
		name   string
		labels map[string]string
	}{
		{MetricDependencyUp, map[string]string{"dependency": DependencyPostgres}},
		{MetricDependencyUp, map[string]string{"dependency": DependencyRedis}},
		{MetricOutboxUnpublished, nil},
		{MetricMigrationsVersion, nil},
		{MetricStreamPending, map[string]string{"stream": "ncs:stream:order-event"}},
		{MetricStreamLag, map[string]string{"stream": "ncs:stream:order-event"}},
		{MetricDeadLetterLength, nil},
		{MetricWorkerLastSuccess, nil},
		{MetricPublisherLastPublish, nil},
		{MetricPublisherStandby, nil},
	}
	for _, want := range required {
		if _, present := registry.Gauge(want.name, want.labels); !present {
			t.Errorf("gauge %s%v is missing from the initial exposition", want.name, want.labels)
		}
	}
	for _, name := range []string{MetricPublisherPublishedTotal, MetricPublisherPassFailuresTotal, MetricPublisherLockLossesTotal} {
		if _, present := registry.Counter(name, nil); !present {
			t.Errorf("counter %s is missing from the initial exposition", name)
		}
	}
	// Traffic-labelled families must NOT be pre-created: a fabricated label set would create a series
	// that can never move and would sit beside the real one as a permanent zero.
	for _, name := range []string{MetricEventsTotal, MetricRetriesTotal, MetricDeadLetteredTotal} {
		if registry.Len() > 0 {
			for _, sample := range registry.Snapshot() {
				if sample.Name == name {
					t.Errorf("%s must not be pre-created: its labels come from traffic (%v)", name, sample.Labels)
				}
			}
		}
	}
	// The dependency gauges start at 0: unknown is not up, and the first probe sample overwrites them.
	for _, dependency := range []string{DependencyPostgres, DependencyRedis} {
		value, _ := registry.Gauge(MetricDependencyUp, map[string]string{"dependency": dependency})
		if value != 0 {
			t.Errorf("%s starts at %v, want 0 until a probe says otherwise", dependency, value)
		}
	}

	// A counter that only pre-registers is still a counter: it must be readable as 0, and the
	// exposition must show it.
	var builder strings.Builder
	if _, err := registry.WritePrometheus(&builder); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	if !strings.Contains(builder.String(), "ncs_publisher_lock_losses_total 0") {
		t.Fatalf("a pre-registered counter is missing from the exposition:\n%s", builder.String())
	}
}
