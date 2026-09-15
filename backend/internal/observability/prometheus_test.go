package observability

import (
	"math"
	"strings"
	"testing"
)

// The exporter is what turns the registry into something a Prometheus server can scrape, so these
// tests check the format rules a scraper rejects rather than the values it parses: one # TYPE line
// per family, escaped label values, cumulative buckets including +Inf, and a body that survives a
// label value containing a quote or a newline.

func TestWritePrometheusRendersCountersAndGauges(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter(MetricEventsTotal, map[string]string{"outcome": "succeeded"}, 3)
	registry.AddCounter(MetricEventsTotal, map[string]string{"outcome": "failed"}, 1)
	registry.SetGauge(MetricStreamPending, map[string]string{"stream": "ncs:stream:charge-event"}, 7)

	var builder strings.Builder
	written, err := registry.WritePrometheus(&builder)
	if err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	if written != 3 {
		t.Fatalf("wrote %d series, want 3", written)
	}

	body := builder.String()
	for _, want := range []string{
		"# TYPE ncs_worker_events_total counter\n",
		"# TYPE ncs_stream_pending gauge\n",
		`ncs_worker_events_total{outcome="failed"} 1` + "\n",
		`ncs_worker_events_total{outcome="succeeded"} 3` + "\n",
		`ncs_stream_pending{stream="ncs:stream:charge-event"} 7` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body is missing %q:\n%s", want, body)
		}
	}
	// One header per family, not one per series: a repeated TYPE line for the same metric is a
	// parse error for a strict scraper.
	if count := strings.Count(body, "# TYPE ncs_worker_events_total"); count != 1 {
		t.Fatalf("expected exactly one TYPE line for the family, got %d:\n%s", count, body)
	}
	// HELP travels with the number when it is known.
	if !strings.Contains(body, "# HELP ncs_worker_events_total ") {
		t.Fatalf("expected a HELP line for a known metric:\n%s", body)
	}
}

func TestWritePrometheusEscapesLabelValues(t *testing.T) {
	registry := NewRegistry()
	registry.AddCounter(MetricDeadLetteredTotal, map[string]string{
		"reason": "invalid \"payload\"\nsecond line\\end",
	}, 1)

	var builder strings.Builder
	if _, err := registry.WritePrometheus(&builder); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	body := builder.String()

	// The escaped form is exactly what the format defines; the raw characters would end the
	// series early or start a new line the scraper reads as a separate sample.
	if !strings.Contains(body, `reason="invalid \"payload\"\nsecond line\\end"`) {
		t.Fatalf("label value was not escaped for the text format:\n%s", body)
	}
	if strings.Contains(body, "second line\n") {
		t.Fatalf("a newline inside a label value leaked into the body:\n%s", body)
	}
	// One line per series: a value with a newline must not become two samples.
	seriesLines := 0
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if !strings.HasPrefix(line, "#") {
			seriesLines++
		}
	}
	if seriesLines != 1 {
		t.Fatalf("expected one series line, got %d:\n%s", seriesLines, body)
	}
}

func TestWritePrometheusRendersHistogramBuckets(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterHistogram(MetricRequestDuration, []float64{0.1, 0.5, 1})
	labels := map[string]string{"method": "POST", "route": "/api/v1/orders", "status": "201"}
	for _, value := range []float64{0.02, 0.2, 0.9, 3.5} {
		registry.ObserveHistogram(MetricRequestDuration, labels, value)
	}

	var builder strings.Builder
	written, err := registry.WritePrometheus(&builder)
	if err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d series, want 1 histogram", written)
	}

	body := builder.String()
	for _, want := range []string{
		"# TYPE ncs_api_request_duration_seconds histogram\n",
		`ncs_api_request_duration_seconds_bucket{le="0.1",method="POST",route="/api/v1/orders",status="201"} 1`,
		`ncs_api_request_duration_seconds_bucket{le="0.5",method="POST",route="/api/v1/orders",status="201"} 2`,
		`ncs_api_request_duration_seconds_bucket{le="1",method="POST",route="/api/v1/orders",status="201"} 3`,
		`ncs_api_request_duration_seconds_bucket{le="+Inf",method="POST",route="/api/v1/orders",status="201"} 4`,
		`ncs_api_request_duration_seconds_sum{method="POST",route="/api/v1/orders",status="201"} 4.62`,
		`ncs_api_request_duration_seconds_count{method="POST",route="/api/v1/orders",status="201"} 4`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body is missing %q:\n%s", want, body)
		}
	}
}

func TestHistogramBucketsAreCumulativeAndBounded(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterHistogram("ncs_test_latency_seconds", []float64{0.5, 1})

	// 0.4 falls in the first bucket only; 0.75 in the first two; 9 in none of the finite ones but
	// always in +Inf. A non-cumulative histogram would report 1/1/1 here and make every
	// histogram_quantile() wrong.
	for _, value := range []float64{0.4, 0.75, 9} {
		registry.ObserveHistogram("ncs_test_latency_seconds", nil, value)
	}

	samples := registry.HistogramSnapshot()
	if len(samples) != 1 {
		t.Fatalf("expected one histogram series, got %d", len(samples))
	}
	got := samples[0]
	want := []uint64{1, 2, 3}
	if len(got.Counts) != len(want) {
		t.Fatalf("counts = %v, want %v", got.Counts, want)
	}
	for index, value := range want {
		if got.Counts[index] != value {
			t.Fatalf("counts = %v, want %v", got.Counts, want)
		}
	}
	if got.Count != 3 {
		t.Fatalf("count = %d, want 3", got.Count)
	}
	if got.Sum != 10.15 {
		t.Fatalf("sum = %v, want 10.15", got.Sum)
	}
}

// A histogram is registered before traffic so its buckets fit the workload; observing without
// registering must still work, and a caller must not be able to redefine a family mid-flight
// (buckets cannot change for a running series).
func TestRegisterHistogramKeepsTheFirstBucketSet(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterHistogram("ncs_test_latency_seconds", []float64{1, 2})
	registry.RegisterHistogram("ncs_test_latency_seconds", []float64{5})

	registry.ObserveHistogram("ncs_test_latency_seconds", nil, 1.5)

	samples := registry.HistogramSnapshot()
	if len(samples) != 1 {
		t.Fatalf("expected one family, got %d", len(samples))
	}
	if len(samples[0].Bounds) != 2 || samples[0].Bounds[1] != 2 {
		t.Fatalf("bounds were replaced by the second registration: %v", samples[0].Bounds)
	}

	// Bound validation: duplicates collapse, unusable values are dropped, ascending order is
	// guaranteed, and an empty set falls back to the defaults rather than creating a histogram
	// with only an +Inf bucket.
	validated := validateBounds([]float64{2, 1, 1, -5, 3})
	want := []float64{1, 2, 3}
	if len(validated) != len(want) {
		t.Fatalf("validateBounds() = %v, want %v", validated, want)
	}
	for index := range want {
		if validated[index] != want[index] {
			t.Fatalf("validateBounds() = %v, want %v", validated, want)
		}
	}
	if got := validateBounds(nil); len(got) != len(DefaultDurationBuckets) {
		t.Fatalf("empty bounds must fall back to the defaults, got %v", got)
	}

	// Observing an unregistered family still records, with the default buckets.
	registry.ObserveHistogram("ncs_test_unregistered_seconds", nil, 0.01)
	if len(registry.HistogramSnapshot()) != 2 {
		t.Fatal("an unregistered family must be created on first observation")
	}
}

// A body that cannot be scraped is worse than a body missing one series: an unusable metric name or
// label name is skipped, and the rest of the exposition still parses.
func TestWritePrometheusSkipsSeriesAScraperWouldReject(t *testing.T) {
	var builder strings.Builder
	written, err := WritePrometheus(&builder, []Sample{
		{Name: "ncs_bad name", Kind: KindCounter, Value: 1},
		{Name: "ncs_bad_label", Kind: KindGauge, Value: 2, Labels: map[string]string{"a-b": "x"}},
		{Name: "ncs_good_total", Kind: KindCounter, Value: 3},
		{Name: "ncs_unknown_kind", Kind: Kind("summary-ish"), Value: 4},
	})
	if err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d series, want only the valid one", written)
	}
	body := builder.String()
	if strings.Contains(body, "ncs_bad name") || strings.Contains(body, "a-b") {
		t.Fatalf("an invalid series reached the body:\n%s", body)
	}
	if !strings.Contains(body, "ncs_good_total 3") {
		t.Fatalf("the valid series is missing:\n%s", body)
	}
}

// Special float values have their own spelling in the format; "Inf" or "NaN" written any other way
// is a parse error, and a gauge of 0-vs-NaN is exactly how an operator sees "unknown".
func TestPrometheusValueFormatting(t *testing.T) {
	cases := map[float64]string{
		1:            "1",
		1.5:          "1.5",
		0:            "0",
		1e21:         "1e+21",
		math.Inf(1):  "+Inf",
		math.Inf(-1): "-Inf",
		math.NaN():   "NaN",
	}
	for value, want := range cases {
		if got := formatValue(value); got != want {
			t.Fatalf("formatValue(%v) = %q, want %q", value, got, want)
		}
	}
	// Bound labels keep the compact form Prometheus uses for le.
	if got := formatBound(0.000001); got != "1e-06" {
		t.Fatalf("formatBound(0.000001) = %q", got)
	}
	if got := formatBound(2.5); got != "2.5" {
		t.Fatalf("formatBound(2.5) = %q", got)
	}
}
