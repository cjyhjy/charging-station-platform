package observability

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Prometheus text exposition.
//
// The registry could already report itself as a line of text for the log, but a log line is not
// something an operator can alert on: it cannot be scraped, it cannot be rate()d, and it is gone as
// soon as the process restarts. This renders the same series in the Prometheus text format (version
// 0.0.4) so the API can expose /metrics and an existing monitoring stack can scrape it without this
// project taking on a metrics client dependency. See docs/backend-architecture.md §8: metrics stay
// on the internal network, and Nginx refuses them to anything but the ops range.
const (
	// PrometheusContentType is the content type of the text exposition format.
	PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8"
	// prometheusInfLabel is the le label value of the catch-all bucket.
	prometheusInfLabel = "+Inf"
)

// metricHelp documents the series this registry produces.
//
// HELP is optional in the format, but a dashboard built by somebody who did not write the code is
// where a metric name like ncs_worker_dead_letter_suppressed_total gets misread, so the meaning
// travels with the number.
var metricHelp = map[string]string{
	MetricRetriesTotal:                 "Worker deliveries that failed transiently and stayed pending for another attempt, by attempt number.",
	MetricEventsTotal:                  "Worker deliveries finished, by event type and outcome.",
	MetricDeadLetteredTotal:            "Events parked on the dead-letter stream, by reason.",
	MetricDeadLetterSuppressedTotal:    "Dead-letter writes skipped because the event was already parked.",
	MetricRedisCapabilityFailuresTotal: "Redis capability failures handled by the degradation policy, by capability and decision.",
	MetricPendingRecoveredTotal:        "Entries reclaimed by the pending-recovery pass.",
	MetricStreamLag:                    "Entries produced on a consumed stream but not yet served to the group.",
	MetricStreamPending:                "Entries delivered to the group but not yet acknowledged.",
	MetricStreamLength:                 "Entries currently in the stream.",
	MetricStreamGroupMissing:           "1 while a consumed stream has no consumer group yet.",
	MetricDeadLetterLength:             "Entries currently parked on the dead-letter stream.",
	MetricCollectorErrorsTotal:         "Stream sampling failures; a rising value means the gauges above are stale.",

	MetricRequestsTotal:      "HTTP requests finished by the API, by method, registered route pattern and status.",
	MetricRequestDuration:    "HTTP request latency in seconds, by method, registered route pattern and status.",
	MetricRequestsInFlight:   "HTTP requests being served right now.",
	MetricDependencyUp:       "1 while the dependency answers, 0 while it does not.",
	MetricMigrationsVersion:  "Highest applied PostgreSQL migration version.",
	MetricOutboxUnpublished:  "Outbox rows not yet published to the stream.",
	MetricHTTPRejectedTotal:  "Requests rejected before reaching a handler, by reason.",
	MetricProbeFailuresTotal: "Dependency probe failures, by dependency.",
}

// MetricRejectedLabel values for MetricHTTPRejectedTotal.
const (
	MetricHTTPRejectedTotal  = "ncs_api_rejected_total"
	MetricProbeFailuresTotal = "ncs_api_probe_failures_total"
)

// WritePrometheus renders every scalar series of the registry.
func (r *Registry) WritePrometheus(w io.Writer) (int, error) {
	samples := r.Snapshot()
	written, err := writeScalarSamples(w, samples)
	if err != nil {
		return written, err
	}
	histograms, err := writeHistograms(w, r.HistogramSnapshot())
	if err != nil {
		return written + histograms, err
	}
	return written + histograms, nil
}

// WritePrometheus renders scalar samples in the Prometheus text format and reports how many series
// it wrote.
//
// It is exported so a caller holding a snapshot can render it, and so a test can render a series
// set it built by hand.
func WritePrometheus(w io.Writer, samples []Sample) (int, error) {
	return writeScalarSamples(w, samples)
}

func writeScalarSamples(w io.Writer, samples []Sample) (int, error) {
	written := 0
	current := ""
	for _, sample := range samples {
		if !validMetricName(sample.Name) {
			// A series whose name cannot be scraped is skipped rather than allowed to make the
			// whole body unparseable: one bad series must not take the endpoint down.
			continue
		}
		if sample.Kind != KindCounter && sample.Kind != KindGauge {
			continue
		}
		if sample.Name != current {
			if err := writeHeader(w, sample.Name, string(sample.Kind)); err != nil {
				return written, err
			}
			current = sample.Name
		}
		line, err := formatSample(sample.Name, sample.Labels, sample.Value)
		if err != nil {
			continue
		}
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

func writeHistograms(w io.Writer, samples []HistogramSample) (int, error) {
	written := 0
	current := ""
	for _, sample := range samples {
		if !validMetricName(sample.Name) || len(sample.Counts) != len(sample.Bounds)+1 {
			continue
		}
		if sample.Name != current {
			if err := writeHeader(w, sample.Name, "histogram"); err != nil {
				return written, err
			}
			current = sample.Name
		}
		// Buckets are emitted in the order the bounds were declared, plus the +Inf bucket that
		// makes the cumulative series complete.
		for index, bound := range sample.Bounds {
			labels := withLabel(sample.Labels, "le", formatBound(bound))
			line, err := formatSample(sample.Name+"_bucket", labels, float64(sample.Counts[index]))
			if err != nil {
				return written, err
			}
			if _, err := io.WriteString(w, line+"\n"); err != nil {
				return written, err
			}
		}
		labels := withLabel(sample.Labels, "le", prometheusInfLabel)
		line, err := formatSample(sample.Name+"_bucket", labels, float64(sample.Counts[len(sample.Bounds)]))
		if err != nil {
			return written, err
		}
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return written, err
		}
		sumLine, err := formatSample(sample.Name+"_sum", sample.Labels, sample.Sum)
		if err != nil {
			return written, err
		}
		countLine, err := formatSample(sample.Name+"_count", sample.Labels, float64(sample.Count))
		if err != nil {
			return written, err
		}
		if _, err := io.WriteString(w, sumLine+"\n"+countLine+"\n"); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

func writeHeader(w io.Writer, name, kind string) error {
	var builder strings.Builder
	if help, ok := metricHelp[name]; ok {
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteByte(' ')
		builder.WriteString(escapeHelp(help))
		builder.WriteByte('\n')
	}
	builder.WriteString("# TYPE ")
	builder.WriteString(name)
	builder.WriteByte(' ')
	builder.WriteString(kind)
	builder.WriteByte('\n')
	_, err := io.WriteString(w, builder.String())
	return err
}

func formatSample(name string, labels map[string]string, value float64) (string, error) {
	var builder strings.Builder
	builder.WriteString(name)
	if len(labels) > 0 {
		rendered, err := formatLabels(labels)
		if err != nil {
			return "", err
		}
		builder.WriteByte('{')
		builder.WriteString(rendered)
		builder.WriteByte('}')
	}
	builder.WriteByte(' ')
	builder.WriteString(formatValue(value))
	return builder.String(), nil
}

// formatLabels renders labels in the order given by the registry, which is alphabetical, so the
// body is byte-identical between two scrapes of the same state and diffs stay readable.
func formatLabels(labels map[string]string) (string, error) {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if !validLabelName(key) {
			return "", fmt.Errorf("observability: invalid label name %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+`="`+escapeLabelValue(labels[key])+`"`)
	}
	return strings.Join(parts, ","), nil
}

// withLabel returns a copy of labels with one added key.
func withLabel(labels map[string]string, key, value string) map[string]string {
	combined := make(map[string]string, len(labels)+1)
	for existing, existingValue := range labels {
		combined[existing] = existingValue
	}
	combined[key] = value
	return combined
}

// formatValue renders a number the way the text format expects.
//
// strconv already spells the special values the format uses: FormatFloat gives "NaN", "+Inf" and
// "-Inf", and a finite value round-trips through the shortest representation.
func formatValue(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// escapeLabelValue escapes a label value: backslash, double quote and newline are the only
// characters the format requires to be escaped, and an unescaped newline would split one series
// into two lines that a scraper reads as a parse error.
func escapeLabelValue(value string) string {
	var builder strings.Builder
	for _, char := range value {
		switch char {
		case '\\':
			builder.WriteString(`\\`)
		case '"':
			builder.WriteString(`\"`)
		case '\n':
			builder.WriteString(`\n`)
		default:
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

// escapeHelp escapes the HELP text: only backslash and newline may appear escaped there.
func escapeHelp(text string) string {
	var builder strings.Builder
	for _, char := range text {
		switch char {
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		default:
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

// validMetricName reports whether a name matches [a-zA-Z_:][a-zA-Z0-9_:]*.
func validMetricName(name string) bool {
	if name == "" {
		return false
	}
	for index, char := range name {
		if isNameChar(char, index == 0) {
			continue
		}
		return false
	}
	return true
}

// validLabelName reports whether a label name matches [a-zA-Z_][a-zA-Z0-9_]*.
func validLabelName(name string) bool {
	if name == "" {
		return false
	}
	for index, char := range name {
		if isNameChar(char, index == 0) || (char == ':' && index > 0) {
			continue
		}
		return false
	}
	return true
}

func isNameChar(char rune, first bool) bool {
	switch {
	case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		return true
	case !first && char >= '0' && char <= '9':
		return true
	case !first && char == ':':
		return true
	}
	return false
}
