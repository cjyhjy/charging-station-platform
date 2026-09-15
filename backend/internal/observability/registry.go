// Package observability holds the B line's metrics registry, its trace-id propagation
// and the collector that samples Redis Streams state.
//
// It is deliberately dependency free apart from the Streams inspector it samples: a
// metrics pipeline that needs a third-party agent before it can report anything would
// not have been usable in this environment, and the frozen lock files make adding one a
// process decision rather than an implementation detail.
package observability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Metric names.
//
// Counters end in _total and only rise; gauges are instantaneous values. The prefix
// keeps the B line's series distinguishable from anything an A-line or H5 metric
// pipeline adds later.
const (
	// MetricRetriesTotal counts deliveries that failed transiently and stayed pending for
	// another attempt, labelled by the attempt number that failed.
	MetricRetriesTotal = "ncs_worker_retries_total"
	// MetricEventsTotal counts every delivery the worker finished, labelled by outcome.
	MetricEventsTotal = "ncs_worker_events_total"
	// MetricDeadLetteredTotal counts events parked on the dead-letter stream, labelled by
	// the reason so an operator can separate "invalid payload" from "retry exhausted".
	MetricDeadLetteredTotal = "ncs_worker_dead_lettered_total"
	// MetricDeadLetterSuppressedTotal counts dead-letter writes skipped because the event was
	// already parked.
	//
	// It is separate from the written counter because the two answer different questions: the
	// written counter is the parked backlog an operator works through, while this one is the
	// evidence that the idempotent write is doing its job. A spike here without a matching
	// written count means retries are recovering after partial failures rather than producing
	// duplicates.
	MetricDeadLetterSuppressedTotal = "ncs_worker_dead_letter_suppressed_total"
	// MetricRedisCapabilityFailuresTotal counts Redis capability failures that the degradation
	// policy handled, labelled by capability and by the decision taken.
	//
	// Without it the policy is invisible: a deployment whose cache silently stopped caching, or
	// whose duplicate guard stopped guarding, would look healthy in every other metric because
	// the failures never reach the worker.
	MetricRedisCapabilityFailuresTotal = "ncs_redis_capability_failures_total"
	// MetricPendingRecoveredTotal counts entries reclaimed by the pending-recovery pass,
	// which is the evidence that a restart or a crashed consumer recovered its work.
	MetricPendingRecoveredTotal = "ncs_worker_pending_recovered_total"
	// MetricStreamLag is the backlog of a consumed stream: entries produced but not yet
	// served to the group.
	MetricStreamLag = "ncs_stream_lag"
	// MetricStreamPending is the number of entries delivered but not acknowledged.
	MetricStreamPending = "ncs_stream_pending"
	// MetricStreamLength is the number of entries currently in the stream.
	MetricStreamLength = "ncs_stream_length"
	// MetricStreamGroupMissing is 1 while a consumed stream has no consumer group yet.
	//
	// It exists because lag cannot be reported honestly in that state: the entries already in the
	// stream have not been served to any group, and whether they ever will depends on the start
	// position the group is created with. Without this series a first deployment with a backlog
	// would look healthy, because there is simply no group to be behind.
	MetricStreamGroupMissing = "ncs_stream_group_missing"
	// MetricDeadLetterLength is the number of entries parked on the dead-letter stream.
	MetricDeadLetterLength = "ncs_stream_dead_letter_length"
	// MetricCollectorErrorsTotal counts sampling failures, so a broken collector is
	// visible instead of silently reporting stale gauges.
	MetricCollectorErrorsTotal = "ncs_observability_collector_errors_total"

	// API process metrics (B-06). The API is the process an operator looks at first when a
	// request fails, so it reports its own traffic and the state of its dependencies rather
	// than leaving that to the worker's metrics.
	//
	// MetricRequestsTotal counts finished requests, labelled by method, the registered route
	// pattern and the status class. The route pattern is used instead of the request path
	// deliberately: a path label would create one series per order number and turn the metrics
	// endpoint into a memory leak.
	MetricRequestsTotal = "ncs_api_requests_total"
	// MetricRequestDuration is a histogram of request latency in seconds.
	MetricRequestDuration = "ncs_api_request_duration_seconds"
	// MetricRequestsInFlight is the number of requests being served right now.
	MetricRequestsInFlight = "ncs_api_requests_in_flight"
	// MetricDependencyUp is 1 while a dependency answers, 0 while it does not. It is what the
	// readiness probe and the alert "the API is up but its database is not" are built on.
	MetricDependencyUp = "ncs_dependency_up"
	// MetricMigrationsVersion is the highest applied migration version. A worker that cannot
	// reach the expected version refuses to start, so an operator needs to see the number the
	// database actually holds.
	MetricMigrationsVersion = "ncs_pg_migrations_version"
	// MetricOutboxUnpublished is the number of outbox rows not yet published. It is the backlog
	// between a committed business transaction and the stream, and the first thing to grow when
	// the publisher is down.
	MetricOutboxUnpublished = "ncs_pg_outbox_unpublished"
)

// Dependency label values for MetricDependencyUp.
const (
	DependencyPostgres = "postgres"
	DependencyRedis    = "redis"
)

// Kind distinguishes a monotonic counter from an instantaneous gauge.
type Kind string

const (
	KindCounter Kind = "counter"
	KindGauge   Kind = "gauge"
)

// Sample is one metric value with its labels.
type Sample struct {
	Name   string
	Kind   Kind
	Value  float64
	Labels map[string]string
}

// String renders a sample as name{label="value"} with labels sorted, so output is stable
// and comparable across runs.
func (s Sample) String() string {
	if len(s.Labels) == 0 {
		return fmt.Sprintf("%s=%s", s.Name, formatFloat(s.Value))
	}
	keys := make([]string, 0, len(s.Labels))
	for key := range s.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", key, s.Labels[key]))
	}
	return fmt.Sprintf("%s{%s}=%s", s.Name, strings.Join(parts, ","), formatFloat(s.Value))
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// Registry is a concurrency-safe metric registry.
//
// Label cardinality is bounded by construction at every call site: streams come from the
// frozen stream list and event types from the frozen event list, so the series set cannot
// grow with traffic. An unbounded label such as an event id or a charger id would defeat
// that, which is why none is used.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*entry
	gauges     map[string]*entry
	histograms map[string]*histogramSeries
}

type entry struct {
	name   string
	labels map[string]string
	value  float64
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*entry),
		gauges:     make(map[string]*entry),
		histograms: make(map[string]*histogramSeries),
	}
}

// IncCounter adds delta to a counter, creating it at zero first.
func (r *Registry) IncCounter(name string, labels map[string]string, delta float64) {
	r.AddCounter(name, labels, delta)
}

// AddCounter adds delta to a counter. A negative delta is rejected by clamping at zero,
// because a counter that can fall is not a counter and would make rate calculations
// nonsense.
func (r *Registry) AddCounter(name string, labels map[string]string, delta float64) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.counters[key]
	if !ok {
		current = &entry{name: name, labels: copyLabels(labels)}
		r.counters[key] = current
	}
	current.value += delta
	if current.value < 0 {
		current.value = 0
	}
}

// IncGauge adds delta to a gauge, creating it at zero first.
//
// A gauge can move in both directions - requests in flight rise and fall - so unlike a counter it is
// not clamped at zero. Clamping would hide a bookkeeping error, and a gauge stuck at 0 while
// requests are in flight is a number an operator would act on.
func (r *Registry) IncGauge(name string, labels map[string]string, delta float64) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.gauges[key]
	if !ok {
		current = &entry{name: name, labels: copyLabels(labels)}
		r.gauges[key] = current
	}
	current.value += delta
}

// DeleteGauge removes a gauge series.
//
// It exists because a gauge that can become unknowable must be able to disappear: leaving the
// previous value in place would report a stale number as if it were current, and a snapshot that
// silently repeats an old backlog is worse than one that shows nothing for that stream.
func (r *Registry) DeleteGauge(name string, labels map[string]string) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.gauges, key)
}

// SetGauge records an instantaneous value.
func (r *Registry) SetGauge(name string, labels map[string]string, value float64) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.gauges[key]
	if !ok {
		current = &entry{name: name, labels: copyLabels(labels)}
		r.gauges[key] = current
	}
	current.value = value
}

// Counter returns a counter value.
func (r *Registry) Counter(name string, labels map[string]string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.counters[seriesKey(name, labels)]
	if !ok {
		return 0, false
	}
	return current.value, true
}

// Gauge returns a gauge value.
func (r *Registry) Gauge(name string, labels map[string]string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.gauges[seriesKey(name, labels)]
	if !ok {
		return 0, false
	}
	return current.value, true
}

// Snapshot returns every series, ordered by name and then by labels so output and tests
// are stable.
func (r *Registry) Snapshot() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()

	samples := make([]Sample, 0, len(r.counters)+len(r.gauges))
	for _, current := range r.counters {
		samples = append(samples, Sample{Name: current.name, Kind: KindCounter, Value: current.value, Labels: copyLabels(current.labels)})
	}
	for _, current := range r.gauges {
		samples = append(samples, Sample{Name: current.name, Kind: KindGauge, Value: current.value, Labels: copyLabels(current.labels)})
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Name != samples[j].Name {
			return samples[i].Name < samples[j].Name
		}
		return labelsKey(samples[i].Labels) < labelsKey(samples[j].Labels)
	})
	return samples
}

// Len reports how many series are tracked, counting one histogram family as one series.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.counters) + len(r.gauges) + len(r.histograms)
}

// Reset clears every series. It exists for tests and for a manual ops reset.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = make(map[string]*entry)
	r.gauges = make(map[string]*entry)
	r.histograms = make(map[string]*histogramSeries)
}

func seriesKey(name string, labels map[string]string) string {
	return name + "|" + labelsKey(labels)
}

func labelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(labels[key])
		builder.WriteByte('|')
	}
	return builder.String()
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	copyOf := make(map[string]string, len(labels))
	for key, value := range labels {
		copyOf[key] = value
	}
	return copyOf
}
