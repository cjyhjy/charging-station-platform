package observability

import (
	"sort"
	"strconv"
)

// Histograms.
//
// The registry started with counters and gauges, which answer "how many" and "how much right
// now". Neither answers the question an operator actually asks about an API: how slow is it, and
// how slow is the slow tail. A histogram is the smallest thing that can answer it, and Prometheus
// derives percentiles from the buckets with histogram_quantile, so no quantile bookkeeping (and no
// third-party client library) is needed here.

// DefaultDurationBuckets are the bucket bounds for request latency in seconds.
//
// They follow Prometheus' own default latency buckets. The upper end matters for this platform: a
// device command dispatch waits on the charger gateway, so a healthy request is fast and anything
// past a few seconds is either a gateway problem or a lock wait, and the buckets have to separate
// those from normal traffic rather than lumping them together.
var DefaultDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// HistogramSample is one histogram series as it is exposed.
type HistogramSample struct {
	Name   string
	Labels map[string]string
	// Bounds are the finite upper bounds, ascending, without +Inf.
	Bounds []float64
	// Counts has len(Bounds)+1 elements: one per finite bound plus the +Inf bucket.
	Counts []uint64
	Sum    float64
	Count  uint64
}

// histogramSeries is one histogram family: its bounds and every label set observed for it.
//
// counts[i] counts observations at or below bounds[i], and the last element counts every
// observation including the ones above the highest finite bound (the +Inf bucket). Buckets are
// cumulative on purpose: that is the form Prometheus expects, and it is what makes a bucket
// boundary readable without adding up the ones before it.
type histogramSeries struct {
	name   string
	bounds []float64
	// members are the label sets observed for this family.
	members map[string]*histogramMember
}

// RegisterHistogram declares a histogram family with explicit bucket bounds.
//
// Registering is separate from observing so a deployment can choose buckets before traffic
// arrives: buckets cannot be changed retroactively for a running series, and a histogram whose
// bounds do not fit the workload is worse than no histogram.
func (r *Registry) RegisterHistogram(name string, bounds []float64) {
	if name == "" {
		return
	}
	validated := validateBounds(bounds)

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.histograms[name]; exists {
		return
	}
	r.histograms[name] = newHistogramSeries(name, validated)
}

// ObserveHistogram records one observation in a histogram, creating the family with the default
// duration buckets when it was not registered explicitly.
func (r *Registry) ObserveHistogram(name string, labels map[string]string, value float64) {
	if name == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.histograms == nil {
		r.histograms = make(map[string]*histogramSeries)
	}
	series, ok := r.histograms[name]
	if !ok {
		series = newHistogramSeries(name, DefaultDurationBuckets)
		r.histograms[name] = series
	}
	series.observe(labels, value)
}

// HistogramSnapshot returns every histogram series, ordered by name and then labels, so output
// and tests are stable.
func (r *Registry) HistogramSnapshot() []HistogramSample {
	r.mu.Lock()
	defer r.mu.Unlock()

	samples := make([]HistogramSample, 0, len(r.histograms))
	for _, series := range r.histograms {
		for _, member := range series.orderedMembers() {
			counts := make([]uint64, len(member.counts))
			copy(counts, member.counts)
			bounds := make([]float64, len(series.bounds))
			copy(bounds, series.bounds)
			samples = append(samples, HistogramSample{
				Name:   series.name,
				Labels: copyLabels(member.labels),
				Bounds: bounds,
				Counts: counts,
				Sum:    member.sum,
				Count:  member.count,
			})
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Name != samples[j].Name {
			return samples[i].Name < samples[j].Name
		}
		return labelsKey(samples[i].Labels) < labelsKey(samples[j].Labels)
	})
	return samples
}

// validateBounds returns the finite upper bounds in ascending order, dropping duplicates and
// non-finite values. A bucket boundary that is NaN or -Inf cannot be labelled meaningfully, and a
// duplicate would silently merge two buckets.
func validateBounds(bounds []float64) []float64 {
	if len(bounds) == 0 {
		return append([]float64(nil), DefaultDurationBuckets...)
	}
	ordered := make([]float64, 0, len(bounds))
	for _, bound := range bounds {
		if bound != bound || bound < 0 { // NaN or negative: neither is a latency boundary
			continue
		}
		ordered = append(ordered, bound)
	}
	sort.Float64s(ordered)
	deduped := ordered[:0]
	for index, bound := range ordered {
		if index > 0 && bound == ordered[index-1] {
			continue
		}
		deduped = append(deduped, bound)
	}
	if len(deduped) == 0 {
		return append([]float64(nil), DefaultDurationBuckets...)
	}
	return deduped
}

type histogramMember struct {
	labels map[string]string
	counts []uint64
	sum    float64
	count  uint64
}

func newHistogramSeries(name string, bounds []float64) *histogramSeries {
	return &histogramSeries{name: name, bounds: bounds, members: make(map[string]*histogramMember)}
}

func (s *histogramSeries) observe(labels map[string]string, value float64) {
	key := labelsKey(labels)
	member, ok := s.members[key]
	if !ok {
		member = &histogramMember{labels: copyLabels(labels), counts: make([]uint64, len(s.bounds)+1)}
		s.members[key] = member
	}
	// Buckets are cumulative, which is what the exposition format and histogram_quantile()
	// require: the +Inf bucket always counts the observation, and so does every finite bucket
	// whose upper bound covers it. Incrementing only the first covering bucket would report
	// le="0.5" without the observations already counted under le="0.1", and every quantile
	// derived from it would be wrong.
	member.counts[len(s.bounds)]++
	for index, bound := range s.bounds {
		if value > bound {
			continue
		}
		for fill := index; fill < len(s.bounds); fill++ {
			member.counts[fill]++
		}
		break
	}
	member.sum += value
	member.count++
}

// orderedMembers returns the family's label sets in a stable order.
func (s *histogramSeries) orderedMembers() []*histogramMember {
	ordered := make([]*histogramMember, 0, len(s.members))
	for _, member := range s.members {
		ordered = append(ordered, member)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return labelsKey(ordered[i].labels) < labelsKey(ordered[j].labels)
	})
	return ordered
}

// formatBound renders a bucket bound the way Prometheus expects the le label: the shortest
// representation that round-trips, with the special values spelled out.
func formatBound(bound float64) string {
	return strconv.FormatFloat(bound, 'g', -1, 64)
}
