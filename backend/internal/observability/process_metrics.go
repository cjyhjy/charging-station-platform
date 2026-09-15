package observability

// Pre-registered process metrics.
//
// A counter that has never been incremented has no series, and a monitoring team reasonably reads a
// missing series as "this build does not export it". The ruling lists the metrics each process must
// provide, so the fixed ones - the counters and the gauges that do not depend on traffic - are
// created at startup with their initial value. Series whose labels come from traffic (per-route API
// requests, per-event-type consumption) are deliberately not pre-created: inventing label values
// would produce series that can never move.

// ProcessMetricsConfig describes what a process exposes.
type ProcessMetricsConfig struct {
	// Streams are the streams this process consumes or observes, for the per-stream gauges. The
	// worker passes its frozen stream list; the publisher has none.
	Streams []string
	// Worker adds the consumption counters.
	Worker bool
	// Publisher adds the publishing counters and gauges.
	Publisher bool
}

// RegisterProcessMetrics creates the fixed series with their initial values.
func RegisterProcessMetrics(registry *Registry, cfg ProcessMetricsConfig) {
	if registry == nil {
		return
	}

	// Dependency state starts at 0 - unknown is not "up" - and the first probe sample overwrites it.
	registry.SetGauge(MetricDependencyUp, map[string]string{"dependency": DependencyPostgres}, 0)
	registry.SetGauge(MetricDependencyUp, map[string]string{"dependency": DependencyRedis}, 0)

	for _, stream := range cfg.Streams {
		labels := map[string]string{"stream": stream}
		registry.SetGauge(MetricStreamLag, labels, 0)
		registry.SetGauge(MetricStreamPending, labels, 0)
		registry.SetGauge(MetricStreamLength, labels, 0)
	}
	registry.SetGauge(MetricDeadLetterLength, nil, 0)
	registry.SetGauge(MetricMigrationsVersion, nil, 0)
	registry.SetGauge(MetricOutboxUnpublished, nil, 0)

	if cfg.Worker {
		registry.SetGauge(MetricWorkerLastSuccess, nil, 0)
	}
	// The worker's consumption counters are deliberately NOT pre-created. Their labels come from
	// traffic - event type, stream, outcome, attempt, dead-letter reason - so a pre-created series
	// would have to invent label values that can never move, and a permanent zero beside the real
	// counter is worse than an absent series: it looks like traffic that never happened. The
	// mapping from "consumption succeeded / failed / retried / dead-lettered" to those series is in
	// backend/deploy/README.md, and the series appear the first time the worker handles an event.

	if cfg.Publisher {
		registry.AddCounter(MetricPublisherPublishedTotal, nil, 0)
		registry.AddCounter(MetricPublisherPassFailuresTotal, nil, 0)
		registry.AddCounter(MetricPublisherLockLossesTotal, nil, 0)
		// A publisher starts by competing for the lock, so "not yet holding it" is the honest
		// initial state; the loop sets it to 0 the moment it acquires.
		registry.SetGauge(MetricPublisherStandby, nil, 1)
		registry.SetGauge(MetricPublisherLastPublish, nil, 0)
	}

	registry.AddCounter(MetricProbeFailuresTotal, map[string]string{"dependency": DependencyPostgres}, 0)
	registry.AddCounter(MetricProbeFailuresTotal, map[string]string{"dependency": DependencyRedis}, 0)
}
