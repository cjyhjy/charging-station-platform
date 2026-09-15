package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// StreamTarget is one stream/group pair to sample.
type StreamTarget struct {
	Stream string
	Group  string
}

// Collector samples the state of consumed streams into a registry.
//
// It answers the questions an operator has during an incident, which the worker's own
// counters cannot: how far behind is each stream (lag), how much work is delivered but
// unfinished (pending), how large the streams have grown, and how many events are parked
// on the dead-letter stream. A worker counter can say "we are retrying"; only the stream
// state can say "and 40 000 events are queued behind us".
type Collector struct {
	inspector StreamInspector
	registry  *Registry
	targets   []StreamTarget
	// deadLetterStream is sampled for its length so the parked backlog is visible.
	deadLetterStream string
	// degradation publishes the Redis capability failures the policy absorbed. It is optional:
	// without it the policy stays invisible, which is the situation this collector exists to
	// fix, so a production caller should always set it.
	degradation *DegradationSampler
}

// StreamInspector reports the state of one stream.
//
// The interface is declared here as well as in the Redis package so the collector
// depends on the shape it needs rather than on the whole Streams boundary; a Redis
// StreamsClient satisfies it structurally.
type StreamInspector interface {
	StreamInfo(ctx context.Context, stream string) (redisrepo.StreamInfo, error)
	GroupInfo(ctx context.Context, stream, group string) (redisrepo.GroupInfo, error)
	StreamLen(ctx context.Context, stream string) (int64, error)
}

var _ StreamInspector = (*redisrepo.StreamsClient)(nil)

// NewCollector validates its inputs.
func NewCollector(inspector StreamInspector, registry *Registry, targets []StreamTarget, deadLetterStream string) (*Collector, error) {
	if inspector == nil {
		return nil, fmt.Errorf("collector: stream inspector is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("collector: registry is required")
	}
	for _, target := range targets {
		if target.Stream == "" || target.Group == "" {
			return nil, fmt.Errorf("collector: every target needs a stream and a group")
		}
	}
	return &Collector{
		inspector:        inspector,
		registry:         registry,
		targets:          targets,
		deadLetterStream: deadLetterStream,
	}, nil
}

// SetDegradationSampler attaches the Redis capability failure bridge.
//
// It is a setter rather than a constructor argument so a caller that has no degradation source
// (a test, or a process without the Redis foundation) does not have to pass a nil placeholder.
func (c *Collector) SetDegradationSampler(sampler *DegradationSampler) {
	c.degradation = sampler
}

// Collect performs one sampling pass.
//
// A stream that has never received an entry does not exist as a key, so Redis reports it
// as missing. That is reported as a zero gauge rather than a failure: a freshly deployed
// worker with no traffic is healthy, and treating it as an error would make the collector
// useless exactly when a deployment is being verified.
//
// Sampling failures are counted and returned together, so one unresolvable stream does
// not stop the others from being reported.
func (c *Collector) Collect(ctx context.Context) error {
	var failures []error

	for _, target := range c.targets {
		streamInfo, err := c.inspector.StreamInfo(ctx, target.Stream)
		if err != nil && !errors.Is(err, redisrepo.ErrNotFound) {
			failures = append(failures, fmt.Errorf("stream info %s: %w", target.Stream, err))
			c.countFailure(target.Stream)
			continue
		}
		c.registry.SetGauge(MetricStreamLength, map[string]string{"stream": target.Stream}, float64(streamInfo.Length))

		groupInfo, err := c.inspector.GroupInfo(ctx, target.Stream, target.Group)
		switch {
		case errors.Is(err, redisrepo.ErrNotFound), errors.Is(err, redisrepo.ErrGroupMissing):
			// No consumer group yet. Pending is genuinely zero, but lag is not: the entries already
			// in the stream have not been served to any group, and whether they ever will depends on
			// the start position that group is created with. Reporting zero would show a first
			// deployment with a backlog as a healthy stream with nothing behind, so lag is left unset
			// and the missing group is reported on its own series.
			c.registry.SetGauge(MetricStreamPending, map[string]string{"stream": target.Stream}, 0)
			c.registry.SetGauge(MetricStreamGroupMissing, map[string]string{"stream": target.Stream}, 1)
			c.clearLag(target.Stream)
		case err != nil:
			failures = append(failures, fmt.Errorf("group info %s/%s: %w", target.Stream, target.Group, err))
			c.countFailure(target.Stream)
		default:
			c.registry.SetGauge(MetricStreamPending, map[string]string{"stream": target.Stream}, float64(groupInfo.Pending))
			c.registry.SetGauge(MetricStreamGroupMissing, map[string]string{"stream": target.Stream}, 0)
			if lag, known := redisrepo.EffectiveLag(streamInfo, groupInfo); known {
				c.registry.SetGauge(MetricStreamLag, map[string]string{"stream": target.Stream}, float64(lag))
			} else {
				// Redis cannot compute the backlog and the fallback has nothing to derive it from.
				// The previous value must go, or every later snapshot would repeat it as current.
				c.clearLag(target.Stream)
			}
		}
	}

	if c.deadLetterStream != "" {
		length, err := c.inspector.StreamLen(ctx, c.deadLetterStream)
		if err != nil {
			failures = append(failures, fmt.Errorf("dead-letter length: %w", err))
			c.countFailure(c.deadLetterStream)
		} else {
			// XLEN reports a missing key as zero, so this gauge is meaningful before the
			// first dead letter exists.
			c.registry.SetGauge(MetricDeadLetterLength, nil, float64(length))
		}
	}

	// Redis capability failures are sampled last so the stream gauges describe the same instant
	// as the counters, which keeps one snapshot internally consistent.
	if c.degradation != nil {
		c.degradation.Sample()
	}

	return errors.Join(failures...)
}

// clearLag removes a stream's lag series when the backlog cannot be determined, so a snapshot never
// reports a stale number as if it were current.
func (c *Collector) clearLag(stream string) {
	c.registry.DeleteGauge(MetricStreamLag, map[string]string{"stream": stream})
}

func (c *Collector) countFailure(stream string) {
	c.registry.AddCounter(MetricCollectorErrorsTotal, map[string]string{"stream": stream}, 1)
}

// Run samples every interval until the context is cancelled, logging a snapshot each
// pass.
//
// It is the reporting path used when no metrics endpoint is available, which is the case
// for this module: the contract requires B-line ops endpoints to be registered in
// OpenAPI first, and that file belongs to the integration owner. Logging the snapshot
// keeps lag, pending, retry and dead-letter counts observable in the meantime.
func (c *Collector) Run(ctx context.Context, interval time.Duration, logger *slog.Logger) error {
	if interval <= 0 {
		return fmt.Errorf("collector: interval must be greater than zero")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.Collect(ctx); err != nil {
				// A sampling failure must not stop reporting: the next pass may succeed and
				// the error is already counted.
				if logger != nil {
					logger.WarnContext(ctx, "stream sampling failed", "error", err.Error())
				}
			}
			if logger != nil {
				logger.InfoContext(ctx, "stream state", "metrics", c.Summary())
			}
		}
	}
}

// Summary renders the current snapshot as a compact string for a log line.
func (c *Collector) Summary() string {
	samples := c.registry.Snapshot()
	parts := make([]string, 0, len(samples))
	for _, sample := range samples {
		parts = append(parts, sample.String())
	}
	return strings.Join(parts, " ")
}
