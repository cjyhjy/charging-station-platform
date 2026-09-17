package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// StreamConfig describes one consumed stream.
type StreamConfig struct {
	Stream   string
	Group    string
	Consumer string
	// Count, Block, PendingInterval, RetryAfter and MaxAttempts mirror Config and
	// are applied to the worker for this stream.
	Count           int
	Block           time.Duration
	PendingInterval time.Duration
	RetryAfter      time.Duration
	MaxAttempts     int
	// StartID is the position a newly created consumer group starts from. It mirrors
	// Config.StartID and must be carried here too: without it the runner silently used the
	// default for every stream, so an operator who configured "$" - the documented way to
	// ignore pre-existing history - got the backlog replayed instead, and the only way to get
	// the documented behaviour was to bypass the runner entirely.
	//
	// An empty value means Config's default, which is the beginning of the stream.
	StartID string
}

// toConfig converts a StreamConfig into the per-worker Config.
func (s StreamConfig) toConfig() Config {
	defaults := DefaultConfig()
	config := Config{
		Stream:          s.Stream,
		Group:           s.Group,
		Consumer:        s.Consumer,
		Count:           s.Count,
		Block:           s.Block,
		PendingInterval: s.PendingInterval,
		RetryAfter:      s.RetryAfter,
		MaxAttempts:     s.MaxAttempts,
		StartID:         s.StartID,
	}
	if config.Count <= 0 {
		config.Count = defaults.Count
	}
	if config.Block <= 0 {
		config.Block = defaults.Block
	}
	if config.PendingInterval <= 0 {
		config.PendingInterval = defaults.PendingInterval
	}
	if config.RetryAfter <= 0 {
		config.RetryAfter = defaults.RetryAfter
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaults.MaxAttempts
	}
	if config.StartID == "" {
		// An explicit "" is treated as "unset" rather than passed through: the worker's own
		// default is the beginning of the stream, and leaving the field empty would otherwise
		// depend on which layer filled it in.
		config.StartID = defaults.StartID
	}
	return config
}

func (s StreamConfig) validate() error {
	if s.Stream == "" {
		return fmt.Errorf("runner: stream is required")
	}
	if s.Group == "" {
		return fmt.Errorf("runner: group is required for stream %s", s.Stream)
	}
	if s.Consumer == "" {
		return fmt.Errorf("runner: consumer is required for stream %s", s.Stream)
	}
	return nil
}

// Runner supervises one Worker per stream.
//
// The B line consumes three streams (order events, charge events, charger commands),
// and each needs its own consumer group and pending-recovery state. Running them in one
// process with one shared connection pool is what makes a single worker binary able to
// serve the whole async chain.
//
// Failure policy: the first fatal error cancels every other worker and is returned. A
// worker that cannot read its stream is not making progress, so limping along with a
// partially working chain would hide the outage; the process exits and the supervisor
// restarts it.
type Runner struct {
	client  redisrepo.StreamClient
	handler Handler
	streams []StreamConfig
	// deadLetterRecorder is forwarded to every worker.
	deadLetterRecorder DeadLetterRecorder
	// deadLetterGuard is forwarded to every worker, making the dead-letter write idempotent per
	// event across retries and restarts.
	deadLetterGuard DeadLetterGuard
	// observer and logger are forwarded to every worker so the whole runner reports
	// through one vocabulary and one log stream.
	observer observability.Observer
	logger   *slog.Logger
	// onStart is called once per stream after its group is ready, for startup logging.
	onStart func(StreamConfig)
	// onReady is called once, after every stream has been prepared, so a caller can sample stream
	// state knowing that every consumer group exists and a backlog reading is meaningful.
	onReady func()
}

// NewRunner validates the configuration. Streams must be non-empty, and each must
// carry its own consumer name so two workers in one process never share a consumer
// identity, which would make pending recovery ambiguous.
func NewRunner(client redisrepo.StreamClient, handler Handler, streams []StreamConfig) (*Runner, error) {
	if client == nil {
		return nil, fmt.Errorf("runner: stream client is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("runner: handler is required")
	}
	if len(streams) == 0 {
		return nil, fmt.Errorf("runner: at least one stream is required")
	}
	seen := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if err := stream.validate(); err != nil {
			return nil, err
		}
		key := stream.Stream + "\x00" + stream.Group + "\x00" + stream.Consumer
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("runner: duplicate stream/group/consumer %s/%s/%s", stream.Stream, stream.Group, stream.Consumer)
		}
		seen[key] = struct{}{}
	}
	return &Runner{client: client, handler: handler, streams: streams}, nil
}

// SetDeadLetterRecorder attaches a recorder for dead-lettered events on every stream.
func (r *Runner) SetDeadLetterRecorder(recorder DeadLetterRecorder) {
	r.deadLetterRecorder = recorder
}

// SetDeadLetterGuard attaches the idempotent dead-letter writer to every stream worker.
func (r *Runner) SetDeadLetterGuard(guard DeadLetterGuard) {
	r.deadLetterGuard = guard
}

// SetObserver attaches the metrics observer to every stream worker.
func (r *Runner) SetObserver(observer observability.Observer) {
	r.observer = observer
}

// SetLogger attaches the logger to every stream worker, so each worker's event lines
// carry the trace id of the event it is handling.
func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// SetOnStart registers a callback invoked once per stream when its worker begins.
func (r *Runner) SetOnStart(callback func(StreamConfig)) {
	r.onStart = callback
}

// SetOnReady registers a callback invoked once after every stream has been prepared.
//
// It exists so observability can wait for readiness instead of guessing: sampling stream state
// before the consumer groups exist cannot report a meaningful backlog, and reporting zero for a
// stream that has a backlog is the one answer an operator must not get.
func (r *Runner) SetOnReady(callback func()) {
	r.onReady = callback
}

// Streams returns the configured streams.
func (r *Runner) Streams() []StreamConfig {
	out := make([]StreamConfig, len(r.streams))
	copy(out, r.streams)
	return out
}

// Run starts every stream worker and blocks until the context is cancelled or one
// worker fails.
//
// Every worker stops before Run returns, so a caller can close the connection pool
// immediately afterwards without racing an in-flight command.
func (r *Runner) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := make([]*Worker, 0, len(r.streams))
	for _, stream := range r.streams {
		worker, err := New(r.client, r.handler, stream.toConfig())
		if err != nil {
			return fmt.Errorf("runner: create worker for %s: %w", stream.Stream, err)
		}
		worker.SetDeadLetterRecorder(r.deadLetterRecorder)
		worker.SetDeadLetterGuard(r.deadLetterGuard)
		worker.SetObserver(r.observer)
		worker.SetLogger(r.logger)
		workers = append(workers, worker)
	}

	// Every worker is prepared before any of them serves. Preparation is what creates the consumer
	// groups and reclaims pending work, so doing it first means the readiness callback below is
	// truthful and a failure here stops the process before it starts consuming.
	for index, stream := range r.streams {
		worker := workers[index]
		if err := worker.Prepare(runCtx); err != nil {
			cancel()
			// A preparation that a shutdown interrupted is a clean stop, not a failure to start: the
			// process was already on its way out when the group was being created.
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("runner: prepare stream %s: %w", stream.Stream, err)
		}
		if r.onStart != nil {
			r.onStart(stream)
		}
	}
	if r.onReady != nil {
		r.onReady()
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(workers))

	for index, stream := range r.streams {
		worker := workers[index]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.Serve(runCtx); err != nil && runCtx.Err() == nil {
				// Report the first real failure and stop the other streams, so the process fails fast
				// instead of serving a partial chain.
				select {
				case errs <- fmt.Errorf("stream %s: %w", stream.Stream, err):
				default:
				}
				cancel()
			}
		}()
	}

	// Wait for every worker, then surface the first error if there was one.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-runCtx.Done():
		// A cancellation still needs the workers to finish their current command.
		<-done
	}

	// A worker failure is surfaced even though the cancellation it triggered makes the
	// other workers return cleanly.
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}
