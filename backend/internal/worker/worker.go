// Package worker provides the Redis Streams consumer loop shared by event
// workers. Business handlers are injected so the transport can be tested with
// the in-memory Redis abstraction.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

type Handler interface {
	Handle(context.Context, event.Event) error
}

// DeliveryInfo carries the transport metadata a handler needs to be idempotent and
// observant. The attempt count is only known to the worker, so a handler that must
// record or act on it needs this.
type DeliveryInfo struct {
	Stream   string
	StreamID string
	Consumer string
	// Attempt is the delivery count reported by the transport, starting at one. A
	// value above one means the entry is a retry.
	Attempt int
}

// DeliveryAwareHandler is implemented by handlers that need DeliveryInfo.
//
// It is a separate interface so the Handler contract used by existing callers and
// tests stays unchanged: the worker prefers this method when a handler provides it
// and falls back to Handle otherwise.
type DeliveryAwareHandler interface {
	HandleDelivery(ctx context.Context, e event.Event, delivery DeliveryInfo) error
}

type HandlerFunc func(context.Context, event.Event) error

func (f HandlerFunc) Handle(ctx context.Context, e event.Event) error { return f(ctx, e) }

type Config struct {
	Stream          string
	Group           string
	Consumer        string
	Count           int
	Block           time.Duration
	PendingInterval time.Duration
	RetryAfter      time.Duration
	MaxAttempts     int
	// StartID is the position a newly created consumer group starts from.
	//
	// It defaults to "0-0", which delivers every entry already in the stream. That is the
	// only safe default for the closed loop this platform needs: the outbox publishes events
	// as soon as the API commits them, so a worker deployed after the first publish must
	// still consume that backlog. Creating the group at "$" instead would skip everything
	// published before the worker's first start, permanently and silently.
	//
	// The tradeoff is deliberate and belongs to the operator: a deployment that genuinely
	// wants to ignore pre-existing history sets "$" explicitly. Re-creating a group is
	// idempotent, so this value only takes effect the first time the group is created.
	StartID string
}

func DefaultConfig() Config {
	return Config{
		Stream:          event.StreamOrderEvent,
		Group:           "order-event-workers",
		Consumer:        "worker-1",
		Count:           10,
		Block:           500 * time.Millisecond,
		PendingInterval: 1 * time.Second,
		RetryAfter:      250 * time.Millisecond,
		MaxAttempts:     3,
		// Consume the whole backlog on first creation, so events published before the worker
		// ever started are not skipped.
		StartID: defaultGroupStartID,
	}
}

// defaultGroupStartID is the beginning of the stream.
const defaultGroupStartID = "0-0"

// DeadLetterRecorder records the dead-letter decision in the authoritative consumption store,
// in two phases: a claim that only the current attempt may take, and the terminal outcome that
// only the claim holder may write.
//
// It is not optional. Without it a worker cannot tell whether it is still the current attempt,
// so it cannot safely park or acknowledge anything: a delivery whose lease expired while its
// handler was running could overwrite the outcome of the attempt that took the event over -
// including a success - and would report completed work as dead-lettered. A worker without a
// recorder therefore refuses to serve rather than run without the guarantee.
//
// The two phases exist because the parked entry is written outside the store. The claim is
// what makes the decision atomic; the finalisation is what stops the record being overwritten
// by a generation the event has since moved past.
type DeadLetterRecorder interface {
	// BeginDeadLettered claims the dead-letter decision for a reservation.
	BeginDeadLettered(ctx context.Context, reservation event.Reservation, reason string) (event.DeadLetterClaim, error)
	// FinalizeDeadLettered records the terminal outcome, and reports whether the reservation
	// still owned the event.
	FinalizeDeadLettered(ctx context.Context, reservation event.Reservation, record event.ConsumptionRecord, reason string) (bool, error)
	// AbortDeadLettered gives the claim back when the parked entry could not be written, so the
	// retry may write it immediately instead of waiting for the claim's lease to expire.
	AbortDeadLettered(ctx context.Context, reservation event.Reservation, reason string) (bool, error)
}

// DeadLetterGuard makes the dead-letter write idempotent, and tells a delivery what it may
// conclude from a claim it could not take.
//
// Without it, a worker that parked an entry and then failed to acknowledge it would produce a
// second dead letter on the retry, so the parked backlog an operator works through would
// contain duplicates of the same event.
//
// The reservations is only valid once the write succeeded, and that is not the same as "the
// claim is taken":
//
//   - A claim that is taken by somebody else may belong to a consumer that is writing RIGHT
//     NOW. Treating it as "a dead letter already exists" is how an event is lost: this delivery
//     would record the terminal outcome and acknowledge the source entry, and if that other
//     write then fails the event is in neither stream. A claim alone therefore means nothing
//     about the outcome, and the only safe action is to leave the source entry pending.
//   - Only a published completed write lets a retry skip the write.
//
// That is why the claim reports a state rather than a boolean, and why MarkDeadLetterWritten
// exists: the transition from "being written" to "written" is the fact the retry needs.
type DeadLetterClaimState string

const (
	// DeadLetterClaimTaken means this delivery owns the write and must perform it.
	DeadLetterClaimTaken DeadLetterClaimState = "taken"
	// DeadLetterClaimWritten means a completed write exists, so only the remaining steps run.
	DeadLetterClaimWritten DeadLetterClaimState = "written"
	// DeadLetterClaimWriting means another delivery holds the write right now, and whether its
	// write will succeed is unknown.
	DeadLetterClaimWriting DeadLetterClaimState = "writing"
)

type DeadLetterGuard interface {
	// ClaimDeadLetter reports what this delivery may do about the event's parked entry.
	//
	// token identifies this claim and is required by MarkDeadLetterWritten and
	// ReleaseDeadLetter; it is empty unless the state is DeadLetterClaimTaken.
	ClaimDeadLetter(ctx context.Context, eventID string) (token string, state DeadLetterClaimState, err error)
	// MarkDeadLetterWritten publishes that the parked entry exists, which is what allows a later
	// retry to finish the remaining steps instead of writing a second copy.
	//
	// An error means the evidence could not be published, not that the write failed: the entry
	// is already in the dead-letter stream, so the caller goes on to record and acknowledge. The
	// cost of an error is a duplicate on a later retry, never a lost event.
	MarkDeadLetterWritten(ctx context.Context, eventID string, token string) error
	// ReleaseDeadLetter gives the claim back after the write failed, so a retry writes the
	// entry instead of waiting for the claim to expire.
	//
	// Releasing after a write whose outcome is unknown can park a duplicate. That trade is
	// deliberate: a duplicate dead letter is visible and an operator can delete it, while a lost
	// event is neither.
	ReleaseDeadLetter(ctx context.Context, eventID string, token string) error
}

type Worker struct {
	client  redisrepo.StreamClient
	handler Handler
	config  Config
	// deadLetterRecorder is optional. When set, every dead letter also updates the
	// authoritative consumption record.
	deadLetterRecorder DeadLetterRecorder
	// deadLetterGuard is optional. When set, the dead-letter write becomes idempotent per
	// event, so a retry after a partial failure does not park a second copy.
	deadLetterGuard DeadLetterGuard
	// observer is never nil: it defaults to a discarding implementation so the consumer
	// loop has no nil checks to forget.
	observer observability.Observer
	// logger is optional. When set, every finished delivery is logged with its trace id,
	// which is what makes an incident searchable by trace.
	logger *slog.Logger
}

// SetDeadLetterRecorder attaches a recorder for dead-lettered events. A nil recorder
// disables the extra write.
func (w *Worker) SetDeadLetterRecorder(recorder DeadLetterRecorder) {
	w.deadLetterRecorder = recorder
}

// SetDeadLetterGuard attaches the guard that makes the dead-letter write idempotent. A nil
// guard writes unconditionally, which means a retry after a partial failure can park the same
// event twice.
func (w *Worker) SetDeadLetterGuard(guard DeadLetterGuard) {
	w.deadLetterGuard = guard
}

// SetObserver attaches the metrics observer. A nil observer discards every fact.
func (w *Worker) SetObserver(observer observability.Observer) {
	if observer == nil {
		w.observer = observability.NoopObserver()
		return
	}
	w.observer = observer
}

// SetLogger attaches a logger for the worker's own event lines.
//
// The worker logs the event's trace id on every line so an operator can follow one
// business request across the async hop, which is the point of the trace id in the
// envelope.
func (w *Worker) SetLogger(logger *slog.Logger) {
	w.logger = logger
}

func New(client redisrepo.StreamClient, handler Handler, config Config) (*Worker, error) {
	if client == nil {
		return nil, fmt.Errorf("redis stream client is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("event handler is required")
	}
	if config.Stream == "" || config.Group == "" || config.Consumer == "" {
		return nil, fmt.Errorf("stream, group and consumer are required")
	}
	if config.Count <= 0 {
		config.Count = 1
	}
	if config.Block <= 0 {
		config.Block = 500 * time.Millisecond
	}
	if config.PendingInterval <= 0 {
		config.PendingInterval = time.Second
	}
	if config.RetryAfter < 0 {
		config.RetryAfter = 0
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.StartID == "" {
		config.StartID = defaultGroupStartID
	}
	return &Worker{
		client:   client,
		handler:  handler,
		config:   config,
		observer: observability.NoopObserver(),
	}, nil
}

// Run prepares the worker and then consumes until the context is cancelled.
//
// The recovery pass runs before the first read, so a restart reclaims the entries a
// previous process left pending before it starts taking new work. That is what makes a
// crash safe to restart: without it, entries delivered to the dead process would sit
// pending until something reclaimed them.
//
// It is the single call most callers want. A caller that has to know when the worker can actually
// consume - to sample stream state, or to report readiness - should call Prepare and Serve itself.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.Prepare(ctx); err != nil {
		// A preparation interrupted by shutdown is not a failure: the same cancellation that stopped
		// it would have stopped the consume loop one instant later.
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return w.Serve(ctx)
}

// Prepare establishes everything the worker needs before it can consume: the guarantees it cannot
// run without, a reachable Redis, the consumer group, and the first recovery pass that reclaims
// entries a previous process left pending.
//
// Splitting this out of Run matters for observability. Stream state can only be sampled honestly
// once the group exists: before that, lag has no meaning, so a snapshot taken earlier would either
// report nothing or, worse, report a healthy zero for a stream that actually has a backlog. A caller
// that samples stream state can therefore await Prepare rather than guess.
//
// It is also the readiness gate, which is why the dead-letter recorder is checked here rather than
// in Run: a runner prepares every stream before any of them serves, and moving this check to Run
// would let a caller that uses Prepare and Serve directly bypass it.
func (w *Worker) Prepare(ctx context.Context) error {
	if err := w.checkReadyToConsume(); err != nil {
		return err
	}
	if err := w.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping redis stream: %w", err)
	}
	// A newly created group starts from the configured position, which defaults to the
	// beginning of the stream. Creating it at the end would silently skip every event the
	// outbox published before this worker's first start.
	if err := w.client.EnsureGroup(ctx, w.config.Stream, w.config.Group, w.config.StartID); err != nil {
		return fmt.Errorf("ensure consumer group: %w", err)
	}

	// The group's start position is worth one line at startup: it decides whether a backlog that
	// was published before this process first ran will be consumed or skipped, and an operator
	// checking "why did we never process last night's events" needs it in the log.
	w.log(ctx, slog.LevelInfo, "consumer group ready",
		"stream", w.config.Stream,
		"group", w.config.Group,
		"consumer", w.config.Consumer,
		"start_id", w.config.StartID,
		"max_attempts", w.config.MaxAttempts,
	)

	// The first recovery pass belongs to preparation: it is what makes a restart reclaim its
	// predecessor's unfinished work before it takes anything new, and a caller waiting for readiness
	// should see that work accounted for.
	if err := w.recoverPending(ctx); err != nil {
		// The error is returned rather than swallowed, even when cancellation caused it: a caller
		// waiting for readiness must not be told the worker is ready when it is not. Run and the
		// runner turn a cancelled preparation into a clean stop themselves.
		return fmt.Errorf("recover pending entries: %w", err)
	}
	return nil
}

// Serve consumes until the context is cancelled. Prepare must have succeeded first.
func (w *Worker) Serve(ctx context.Context) error {
	nextPendingRecovery := time.Now().Add(w.config.PendingInterval)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if !time.Now().Before(nextPendingRecovery) {
			if err := w.recoverPending(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			nextPendingRecovery = time.Now().Add(w.config.PendingInterval)
		}

		deliveries, err := w.client.ReadGroup(ctx, redisrepo.ReadGroupOptions{
			Stream:   w.config.Stream,
			Group:    w.config.Group,
			Consumer: w.config.Consumer,
			Count:    w.config.Count,
			Block:    w.config.Block,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read stream %s: %w", w.config.Stream, err)
		}
		for _, delivery := range deliveries {
			if err := w.process(ctx, delivery); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (w *Worker) recoverPending(ctx context.Context) error {
	pending, err := w.client.Pending(ctx, w.config.Stream, w.config.Group)
	if err != nil {
		return fmt.Errorf("read pending messages: %w", err)
	}
	ids := make([]string, 0, len(pending))
	for _, message := range pending {
		if message.Idle >= w.config.RetryAfter {
			ids = append(ids, message.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	deliveries, err := w.client.Claim(ctx, w.config.Stream, w.config.Group, w.config.Consumer, ids, w.config.RetryAfter)
	if err != nil {
		return fmt.Errorf("claim pending messages: %w", err)
	}
	// Recovered work is reported separately from normal consumption: it is the evidence
	// that a restart or a replaced consumer picked its unfinished entries back up.
	if len(deliveries) > 0 {
		w.observer.PendingRecovered(w.config.Stream, len(deliveries))
		w.log(ctx, slog.LevelInfo, "recovered pending entries",
			"stream", w.config.Stream,
			"group", w.config.Group,
			"consumer", w.config.Consumer,
			"recovered", len(deliveries),
		)
	}
	for _, delivery := range deliveries {
		if err := w.process(ctx, delivery); err != nil {
			return err
		}
	}
	return nil
}

// process decodes one delivery and applies the retry policy.
//
// The decision tree is the whole point of this layer:
//
//   - an undecodable entry is dead-lettered at once, because no retry can fix it;
//   - a duplicate is acknowledged without a failure, because at-least-once delivery
//     makes duplicates normal and treating them as failures would burn the retry
//     budget and eventually dead-letter a perfectly good event;
//   - an entry held by another live reservation lease is NOT acknowledged, because
//     "somebody is handling this" is not "this is finished": if that holder has died, this
//     delivery is the only thing that can still apply the event;
//   - a permanent failure is dead-lettered at once, so it does not consume the
//     budget a transient failure needs;
//   - any other failure keeps the entry pending, and the recovery pass claims it
//     again after RetryAfter until MaxAttempts is exhausted.
//
// Dead-lettering is conditional on the delivery still being the current attempt. A handler can
// outlive its own lease, in which case another consumer owns the event now and may be applying
// it, or may already have applied it: parking it then would record completed work as
// dead-lettered, and acknowledging it would delete the only copy of work still in progress.
func (w *Worker) process(ctx context.Context, delivery redisrepo.Delivery) error {
	e, err := event.FromFields(delivery.Values)
	if err != nil {
		// There is no decoded event, so there is nothing to correlate and no event type to label
		// with. An entry that cannot be decoded never went through the pipeline, so it has no
		// reservation either; parkDeadLetter reports whatever the parking attempt achieved.
		return w.parkDeadLetter(ctx, delivery, deadLetterReasonInvalidEvent, "invalid_event: "+err.Error(), event.Event{}, delivery.DeliveryCount, event.Reservation{})
	}

	// Handlers and their downstream calls inherit the event's trace id, so every line they write is
	// searchable by the originating request.
	eventCtx := observability.WithTraceID(ctx, e.TraceID)

	// The attempt number the budget is measured against comes from the consumption store when the
	// handler reports one, because the transport's delivery count also rises for reclaims that
	// were never attempts. See attemptError.
	attempt := delivery.DeliveryCount

	err = deliver(eventCtx, w.handler, e, DeliveryInfo{
		Stream:   w.config.Stream,
		StreamID: delivery.ID,
		Consumer: w.config.Consumer,
		Attempt:  delivery.DeliveryCount,
	})
	if authoritative, ok := attemptOfError(err); ok {
		attempt = authoritative
	}
	// The reservation the attempt was granted travels with the failure, because the dead-letter
	// path has to prove it is still the current attempt before it writes or acknowledges anything.
	// A handler that does not run through the pipeline reports none, and then there is no record
	// to check against: see parkDeadLetter.
	reservation, _ := reservationOfError(err)

	switch {
	case err == nil:
		if ackErr := w.ack(ctx, delivery); ackErr != nil {
			return ackErr
		}
		w.reportHandled(e, observability.OutcomeSucceeded, attempt)
		w.log(eventCtx, slog.LevelInfo, "event consumed",
			"event_id", e.EventID, "event_type", string(e.EventType),
			"aggregate_id", e.AggregateID, "stream_id", delivery.ID,
			"attempt", attempt, "outcome", string(observability.OutcomeSucceeded))
		return nil

	case errors.Is(err, ErrDuplicate):
		if ackErr := w.ack(ctx, delivery); ackErr != nil {
			return ackErr
		}
		w.reportHandled(e, observability.OutcomeDuplicate, attempt)
		w.log(eventCtx, slog.LevelInfo, "duplicate event acknowledged",
			"event_id", e.EventID, "event_type", string(e.EventType),
			"stream_id", delivery.ID, "attempt", attempt)
		return nil

	case errors.Is(err, ErrLeaseHeld):
		// The entry is deliberately left pending and NOT acknowledged: "somebody is handling this" is
		// not "this is finished", and if that holder has died this delivery is the only thing that can
		// still apply the event.
		//
		// It is also not counted against the retry budget. The reservation this delivery was refused
		// never became an attempt, and Redis increments the transport delivery count every time the
		// recovery pass reclaims the entry while it waits.
		//
		// It is reported as its own outcome rather than as a retry: a burst of these means another
		// consumer owns the work, or a holder has died and its lease has not expired, which is a
		// different operational situation from a failing handler.
		w.reportHandled(e, observability.OutcomeLeaseHeld, attempt)
		w.log(eventCtx, slog.LevelInfo, "event held by another delivery; left pending",
			"event_id", e.EventID, "event_type", string(e.EventType),
			"stream_id", delivery.ID, "attempt", attempt)
		return nil

	case IsPermanent(err):
		return w.parkDeadLetter(eventCtx, delivery, deadLetterReasonPermanent, err.Error(), e, attempt, reservation)

	default:
		if attempt < w.config.MaxAttempts {
			// Keep the message pending. The next recovery pass will claim it after RetryAfter, which
			// gives transient failures a backoff. No transport action is needed, so the outcome can be
			// reported immediately.
			w.reportHandled(e, observability.OutcomeRetried, attempt)
			w.log(eventCtx, slog.LevelWarn, "event retry scheduled",
				"event_id", e.EventID, "event_type", string(e.EventType),
				"stream_id", delivery.ID, "attempt", attempt,
				"max_attempts", w.config.MaxAttempts, "error", err.Error())
			return nil
		}
		return w.parkDeadLetter(eventCtx, delivery, deadLetterReasonExhausted, err.Error(), e, attempt, reservation)
	}
}

// parkDeadLetter runs the dead-letter path for one delivery and reports what it actually achieved.
//
// It exists because the four outcomes need four different reports, and only two of them may be
// described as the end of the event:
//
//   - parked or skipped: the event is on the dead-letter stream. The write counter is published by
//     the dead-letter path itself, once the entry is really written and the source entry really
//     acknowledged; this reports the delivery outcome and the operator-facing line.
//   - deferred: another consumer owns the write and its outcome is unknown, so nothing is claimed
//     as terminal and the entry stays pending.
//   - superseded: the event belongs to a newer attempt, so this delivery must stay silent about it
//   - no parked entry, no record, no acknowledgement. It is reported as its own outcome because a
//     handler that outlived its lease is worth seeing: it means the lease is too short or the
//     handler too slow, and it is invisible in every other series.
func (w *Worker) parkDeadLetter(ctx context.Context, delivery redisrepo.Delivery, reason deadLetterReason, detail string, e event.Event, attempt int, reservation event.Reservation) error {
	result, err := w.deadLetter(ctx, delivery, reason, detail, e, attempt, reservation)
	if err != nil {
		return err
	}

	fields := []any{
		"event_id", e.EventID, "event_type", string(e.EventType),
		"stream_id", delivery.ID, "attempt", attempt,
		"reason", string(reason),
	}
	if e.EventID == "" {
		// An undecodable entry has no envelope to read an event id or a type from.
		fields = []any{"stream_id", delivery.ID, "attempt", attempt, "reason", string(reason)}
	}

	switch result {
	case deadLetterSuperseded:
		// Nothing was written and nothing was acknowledged: the event belongs to a newer attempt
		// that may be applying it right now.
		w.reportHandled(e, observability.OutcomeSuperseded, attempt)
		w.log(ctx, slog.LevelWarn, "delivery superseded by a newer attempt; left untouched", fields...)
		return nil
	case deadLetterDeferred:
		w.reportHandled(e, observability.OutcomeDeadLetterHeld, attempt)
		w.log(ctx, slog.LevelWarn, "dead-letter write already in flight; leaving the entry pending", fields...)
		return nil
	case deadLetterSkipped:
		// A completed write existed, so this delivery only finished the remaining steps.
		w.reportHandled(e, observability.OutcomeDeadLettered, attempt)
		w.log(ctx, slog.LevelInfo, "dead letter already parked; recorded and acknowledged", fields...)
		return nil
	}

	w.reportHandled(e, observability.OutcomeDeadLettered, attempt)
	switch reason {
	case deadLetterReasonExhausted:
		w.log(ctx, slog.LevelError, "event dead-lettered after exhausting the retry budget", append(fields, "error", detail)...)
	case deadLetterReasonInvalidEvent:
		w.log(ctx, slog.LevelWarn, "undecodable entry dead-lettered", append(fields, "error", detail)...)
	default:
		w.log(ctx, slog.LevelWarn, "event dead-lettered after a permanent failure", append(fields, "error", detail)...)
	}
	return nil
}

// deadLetterReason is why an event was parked.
//
// It is a closed type because it is used as a metric label, so an operator can separate a bad
// payload from an exhausted retry budget. The human-readable error text travels separately as the
// dead-letter detail: using it as a label would make the series set grow with every distinct error
// message, which any publisher can influence.
type deadLetterReason string

const (
	deadLetterReasonInvalidEvent deadLetterReason = "invalid_event"
	deadLetterReasonPermanent    deadLetterReason = "permanent_failure"
	deadLetterReasonExhausted    deadLetterReason = "retry_exhausted"
)

// unknownEventTypeLabel is the label used for an event type outside the frozen list.
//
// The envelope only requires event_type to be non-empty, so a publisher - or anyone able to send
// an event - can invent unbounded values. Every distinct value used as a metric label becomes a
// permanent in-memory time series, so an unbounded one is a memory-exhaustion path that needs no
// privilege at all. Anything outside the frozen event list is reported as "unknown", which bounds
// the cardinality by the contract rather than by traffic.
const unknownEventTypeLabel = "unknown"

// boundedEventTypeLabel maps an event type onto a value that is safe to keep as a metric label.
func boundedEventTypeLabel(eventType event.Type) string {
	switch eventType {
	case event.OrderCreated,
		event.ChargeStartRequested,
		event.ChargeStarted,
		event.ChargeStopRequested,
		event.ChargeStopped,
		event.OrderCompleted,
		event.ChargerCommandRequested,
		event.ChargerCommandCompleted:
		return string(eventType)
	default:
		return unknownEventTypeLabel
	}
}

func (w *Worker) reportHandled(e event.Event, outcome observability.Outcome, attempt int) {
	w.observer.EventHandled(w.config.Stream, boundedEventTypeLabel(e.EventType), outcome, attempt)
}

// log writes one worker line, tolerating an unset logger so callers do not have to guard.
func (w *Worker) log(ctx context.Context, level slog.Level, message string, args ...any) {
	if w.logger == nil {
		return
	}
	w.logger.Log(ctx, level, message, args...)
}

// deliver invokes a handler, preferring the metadata-aware entry point when the
// handler provides one.
func deliver(ctx context.Context, handler Handler, e event.Event, delivery DeliveryInfo) error {
	if aware, ok := handler.(DeliveryAwareHandler); ok {
		return aware.HandleDelivery(ctx, e, delivery)
	}
	return handler.Handle(ctx, e)
}

// attemptError carries the authoritative attempt number alongside a handler failure.
//
// The transport's delivery count is not a retry budget. An entry whose processing is held by
// another live reservation lease is left pending and reclaimed repeatedly, and every reclaim
// increments the Redis delivery counter: a five minute lease with a quarter second retry
// interval would add around a thousand deliveries that were never attempts. Measuring the
// budget against that counter means the first genuine transient failure after a lease wait goes
// straight to the dead-letter stream.
//
// The consumption store only advances its reservation count when an attempt is actually granted,
// so that is what the budget is measured against. Unwrap keeps errors.Is and errors.As working,
// so ErrDuplicate and the permanent marker still reach the worker unchanged.
type attemptError struct {
	attempt     int
	reservation event.Reservation
	err         error
}

func (e *attemptError) Error() string { return e.err.Error() }

func (e *attemptError) Unwrap() error { return e.err }

// Attempt reports the attempt number the consumption store granted.
func (e *attemptError) Attempt() int { return e.attempt }

// Reservation reports the reservation the attempt was granted, which the dead-letter path needs
// in order to prove it is still the current attempt.
func (e *attemptError) Reservation() event.Reservation { return e.reservation }

// attemptReporter is implemented by an error that knows which attempt it came from.
type attemptReporter interface {
	Attempt() int
}

// reservationReporter is implemented by an error that carries the reservation the attempt was
// granted under.
type reservationReporter interface {
	Reservation() event.Reservation
}

// reservationOfError returns the reservation the failed attempt was granted.
//
// It is what lets the dead-letter path ask the store whether this delivery is still the current
// attempt. An error without one comes from a handler that does not run through the pipeline, so
// no record exists to check and none can be overwritten.
func reservationOfError(err error) (event.Reservation, bool) {
	var reporter reservationReporter
	if !errors.As(err, &reporter) {
		return event.Reservation{}, false
	}
	reservation := reporter.Reservation()
	if reservation.EventID == "" {
		return event.Reservation{}, false
	}
	return reservation, true
}

// attemptOfError returns the authoritative attempt number when the error carries one.
//
// The second result reports whether the error carried an attempt at all, so a caller can fall
// back to the transport count for a handler that does not run through the pipeline.
func attemptOfError(err error) (int, bool) {
	var reporter attemptReporter
	if !errors.As(err, &reporter) {
		return 0, false
	}
	attempt := reporter.Attempt()
	if attempt < 1 {
		return 1, true
	}
	return attempt, true
}

// checkReadyToConsume refuses to start a worker that cannot honour the dead-letter guarantees.
//
// The dead-letter recorder is what the worker asks whether a delivery is still the current
// attempt, and that question is what stops a handler that outlived its lease from overwriting
// the outcome of the attempt that took the event over. Without it, a stale delivery can record
// applied work as dead-lettered and acknowledge the only copy of work another consumer is still
// doing, so running is not a smaller failure than not running: it is a silent corruption of the
// consumption record.
//
// This is checked here rather than in New because the recorder is attached after construction,
// and here rather than in the dead-letter path because a worker that cannot park an event safely
// must not consume at all - it would have to leave every unparkable event pending forever.
func (w *Worker) checkReadyToConsume() error {
	if w.deadLetterRecorder == nil {
		return fmt.Errorf("dead-letter recorder is required: without it the worker cannot tell whether a delivery is still the current attempt, and a stale delivery could overwrite a newer attempt's terminal outcome")
	}
	return nil
}

// ack acknowledges an entry so it leaves the pending list.
func (w *Worker) ack(ctx context.Context, delivery redisrepo.Delivery) error {
	if _, err := w.client.Ack(ctx, w.config.Stream, w.config.Group, delivery.ID); err != nil {
		return fmt.Errorf("ack stream message %s: %w", delivery.ID, err)
	}
	return nil
}

// deadLetterResult is what the dead-letter path actually achieved.
//
// The caller needs it because the four outcomes demand different reports, and only two of them
// may be described as terminal: a deferred or superseded park leaves the event nowhere near its
// end.
type deadLetterResult int

const (
	// deadLetterParked means the entry was written to the dead-letter stream, recorded and
	// acknowledged.
	deadLetterParked deadLetterResult = iota
	// deadLetterSkipped means a completed write already existed, so only the remaining steps ran
	// and no second copy was written.
	deadLetterSkipped
	// deadLetterDeferred means another delivery is writing this event's parked entry right now.
	// Nothing was recorded as terminal and nothing was acknowledged.
	deadLetterDeferred
	// deadLetterSuperseded means the event no longer belongs to this delivery's attempt: a newer
	// attempt took it over while this handler was still running, or the event already reached a
	// terminal outcome.
	//
	// Nothing may be written, recorded or acknowledged in that case, and nothing may be reported
	// as dead-lettered. A stale generation that parks the event anyway writes a dead letter for
	// work another consumer has already applied, and one that acknowledges deletes the only copy
	// of work that consumer may still be doing.
	deadLetterSuperseded
)

// deadLetter parks an entry on the dead-letter stream, records the terminal outcome and
// acknowledges the source entry, in that order.
//
// See the comment at the top of dead_letter.go for why the order is guard -> DLQ -> REC ->
// ACK and what each step protects against. The essential property is that ACK happens last:
// it is the only irreversible step, and everything an operator needs must already be durable
// before the entry can no longer be rediscovered.
//
// A deferred or superseded result is returned without an error: neither is a failure of this
// delivery. A deferred park means another consumer owns the write, and a superseded one means
// the event moved on; reporting either as an error would make normal behaviour look like a
// crash.
//
// reservation is the reservation the attempt was granted, and it is what this path uses to
// prove it is still the current attempt. It is empty only for an entry that cannot be decoded,
// which never had a reservation.
func (w *Worker) deadLetter(ctx context.Context, delivery redisrepo.Delivery, reason deadLetterReason, detail string, e event.Event, attempt int, reservation event.Reservation) (deadLetterResult, error) {
	// Step 1: claim the dead-letter decision in the authoritative store.
	//
	// This happens before anything is written, because the store is the only thing that can
	// answer "is this delivery still the current attempt?" - and the answer decides whether this
	// delivery is allowed to write at all. The Redis guard below only makes the write idempotent;
	// it cannot order two generations of the same event.
	if claim, err := w.beginDeadLetter(ctx, reservation, reason); err != nil {
		return deadLetterParked, err
	} else if claim == event.DeadLetterClaimSuperseded {
		return deadLetterSuperseded, nil
	}

	// Step 2: claim the dead-letter write for this event, so a retry after a failure below does
	// not park a second copy of the same event.
	claimToken := ""
	if w.deadLetterGuard != nil {
		token, state, err := w.deadLetterGuard.ClaimDeadLetter(ctx, e.EventID)
		if err != nil {
			return deadLetterParked, fmt.Errorf("claim dead letter for %s: %w", delivery.ID, err)
		}
		switch state {
		case DeadLetterClaimWriting:
			// Somebody else holds the claim and their write has not been published as complete.
			// Their write may still fail, and if it does, acknowledging here would leave the event
			// in neither stream. So nothing is recorded as terminal and nothing is acknowledged:
			// the entry stays pending and this path runs again once the claim is released or
			// expires.
			return deadLetterDeferred, nil
		case DeadLetterClaimWritten:
			// A completed write exists for this event, so only the remaining steps run. Nothing is
			// written twice.
			finalized, err := w.finalizeDeadLetter(ctx, reservation, e, delivery, reason, attempt)
			if err != nil {
				return deadLetterSkipped, err
			}
			if !finalized {
				// The event moved on while the parked entry was being written: this delivery must
				// not record it as dead-lettered, and must not acknowledge it either.
				return deadLetterSuperseded, nil
			}
			if err := w.ackDeadLettered(ctx, delivery); err != nil {
				return deadLetterSkipped, err
			}
			// The suppression is reported INSTEAD of a write, and only once the remaining steps
			// have really finished. Reporting both would count one event as both parked and
			// skipped, so the written total would overstate the backlog an operator works through.
			w.observer.DeadLetterSuppressed(w.config.Stream, boundedEventTypeLabel(e.EventType))
			return deadLetterSkipped, nil
		}
		claimToken = token
	}

	// Step 3: park the entry.
	values := make(map[string]string, len(delivery.Values)+6)
	for key, value := range delivery.Values {
		values[key] = value
	}
	// The reason is the bounded label an operator groups by; the detail is the free text they read.
	values["dead_letter_reason"] = string(reason)
	values["dead_letter_detail"] = detail
	values["dead_letter_attempts"] = fmt.Sprintf("%d", attempt)
	values["dead_letter_consumer"] = w.config.Consumer
	values["dead_lettered_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	// The event id is written explicitly so an operator, or a later deduplication job, can
	// identify the dead letter without re-parsing the stream entry's id.
	if e.EventID != "" {
		values["dead_letter_event_id"] = e.EventID
	}
	values["dead_letter_stream_id"] = delivery.ID
	if _, err := w.client.Add(ctx, event.StreamDeadLetter, values); err != nil {
		// The claim must not outlive a write that did not happen. Without this release the retry
		// would see the claim held, wait for it to expire, and write later than it had to.
		if w.deadLetterGuard != nil && claimToken != "" {
			if releaseErr := w.deadLetterGuard.ReleaseDeadLetter(ctx, e.EventID, claimToken); releaseErr != nil {
				return deadLetterParked, fmt.Errorf("write dead-letter message %s: %w (releasing its claim also failed: %v)", delivery.ID, err, releaseErr)
			}
		}
		if abortErr := w.abortDeadLetter(ctx, reservation, reason); abortErr != nil {
			return deadLetterParked, fmt.Errorf("write dead-letter message %s: %w (releasing its claim also failed: %v)", delivery.ID, err, abortErr)
		}
		return deadLetterParked, fmt.Errorf("write dead-letter message %s: %w", delivery.ID, err)
	}

	// Step 4: publish that the entry exists, which is what lets a later delivery finish the
	// remaining steps without writing a second copy.
	if w.deadLetterGuard != nil {
		if err := w.deadLetterGuard.MarkDeadLetterWritten(ctx, e.EventID, claimToken); err != nil {
			// Deliberately not fatal and deliberately not returned. The entry IS parked, so the
			// only consequence of a missing mark is that a later retry cannot tell and writes a
			// duplicate; failing here would instead leave a parked event unacknowledged and
			// reprocessed for no gain. The condition is not silent: the guard records it against
			// its capability's degradation counters, which the observability module publishes.
			_ = err
		}
	}

	// Step 5: record the terminal outcome, so the consumption record and the parked entry
	// agree - but only if this delivery still owns the event.
	//
	// The parked entry was written outside the store, so the lease can have expired in between,
	// a newer attempt can have taken the event over, and that attempt can have succeeded.
	// Overwriting the record then would report applied work as dead-lettered, so the store is
	// asked to confirm ownership instead of being told what to write. When it refuses, this
	// delivery goes silent: the parked copy it wrote is a duplicate (visible and deletable),
	// while acknowledging the source entry could discard work the newer attempt is still doing.
	finalized, err := w.finalizeDeadLetter(ctx, reservation, e, delivery, reason, attempt)
	if err != nil {
		return deadLetterParked, err
	}
	if !finalized {
		return deadLetterSuperseded, nil
	}

	// Step 6: acknowledge, which is only now safe.
	if err := w.ackDeadLettered(ctx, delivery); err != nil {
		return deadLetterParked, err
	}

	// The written counter is reported only here, after the entry is really in the dead-letter stream
	// and the source entry is really acknowledged. Reporting it before either would count a write
	// that a later failure sends back for another attempt.
	w.observer.EventDeadLettered(w.config.Stream, boundedEventTypeLabel(e.EventType), string(reason), attempt)
	return deadLetterParked, nil
}

// beginDeadLetter asks the store whether this delivery may still park the event.
//
// A worker without a recorder cannot ask, and refuses to serve for exactly that reason (see
// checkReadyToConsume). The unit tests that drive process directly without one therefore exercise
// the write path only, and the empty answer below grants the claim so that path keeps working:
// what enforces the guarantee in a real deployment is the refusal to serve.
func (w *Worker) beginDeadLetter(ctx context.Context, reservation event.Reservation, reason deadLetterReason) (event.DeadLetterClaim, error) {
	if w.deadLetterRecorder == nil {
		return event.DeadLetterClaimGranted, nil
	}
	claim, err := w.deadLetterRecorder.BeginDeadLettered(ctx, reservation, string(reason))
	if err != nil {
		return "", fmt.Errorf("claim dead-letter record for %s: %w", reservation.EventID, err)
	}
	return claim, nil
}

// abortDeadLetter gives the store claim back after a failed write, so the retry can park the event
// immediately rather than waiting for the claim's lease to expire.
//
// A worker without a recorder has no claim to give back, and that is the same configuration that
// cannot serve at all: see checkReadyToConsume.
func (w *Worker) abortDeadLetter(ctx context.Context, reservation event.Reservation, reason deadLetterReason) error {
	if w.deadLetterRecorder == nil {
		return nil
	}
	// A false result means the claim had already expired or moved on. That is not an error: the
	// event is recovered either by the claim's own expiry or by whichever attempt holds it now.
	_, err := w.deadLetterRecorder.AbortDeadLettered(ctx, reservation, string(reason))
	return err
}

// finalizeDeadLetter writes the terminal outcome when the store still recognises this delivery
// as the current attempt, and reports whether it did.
func (w *Worker) finalizeDeadLetter(ctx context.Context, reservation event.Reservation, e event.Event, delivery redisrepo.Delivery, reason deadLetterReason, attempt int) (bool, error) {
	if w.deadLetterRecorder == nil {
		return true, nil
	}
	record := consumptionRecordFor(e, DeliveryInfo{
		Stream:   w.config.Stream,
		StreamID: delivery.ID,
		Consumer: w.config.Consumer,
		Attempt:  attempt,
	}, attempt)
	if record.EventID == "" {
		// An undecodable entry has no event id, so there is no record to finalise: the parked
		// entry itself is the record.
		return true, nil
	}
	finalized, err := w.deadLetterRecorder.FinalizeDeadLettered(ctx, reservation, record, string(reason))
	if err != nil {
		return false, fmt.Errorf("record dead letter for %s: %w", record.EventID, err)
	}
	return finalized, nil
}

func (w *Worker) ackDeadLettered(ctx context.Context, delivery redisrepo.Delivery) error {
	if _, err := w.client.Ack(ctx, w.config.Stream, w.config.Group, delivery.ID); err != nil {
		return fmt.Errorf("ack dead-lettered message %s: %w", delivery.ID, err)
	}
	return nil
}
