package observability

import (
	"strconv"
)

// Outcome is the terminal state of one delivery from the worker's point of view.
//
// The set is closed on purpose: it is used as a metric label, so adding a case is a
// deliberate change rather than something that appears because a new error path was
// written.
type Outcome string

const (
	// OutcomeSucceeded means the handler applied the event and it was acknowledged.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeDuplicate means the event was already consumed and was acknowledged without
	// being applied again. A steady trickle of these is normal under at-least-once
	// delivery; a burst means a consumer was replaced or an event was delivered twice.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeRetried means the event failed transiently and stays pending for the
	// recovery pass.
	OutcomeRetried Outcome = "retried"
	// OutcomeLeaseHeld means a live reservation lease owned by another delivery held the
	// event, so this delivery left it pending untouched.
	//
	// It is separate from OutcomeRetried because nothing failed: a burst of these means
	// another consumer owns the work, or a holder has died and its lease has not expired
	// yet. Reporting them as retries would suggest a failing handler where there is none.
	OutcomeLeaseHeld Outcome = "lease_held"
	// OutcomeDeadLettered means the event was parked on the dead-letter stream, either
	// because retrying could not fix it or because the retry budget was exhausted.
	OutcomeDeadLettered Outcome = "dead_lettered"
	// OutcomeSuperseded means the event no longer belongs to this delivery's attempt: another
	// consumer took it over - because this handler outlived its own lease - and may have applied
	// it already.
	//
	// Nothing was written, recorded or acknowledged for it. It is reported on its own because a
	// handler that outlives its lease is invisible everywhere else: it means the lease is shorter
	// than the work, or the consumer is slower than the deployment assumed, and both are worth
	// alerting on before they turn into duplicate work.
	OutcomeSuperseded Outcome = "superseded"
	// OutcomeDeadLetterHeld means this delivery could not park the event because another
	// consumer owns the dead-letter write for it, so the entry was left pending untouched.
	//
	// It is separate from OutcomeDeadLettered because the event is not parked yet, and
	// separate from OutcomeLeaseHeld because what is held is the parking write rather than
	// the processing reservation. Reporting it as parked would tell an operator that a
	// backlog exists that may never be written - the other writer's XADD can still fail.
	OutcomeDeadLetterHeld Outcome = "dead_letter_held"
)

// Observer receives the reliability facts a worker produces.
//
// The worker depends on this vocabulary rather than on the registry, so its tests can
// assert on the facts without reading metric series, and the metrics layer can change
// without touching the consumer loop.
//
// Every method must be safe for concurrent use and must not block: it is called on the
// consumer loop, and a slow observer would throttle consumption.
type Observer interface {
	// EventHandled reports one finished delivery.
	EventHandled(stream string, eventType string, outcome Outcome, attempt int)
	// EventDeadLettered reports one event parked on the dead-letter stream, with the
	// reason so an operator can separate a bad payload from an exhausted retry budget.
	EventDeadLettered(stream string, eventType string, reason string, attempt int)
	// DeadLetterSuppressed reports that a dead-letter write was skipped because the event was
	// already parked.
	//
	// It is not the same fact as EventDeadLettered and must not be folded into it: nothing was
	// written here. Reporting it separately is what lets an operator tell a retry that
	// recovered after a partial failure from a retry that duplicated work.
	DeadLetterSuppressed(stream string, eventType string)
	// PendingRecovered reports how many entries a recovery pass reclaimed, which is the
	// evidence that a restart picked its unfinished work back up.
	PendingRecovered(stream string, count int)
}

// Registry implements Observer.
var _ Observer = (*Registry)(nil)

// maxAttemptLabel is the highest attempt number reported as itself.
//
// The retry budget is configuration, not a constant: an operator who raises MaxAttempts to a
// thousand would otherwise create a thousand label values per event type, and a metric with
// unbounded cardinality is worse than no metric. Everything at or above this value is reported as
// "20+", which still answers the question the label exists for, because an event that reached its
// twentieth attempt is a problem whether its budget is 20 or 1000.
const maxAttemptLabel = 20

// attemptLabel renders an attempt number as a bounded label.
func attemptLabel(attempt int) string {
	if attempt < 1 {
		attempt = 1
	}
	if attempt >= maxAttemptLabel {
		return strconv.Itoa(maxAttemptLabel) + "+"
	}
	return strconv.Itoa(attempt)
}

// EventHandled records a finished delivery.
func (r *Registry) EventHandled(stream string, eventType string, outcome Outcome, attempt int) {
	labels := map[string]string{
		"stream":     stream,
		"event_type": eventType,
		"outcome":    string(outcome),
	}
	r.AddCounter(MetricEventsTotal, labels, 1)

	// A retry is the outcome where the attempt number is the interesting part.
	if outcome == OutcomeRetried {
		r.AddCounter(MetricRetriesTotal, map[string]string{
			"stream":     stream,
			"event_type": eventType,
			"attempt":    attemptLabel(attempt),
		}, 1)
	}
}

// EventDeadLettered records a parked event.
func (r *Registry) EventDeadLettered(stream string, eventType string, reason string, attempt int) {
	r.AddCounter(MetricDeadLetteredTotal, map[string]string{
		"stream":     stream,
		"event_type": eventType,
		"reason":     reason,
		"attempt":    attemptLabel(attempt),
	}, 1)
}

// DeadLetterSuppressed records a skipped dead-letter write.
func (r *Registry) DeadLetterSuppressed(stream string, eventType string) {
	r.AddCounter(MetricDeadLetterSuppressedTotal, map[string]string{
		"stream":     stream,
		"event_type": eventType,
	}, 1)
}

// PendingRecovered records a recovery pass.
func (r *Registry) PendingRecovered(stream string, count int) {
	if count <= 0 {
		return
	}
	r.AddCounter(MetricPendingRecoveredTotal, map[string]string{"stream": stream}, float64(count))
}

// nilObserver is the observer used when none is configured, so the worker has no nil
// checks to forget.
type nilObserver struct{}

func (nilObserver) EventHandled(string, string, Outcome, int)     {}
func (nilObserver) EventDeadLettered(string, string, string, int) {}
func (nilObserver) DeadLetterSuppressed(string, string)           {}
func (nilObserver) PendingRecovered(string, int)                  {}

// NoopObserver returns an Observer that discards everything.
func NoopObserver() Observer { return nilObserver{} }
