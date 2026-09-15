package event

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// OutboxRecord is the storage-neutral representation of an unpublished
// outbox row. The PostgreSQL adapter is responsible for mapping its row into
// this type; the publisher deliberately does not know about SQL.
type OutboxRecord struct {
	ID            string
	EventID       string
	EventType     Type
	AggregateType string
	AggregateID   string
	OccurredAt    time.Time
	TraceID       string
	Payload       json.RawMessage
	// Stream is optional. When it is empty, Publisher derives the stream from
	// EventType. Keeping it on the record also supports event types introduced
	// by a later module without changing the publisher interface.
	Stream string
}

// ToEvent converts a storage record into the canonical event envelope used by
// Redis Streams. EventID is intentionally not generated here: an outbox row
// must retain the same identity across retries.
func (r OutboxRecord) ToEvent() (Event, error) {
	e := Event{
		EventID:       r.EventID,
		EventType:     r.EventType,
		AggregateType: r.AggregateType,
		AggregateID:   r.AggregateID,
		OccurredAt:    r.OccurredAt,
		TraceID:       r.TraceID,
		Payload:       r.Payload,
	}
	if err := e.Validate(); err != nil {
		return Event{}, fmt.Errorf("outbox record %s: %w", r.ID, err)
	}
	return e, nil
}

// StreamName returns the configured stream or derives the default stream for
// a known event type. Unknown event types must carry an explicit stream so a
// future event can be added without silently going to the wrong destination.
func (r OutboxRecord) StreamName() (string, error) {
	if stream := strings.TrimSpace(r.Stream); stream != "" {
		return stream, nil
	}
	switch r.EventType {
	case OrderCreated, OrderCompleted:
		return StreamOrderEvent, nil
	case ChargeStartRequested, ChargeStarted, ChargeStopRequested, ChargeStopped:
		return StreamChargeEvent, nil
	case ChargerCommandRequested, ChargerCommandCompleted:
		return StreamChargerCommand, nil
	default:
		return "", fmt.Errorf("outbox record %s: stream is required for event type %q", r.ID, r.EventType)
	}
}

// OutboxSource reads unpublished rows and marks a row only after the stream
// append succeeds. Implementations should make MarkPublished conditional on
// the row still being unpublished so multiple publishers remain safe.
type OutboxSource interface {
	ListUnpublished(context.Context, int) ([]OutboxRecord, error)
	MarkPublished(context.Context, string, time.Time) error
}
