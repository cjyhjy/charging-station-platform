// Package event defines the versioned event envelope shared by publishers and
// Redis Streams workers.
package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StreamOrderEvent     = "ncs:stream:order-event"
	StreamChargeEvent    = "ncs:stream:charge-event"
	StreamChargerCommand = "ncs:stream:charger-command"
	StreamNotification   = "ncs:stream:notification"
	StreamDeadLetter     = "ncs:stream:dead-letter"
)

// Type is intentionally a string so new event types can be added without
// changing the transport abstraction or the Redis adapter.
type Type string

const (
	OrderCreated            Type = "ORDER_CREATED"
	ChargeStartRequested    Type = "CHARGE_START_REQUESTED"
	ChargeStarted           Type = "CHARGE_STARTED"
	ChargeStopRequested     Type = "CHARGE_STOP_REQUESTED"
	ChargeStopped           Type = "CHARGE_STOPPED"
	OrderCompleted          Type = "ORDER_COMPLETED"
	ChargerCommandRequested Type = "CHARGER_COMMAND_REQUESTED"
	ChargerCommandCompleted Type = "CHARGER_COMMAND_COMPLETED"
)

var (
	ErrInvalidEvent = errors.New("invalid event")
	ErrInvalidID    = errors.New("invalid stream id")
)

// Event is the canonical event envelope. Payload is kept as JSON so the event
// package does not depend on domain-specific DTOs.
type Event struct {
	EventID       string          `json:"event_id"`
	EventType     Type            `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

// New creates a validated event with a UTC timestamp and a generated ID.
func New(eventType Type, aggregateType, aggregateID, traceID string, payload any) (Event, error) {
	raw := json.RawMessage(`{}`)
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return Event{}, fmt.Errorf("marshal event payload: %w", err)
		}
		raw = encoded
	}
	e := Event{
		EventID:       NewID("evt"),
		EventType:     eventType,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		OccurredAt:    time.Now().UTC(),
		TraceID:       traceID,
		Payload:       raw,
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

// NewID produces an opaque ID suitable for event_id and trace_id values.
func NewID(prefix string) string {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		// crypto/rand failure is exceptionally rare. The timestamp keeps the
		// fallback unique enough for a process-local event ID and avoids panic
		// while the caller can still persist the event.
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(random[:])
}

// Validate checks fields that are required by every publisher and consumer.
func (e Event) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(string(e.EventType)) == "" {
		return fmt.Errorf("%w: event_type is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.AggregateType) == "" {
		return fmt.Errorf("%w: aggregate_type is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.AggregateID) == "" {
		return fmt.Errorf("%w: aggregate_id is required", ErrInvalidEvent)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidEvent)
	}
	if e.Payload == nil || !json.Valid(e.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEvent)
	}
	return nil
}

// Fields converts the envelope to Redis Stream fields. Every value is a
// string because XADD is a field/value transport.
func (e Event) Fields() (map[string]string, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		"event_id":       e.EventID,
		"event_type":     string(e.EventType),
		"aggregate_type": e.AggregateType,
		"aggregate_id":   e.AggregateID,
		"occurred_at":    e.OccurredAt.UTC().Format(time.RFC3339Nano),
		"trace_id":       e.TraceID,
		"payload":        string(e.Payload),
	}, nil
}

// FromFields decodes a Redis Stream record back to an event.
func FromFields(fields map[string]string) (Event, error) {
	parse := func(name string) (string, error) {
		value, ok := fields[name]
		if !ok {
			return "", fmt.Errorf("%w: missing %s", ErrInvalidEvent, name)
		}
		return value, nil
	}
	eventID, err := parse("event_id")
	if err != nil {
		return Event{}, err
	}
	eventType, err := parse("event_type")
	if err != nil {
		return Event{}, err
	}
	aggregateType, err := parse("aggregate_type")
	if err != nil {
		return Event{}, err
	}
	aggregateID, err := parse("aggregate_id")
	if err != nil {
		return Event{}, err
	}
	occurredAtRaw, err := parse("occurred_at")
	if err != nil {
		return Event{}, err
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, occurredAtRaw)
	if err != nil {
		return Event{}, fmt.Errorf("%w: parse occurred_at: %v", ErrInvalidEvent, err)
	}
	traceID, err := parse("trace_id")
	if err != nil {
		return Event{}, err
	}
	payload, err := parse("payload")
	if err != nil {
		return Event{}, err
	}
	e := Event{
		EventID:       eventID,
		EventType:     Type(eventType),
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		OccurredAt:    occurredAt.UTC(),
		TraceID:       traceID,
		Payload:       json.RawMessage(payload),
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}
