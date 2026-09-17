package event

import (
	"errors"
	"testing"
	"time"
)

func TestEventRoundTripThroughStreamFields(t *testing.T) {
	original, err := New(ChargeStarted, "order", "order_01", "trace_01", map[string]any{"energy_wh": 1200})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	fields, err := original.Fields()
	if err != nil {
		t.Fatalf("Fields() error = %v", err)
	}
	decoded, err := FromFields(fields)
	if err != nil {
		t.Fatalf("FromFields() error = %v", err)
	}
	if decoded.EventID != original.EventID || decoded.EventType != original.EventType || decoded.AggregateID != original.AggregateID {
		t.Fatalf("round trip mismatch: %#v != %#v", decoded, original)
	}
	if string(decoded.Payload) != string(original.Payload) || !decoded.OccurredAt.Equal(original.OccurredAt) {
		t.Fatalf("round trip payload/time mismatch: %#v != %#v", decoded, original)
	}
}

func TestFromFieldsRejectsInvalidEvent(t *testing.T) {
	_, err := FromFields(map[string]string{"event_id": "evt_01"})
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	invalid := Event{EventID: "evt_01", EventType: OrderCreated, AggregateType: "order", AggregateID: "order_01", OccurredAt: time.Now(), Payload: []byte("not-json")}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected invalid payload error, got %v", err)
	}
}
