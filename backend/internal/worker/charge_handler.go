package worker

import (
	"context"
	"fmt"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// ChargeEventApplier applies a charge or order event to the authoritative domain
// state.
//
// PostgreSQL is the source of truth (contract 4.3) and backend/internal/order belongs
// to the A line, so the B line defines this contract and the A line supplies the
// implementation. attempt is the delivery count, which lets the implementation tell a
// first application from a retry.
//
// Apply must be idempotent for one event_id, or must perform its work inside a
// transaction that is guarded by the same key: the pipeline guarantees Apply is not
// called for an event the consumption store already accepted, but it cannot undo a
// partially applied change.
type ChargeEventApplier interface {
	Apply(ctx context.Context, e event.Event, attempt int) error
}

// ChargeHandlerConfig configures the charge-event handler.
type ChargeHandlerConfig struct {
	// Consumer identifies this worker in the consumption record, so an operator can
	// see which process handled an event.
	Consumer string
	// Scope namespaces the duplicate-guard keys. It must be stable per handler so a
	// restart reuses the same namespace.
	Scope string
	// Types lists the event types this handler accepts. Anything else is a permanent
	// failure, because no retry can make the handler understand it.
	Types []event.Type
}

// DefaultChargeEventTypes are the charge and order lifecycle events the handler
// accepts by default, matching the frozen event list in the migration contract.
func DefaultChargeEventTypes() []event.Type {
	return []event.Type{
		event.OrderCreated,
		event.ChargeStartRequested,
		event.ChargeStarted,
		event.ChargeStopRequested,
		event.ChargeStopped,
		event.OrderCompleted,
	}
}

// ChargeHandler consumes charge and order lifecycle events.
//
// It owns the reliability semantics, not the domain mutation: validate, suppress
// duplicates, apply, and record the outcome. Which event types arrive is decided by the
// router, and this handler refuses anything outside its declared set so a
// misconfiguration fails loudly instead of being silently ignored.
type ChargeHandler struct {
	applier ChargeEventApplier
	pipe    pipeline
	types   map[event.Type]struct{}
}

// NewChargeHandler validates its inputs.
func NewChargeHandler(applier ChargeEventApplier, store event.ConsumptionStore, guard DuplicateGuard, config ChargeHandlerConfig) (*ChargeHandler, error) {
	if applier == nil {
		return nil, fmt.Errorf("charge event applier is required")
	}
	if config.Consumer == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	types := config.Types
	if len(types) == 0 {
		types = DefaultChargeEventTypes()
	}
	accepted := make(map[event.Type]struct{}, len(types))
	for _, t := range types {
		if t == "" {
			return nil, fmt.Errorf("charge handler event type must not be empty")
		}
		accepted[t] = struct{}{}
	}
	pipe, err := newPipeline(store, guard, config.Scope)
	if err != nil {
		return nil, err
	}
	return &ChargeHandler{applier: applier, pipe: pipe, types: accepted}, nil
}

// Handle implements Handler for callers that have no transport metadata.
func (h *ChargeHandler) Handle(ctx context.Context, e event.Event) error {
	return h.HandleDelivery(ctx, e, DeliveryInfo{})
}

// HandleDelivery consumes one charge event.
func (h *ChargeHandler) HandleDelivery(ctx context.Context, e event.Event, delivery DeliveryInfo) error {
	if _, ok := h.types[e.EventType]; !ok {
		return Permanentf("%w: %s", ErrUnknownEventType, e.EventType)
	}
	// A malformed envelope is permanent: the bytes will not become valid on a retry.
	if err := e.Validate(); err != nil {
		return Permanent(err)
	}
	return h.pipe.run(ctx, e, delivery, func(ctx context.Context, attempt int) error {
		return h.applier.Apply(ctx, e, attempt)
	})
}
