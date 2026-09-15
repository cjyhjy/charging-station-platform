package worker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// Router dispatches an event to the handler registered for its type.
//
// It exists because the B line owns three streams (order events, charge events and
// charger commands) while several workers consume them. A router lets one handler
// instance serve several streams and makes an unregistered type a permanent failure
// rather than a silent drop, which is the failure mode that would otherwise lose
// events without any operator-visible signal.
type Router struct {
	mu       sync.RWMutex
	handlers map[event.Type]Handler
}

// NewRouter returns an empty router.
func NewRouter() *Router {
	return &Router{handlers: make(map[event.Type]Handler)}
}

// Register binds an event type to a handler.
//
// Registering the same type twice is an error rather than an overwrite: a duplicate
// registration means two handlers believe they own the type, and silently keeping one
// would make behaviour depend on startup order.
func (r *Router) Register(eventType event.Type, handler Handler) error {
	if eventType == "" {
		return fmt.Errorf("router: event type is required")
	}
	if handler == nil {
		return fmt.Errorf("router: handler for %s is required", eventType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[eventType]; exists {
		return fmt.Errorf("router: %s already has a handler", eventType)
	}
	r.handlers[eventType] = handler
	return nil
}

// RegisterAll binds every type in types to one handler.
func (r *Router) RegisterAll(types []event.Type, handler Handler) error {
	for _, eventType := range types {
		if err := r.Register(eventType, handler); err != nil {
			return err
		}
	}
	return nil
}

// Handle implements Handler, so a router can be handed to a Worker directly.
//
// An unregistered type is permanent: the worker dead-letters it immediately instead of
// retrying an event no handler will ever accept.
func (r *Router) Handle(ctx context.Context, e event.Event) error {
	return r.HandleDelivery(ctx, e, DeliveryInfo{})
}

// HandleDelivery implements DeliveryAwareHandler and forwards the transport metadata
// to the selected handler.
func (r *Router) HandleDelivery(ctx context.Context, e event.Event, delivery DeliveryInfo) error {
	r.mu.RLock()
	handler, ok := r.handlers[e.EventType]
	r.mu.RUnlock()
	if !ok {
		return Permanentf("%w: %s", ErrUnknownEventType, e.EventType)
	}
	return deliver(ctx, handler, e, delivery)
}

// Types lists the registered event types in a stable order, for startup logging and
// for tests.
func (r *Router) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for eventType := range r.handlers {
		names = append(names, string(eventType))
	}
	sort.Strings(names)
	return names
}

// Len reports how many event types are registered.
func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// String renders the routing table for a startup log.
func (r *Router) String() string {
	return "routes=" + strings.Join(r.Types(), ",")
}
