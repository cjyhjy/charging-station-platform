package observability

import (
	"context"
	"log/slog"
)

// This file carries the business trace id across a process boundary the event envelope
// cannot reach.
//
// The trace id already travels end to end inside the event envelope, so a consumer knows
// which request produced an event. What that does not cover is everything the consumer
// does afterwards: a context value lets a handler's own logging and any downstream call
// inherit the same id without passing it through every signature. The migration contract
// requires the trace id on key business operations, and this is what makes that possible
// beyond the single log line that names the event.

type traceIDKey struct{}

// WithTraceID returns a context carrying traceID.
//
// An empty id is ignored rather than stored, so an absent trace id stays absent and
// logging reports it as unknown instead of printing an empty string that looks populated.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext returns the trace id carried by ctx, or an empty string.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	traceID, _ := ctx.Value(traceIDKey{}).(string)
	return traceID
}

// ContextHandler is a slog handler that adds the context's trace id to every record.
//
// It is what turns "we include the trace id in one place" into "every line a worker
// writes while handling an event is searchable by trace id", which is the difference
// between an id that exists and an id that is usable during an incident.
type ContextHandler struct {
	slog.Handler
}

// NewContextHandler wraps handler so records carry trace_id when the context has one.
func NewContextHandler(handler slog.Handler) *ContextHandler {
	return &ContextHandler{Handler: handler}
}

// Handle adds trace_id from the context before delegating.
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		// The attribute is added to a clone so the caller's record is not mutated, which
		// matters when several handlers share one record.
		clone := record.Clone()
		clone.AddAttrs(slog.String("trace_id", traceID))
		return h.Handler.Handle(ctx, clone)
	}
	return h.Handler.Handle(ctx, record)
}

// WithAttrs implements slog.Handler.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithGroup(name)}
}
