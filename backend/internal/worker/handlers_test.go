package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// stubApplier records what the domain would have seen.
type stubApplier struct {
	calls   int
	last    event.Event
	attempt int
	err     error
	// results records ApplyCommandResult calls separately from Apply.
	resultCalls int
}

func (a *stubApplier) Apply(_ context.Context, e event.Event, attempt int) error {
	a.calls++
	a.last = e
	a.attempt = attempt
	return a.err
}

func (a *stubApplier) ApplyCommandResult(_ context.Context, e event.Event, attempt int) error {
	a.resultCalls++
	a.last = e
	a.attempt = attempt
	return a.err
}

// stubDispatcher records the dispatched commands.
type stubDispatcher struct {
	calls   int
	last    ChargerCommand
	attempt int
	err     error
}

func (d *stubDispatcher) Dispatch(_ context.Context, command ChargerCommand, attempt int) error {
	d.calls++
	d.last = command
	d.attempt = attempt
	return d.err
}

func newChargeHandlerFixture(t *testing.T, applier ChargeEventApplier, guard DuplicateGuard) (*ChargeHandler, *event.MemoryConsumptionStore) {
	t.Helper()
	store := event.NewMemoryConsumptionStore()
	handler, err := NewChargeHandler(applier, store, guard, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	return handler, store
}

func TestNewChargeHandlerValidatesInputs(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	if _, err := NewChargeHandler(nil, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "s"}); err == nil {
		t.Fatal("expected a nil applier to be rejected")
	}
	if _, err := NewChargeHandler(&stubApplier{}, store, nil, ChargeHandlerConfig{Scope: "s"}); err == nil {
		t.Fatal("expected a missing consumer to be rejected")
	}
	if _, err := NewChargeHandler(&stubApplier{}, nil, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "s"}); err == nil {
		t.Fatal("expected a nil store to be rejected")
	}
	if _, err := NewChargeHandler(&stubApplier{}, store, nil, ChargeHandlerConfig{Consumer: "c1"}); err == nil {
		t.Fatal("expected a missing scope to be rejected")
	}
	if _, err := NewChargeHandler(&stubApplier{}, store, nil, ChargeHandlerConfig{
		Consumer: "c1", Scope: "s", Types: []event.Type{""},
	}); err == nil {
		t.Fatal("expected an empty event type to be rejected")
	}
}

func TestChargeHandlerAppliesAcceptedTypes(t *testing.T) {
	applier := &stubApplier{}
	handler, store := newChargeHandlerFixture(t, applier, nil)
	e := newChargeStartedEvent(t, "evt_apply")

	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{Attempt: 1}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("expected 1 apply, got %d", applier.calls)
	}
	if applier.last.EventID != e.EventID {
		t.Fatalf("expected the event to reach the domain, got %s", applier.last.EventID)
	}
	if entry, _ := store.Entry(e.EventID); entry.Outcome != event.OutcomeSucceeded {
		t.Fatalf("expected success recorded, got %s", entry.Outcome)
	}
}

// An event type the handler does not own is permanent: retrying cannot make the handler
// understand it, and silently ignoring it would lose the event.
func TestChargeHandlerRejectsUndeclaredTypesPermanently(t *testing.T) {
	applier := &stubApplier{}
	handler, _ := newChargeHandlerFixture(t, applier, nil)

	other, err := event.New(event.ChargerCommandRequested, "charger", "ch_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	err = handler.HandleDelivery(context.Background(), other, DeliveryInfo{})
	if !IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if applier.calls != 0 {
		t.Fatalf("the domain must not see an undeclared type, got %d calls", applier.calls)
	}
}

func TestChargeHandlerRejectsAMalformedEnvelopePermanently(t *testing.T) {
	applier := &stubApplier{}
	handler, _ := newChargeHandlerFixture(t, applier, nil)

	broken := event.Event{EventID: "evt_broken", EventType: event.ChargeStarted, Payload: json.RawMessage(`not-json`)}
	err := handler.HandleDelivery(context.Background(), broken, DeliveryInfo{})
	if !IsPermanent(err) {
		t.Fatalf("expected a permanent error for an invalid envelope, got %v", err)
	}
	if applier.calls != 0 {
		t.Fatalf("the domain must not see an invalid envelope, got %d calls", applier.calls)
	}
}

func TestChargeHandlerSkipsADuplicate(t *testing.T) {
	applier := &stubApplier{}
	handler, _ := newChargeHandlerFixture(t, applier, nil)
	e := newChargeStartedEvent(t, "evt_dup_handler")

	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{}); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{Attempt: 2})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate on redelivery, got %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("the domain must see the event once, got %d calls", applier.calls)
	}
}

// A custom type set must be honoured, so a deployment can narrow a handler's scope.
func TestChargeHandlerHonoursAnExplicitTypeSet(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{
		Consumer: "c1",
		Scope:    "narrow",
		Types:    []event.Type{event.ChargeStopped},
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	if err := handler.HandleDelivery(context.Background(), newChargeStartedEvent(t, "evt_out_of_scope"), DeliveryInfo{}); !IsPermanent(err) {
		t.Fatalf("expected ChargeStarted to be rejected, got %v", err)
	}

	stopped, err := event.New(event.ChargeStopped, "order", "order_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	if err := handler.HandleDelivery(context.Background(), stopped, DeliveryInfo{}); err != nil {
		t.Fatalf("expected ChargeStopped to be accepted, got %v", err)
	}
}

func newCommandRequestFixture(t *testing.T, dispatcher CommandDispatcher, guard DuplicateGuard) *CommandRequestHandler {
	t.Helper()
	handler, err := NewCommandRequestHandler(dispatcher, event.NewMemoryConsumptionStore(), guard, CommandHandlerConfig{
		Consumer: "c1", Scope: "charger-command",
	})
	if err != nil {
		t.Fatalf("new command request handler: %v", err)
	}
	return handler
}

func newCommandRequestEvent(t *testing.T, id, payload string) event.Event {
	t.Helper()
	e, err := event.New(event.ChargerCommandRequested, "charger", "ch_01", "trace_cmd", json.RawMessage(payload))
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	e.EventID = id
	return e
}

func TestNewCommandRequestHandlerValidatesInputs(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	if _, err := NewCommandRequestHandler(nil, store, nil, CommandHandlerConfig{Consumer: "c1", Scope: "s"}); err == nil {
		t.Fatal("expected a nil dispatcher to be rejected")
	}
	if _, err := NewCommandRequestHandler(&stubDispatcher{}, store, nil, CommandHandlerConfig{Scope: "s"}); err == nil {
		t.Fatal("expected a missing consumer to be rejected")
	}
	if _, err := NewCommandRequestHandler(&stubDispatcher{}, nil, nil, CommandHandlerConfig{Consumer: "c1", Scope: "s"}); err == nil {
		t.Fatal("expected a nil store to be rejected")
	}
}

func TestCommandRequestHandlerDispatchesAValidCommand(t *testing.T) {
	dispatcher := &stubDispatcher{}
	handler := newCommandRequestFixture(t, dispatcher, nil)
	e := newCommandRequestEvent(t, "evt_cmd_ok", `{"command_id":"cmd_01","charger_id":"ch_01","action":"RESTART"}`)

	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{Attempt: 1}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if dispatcher.calls != 1 {
		t.Fatalf("expected 1 dispatch, got %d", dispatcher.calls)
	}
	want := ChargerCommand{CommandID: "cmd_01", ChargerID: "ch_01", Action: CommandRestart, TraceID: "trace_cmd"}
	if dispatcher.last != want {
		t.Fatalf("expected %+v, got %+v", want, dispatcher.last)
	}
	if dispatcher.attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", dispatcher.attempt)
	}
}

// A charge command names the order it belongs to (BE-I-02): the order number must survive
// parsing and reach the dispatcher, because it is what ties a device command back to an order.
func TestCommandRequestHandlerDispatchesAChargeCommandWithItsOrder(t *testing.T) {
	dispatcher := &stubDispatcher{}
	handler := newCommandRequestFixture(t, dispatcher, nil)
	e := newCommandRequestEvent(t, "evt_start_ok",
		`{"command_id":"cmd_start","charger_id":"ch_01","order_no":"ORD20240101001","action":"START_CHARGING"}`)

	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{Attempt: 1}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	want := ChargerCommand{
		CommandID: "cmd_start", ChargerID: "ch_01", OrderNo: "ORD20240101001",
		Action: CommandStartCharging, TraceID: "trace_cmd",
	}
	if dispatcher.calls != 1 || dispatcher.last != want {
		t.Fatalf("expected 1 dispatch of %+v, got %d of %+v", want, dispatcher.calls, dispatcher.last)
	}

	// STOP_CHARGING is the other half of the pair and follows the same rule, including the
	// lowercase normalisation the station RESTART already allowed.
	stop := newCommandRequestEvent(t, "evt_stop_ok",
		`{"command_id":"cmd_stop","charger_id":"ch_01","order_no":"ORD20240101001","action":"stop_charging"}`)
	if err := handler.HandleDelivery(context.Background(), stop, DeliveryInfo{}); err != nil {
		t.Fatalf("handle stop: %v", err)
	}
	if dispatcher.last.Action != CommandStopCharging || dispatcher.last.OrderNo != "ORD20240101001" {
		t.Fatalf("expected a normalised STOP_CHARGING carrying the order, got %+v", dispatcher.last)
	}
}

// A charge command without an order number would produce a device action nobody can attribute,
// so it is rejected before the dispatcher sees it and must not consume the retry budget.
func TestCommandRequestHandlerRejectsAChargeCommandWithoutAnOrder(t *testing.T) {
	for _, payload := range []string{
		`{"command_id":"cmd_01","charger_id":"ch_01","action":"START_CHARGING"}`,
		`{"command_id":"cmd_01","charger_id":"ch_01","action":"STOP_CHARGING"}`,
		`{"command_id":"cmd_01","charger_id":"ch_01","order_no":"","action":"START_CHARGING"}`,
	} {
		dispatcher := &stubDispatcher{}
		handler := newCommandRequestFixture(t, dispatcher, nil)

		err := handler.HandleDelivery(context.Background(), newCommandRequestEvent(t, "evt_no_order", payload), DeliveryInfo{})
		if !IsPermanent(err) {
			t.Fatalf("payload %s: expected a permanent error, got %v", payload, err)
		}
		if dispatcher.calls != 0 {
			t.Fatalf("payload %s: an unattributable command must not be dispatched", payload)
		}
	}
}

// A payload the handler cannot act on will not improve on a retry, so it must be
// permanent rather than burning the retry budget.
func TestCommandRequestHandlerRejectsBadPayloadsPermanently(t *testing.T) {
	// Payloads are valid JSON but not a command object. Invalid JSON cannot reach the
	// decoder at all, because the envelope validation rejects it first.
	tests := []struct {
		name    string
		payload string
	}{
		{"json array instead of object", `[1,2,3]`},
		{"json string instead of object", `"just-a-string"`},
		{"missing command id", `{"charger_id":"ch_01","action":"RESTART"}`},
		{"missing charger id", `{"command_id":"cmd_01","action":"RESTART"}`},
		{"missing action", `{"command_id":"cmd_01","charger_id":"ch_01"}`},
		{"unsupported action", `{"command_id":"cmd_01","charger_id":"ch_01","action":"LAUNCH_MISSILE"}`},
		{"blank command id", `{"command_id":"   ","charger_id":"ch_01","action":"RESTART"}`},
		{"charge command without an order", `{"command_id":"cmd_01","charger_id":"ch_01","action":"START_CHARGING"}`},
		{"stop command without an order", `{"command_id":"cmd_01","charger_id":"ch_01","action":"STOP_CHARGING"}`},
		{"blank order number on a charge command", `{"command_id":"cmd_01","charger_id":"ch_01","order_no":"   ","action":"START_CHARGING"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatcher := &stubDispatcher{}
			handler := newCommandRequestFixture(t, dispatcher, nil)
			e := newCommandRequestEvent(t, "evt_bad", test.payload)

			err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{})
			if !IsPermanent(err) {
				t.Fatalf("expected a permanent error, got %v", err)
			}
			if dispatcher.calls != 0 {
				t.Fatalf("an invalid command must not be dispatched, got %d calls", dispatcher.calls)
			}
		})
	}
}

func TestCommandRequestHandlerAcceptsALowercaseAction(t *testing.T) {
	dispatcher := &stubDispatcher{}
	handler := newCommandRequestFixture(t, dispatcher, nil)
	e := newCommandRequestEvent(t, "evt_lower", `{"command_id":"cmd_01","charger_id":"ch_01","action":"restart"}`)

	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if dispatcher.last.Action != CommandRestart {
		t.Fatalf("expected the action normalised to %s, got %s", CommandRestart, dispatcher.last.Action)
	}
}

// A dispatcher signalling an unretryable failure must keep that classification, because
// the worker uses it to choose dead-letter over retry.
func TestCommandRequestHandlerPreservesPermanentDispatchFailures(t *testing.T) {
	dispatcher := &stubDispatcher{err: Permanent(errors.New("unknown charger"))}
	handler := newCommandRequestFixture(t, dispatcher, nil)
	e := newCommandRequestEvent(t, "evt_perm_dispatch", `{"command_id":"cmd_01","charger_id":"ch_01","action":"RESTART"}`)

	err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{})
	if !IsPermanent(err) {
		t.Fatalf("expected the permanent failure to survive, got %v", err)
	}
}

func TestCommandRequestHandlerRejectsUnexpectedEventTypes(t *testing.T) {
	dispatcher := &stubDispatcher{}
	handler := newCommandRequestFixture(t, dispatcher, nil)

	other := newChargeStartedEvent(t, "evt_wrong_type")
	if err := handler.HandleDelivery(context.Background(), other, DeliveryInfo{}); !IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
}

func TestCommandCompletionHandlerAppliesTheResult(t *testing.T) {
	applier := &stubApplier{}
	handler, err := NewCommandCompletionHandler(applier, event.NewMemoryConsumptionStore(), nil, CommandHandlerConfig{
		Consumer: "c1", Scope: "charger-command-result",
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	e, err := event.New(event.ChargerCommandCompleted, "charger", "ch_01", "trace", map[string]string{"command_id": "cmd_01"})
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	if err := handler.HandleDelivery(context.Background(), e, DeliveryInfo{Attempt: 1}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if applier.resultCalls != 1 {
		t.Fatalf("expected 1 result apply, got %d", applier.resultCalls)
	}

	if err := handler.HandleDelivery(context.Background(), newChargeStartedEvent(t, "evt_wrong"), DeliveryInfo{}); !IsPermanent(err) {
		t.Fatalf("expected a permanent error for the wrong type, got %v", err)
	}
}

// The action set is a frozen contract (BE-I-02): RESTART for station maintenance, and the two
// charge actions the order transaction produces. A change here breaks the gateway and the receipt
// contract at the same time, so the set is pinned explicitly.
func TestSupportedCommandActionsIsStable(t *testing.T) {
	actions := SupportedCommandActions()
	want := []CommandAction{CommandRestart, CommandStartCharging, CommandStopCharging}
	if len(actions) != len(want) {
		t.Fatalf("expected %v, got %v", want, actions)
	}
	for index, action := range want {
		if actions[index] != action {
			t.Fatalf("expected %v, got %v", want, actions)
		}
	}

	// A charge command names its order; a station restart does not.
	if CommandRestart.RequiresOrderNo() {
		t.Fatal("RESTART is station-level and must not require an order number")
	}
	for _, action := range ChargeCommandActions() {
		if !action.RequiresOrderNo() {
			t.Fatalf("%s must require an order number", action)
		}
	}
}

func TestPermanentHelpers(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("expected Permanent(nil) to stay nil")
	}
	base := errors.New("boom")
	if !IsPermanent(Permanent(base)) {
		t.Fatal("expected a marked error to report permanent")
	}
	if IsPermanent(base) {
		t.Fatal("an unmarked error must not report permanent")
	}
	if !errors.Is(Permanent(base), base) {
		t.Fatal("expected the underlying error to stay reachable")
	}
	if !IsPermanent(fmt.Errorf("context: %w", Permanent(base))) {
		t.Fatal("expected the marker to survive wrapping")
	}
	if !IsPermanent(Permanentf("charger %s is unknown", "ch_99")) {
		t.Fatal("expected Permanentf to mark the error")
	}
}
