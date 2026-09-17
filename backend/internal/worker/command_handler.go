package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// CommandAction is a device command the platform can issue.
type CommandAction string

// Device command actions.
//
// RESTART is the station-level maintenance action (the management P0 scope). START_CHARGING and
// STOP_CHARGING are the charge-flow actions: the order transaction produces them when a user starts
// or stops charging, and they must never be impersonated by RESTART - a restart aborts a pending
// start on the physical device, it does not start or stop a charge. Adding an action is a contract
// change: it has to be registered here so an unknown action fails permanently instead of being
// dispatched as something else.
const (
	CommandRestart       CommandAction = "RESTART"
	CommandStartCharging CommandAction = "START_CHARGING"
	CommandStopCharging  CommandAction = "STOP_CHARGING"
)

// ChargeCommandActions lists the actions that belong to an order. They carry an order number,
// because the gateway has to know which order a command - and the receipt it sends back - belongs
// to. RESTART is station-level and carries none.
func ChargeCommandActions() []CommandAction {
	return []CommandAction{CommandStartCharging, CommandStopCharging}
}

// RequiresOrderNo reports whether an action must name the order it belongs to.
func (a CommandAction) RequiresOrderNo() bool {
	for _, action := range ChargeCommandActions() {
		if a == action {
			return true
		}
	}
	return false
}

// SupportedCommandActions lists the actions this worker accepts. It is exported so a
// caller building a command can validate against the same set.
func SupportedCommandActions() []CommandAction {
	return []CommandAction{CommandRestart, CommandStartCharging, CommandStopCharging}
}

// ChargerCommand is a validated outbound device command.
type ChargerCommand struct {
	CommandID string
	ChargerID string
	// OrderNo names the order a charge command belongs to. It is required for START_CHARGING and
	// STOP_CHARGING and empty for the station-level RESTART, because a receipt that cannot be tied
	// to an order cannot advance one.
	OrderNo string
	Action  CommandAction
	TraceID string
}

// CommandDispatcher sends a command to a charger and reports the transport outcome.
//
// The device protocol belongs to the charger gateway, not to the B line, so the
// dispatcher is injected. A dispatcher must signal a failure that retrying cannot fix
// with worker.Permanent, so an unknown charger is dead-lettered immediately while a
// device timeout consumes the retry budget.
type CommandDispatcher interface {
	Dispatch(ctx context.Context, command ChargerCommand, attempt int) error
}

// CommandResultApplier records the outcome of a dispatched command on the domain.
//
// As with ChargeEventApplier, PostgreSQL is the source of truth and the owning domain
// is the A line's, so only the contract lives here.
type CommandResultApplier interface {
	ApplyCommandResult(ctx context.Context, e event.Event, attempt int) error
}

// chargerCommandPayload is the payload of CHARGER_COMMAND_REQUESTED.
type chargerCommandPayload struct {
	CommandID string `json:"command_id"`
	ChargerID string `json:"charger_id"`
	OrderNo   string `json:"order_no"`
	Action    string `json:"action"`
}

// CommandHandlerConfig configures the charger-command handlers.
type CommandHandlerConfig struct {
	Consumer string
	Scope    string
}

// CommandRequestHandler consumes CHARGER_COMMAND_REQUESTED and dispatches the command
// to the device.
//
// The request itself is authoritative in PostgreSQL: the A line writes the command row
// and the outbox row in one transaction, and this handler is the transport that carries
// it to the charger. Emitting CHARGER_COMMAND_COMPLETED is therefore also the domain's
// job, because it must be written with the command row in the same transaction; the
// handler only records that the dispatch happened.
type CommandRequestHandler struct {
	dispatcher CommandDispatcher
	pipe       pipeline
}

// NewCommandRequestHandler validates its inputs.
func NewCommandRequestHandler(dispatcher CommandDispatcher, store event.ConsumptionStore, guard DuplicateGuard, config CommandHandlerConfig) (*CommandRequestHandler, error) {
	if dispatcher == nil {
		return nil, fmt.Errorf("command dispatcher is required")
	}
	if config.Consumer == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	pipe, err := newPipeline(store, guard, config.Scope)
	if err != nil {
		return nil, err
	}
	return &CommandRequestHandler{dispatcher: dispatcher, pipe: pipe}, nil
}

// Handle implements Handler.
func (h *CommandRequestHandler) Handle(ctx context.Context, e event.Event) error {
	return h.HandleDelivery(ctx, e, DeliveryInfo{})
}

// HandleDelivery validates and dispatches one command request.
func (h *CommandRequestHandler) HandleDelivery(ctx context.Context, e event.Event, delivery DeliveryInfo) error {
	if e.EventType != event.ChargerCommandRequested {
		return Permanentf("%w: %s", ErrUnknownEventType, e.EventType)
	}
	if err := e.Validate(); err != nil {
		return Permanent(err)
	}
	command, err := parseChargerCommand(e)
	if err != nil {
		// A payload that cannot be parsed or names an unsupported action will not
		// improve on a retry.
		return Permanent(err)
	}
	return h.pipe.run(ctx, e, delivery, func(ctx context.Context, attempt int) error {
		return h.dispatcher.Dispatch(ctx, command, attempt)
	})
}

// parseChargerCommand decodes and validates the command payload.
func parseChargerCommand(e event.Event) (ChargerCommand, error) {
	var payload chargerCommandPayload
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		return ChargerCommand{}, fmt.Errorf("decode charger command payload: %w", err)
	}
	commandID := strings.TrimSpace(payload.CommandID)
	chargerID := strings.TrimSpace(payload.ChargerID)
	if commandID == "" {
		return ChargerCommand{}, fmt.Errorf("charger command: command_id is required")
	}
	if chargerID == "" {
		return ChargerCommand{}, fmt.Errorf("charger command: charger_id is required")
	}
	action := CommandAction(strings.TrimSpace(strings.ToUpper(payload.Action)))
	if action == "" {
		return ChargerCommand{}, fmt.Errorf("charger command: action is required")
	}
	if !isSupportedAction(action) {
		return ChargerCommand{}, fmt.Errorf("charger command: unsupported action %q, supported actions are %s", payload.Action, supportedActionList())
	}
	orderNo := strings.TrimSpace(payload.OrderNo)
	if action.RequiresOrderNo() && orderNo == "" {
		// A charge command without an order number could not be tied back to an order, so its
		// receipt would have nowhere to go. That is a malformed command, not a retryable one.
		return ChargerCommand{}, fmt.Errorf("charger command: %s requires order_no", action)
	}
	return ChargerCommand{
		CommandID: commandID,
		ChargerID: chargerID,
		OrderNo:   orderNo,
		Action:    action,
		TraceID:   e.TraceID,
	}, nil
}

func isSupportedAction(action CommandAction) bool {
	for _, supported := range SupportedCommandActions() {
		if supported == action {
			return true
		}
	}
	return false
}

func supportedActionList() string {
	names := make([]string, 0, 1)
	for _, action := range SupportedCommandActions() {
		names = append(names, string(action))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// CommandCompletionHandler consumes CHARGER_COMMAND_COMPLETED and records the device
// outcome on the domain.
type CommandCompletionHandler struct {
	applier CommandResultApplier
	pipe    pipeline
}

// NewCommandCompletionHandler validates its inputs.
func NewCommandCompletionHandler(applier CommandResultApplier, store event.ConsumptionStore, guard DuplicateGuard, config CommandHandlerConfig) (*CommandCompletionHandler, error) {
	if applier == nil {
		return nil, fmt.Errorf("command result applier is required")
	}
	if config.Consumer == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	pipe, err := newPipeline(store, guard, config.Scope)
	if err != nil {
		return nil, err
	}
	return &CommandCompletionHandler{applier: applier, pipe: pipe}, nil
}

// Handle implements Handler.
func (h *CommandCompletionHandler) Handle(ctx context.Context, e event.Event) error {
	return h.HandleDelivery(ctx, e, DeliveryInfo{})
}

// HandleDelivery applies one command completion.
func (h *CommandCompletionHandler) HandleDelivery(ctx context.Context, e event.Event, delivery DeliveryInfo) error {
	if e.EventType != event.ChargerCommandCompleted {
		return Permanentf("%w: %s", ErrUnknownEventType, e.EventType)
	}
	if err := e.Validate(); err != nil {
		return Permanent(err)
	}
	return h.pipe.run(ctx, e, delivery, func(ctx context.Context, attempt int) error {
		return h.applier.ApplyCommandResult(ctx, e, attempt)
	})
}
