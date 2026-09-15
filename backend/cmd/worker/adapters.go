package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/worker"
)

// This file is the composition layer BE-I-01 exists for: it implements the B line's worker
// interfaces on top of the A line's services, so neither line has to reach into the other's
// code.
//
// It lives in the worker command because that is the process that owns the wiring: the worker
// package must keep knowing nothing about the order domain, and the order package must keep
// knowing nothing about events or streams. The adapters below are the only place the two meet.

// orderChargeApplier consumes charge and order lifecycle events.
//
// Every one of these types is a NOTIFICATION: the transactions that produce them - creating the
// order, starting it, confirming the device-side start and stop - write the new state and the
// event in the same PostgreSQL transaction, which is why the event can be trusted to describe
// something that already happened. Applying the transition again here would be a second
// application of the same change, and not a harmless one: the domain would reject it as an
// invalid transition and the consumer would park a perfectly good event on the dead-letter
// stream. That is exactly what the closed-loop verification caught when this applier briefly
// called ConfirmStart and ConfirmStop.
//
// What the handler still does for these events is what the B line owns: consume them exactly
// once (the consumption record), retry what is retryable, park what is not, and report the
// outcome. The payload is validated rather than ignored, so an event that cannot be understood is
// parked instead of being acknowledged as if it had been applied.
type orderChargeApplier struct {
	// orders is the domain service. It is deliberately not used to apply these events; it stays
	// on the adapter so the composition is documented in one place and a future downstream effect
	// has an obvious home.
	orders *order.Service
	logger *slog.Logger
}

var _ worker.ChargeEventApplier = (*orderChargeApplier)(nil)

// Apply consumes one charge or order lifecycle event.
func (a orderChargeApplier) Apply(_ context.Context, e event.Event, attempt int) error {
	switch e.EventType {
	case event.OrderCreated, event.ChargeStartRequested, event.ChargeStopRequested, event.OrderCompleted:
		a.log().Debug("order event consumed as a notification",
			"event_id", e.EventID, "event_type", string(e.EventType), "attempt", attempt)
		return nil

	case event.ChargeStarted, event.ChargeStopped:
		// The device confirmation is applied by whoever received it, and that transaction emitted
		// this event. Here it only has to be understood: an event whose payload cannot be read is
		// malformed rather than a notification.
		var payload struct {
			OrderNo string `json:"orderNo"`
		}
		if err := decodePayload(e, &payload); err != nil {
			return worker.Permanent(err)
		}
		if strings.TrimSpace(payload.OrderNo) == "" {
			return worker.Permanent(fmt.Errorf("%s event %s is missing orderNo", e.EventType, e.EventID))
		}
		a.log().Debug("device confirmation consumed as a notification",
			"event_id", e.EventID, "event_type", string(e.EventType), "order_no", payload.OrderNo)
		return nil

	default:
		// A type the router accepted but this adapter does not know is a wiring mistake, not a
		// transient failure: retrying will not help.
		return worker.Permanentf("order applier: no application for event type %q", e.EventType)
	}
}

func (a orderChargeApplier) log() *slog.Logger {
	if a.logger == nil {
		return slog.Default()
	}
	return a.logger
}

// orderCommandResultApplier applies the device outcome of a command to the domain.
type orderCommandResultApplier struct {
	orders *order.Service
}

var _ worker.CommandResultApplier = (*orderCommandResultApplier)(nil)

// ApplyCommandResult records that a charger finished a command.
func (a orderCommandResultApplier) ApplyCommandResult(ctx context.Context, e event.Event, attempt int) error {
	var payload struct {
		CommandID string `json:"command_id"`
		ChargerID string `json:"charger_id"`
		Action    string `json:"action"`
		Result    string `json:"result"`
	}
	if err := decodePayload(e, &payload); err != nil {
		return worker.Permanent(err)
	}
	chargerID, err := strconv.ParseInt(strings.TrimSpace(payload.ChargerID), 10, 64)
	if err != nil || chargerID < 1 {
		return worker.Permanent(fmt.Errorf("command completion %s carries an unusable charger_id %q", e.EventID, payload.ChargerID))
	}
	// The transaction that emitted this event already applied the outcome, and the update is
	// guarded, so applying it twice changes nothing and reports success. The result travels with
	// it because the domain must not undo a FAULT: only a COMPLETED restart returns the charger to
	// IDLE, everything else leaves it parked for an operator.
	if _, err := a.orders.CompleteChargerCommand(ctx, chargerID,
		strings.ToUpper(strings.TrimSpace(payload.Action)),
		strings.ToUpper(strings.TrimSpace(payload.Result))); err != nil {
		return classifyOrderError(err, "complete charger command")
	}
	return nil
}

// decodePayload decodes the event payload, rejecting one that is not an object.
func decodePayload(e event.Event, target any) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("event %s has an empty payload", e.EventID)
	}
	if err := json.Unmarshal(e.Payload, target); err != nil {
		return fmt.Errorf("decode %s payload for event %s: %w", e.EventType, e.EventID, err)
	}
	return nil
}

// classifyOrderError separates "retrying cannot fix this" from "try again".
//
// A missing order, an invalid transition and a malformed command are permanent: the payload or
// the domain state will be exactly the same on the next attempt, and retrying only burns the
// budget before parking the event anyway. Everything else - a database that is momentarily
// unreachable, a lock timeout - is left retryable.
func classifyOrderError(err error, operation string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, order.ErrOrderNotFound),
		errors.Is(err, order.ErrInvalidStateTransition),
		errors.Is(err, order.ErrInvalidChargerID),
		errors.Is(err, order.ErrInvalidOrderNo),
		errors.Is(err, order.ErrInsufficientBalance),
		errors.Is(err, order.ErrUserFrozen),
		errors.Is(err, order.ErrChargerUnavailable):
		return worker.Permanentf("%s: %w", operation, err)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

// httpCommandDispatcher sends a device command to the charger gateway over HTTP.
//
// It is a real HTTP client, not a stub: the first phase's gateway is reached over HTTP, and a
// dispatcher that only pretends to send would make the command half of the loop untested. The
// mock gateway in cmd/mock-gateway speaks the same contract for development.
type httpCommandDispatcher struct {
	client  *http.Client
	baseURL *url.URL
	// completion records the device outcome in the domain, which is what produces
	// CHARGER_COMMAND_COMPLETED. It may be nil in a deployment that has no command completion
	// path, but then the loop is not closed and the module says so.
	completion commandCompletionRecorder
	traceID    func(context.Context) string
	// logger records what was dispatched and what the device answered; a device command is an
	// action with physical consequences, so every one of them belongs in the log.
	logger *slog.Logger
}

// commandCompletionRecorder records a device outcome and emits its completion event.
//
// The order number travels with the outcome because the platform has to attribute a failure to the
// order whose command it sent: a START_CHARGING the device refused fails that order, and the
// dispatcher is the only place that still knows which order the command was for.
type commandCompletionRecorder interface {
	RecordChargerCommandResult(ctx context.Context, commandNo, orderNo string, chargerID int64, action, result, traceID string) (bool, error)
}

var _ worker.CommandDispatcher = (*httpCommandDispatcher)(nil)

// deviceCommandRequest is the body sent to the gateway.
type deviceCommandRequest struct {
	CommandID string `json:"command_id"`
	ChargerID string `json:"charger_id"`
	// OrderNo is present for the charge actions and absent for the station-level restart.
	OrderNo string `json:"order_no,omitempty"`
	Action  string `json:"action"`
	TraceID string `json:"trace_id,omitempty"`
}

// deviceCommandResponse is what the gateway answers. A gateway that reports a failure in a 200
// response is normal for device protocols, so the result field decides, not only the status.
type deviceCommandResponse struct {
	Status   string `json:"status"`
	Detail   string `json:"detail"`
	Result   string `json:"result"`
	Accepted *bool  `json:"accepted"`
}

// Dispatch sends one command and records the outcome.
func (d *httpCommandDispatcher) Dispatch(ctx context.Context, command worker.ChargerCommand, attempt int) error {
	if d.client == nil || d.baseURL == nil {
		return worker.Permanent(errors.New("command dispatcher: gateway address is not configured"))
	}
	if command.Action.RequiresOrderNo() && strings.TrimSpace(command.OrderNo) == "" {
		// The command never reaches the gateway without the order it belongs to: a receipt that
		// cannot be tied to an order could not advance one, so sending it would only create work
		// that has to be undone.
		return worker.Permanentf("dispatch command %s: %s requires an order number", command.CommandID, command.Action)
	}
	payload, err := json.Marshal(deviceCommandRequest{
		CommandID: command.CommandID,
		ChargerID: command.ChargerID,
		OrderNo:   command.OrderNo,
		Action:    string(command.Action),
		TraceID:   command.TraceID,
	})
	if err != nil {
		return worker.Permanent(fmt.Errorf("encode device command %s: %w", command.CommandID, err))
	}
	endpoint := d.baseURL.JoinPath("chargers", command.ChargerID, "commands")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return worker.Permanent(fmt.Errorf("build device command request: %w", err))
	}
	request.Header.Set("Content-Type", "application/json")
	// The command id is the idempotency key the gateway uses, so a retry after a timeout does
	// not restart a charger twice.
	request.Header.Set("Idempotency-Key", command.CommandID)

	response, err := d.client.Do(request)
	if err != nil {
		// A timeout or a refused connection may well succeed on the next attempt.
		return fmt.Errorf("dispatch command %s: %w", command.CommandID, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("read gateway response for %s: %w", command.CommandID, err)
	}
	if response.StatusCode >= 400 && response.StatusCode < 500 {
		// A gateway that rejects the request itself will reject it again.
		return worker.Permanentf("dispatch command %s: gateway answered %d: %s", command.CommandID, response.StatusCode, strings.TrimSpace(string(body)))
	}
	if response.StatusCode >= 500 {
		return fmt.Errorf("dispatch command %s: gateway answered %d: %s", command.CommandID, response.StatusCode, strings.TrimSpace(string(body)))
	}

	result := deviceCommandResponse{}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			return worker.Permanentf("dispatch command %s: gateway response is not JSON: %v", command.CommandID, err)
		}
	}
	outcome, err := deviceOutcome(result)
	if err != nil {
		// The error already carries its classification: a missing verdict stays retryable, an
		// unknown status is permanent, and wrapping keeps whichever one it is.
		return fmt.Errorf("dispatch command %s: %w", command.CommandID, err)
	}

	// The dispatch reached the device, so this is where the outcome enters the domain - and the
	// only place that can emit CHARGER_COMMAND_COMPLETED with the state change it describes.
	if d.completion != nil {
		chargerID, err := strconv.ParseInt(strings.TrimSpace(command.ChargerID), 10, 64)
		if err != nil || chargerID < 1 {
			return worker.Permanentf("dispatch command %s: unusable charger id %q", command.CommandID, command.ChargerID)
		}
		traceID := command.TraceID
		if traceID == "" && d.traceID != nil {
			traceID = d.traceID(ctx)
		}
		if _, err := d.completion.RecordChargerCommandResult(ctx, command.CommandID, command.OrderNo, chargerID, string(command.Action), outcome, traceID); err != nil {
			// The device answered but the platform could not record it. Retrying the dispatch is
			// safe: the gateway is idempotent on the command id, and the completion write is
			// guarded.
			return fmt.Errorf("record charger command result %s: %w", command.CommandID, err)
		}
	}
	return nil
}

// parseGatewayURL validates the configured gateway address.
//
// It is one function rather than inline logic so the rule is identical wherever it is applied:
// only an http(s) URL is acceptable, and there is no default, because a dispatcher that falls
// back to a development address would send real device commands to a mock.
func parseGatewayURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("charger gateway address is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid charger gateway address %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("charger gateway address must be an http(s) URL, got %q", raw)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("charger gateway address must include a host, got %q", raw)
	}
	return parsed, nil
}

// deviceOutcome maps the gateway's answer to the frozen device outcome enum.
//
// Only an explicit verdict is an outcome, and there are exactly two of them. Everything else means
// the gateway has not told the platform what the device did, and recording it as a failure would
// break the frozen contract twice over: a START_CHARGING timeout would fail an order the device may
// well be charging for, and a STOP_CHARGING timeout would park a charger as faulty without any
// device saying so. A gateway that is still working on the command is retried; a status outside the
// contract is a protocol violation the operator has to fix, and it is refused loudly rather than
// guessed at.
func deviceOutcome(response deviceCommandResponse) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(response.Status))
	result := strings.ToUpper(strings.TrimSpace(response.Result))

	// The status field is the gateway's verdict; the result field is only consulted when the status
	// is absent, so a leftover result value cannot override a verdict such as TIMED_OUT.
	verdict := status
	if verdict == "" {
		verdict = result
	}
	if verdict == "" {
		if response.Accepted == nil {
			return "", fmt.Errorf("gateway response has neither status nor result: no device verdict to record")
		}
		if *response.Accepted {
			return order.CommandResultCompleted, nil
		}
		return order.CommandResultFailed, nil
	}

	switch verdict {
	case order.CommandResultCompleted, "SUCCESS", "SUCCEEDED", "OK":
		return order.CommandResultCompleted, nil
	case order.CommandResultFailed, "FAILURE", "ERROR", "REJECTED":
		return order.CommandResultFailed, nil
	}

	if pendingDeviceStatus(verdict) {
		// The gateway has no verdict yet. The frozen contract is explicit: this is retried, and no
		// failure may be invented for it. The retry is safe because the command id is the gateway's
		// idempotency key, so a retry is answered with the stored verdict once there is one.
		return "", fmt.Errorf("gateway has no device verdict yet (%s): retry the command", verdict)
	}
	return "", worker.Permanentf("gateway answered an unknown device status %q: it is not a device outcome", verdict)
}

// pendingDeviceStatus lists the statuses that mean "no verdict yet" rather than "the device
// refused". They are the ones a real gateway reports while it is still talking to the device, and
// none of them may be turned into a recorded failure.
func pendingDeviceStatus(status string) bool {
	switch status {
	case "TIMED_OUT", "TIMEOUT", "PENDING", "ACCEPTED", "IN_PROGRESS", "PROCESSING", "QUEUED", "RETRYING":
		return true
	}
	return false
}
