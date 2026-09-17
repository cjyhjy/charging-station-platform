package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/worker"
)

// These tests cover the adapters that connect the worker to the charger gateway: what is sent,
// what a gateway answer means, and which failures are worth retrying. They need no database -
// the appliers' database behaviour is covered by the end-to-end test - so they run everywhere.

// fakeCompletion records command completions instead of writing them to PostgreSQL.
type fakeCompletion struct {
	mu      sync.Mutex
	calls   []completionCall
	failure error
}

type completionCall struct {
	commandNo string
	orderNo   string
	chargerID int64
	action    string
	result    string
	traceID   string
}

func (f *fakeCompletion) RecordChargerCommandResult(_ context.Context, commandNo, orderNo string, chargerID int64, action, result, traceID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure != nil {
		return false, f.failure
	}
	f.calls = append(f.calls, completionCall{commandNo: commandNo, orderNo: orderNo, chargerID: chargerID, action: action, result: result, traceID: traceID})
	return true, nil
}

func (f *fakeCompletion) recorded() []completionCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]completionCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func newTestDispatcher(t *testing.T, server *httptest.Server, completion commandCompletionRecorder) *httpCommandDispatcher {
	t.Helper()
	base, err := parseGatewayURL(server.URL)
	if err != nil {
		t.Fatalf("parse gateway url: %v", err)
	}
	return &httpCommandDispatcher{
		client:     server.Client(),
		baseURL:    base,
		completion: completion,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func testCommand() worker.ChargerCommand {
	return worker.ChargerCommand{CommandID: "cmd_01", ChargerID: "12", Action: worker.CommandRestart, TraceID: "trace_01"}
}

// A successful dispatch posts the frozen command payload and records the device outcome, which
// is what produces CHARGER_COMMAND_COMPLETED.
func TestDispatchSendsTheCommandAndRecordsTheOutcome(t *testing.T) {
	var (
		mu      sync.Mutex
		paths   []string
		body    map[string]string
		headers http.Header
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		headers = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"command_id":"cmd_01","charger_id":"12","status":"COMPLETED"}`))
	}))
	defer server.Close()

	completion := &fakeCompletion{}
	dispatcher := newTestDispatcher(t, server, completion)
	if err := dispatcher.Dispatch(context.Background(), testCommand(), 1); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/chargers/12/commands" {
		t.Fatalf("unexpected request path %v", paths)
	}
	if body["command_id"] != "cmd_01" || body["charger_id"] != "12" || body["action"] != "RESTART" {
		t.Fatalf("unexpected request body %v", body)
	}
	// The command id is the gateway's idempotency key, so a retry cannot restart a charger twice.
	if headers.Get("Idempotency-Key") != "cmd_01" {
		t.Fatalf("expected the command id as the idempotency key, got %q", headers.Get("Idempotency-Key"))
	}

	recorded := completion.recorded()
	if len(recorded) != 1 {
		t.Fatalf("expected one recorded outcome, got %d", len(recorded))
	}
	if recorded[0].commandNo != "cmd_01" || recorded[0].chargerID != 12 || recorded[0].result != "COMPLETED" {
		t.Fatalf("unexpected completion %+v", recorded[0])
	}
	if recorded[0].traceID != "trace_01" {
		t.Fatalf("expected the event trace id to be carried, got %q", recorded[0].traceID)
	}
}

// A charge command must carry the order it belongs to, because the gateway echoes the order
// number back on the receipt and that receipt is what advances the order. A station-level RESTART
// has no order, and an empty order_no in its body would suggest it did.
func TestDispatchCarriesTheOrderNumberForAChargeCommand(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []map[string]string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"COMPLETED"}`))
	}))
	defer server.Close()

	completion := &fakeCompletion{}
	dispatcher := newTestDispatcher(t, server, completion)

	start := worker.ChargerCommand{
		CommandID: "cmd_start", ChargerID: "12", OrderNo: "ORD20240101001",
		Action: worker.CommandStartCharging, TraceID: "trace_02",
	}
	if err := dispatcher.Dispatch(context.Background(), start, 1); err != nil {
		t.Fatalf("Dispatch(start) error = %v", err)
	}
	if err := dispatcher.Dispatch(context.Background(), testCommand(), 1); err != nil {
		t.Fatalf("Dispatch(restart) error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(bodies))
	}
	if bodies[0]["order_no"] != "ORD20240101001" || bodies[0]["action"] != "START_CHARGING" {
		t.Fatalf("a charge command must name its order, got %v", bodies[0])
	}
	if _, present := bodies[1]["order_no"]; present {
		t.Fatalf("a station RESTART must not carry an order number, got %v", bodies[1])
	}
	// The recorded outcome must name the order, otherwise an explicit failure
	// could not be attributed to the order whose command was refused. The second
	// (station) dispatch records its own outcome after it.
	recorded := completion.recorded()
	if len(recorded) != 2 {
		t.Fatalf("expected both dispatches to record an outcome, got %d", len(recorded))
	}
	if recorded[0].orderNo != "ORD20240101001" || recorded[0].commandNo != "cmd_start" {
		t.Fatalf("the charge outcome was recorded against the wrong command: %+v", recorded[0])
	}
	if recorded[1].orderNo != "" {
		t.Fatalf("a station RESTART must not claim an order, got %q", recorded[1].orderNo)
	}
}

// The command never reaches the gateway without an order: a device action that no receipt can
// attribute would create work that has to be undone, so this is permanent, not retryable.
func TestDispatchRejectsAChargeCommandWithoutAnOrder(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dispatcher := newTestDispatcher(t, server, &fakeCompletion{})
	orphan := worker.ChargerCommand{
		CommandID: "cmd_orphan", ChargerID: "12", Action: worker.CommandStopCharging, TraceID: "trace_03",
	}

	err := dispatcher.Dispatch(context.Background(), orphan, 1)
	if !worker.IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("the gateway must not be called, got %d requests", got)
	}
}

// A gateway that answers 200 with a failure result is a normal device protocol outcome, not a
// transport error: the result is recorded and the dispatch does not retry.
func TestDispatchRecordsADeviceFailureAsAnOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"FAILED","detail":"device refused"}`))
	}))
	defer server.Close()

	completion := &fakeCompletion{}
	dispatcher := newTestDispatcher(t, server, completion)
	if err := dispatcher.Dispatch(context.Background(), testCommand(), 1); err != nil {
		t.Fatalf("a device-level failure is an outcome, not an error: %v", err)
	}
	recorded := completion.recorded()
	if len(recorded) != 1 || recorded[0].result != "FAILED" {
		t.Fatalf("expected the failure to be recorded, got %+v", recorded)
	}
}

// Retryable and permanent failures must be separated, because the retry budget is spent on the
// first and not on the second.
func TestDispatchClassifiesTransportFailures(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		wantRetry  bool
		wantRecord int
	}{
		{name: "gateway rejects the request", status: http.StatusBadRequest, wantRetry: false},
		{name: "charger unknown", status: http.StatusNotFound, wantRetry: false},
		{name: "gateway is broken", status: http.StatusBadGateway, wantRetry: true},
		{name: "gateway is overloaded", status: http.StatusServiceUnavailable, wantRetry: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(`{"error":"nope"}`))
			}))
			defer server.Close()

			completion := &fakeCompletion{}
			dispatcher := newTestDispatcher(t, server, completion)
			err := dispatcher.Dispatch(context.Background(), testCommand(), 1)
			if err == nil {
				t.Fatal("expected an error")
			}
			if got := worker.IsPermanent(err); got == testCase.wantRetry {
				t.Fatalf("IsPermanent() = %v, want retryable = %v (error: %v)", got, testCase.wantRetry, err)
			}
			if len(completion.recorded()) != 0 {
				t.Fatal("a failed dispatch must not record a device outcome")
			}
		})
	}
}

// A gateway that does not answer in time leaves the dispatch retryable: the command may still
// have reached the device, and the gateway's idempotency key makes the retry safe.
func TestDispatchRetriesATimeout(t *testing.T) {
	// The handler is released explicitly: a client timeout does not cancel the server-side
	// request, so waiting on the request context would hang server.Close() forever.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	base, err := parseGatewayURL(server.URL)
	if err != nil {
		t.Fatalf("parse gateway url: %v", err)
	}
	dispatcher := &httpCommandDispatcher{
		client:     &http.Client{Timeout: 50 * time.Millisecond},
		baseURL:    base,
		completion: &fakeCompletion{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = dispatcher.Dispatch(context.Background(), testCommand(), 1)
	if err == nil {
		t.Fatal("expected the timeout to surface")
	}
	if worker.IsPermanent(err) {
		t.Fatalf("a timeout must stay retryable, got permanent: %v", err)
	}
}

// A charge command that times out is a transport failure, not a device refusal. Nothing may be
// recorded for it: recording a failure would end the order (START_CHARGING) or park a charger
// (STOP_CHARGING) on the strength of a lost response rather than on anything the device said, and
// the frozen contract is explicit that a timeout is retried instead.
func TestDispatchDoesNotFabricateAFailureForAChargeCommandTimeout(t *testing.T) {
	for _, action := range []worker.CommandAction{worker.CommandStartCharging, worker.CommandStopCharging} {
		t.Run(string(action), func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				<-release
			}))
			defer func() {
				close(release)
				server.Close()
			}()

			base, err := parseGatewayURL(server.URL)
			if err != nil {
				t.Fatalf("parse gateway url: %v", err)
			}
			completion := &fakeCompletion{}
			dispatcher := &httpCommandDispatcher{
				client:     &http.Client{Timeout: 50 * time.Millisecond},
				baseURL:    base,
				completion: completion,
				logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			command := worker.ChargerCommand{
				CommandID: "cmd_timeout", ChargerID: "12", OrderNo: "ORD20240101001",
				Action: action, TraceID: "trace_timeout",
			}
			err = dispatcher.Dispatch(context.Background(), command, 1)
			if err == nil {
				t.Fatal("expected the timeout to surface")
			}
			if worker.IsPermanent(err) {
				t.Fatalf("a timeout must stay retryable, got permanent: %v", err)
			}
			if recorded := completion.recorded(); len(recorded) != 0 {
				t.Fatalf("a timeout recorded a device outcome: %+v", recorded)
			}
		})
	}
}

// A gateway that answers 200 without a verdict - "still working on it", or a status this platform
// does not know - must not be turned into a recorded outcome either. The frozen contract only
// allows an explicit success or an explicit failure to reach an order.
func TestDispatchDoesNotRecordAVerdictlessGatewayAnswer(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantRetry   bool
		wantOutcome string
	}{
		{name: "the gateway timed out", body: `{"status":"TIMED_OUT"}`, wantRetry: true},
		{name: "the device is still working", body: `{"status":"IN_PROGRESS"}`, wantRetry: true},
		{name: "the gateway accepted the request", body: `{"status":"ACCEPTED"}`, wantRetry: true},
		{name: "the gateway invented a status", body: `{"status":"REBOOTING"}`, wantRetry: false},
		{name: "an explicit refusal is an outcome", body: `{"status":"FAILED"}`, wantOutcome: "FAILED"},
		{name: "an explicit completion is an outcome", body: `{"status":"COMPLETED"}`, wantOutcome: "COMPLETED"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer server.Close()

			completion := &fakeCompletion{}
			dispatcher := newTestDispatcher(t, server, completion)
			command := worker.ChargerCommand{
				CommandID: "cmd_verdict", ChargerID: "12", OrderNo: "ORD20240101001",
				Action: worker.CommandStartCharging, TraceID: "trace_verdict",
			}
			err := dispatcher.Dispatch(context.Background(), command, 1)
			recorded := completion.recorded()

			if testCase.wantOutcome == "" {
				if err == nil {
					t.Fatalf("%s must not be dispatched as a settled outcome", testCase.body)
				}
				if worker.IsPermanent(err) == testCase.wantRetry {
					t.Fatalf("IsPermanent() = %v, want retryable = %v (error: %v)", !testCase.wantRetry, testCase.wantRetry, err)
				}
				if len(recorded) != 0 {
					t.Fatalf("a verdictless answer recorded a device outcome: %+v", recorded)
				}
				return
			}
			if err != nil {
				t.Fatalf("an explicit verdict must be recorded, got %v", err)
			}
			if len(recorded) != 1 || recorded[0].result != testCase.wantOutcome {
				t.Fatalf("expected the %s verdict to be recorded, got %+v", testCase.wantOutcome, recorded)
			}
		})
	}
}

// The device outcome is what closes the loop, so a failure to record it must retry the dispatch
// rather than acknowledge work whose result was lost.
func TestDispatchRetriesWhenTheOutcomeCannotBeRecorded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"COMPLETED"}`))
	}))
	defer server.Close()

	completion := &fakeCompletion{failure: errors.New("database unavailable")}
	dispatcher := newTestDispatcher(t, server, completion)
	err := dispatcher.Dispatch(context.Background(), testCommand(), 1)
	if err == nil {
		t.Fatal("expected the recording failure to surface")
	}
	if worker.IsPermanent(err) {
		t.Fatalf("a database failure must stay retryable, got permanent: %v", err)
	}
}

// The payload contract: what the gateway answers is mapped to a bounded outcome value, and nothing
// else is an outcome at all (BE-I-02 review). A timeout or an unknown status means the gateway has
// no verdict, and recording one as a failure is what the frozen contract forbids: it would fail an
// order the device may be charging for, or park a charger no device complained about.
func TestDeviceOutcomeMapping(t *testing.T) {
	verdicts := map[string]string{
		"COMPLETED": "COMPLETED",
		"completed": "COMPLETED",
		"SUCCESS":   "COMPLETED",
		"succeeded": "COMPLETED",
		"ok":        "COMPLETED",
		"FAILED":    "FAILED",
		"failure":   "FAILED",
		"error":     "FAILED",
		"rejected":  "FAILED",
	}
	for raw, want := range verdicts {
		got, err := deviceOutcome(deviceCommandResponse{Status: raw})
		if err != nil {
			t.Fatalf("deviceOutcome(%q) error = %v", raw, err)
		}
		if got != want {
			t.Fatalf("deviceOutcome(%q) = %q, want %q", raw, got, want)
		}
	}

	// No verdict yet: retryable, and nothing may be recorded for it.
	for _, pending := range []string{"TIMED_OUT", "TIMEOUT", "PENDING", "ACCEPTED", "IN_PROGRESS", "PROCESSING", "QUEUED"} {
		got, err := deviceOutcome(deviceCommandResponse{Status: pending})
		if err == nil {
			t.Fatalf("deviceOutcome(%q) = %q, want an error: it is not a device outcome", pending, got)
		}
		if worker.IsPermanent(err) {
			t.Fatalf("deviceOutcome(%q) = %v, want a retryable error", pending, err)
		}
	}
	// A status outside the contract is a protocol violation the operator has to fix, not something
	// to guess at: it is refused permanently and dead-lettered instead of retried forever.
	for _, unknown := range []string{"REBOOTING", "UNKNOWN", "MAYBE", "0"} {
		got, err := deviceOutcome(deviceCommandResponse{Status: unknown})
		if err == nil {
			t.Fatalf("deviceOutcome(%q) = %q, want an error", unknown, got)
		}
		if !worker.IsPermanent(err) {
			t.Fatalf("deviceOutcome(%q) = %v, want a permanent error", unknown, err)
		}
	}

	// The status field is the verdict; a stale result value cannot override it.
	if _, err := deviceOutcome(deviceCommandResponse{Status: "TIMED_OUT", Result: "FAILED"}); err == nil {
		t.Fatal("a TIMED_OUT status must not be read as a failure through the result field")
	}
	// The result field is consulted only when the status is absent.
	if got, err := deviceOutcome(deviceCommandResponse{Result: "COMPLETED"}); err != nil || got != "COMPLETED" {
		t.Fatalf("expected a result-only response to map to COMPLETED, got %q and %v", got, err)
	}

	accepted := true
	if got, err := deviceOutcome(deviceCommandResponse{Accepted: &accepted}); err != nil || got != "COMPLETED" {
		t.Fatalf("expected an accepted response to map to COMPLETED, got %q and %v", got, err)
	}
	refused := false
	if got, err := deviceOutcome(deviceCommandResponse{Accepted: &refused}); err != nil || got != "FAILED" {
		t.Fatalf("expected a refused response to map to FAILED, got %q and %v", got, err)
	}
	if _, err := deviceOutcome(deviceCommandResponse{}); err == nil {
		t.Fatal("expected an empty gateway response to be rejected")
	}
}

// The applier must not retry what retrying cannot fix, and must retry what it can.
func TestClassifyOrderError(t *testing.T) {
	retryable := errors.New("dial tcp: connection refused")
	if worker.IsPermanent(classifyOrderError(retryable, "confirm start")) {
		t.Fatal("an infrastructure error must stay retryable")
	}
	if !worker.IsPermanent(classifyOrderError(order.ErrOrderNotFound, "confirm start")) {
		t.Fatal("a missing order must be permanent")
	}
	if err := classifyOrderError(nil, "confirm start"); err != nil {
		t.Fatalf("no error must stay no error, got %v", err)
	}
}

// The charge applier rejects an event it has no application for instead of acknowledging it,
// because a type the router accepted but the adapter does not know is a wiring mistake.
func TestChargeApplierRejectsAnUnknownEventType(t *testing.T) {
	applier := orderChargeApplier{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	e, err := event.New(event.ChargerCommandCompleted, "charger", "ch_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	if err := applier.Apply(context.Background(), e, 1); !worker.IsPermanent(err) {
		t.Fatalf("expected a permanent failure for an unhandled type, got %v", err)
	}
}

// Every charge and order lifecycle event is a notification of a transaction that already
// committed, so the applier acknowledges it without applying anything a second time. Applying
// them again is the failure mode the closed-loop verification caught: the domain rejects the
// repeated transition and the event is parked.
func TestChargeApplierAcknowledgesNotificationEvents(t *testing.T) {
	applier := orderChargeApplier{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// event.New marshals the payload, so it is passed as a value rather than as bytes.
	for _, eventType := range []event.Type{
		event.OrderCreated, event.ChargeStartRequested, event.ChargeStopRequested, event.OrderCompleted,
		event.ChargeStarted, event.ChargeStopped,
	} {
		e, err := event.New(eventType, "order", "order_01", "trace", map[string]string{"orderNo": "ORD1"})
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		if err := applier.Apply(context.Background(), e, 1); err != nil {
			t.Fatalf("%s must be acknowledged without a domain change, got %v", eventType, err)
		}
	}
}

// A device confirmation whose payload cannot be read is malformed rather than a notification: no
// retry turns it into a readable one, so it is parked.
func TestChargeApplierRejectsMalformedPayloads(t *testing.T) {
	applier := orderChargeApplier{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	e, err := event.New(event.ChargeStarted, "order", "order_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	// The envelope validates, but it carries no orderNo, so the event cannot be understood.
	if err := applier.Apply(context.Background(), e, 1); !worker.IsPermanent(err) {
		t.Fatalf("expected a permanent failure for a payload without orderNo, got %v", err)
	}
}
