package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/order"
)

// The review finding: a charger command result was applied without looking at the result, so a
// restart the device reported as FAILED still returned the charger to IDLE - a device whose state
// is unknown or broken back in the allocation pool. These tests pin the outcome-dependent status
// against a real database, one charger per outcome.

// seedRestartingCharger inserts a charger that a restart command is holding.
func seedRestartingCharger(t *testing.T, db *sql.DB, ctx context.Context, stationID int64, code string) int64 {
	t.Helper()
	var chargerID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO chargers (station_id, code, connector_type, power_watt, status, price_per_kwh_cents)
		 VALUES ($1, $2, 'DC', 120000, 'RESTARTING', 120) RETURNING id`,
		stationID, code).Scan(&chargerID); err != nil {
		t.Fatalf("seed restarting charger: %v", err)
	}
	return chargerID
}

func chargerStatus(t *testing.T, db *sql.DB, ctx context.Context, chargerID int64) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM chargers WHERE id = $1`, chargerID).Scan(&status); err != nil {
		t.Fatalf("read charger status: %v", err)
	}
	return status
}

func TestChargerCommandResultDecidesTheChargerStatus(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	_, _, stationA, _, _ := orderFlowFixture(t, db, ctx, suffix)

	cases := []struct {
		name   string
		result string
		want   string
	}{
		// Only the two frozen outcomes reach the store (BE-I-02 review): the dispatcher refuses
		// everything else, and the store refuses it too rather than deciding what an unknown status
		// means. The "it must not become IDLE" guard is kept below for those values.
		{name: "a completed restart releases the charger", result: "COMPLETED", want: "IDLE"},
		{name: "a failed restart parks the charger", result: "FAILED", want: "FAULT"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// The charger code must be unique inside the station, and the subtest name is what
			// distinguishes the cases (a time-based suffix is identical within the same second).
			marker := strings.ReplaceAll(testCase.name, " ", "-")
			chargerID := seedRestartingCharger(t, db, ctx, stationA, "CMD-"+marker)

			applied, err := store.RecordChargerCommandResult(ctx, "CMDNO-"+marker+"-"+suffix, "", chargerID, "RESTART", testCase.result, "trace-command")
			if err != nil {
				t.Fatalf("RecordChargerCommandResult() error = %v", err)
			}
			if !applied {
				t.Fatal("expected the outcome to be applied to a charger the command was holding")
			}
			if got := chargerStatus(t, db, ctx, chargerID); got != testCase.want {
				t.Fatalf("charger status = %s, want %s", got, testCase.want)
			}

			// The completion event is written with the state change it describes, and it carries
			// the outcome so a consumer cannot apply the wrong status either.
			var (
				eventType string
				payload   []byte
			)
			if err := db.QueryRowContext(ctx,
				`SELECT event_type, payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'CHARGER_COMMAND_COMPLETED' ORDER BY id DESC LIMIT 1`,
				strconvFormatInt64(chargerID)).Scan(&eventType, &payload); err != nil {
				t.Fatalf("read completion event: %v", err)
			}
			if eventType != "CHARGER_COMMAND_COMPLETED" {
				t.Fatalf("unexpected event type %s", eventType)
			}
			// The payload is read as JSON rather than matched as text: PostgreSQL renders jsonb
			// with its own spacing, which would make a substring assertion fragile.
			var decoded map[string]string
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Fatalf("decode completion payload: %v (%s)", err, payload)
			}
			if decoded["result"] != testCase.result {
				t.Fatalf("completion payload result = %q, want %q", decoded["result"], testCase.result)
			}
			if decoded["charger_id"] != strconvFormatInt64(chargerID) {
				t.Fatalf("completion payload charger_id = %q, want %q", decoded["charger_id"], strconvFormatInt64(chargerID))
			}
			if decoded["action"] != "RESTART" {
				t.Fatalf("completion payload action = %q, want RESTART", decoded["action"])
			}
		})
	}
}

// A value outside the frozen outcome enum is refused, and refusing it must not move the charger:
// the review's point is that a timeout is not a device refusal, so applying it as one would fail an
// order or park a charger on a guess. The value is refused loudly instead, which leaves the charger
// exactly as it was - never IDLE, so a device whose state nobody knows cannot be allocated.
func TestChargerCommandResultRefusesAnOutcomeOutsideTheFrozenEnum(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	_, _, stationA, _, _ := orderFlowFixture(t, db, ctx, suffix)

	for _, result := range []string{"TIMED_OUT", "SOMETHING_ELSE", "", "ACCEPTED"} {
		marker := strings.ReplaceAll(result, " ", "-")
		if marker == "" {
			marker = "empty"
		}
		chargerID := seedRestartingCharger(t, db, ctx, stationA, "CMD-REFUSE-"+marker)

		applied, err := store.RecordChargerCommandResult(ctx, "CMDNO-REFUSE-"+marker+"-"+suffix, "", chargerID, "RESTART", result, "trace-command")
		if err == nil {
			t.Fatalf("result %q was accepted as a device outcome", result)
		}
		if applied {
			t.Fatalf("result %q reported an application", result)
		}
		if got := chargerStatus(t, db, ctx, chargerID); got != "RESTARTING" {
			t.Fatalf("charger status = %s, want RESTARTING (refusing an outcome changes nothing)", got)
		}
		var events int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_events
 WHERE event_type = 'CHARGER_COMMAND_COMPLETED' AND aggregate_id = $1`, strconvFormatInt64(chargerID)).Scan(&events); err != nil {
			t.Fatalf("count completion events: %v", err)
		}
		if events != 0 {
			t.Fatalf("result %q produced %d completion event(s)", result, events)
		}
	}
}

// stopConfirmationFixture prepares an order that is waiting for the device to confirm a stop, which
// is the state a failed STOP_CHARGING leaves behind: the order stays STOPPING and the charger is
// parked. It returns the pieces a command-outcome test needs.
func stopConfirmationFixture(t *testing.T, db *sql.DB, ctx context.Context, label string) (*OrderStore, string, int64, string) {
	t.Helper()
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, suffix)

	created, err := store.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userA, ChargerID: chargerA,
		IdempotencyKey: label + "-create-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if _, err := store.StartCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo,
		IdempotencyKey: label + "-start-" + suffix, RequestHash: "h", TraceID: "t",
	}); err != nil {
		t.Fatalf("StartCharging() error = %v", err)
	}
	if _, err := store.ConfirmStart(ctx, order.ConfirmStartCommand{
		OrderNo: created.OrderNo, ChargerID: chargerA, OccurredAt: time.Now().UTC().Add(-time.Minute),
		EventID: "evt_" + label + "_" + created.OrderNo, RequestHash: "d", TraceID: "t",
	}); err != nil {
		t.Fatalf("ConfirmStart() error = %v", err)
	}
	if _, err := store.StopCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo,
		IdempotencyKey: label + "-stop-" + suffix, RequestHash: "h", TraceID: "t",
	}); err != nil {
		t.Fatalf("StopCharging() error = %v", err)
	}
	return store, created.OrderNo, chargerA, suffix
}

// completionEventsFor counts the completion events written for one command.
func completionEventsFor(t *testing.T, db *sql.DB, ctx context.Context, chargerID int64, commandNo string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_events
 WHERE event_type = 'CHARGER_COMMAND_COMPLETED' AND aggregate_id = $1 AND payload->>'command_id' = $2`,
		strconvFormatInt64(chargerID), commandNo).Scan(&count); err != nil {
		t.Fatalf("count completion events: %v", err)
	}
	return count
}

// One command has exactly one outcome. The STOP_CHARGING failure is the case that made this
// necessary: it deliberately leaves the order in STOPPING, so a replay looked exactly like a first
// application and wrote a second CHARGER_COMMAND_COMPLETED. Consumers were then told the same
// device verdict twice, and the recovery sweep counts commands, so the duplicate is not cosmetic.
func TestChargerCommandResultIsRecordedOncePerCommand(t *testing.T) {
	db, ctx := integrationDB(t)
	store, orderNo, chargerA, suffix := stopConfirmationFixture(t, db, ctx, "result-once")

	commandNo := "CMD-RESULT-ONCE-" + suffix
	applied, err := store.RecordChargerCommandResult(ctx, commandNo, orderNo, chargerA, order.CommandStopCharging, order.CommandResultFailed, "t")
	if err != nil || !applied {
		t.Fatalf("first record = %v, %v; want applied", applied, err)
	}
	events := func() int {
		t.Helper()
		return completionEventsFor(t, db, ctx, chargerA, commandNo)
	}
	if events() != 1 {
		t.Fatalf("expected one completion event, got %d", events())
	}

	// The replay is the same verdict: nothing is applied and no event is written again.
	replayed, err := store.RecordChargerCommandResult(ctx, commandNo, orderNo, chargerA, order.CommandStopCharging, order.CommandResultFailed, "t")
	if err != nil {
		t.Fatalf("replay error = %v", err)
	}
	if replayed {
		t.Fatal("a replay must not report a second application")
	}
	if events() != 1 {
		t.Fatalf("the replay wrote another completion event (%d)", events())
	}
	if got := orderStatus(t, db, ctx, orderNo); got != order.StatusStopping {
		t.Fatalf("status = %s, want STOPPING", got)
	}

	// A different verdict under the same command id is a contradiction, not a replay: the device
	// answered once, and the platform must not rewrite what it said.
	if _, err := store.RecordChargerCommandResult(ctx, commandNo, orderNo, chargerA, order.CommandStopCharging, order.CommandResultCompleted, "t"); !errors.Is(err, order.ErrIdempotencyConflict) {
		t.Fatalf("contradictory verdict error = %v, want ErrIdempotencyConflict", err)
	}
	if events() != 1 {
		t.Fatalf("the contradiction wrote another completion event (%d)", events())
	}
}

// The review finding: the outcome was deduplicated through idempotency_records, whose record expires
// after 24 hours, and an order in STOPPING is still meaningful a day later - it is exactly the state
// the STOP recovery sweep works on. Once the record expired, a duplicate delivery of the same STOP
// result was treated as a first one, decided "applied" again (the order stays STOPPING and the
// charger is already FAULT, so nothing changes) and wrote a second CHARGER_COMMAND_COMPLETED.
//
// The clock is advanced past that window deliberately: the record that keeps a command's verdict
// from being applied twice must not be a cache entry.
func TestChargerCommandResultSurvivesTheIdempotencyWindow(t *testing.T) {
	db, ctx := integrationDB(t)
	store, orderNo, chargerA, suffix := stopConfirmationFixture(t, db, ctx, "result-durable")

	commandNo := "CMD-RESULT-DURABLE-" + suffix
	applied, err := store.RecordChargerCommandResult(ctx, commandNo, orderNo, chargerA, order.CommandStopCharging, order.CommandResultFailed, "t")
	if err != nil || !applied {
		t.Fatalf("first record = %v, %v; want applied", applied, err)
	}
	if events := completionEventsFor(t, db, ctx, chargerA, commandNo); events != 1 {
		t.Fatalf("expected one completion event, got %d", events)
	}

	// Two days later: any 24-hour replay window has expired, and the order is still STOPPING with a
	// faulty charger, so the same duplicate is still a duplicate.
	store.clock = func() time.Time { return time.Now().Add(48 * time.Hour) }
	replayed, err := store.RecordChargerCommandResult(ctx, commandNo, orderNo, chargerA, order.CommandStopCharging, order.CommandResultFailed, "t")
	if err != nil {
		t.Fatalf("replay after the idempotency window: %v", err)
	}
	if replayed {
		t.Fatal("a duplicate STOP result was applied again after the idempotency window")
	}
	if events := completionEventsFor(t, db, ctx, chargerA, commandNo); events != 1 {
		t.Fatalf("the duplicate wrote another completion event (%d)", events)
	}

	// The record itself is durable and says what the device answered, so an operator can tell which
	// command produced the state the order is in.
	var (
		storedResult  string
		storedOrderNo string
		storedApplied bool
	)
	if err := db.QueryRowContext(ctx, `SELECT result, order_no, applied FROM charger_command_outcomes WHERE command_id = $1`,
		commandNo).Scan(&storedResult, &storedOrderNo, &storedApplied); err != nil {
		t.Fatalf("read the command outcome record: %v", err)
	}
	if storedResult != order.CommandResultFailed || storedOrderNo != orderNo || !storedApplied {
		t.Fatalf("unexpected record: result=%s order_no=%s applied=%v", storedResult, storedOrderNo, storedApplied)
	}
	if got := orderStatus(t, db, ctx, orderNo); got != order.StatusStopping {
		t.Fatalf("status = %s, want STOPPING", got)
	}
	if got := chargerStatus(t, db, ctx, chargerA); got != "FAULT" {
		t.Fatalf("charger status = %s, want FAULT", got)
	}
}

// Two deliveries of the same outcome at the same time must still produce one application. The
// worker retries whatever it could not record, so the loser of the race has to come back and find
// the outcome already recorded rather than write it a second time.
//
// The STOP_CHARGING failure is used deliberately: it changes no order status and its charger update
// is a no-op once the charger is already in FAULT, so nothing else in the transaction would stop a
// second delivery from reporting "applied" and emitting a second completion event. A station
// restart would not show the defect, because its charger update simply matches no row the second
// time.
func TestConcurrentChargerCommandResultIsAppliedOnce(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, suffix)

	created, err := store.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userA, ChargerID: chargerA,
		IdempotencyKey: "concurrent-result-create-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if _, err := store.StartCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo,
		IdempotencyKey: "concurrent-result-start-" + suffix, RequestHash: "h", TraceID: "t",
	}); err != nil {
		t.Fatalf("StartCharging() error = %v", err)
	}
	if _, err := store.ConfirmStart(ctx, order.ConfirmStartCommand{
		OrderNo: created.OrderNo, ChargerID: chargerA, OccurredAt: time.Now().UTC().Add(-time.Minute),
		EventID: "evt_concurrent_result_" + created.OrderNo, RequestHash: "d", TraceID: "t",
	}); err != nil {
		t.Fatalf("ConfirmStart() error = %v", err)
	}
	if _, err := store.StopCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo,
		IdempotencyKey: "concurrent-result-stop-" + suffix, RequestHash: "h", TraceID: "t",
	}); err != nil {
		t.Fatalf("StopCharging() error = %v", err)
	}
	chargerID := chargerA
	commandNo := "CMD-CONCURRENT-RESULT-" + suffix
	const competing = 4

	type outcome struct {
		applied bool
		err     error
	}
	results := make(chan outcome, competing)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < competing; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			applied, err := store.RecordChargerCommandResult(ctx, commandNo, created.OrderNo, chargerID,
				order.CommandStopCharging, order.CommandResultFailed, "t")
			results <- outcome{applied: applied, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	appliedCount := 0
	for result := range results {
		// Every delivery is answered: the winner applies the outcome, and the others block on the
		// primary key, read the committed row and come back as replays. Anything else - an error, or
		// a second application - is exactly what this test exists to catch.
		if result.err != nil {
			t.Fatalf("unexpected error from a concurrent delivery: %v", result.err)
		}
		if result.applied {
			appliedCount++
		}
	}
	if appliedCount != 1 {
		t.Fatalf("%d deliveries applied the same outcome, want exactly 1", appliedCount)
	}
	var events int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_events
 WHERE event_type = 'CHARGER_COMMAND_COMPLETED' AND aggregate_id = $1 AND payload->'command_id' = to_jsonb($2::text)`,
		strconvFormatInt64(chargerID), commandNo).Scan(&events); err != nil {
		t.Fatalf("count completion events: %v", err)
	}
	if events != 1 {
		t.Fatalf("expected exactly one completion event, got %d", events)
	}
}

// A second result for the same charger is not an event: there is nothing left to apply, and the
// status must not move again (a duplicate FAILED must not overwrite the first outcome).
func TestChargerCommandResultIsIdempotent(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	_, _, stationA, _, _ := orderFlowFixture(t, db, ctx, suffix)
	chargerID := seedRestartingCharger(t, db, ctx, stationA, "CMD-IDEM")

	applied, err := store.RecordChargerCommandResult(ctx, "CMD-IDEM-1-"+suffix, "", chargerID, "RESTART", "FAILED", "trace-1")
	if err != nil || !applied {
		t.Fatalf("first result = %v, %v; want applied", applied, err)
	}
	if got := chargerStatus(t, db, ctx, chargerID); got != "FAULT" {
		t.Fatalf("charger status = %s, want FAULT", got)
	}

	applied, err = store.RecordChargerCommandResult(ctx, "CMD-IDEM-2-"+suffix, "", chargerID, "RESTART", "COMPLETED", "trace-2")
	if err != nil {
		t.Fatalf("second result error = %v", err)
	}
	if applied {
		t.Fatal("a charger the command no longer holds must not be moved again")
	}
	if got := chargerStatus(t, db, ctx, chargerID); got != "FAULT" {
		t.Fatalf("a second result moved the charger to %s, want FAULT", got)
	}
}

// The device outcome must not override an active order: the charger belongs to that order, and a
// late restart result cannot hand a charging device to somebody else.
func TestChargerCommandResultDoesNotReleaseAChargerInUse(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	userA, _, _, _, chargerID := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	suffix := uniqueSuffix(t)
	if _, err := store.CreateOrder(ctx, order.CreateOrderCommand{
		UserID:         userA,
		ChargerID:      chargerID,
		IdempotencyKey: "cmd-inuse-" + suffix,
		RequestHash:    hashRequest("POST", "/api/v1/orders", ""),
		TraceID:        "trace-inuse",
	}); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	// The order holds the charger, so the charger is OCCUPIED rather than RESTARTING: a command
	// could not have been issued for it in the first place.
	if got := chargerStatus(t, db, ctx, chargerID); got != "OCCUPIED" {
		t.Fatalf("charger status = %s, want OCCUPIED", got)
	}
	applied, err := store.RecordChargerCommandResult(ctx, "CMD-INUSE-"+suffix, "", chargerID, "RESTART", "COMPLETED", "trace")
	if err != nil {
		t.Fatalf("RecordChargerCommandResult() error = %v", err)
	}
	if applied {
		t.Fatal("expected the result to be refused while an active order holds the charger")
	}
	if got := chargerStatus(t, db, ctx, chargerID); got != "OCCUPIED" {
		t.Fatalf("charger status = %s, want OCCUPIED", got)
	}
}

// The consumer side must apply the same rule: a FAILED completion event must not return the
// charger to IDLE when the worker applies it.
func TestCompletingACommandAppliesTheOutcome(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	_, _, stationA, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	completedCharger := seedRestartingCharger(t, db, ctx, stationA, "CMD-OK")
	failedCharger := seedRestartingCharger(t, db, ctx, stationA, "CMD-BAD")

	if released, err := store.CompleteChargerCommand(ctx, completedCharger, "RESTART", "COMPLETED"); err != nil || !released {
		t.Fatalf("completed outcome = %v, %v; want released", released, err)
	}
	if got := chargerStatus(t, db, ctx, completedCharger); got != "IDLE" {
		t.Fatalf("completed charger status = %s, want IDLE", got)
	}

	// A failed outcome parks the charger; the call itself is still an application, so it reports
	// that it moved the row.
	if applied, err := store.CompleteChargerCommand(ctx, failedCharger, "RESTART", "FAILED"); err != nil || !applied {
		t.Fatalf("failed outcome = %v, %v; want applied", applied, err)
	}
	if got := chargerStatus(t, db, ctx, failedCharger); got != "FAULT" {
		t.Fatalf("failed charger status = %s, want FAULT", got)
	}

	// Re-applying the failed completion must not move it out of FAULT.
	if applied, err := store.CompleteChargerCommand(ctx, failedCharger, "RESTART", "FAILED"); err != nil || applied {
		t.Fatalf("repeated failed outcome = %v, %v; want no-op", applied, err)
	}
	if got := chargerStatus(t, db, ctx, failedCharger); got != "FAULT" {
		t.Fatalf("failed charger status = %s after a repeat, want FAULT", got)
	}
}
