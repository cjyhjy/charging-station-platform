package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/wallet"
)

func TestWalletTopUpIdempotencyAndLedger(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, suffix)

	// The fixture wallet is seeded at 10000; read it as the baseline.
	baseline, err := store.Wallet(ctx, userA)
	if err != nil {
		t.Fatalf("baseline wallet: %v", err)
	}

	key := "topup-idem-key-0001-" + suffix
	command := wallet.TopUpCommand{UserID: userA, AmountCent: 10000, IdempotencyKey: key, RequestHash: "h", TraceID: "t"}
	first, err := store.TopUp(ctx, command)
	if err != nil || first.BalanceCent != baseline.BalanceCent+10000 {
		t.Fatalf("top-up = %#v, %v", first, err)
	}

	// Replay: same key returns the same view without a second credit.
	replay, err := store.TopUp(ctx, command)
	if err != nil || replay.BalanceCent != baseline.BalanceCent+10000 {
		t.Fatalf("replay = %#v, %v", replay, err)
	}

	// The ledger records exactly one TOP_UP row.
	entries, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 1, PageSize: 100, Type: wallet.TypeTopUp})
	if err != nil || len(entries.Items) != 1 || entries.Meta.Total != 1 {
		t.Fatalf("ledger = %#v, %v", entries, err)
	}
	if entries.Items[0].AmountCent != 10000 || entries.Items[0].TransactionType != wallet.TypeTopUp {
		t.Fatalf("entry = %#v", entries.Items[0])
	}
}

func TestRefundOrderReversesPayment(t *testing.T) {
	db, ctx := integrationDB(t)
	walletStore, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, suffix)

	// Drive an order to completion (bill = 120 cents at the start snapshot).
	created, err := orderStore.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userA, ChargerID: chargerA, IdempotencyKey: "wf-create-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if _, err := orderStore.StartCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo, IdempotencyKey: "wf-start-" + suffix, RequestHash: "h", TraceID: "t"}); err != nil {
		t.Fatalf("StartCharging() error = %v", err)
	}
	if _, err := orderStore.ConfirmStart(ctx, order.ConfirmStartCommand{
		OrderNo: created.OrderNo, ChargerID: chargerA, OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatalf("ConfirmStart() error = %v", err)
	}
	if _, err := orderStore.StopCharging(ctx, order.TransitionCommand{
		UserID: userA, OrderNo: created.OrderNo, IdempotencyKey: "wf-stop-" + suffix, RequestHash: "h", TraceID: "t"}); err != nil {
		t.Fatalf("StopCharging() error = %v", err)
	}
	if _, err := orderStore.ConfirmStop(ctx, order.ConfirmStopCommand{
		OrderNo: created.OrderNo, ChargerID: chargerA, EnergyWh: 1000, OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatalf("ConfirmStop() error = %v", err)
	}

	// UC-U-09 confirm: settle the 120-cent bill from the wallet.
	if _, err := orderStore.SettleOrder(ctx, order.SettleCommand{
		TransitionCommand: order.TransitionCommand{
			UserID: userA, OrderNo: created.OrderNo, IdempotencyKey: "wf-confirm-" + suffix, RequestHash: "h", TraceID: "t"}}); err != nil {
		t.Fatalf("SettleOrder() error = %v", err)
	}

	before, err := walletStore.Wallet(ctx, userA)
	if err != nil {
		t.Fatalf("balance before refund: %v", err)
	}

	// Admin refunds the settled amount: wallet credited, order reverts to
	// PENDING with paid_cents cleared, audit row written.
	refunded, err := walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: created.OrderNo, IdempotencyKey: "wf-refund-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("RefundOrder() error = %v", err)
	}
	if refunded.BalanceCent != before.BalanceCent+120 {
		t.Fatalf("balance after refund = %d, want %d", refunded.BalanceCent, before.BalanceCent+120)
	}

	var paidCents int64
	var paymentStatus string
	if err := db.QueryRowContext(ctx,
		`SELECT paid_cents, payment_status FROM charging_orders WHERE order_no = $1`, created.OrderNo).Scan(&paidCents, &paymentStatus); err != nil {
		t.Fatalf("order state: %v", err)
	}
	if paidCents != 0 || paymentStatus != wallet.PaymentPending {
		t.Fatalf("order payment = %d/%s, want 0/PENDING", paidCents, paymentStatus)
	}

	// The refund is idempotent through the same key.
	replayed, err := walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: created.OrderNo, IdempotencyKey: "wf-refund-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil || replayed.BalanceCent != refunded.BalanceCent {
		t.Fatalf("refund replay = %#v, %v", replayed, err)
	}

	// The audit trail recorded the admin action.
	var audits int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM operation_logs WHERE action = 'wallet.refund' AND resource_id = $1`,
		created.OrderNo).Scan(&audits); err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audits != 1 {
		t.Fatalf("audit rows = %d, want 1", audits)
	}

	// An unsettled order cannot be refunded again.
	_, err = walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: created.OrderNo, IdempotencyKey: "wf-refund2-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if !errors_Is(err, wallet.ErrOrderNotRefundable) {
		t.Fatalf("second refund error = %v, want ErrOrderNotRefundable", err)
	}
}

func errors_Is(err error, target error) bool {
	return errors.Is(err, target)
}
