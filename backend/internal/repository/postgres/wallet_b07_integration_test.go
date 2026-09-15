package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/wallet"
)

// This file is B-07's regression matrix for the PostgreSQL wallet adapter. It
// targets the two defects the module shipped with (a concurrent first credit
// that lost money, and a post-window replay that failed with a duplicate key)
// plus the contract details the adapter must keep: an unknown user is a
// missing wallet, a missing order is not "nothing to refund", the ledger's
// balance chain matches the wallet, the balance can never go negative, the
// idempotency scope is per user, and pagination/type filters stay exact.
//
// Every test is written to fail when the behaviour it protects is removed; the
// module review records which reversal was applied and what it showed.

// b07UniqueDigits returns a digit-only, per-run unique string: phones and
// idempotency keys must not collide with earlier runs in the shared test
// database, and the column is text.
func b07UniqueDigits(t *testing.T) string {
	t.Helper()
	return strings.ReplaceAll(uniqueSuffix(t), ".", "")
}

// b07SeedUser creates an account without a wallet row, which is the state that
// makes a first credit a "row does not exist yet" race.
func b07SeedUser(t *testing.T, db *sql.DB, ctx context.Context, phone string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO user_accounts (phone, password_hash) VALUES ($1, 'x') RETURNING id`,
		phone).Scan(&id); err != nil {
		t.Fatalf("seed user %s: %v", phone, err)
	}
	return id
}

// b07RacePool opens a wide pool: the concurrency tests only race when the pool
// can hand every goroutine its own connection. integrationDB has already
// skipped the test when the DSN is unset.
func b07RacePool(t *testing.T, ctx context.Context, maxConns int) *sql.DB {
	t.Helper()
	dsn := os.Getenv("NCS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NCS_TEST_PG_DSN not set; PostgreSQL integration tests skipped")
	}
	pool, err := Open(ctx, dsn, maxConns)
	if err != nil {
		t.Fatalf("Open race pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// TestB07TopUpConcurrentFirstCreditKeepsEveryCent is the P0 regression: eight
// concurrent first credits of 100 cents on a wallet row that does not exist
// yet must leave 800 cents and 800 cents of ledger, with no error. The shipped
// version lost money here: `SELECT ... FOR UPDATE` locks nothing on a missing
// row, so several transactions read zero and then wrote an absolute balance,
// overwriting each other while every ledger row survived (observed: balance
// 200/400 against a ledger sum of 800).
func TestB07TopUpConcurrentFirstCreditKeepsEveryCent(t *testing.T) {
	db, ctx := integrationDB(t)
	pool := b07RacePool(t, ctx, 16)
	store, err := NewWalletStore(pool)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	userID := b07SeedUser(t, db, ctx, "137"+b07UniqueDigits(t)[:9])

	const racers = 8
	const amount = int64(100)

	var wg sync.WaitGroup
	results := make([]wallet.WalletView, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = store.TopUp(ctx, wallet.TopUpCommand{
				UserID:         userID,
				AmountCent:     amount,
				IdempotencyKey: fmt.Sprintf("b07-first-%d-%s", i, b07UniqueDigits(t)),
				RequestHash:    "h", TraceID: "t",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: TopUp() error = %v", i, err)
		}
	}

	var balance, ledgerSum, ledgerRows int64
	if err := db.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1`, userID).Scan(&balance); err != nil {
		t.Fatalf("wallet balance: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(sum(amount_cents), 0), count(*) FROM wallet_transactions WHERE user_id = $1`, userID).
		Scan(&ledgerSum, &ledgerRows); err != nil {
		t.Fatalf("ledger: %v", err)
	}

	want := amount * racers
	if balance != want {
		t.Fatalf("balance = %d, want %d (lost credit)", balance, want)
	}
	if ledgerSum != want || ledgerRows != racers {
		t.Fatalf("ledger = %d over %d rows, want %d over %d rows", ledgerSum, ledgerRows, want, racers)
	}
	// Every response must agree with the balance the database computed.
	seen := map[int64]bool{}
	for _, view := range results {
		seen[view.BalanceCent] = true
	}
	if len(seen) != racers {
		t.Fatalf("balances returned = %v, want %d distinct after-values", seen, racers)
	}
}

// TestB07TopUpConcurrentSameKeyCreditsOnce races one idempotency key: the
// credit must happen exactly once, and every other racer must be answered with
// that same result or with "in progress" - never with a second credit and
// never with a duplicate-key failure.
func TestB07TopUpConcurrentSameKeyCreditsOnce(t *testing.T) {
	db, ctx := integrationDB(t)
	pool := b07RacePool(t, ctx, 16)
	store, err := NewWalletStore(pool)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	userID := b07SeedUser(t, db, ctx, "138"+b07UniqueDigits(t)[:9])
	key := "b07-samekey-" + b07UniqueDigits(t)

	const racers = 6
	const amount = int64(700)

	var wg sync.WaitGroup
	views := make([]wallet.WalletView, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			views[i], errs[i] = store.TopUp(ctx, wallet.TopUpCommand{
				UserID: userID, AmountCent: amount, IdempotencyKey: key, RequestHash: "h", TraceID: "t",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	applied := 0
	for i, err := range errs {
		switch {
		case err == nil:
			applied++
			if views[i].BalanceCent != amount {
				t.Fatalf("racer %d balance = %d, want %d", i, views[i].BalanceCent, amount)
			}
		case errors.Is(err, wallet.ErrIdempotencyInProgress):
			// Legal: the winner had not committed when this racer arrived.
		default:
			t.Fatalf("racer %d: unexpected error %v", i, err)
		}
	}
	if applied == 0 {
		t.Fatal("no racer succeeded; the key was never credited")
	}

	var balance, rows int64
	if err := db.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1`, userID).Scan(&balance); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_transactions WHERE user_id = $1`, userID).Scan(&rows); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	if balance != amount || rows != 1 {
		t.Fatalf("balance/ledger = %d/%d, want %d/1 (one credit for one key)", balance, rows, amount)
	}
}

// TestB07TopUpReplayAfterCacheExpired is the P1 regression for the response
// cache's 24-hour lifetime: once the record expires, a retry of a credit that
// already happened must return the first result instead of crediting twice or
// dying on the ledger's unique index (observed before: SQLSTATE 23505 on
// wallet_transactions_idempotency_key_key). The clock is advanced instead of
// waiting a day, and no row is deleted, so this covers the supersede branch.
func TestB07TopUpReplayAfterCacheExpired(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	command := wallet.TopUpCommand{UserID: userA, AmountCent: 500, IdempotencyKey: "b07-expire-" + suffix, RequestHash: "h", TraceID: "t"}
	first, err := store.TopUp(ctx, command)
	if err != nil {
		t.Fatalf("first TopUp() error = %v", err)
	}

	// Nothing about the request changes - only time passes.
	store.clock = func() time.Time { return time.Now().Add(idempotencyTTL + time.Hour) }
	defer func() { store.clock = time.Now }()

	replay, err := store.TopUp(ctx, command)
	if err != nil {
		t.Fatalf("TopUp() after the cache window error = %v, want the first result", err)
	}
	if replay.BalanceCent != first.BalanceCent {
		t.Fatalf("replay balance = %d, want %d", replay.BalanceCent, first.BalanceCent)
	}

	var rows, ledgerSum int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), COALESCE(sum(amount_cents), 0) FROM wallet_transactions WHERE user_id = $1`, userA).
		Scan(&rows, &ledgerSum); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if rows != 1 || ledgerSum != 500 {
		t.Fatalf("ledger = %d rows / %d cents, want 1 row / 500 cents", rows, ledgerSum)
	}

	var balance int64
	if err := db.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1`, userA).Scan(&balance); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != first.BalanceCent {
		t.Fatalf("balance = %d, want the replayed %d", balance, first.BalanceCent)
	}
}

// TestB07TopUpReplayAfterCachePurged covers the same P1 through a purged cache
// row rather than expiry: the ledger alone must be enough to answer the retry.
func TestB07TopUpReplayAfterCachePurged(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	key := "b07-purge-" + suffix
	command := wallet.TopUpCommand{UserID: userA, AmountCent: 300, IdempotencyKey: key, RequestHash: "h", TraceID: "t"}
	first, err := store.TopUp(ctx, command)
	if err != nil {
		t.Fatalf("first TopUp() error = %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		fmt.Sprintf("wallet:%d:top-up", userA), key); err != nil {
		t.Fatalf("purge cache row: %v", err)
	}

	replay, err := store.TopUp(ctx, command)
	if err != nil {
		t.Fatalf("TopUp() after the cache row was purged error = %v, want the first result", err)
	}
	if replay.BalanceCent != first.BalanceCent {
		t.Fatalf("replay balance = %d, want %d", replay.BalanceCent, first.BalanceCent)
	}

	// The retry also restores the cache entry, so a third request is a plain replay.
	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		fmt.Sprintf("wallet:%d:top-up", userA), key).Scan(&status); err != nil {
		t.Fatalf("cache row after replay: %v", err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("cache status = %s, want SUCCEEDED", status)
	}
	if _, err := store.TopUp(ctx, command); err != nil {
		t.Fatalf("third TopUp() error = %v", err)
	}
	var rows int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_transactions WHERE user_id = $1`, userA).Scan(&rows); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("ledger rows = %d, want 1", rows)
	}
}

// TestB07RefundReplayAfterCachePurged is the refund half of P1: a refund retry
// after the cache is gone must return the refund that already happened, keep
// the wallet credited once, and leave a single audit row.
func TestB07RefundReplayAfterCachePurged(t *testing.T) {
	db, ctx := integrationDB(t)
	walletStore, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, uniqueSuffix(t))
	orderNo := b07SettledOrder(t, db, ctx, orderStore, userA, chargerA, suffix)

	key := "b07-refund-purge-" + suffix
	command := wallet.RefundCommand{AdminID: 9, OrderNo: orderNo, IdempotencyKey: key, RequestHash: "h", TraceID: "t"}
	first, err := walletStore.RefundOrder(ctx, command)
	if err != nil {
		t.Fatalf("RefundOrder() error = %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		"wallet:refund:"+orderNo, key); err != nil {
		t.Fatalf("purge refund cache row: %v", err)
	}

	replay, err := walletStore.RefundOrder(ctx, command)
	if err != nil {
		t.Fatalf("RefundOrder() after the cache was purged error = %v, want the first result", err)
	}
	if replay.BalanceCent != first.BalanceCent {
		t.Fatalf("refund replay balance = %d, want %d", replay.BalanceCent, first.BalanceCent)
	}

	var refunds, audits int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM wallet_transactions WHERE order_id IS NOT NULL AND transaction_type = 'REFUND' AND user_id = $1`, userA).
		Scan(&refunds); err != nil {
		t.Fatalf("refund ledger: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM operation_logs WHERE action = 'wallet.refund' AND resource_id = $1`, orderNo).
		Scan(&audits); err != nil {
		t.Fatalf("audit rows: %v", err)
	}
	if refunds != 1 || audits != 1 {
		t.Fatalf("refund ledger/audit = %d/%d, want 1/1", refunds, audits)
	}
}

// TestB07RefundDifferentKeyAfterRefundIsRejected pins the semantics the module
// review settled: the ledger key identifies the refund REQUEST
// (refund:<orderNo>:<idempotencyKey>), so a retry of the same request replays,
// while a different request against an already refunded order is rejected as
// "nothing to refund" instead of silently looking like a second success.
func TestB07RefundDifferentKeyAfterRefundIsRejected(t *testing.T) {
	db, ctx := integrationDB(t)
	walletStore, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, uniqueSuffix(t))
	orderNo := b07SettledOrder(t, db, ctx, orderStore, userA, chargerA, suffix)

	first, err := walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: orderNo, IdempotencyKey: "b07-refund-a-" + suffix, RequestHash: "h", TraceID: "t"})
	if err != nil {
		t.Fatalf("first refund: %v", err)
	}

	_, err = walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: orderNo, IdempotencyKey: "b07-refund-b-" + suffix, RequestHash: "h", TraceID: "t"})
	if !errors.Is(err, wallet.ErrOrderNotRefundable) {
		t.Fatalf("second refund with a new key = %v, want ErrOrderNotRefundable", err)
	}

	var balance, refunds int64
	if err := db.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1`, userA).Scan(&balance); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_transactions WHERE user_id = $1 AND transaction_type = 'REFUND'`, userA).Scan(&refunds); err != nil {
		t.Fatalf("refund rows: %v", err)
	}
	if balance != first.BalanceCent || refunds != 1 {
		t.Fatalf("balance/refunds = %d/%d, want %d/1", balance, refunds, first.BalanceCent)
	}
}

// TestB07RefundMissingOrderIsNotRefundable guards the distinction the refund
// contract needs: an order that does not exist is not an order with nothing to
// refund, because the caller cannot fix it by looking at the order state. The
// adapter reports it separately (errOrderNotFound); the API-level 404 is
// pending the A-line error-writer decision recorded in the module review.
func TestB07RefundMissingOrderIsNotRefundable(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	orderNo := "B07-MISSING-" + suffix
	key := "b07-refund-missing-" + suffix
	_, err = store.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: orderNo, IdempotencyKey: key, RequestHash: "h", TraceID: "t"})
	if err == nil {
		t.Fatal("refund of a missing order succeeded")
	}
	if errors.Is(err, wallet.ErrOrderNotRefundable) {
		t.Fatalf("missing order reported as not-refundable: %v", err)
	}
	if !errors.Is(err, errOrderNotFound) {
		t.Fatalf("missing order error = %v, want errOrderNotFound", err)
	}

	// The failed attempt must not leave a claimed idempotency record behind:
	// the transaction rolls back, so the operator can retry the same key.
	var claims int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		"wallet:refund:"+orderNo, key).Scan(&claims); err != nil {
		t.Fatalf("idempotency rows: %v", err)
	}
	if claims != 0 {
		t.Fatalf("idempotency rows = %d, want 0 after a failed refund", claims)
	}
	// Nothing was written for the user, either.
	var rows int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_transactions WHERE user_id = $1`, userA).Scan(&rows); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("ledger rows = %d, want 0", rows)
	}
}

// TestB07UnknownUserIsWalletNotFound: a wallet cannot exist without its
// account, so a write for a user that does not exist must be reported as a
// missing wallet (404) rather than as an internal failure. The foreign key is
// what makes it detectable.
func TestB07UnknownUserIsWalletNotFound(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	const ghost = int64(1 << 40)

	if _, err := store.Wallet(ctx, ghost); !errors.Is(err, wallet.ErrWalletNotFound) {
		t.Fatalf("Wallet() for an unknown user = %v, want ErrWalletNotFound", err)
	}
	_, err = store.TopUp(ctx, wallet.TopUpCommand{
		UserID: ghost, AmountCent: 100, IdempotencyKey: "b07-ghost-" + b07UniqueDigits(t), RequestHash: "h", TraceID: "t"})
	if !errors.Is(err, wallet.ErrWalletNotFound) {
		t.Fatalf("TopUp() for an unknown user = %v, want ErrWalletNotFound", err)
	}
	var rows int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_accounts WHERE user_id = $1`, ghost).Scan(&rows); err != nil {
		t.Fatalf("wallet rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("wallet rows for an unknown user = %d, want 0", rows)
	}
}

// TestB07LedgerBalanceChainMatchesWallet: every ledger row's after-balance must
// be its before-balance plus its amount, the chain must be continuous, and the
// last after-balance must equal the wallet. That is the invariant an operator
// reads to trust the books.
func TestB07LedgerBalanceChainMatchesWallet(t *testing.T) {
	db, ctx := integrationDB(t)
	walletStore, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, uniqueSuffix(t))
	orderNo := b07SettledOrder(t, db, ctx, orderStore, userA, chargerA, suffix)

	for i, amount := range []int64{1000, 250, 75} {
		if _, err := walletStore.TopUp(ctx, wallet.TopUpCommand{
			UserID: userA, AmountCent: amount, IdempotencyKey: fmt.Sprintf("b07-chain-%d-%s", i, suffix),
			RequestHash: "h", TraceID: "t"}); err != nil {
			t.Fatalf("top-up %d: %v", i, err)
		}
	}
	if _, err := walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: orderNo, IdempotencyKey: "b07-chain-refund-" + suffix, RequestHash: "h", TraceID: "t"}); err != nil {
		t.Fatalf("refund: %v", err)
	}

	rows, err := db.QueryContext(ctx, `SELECT transaction_type, amount_cents, balance_before_cents, balance_after_cents
FROM wallet_transactions WHERE user_id = $1 ORDER BY id`, userA)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	defer rows.Close()

	// The fixture seeds the wallet at 10000 cents without a ledger row, so the
	// chain starts there. Four rows follow: the settlement's CHARGE, three
	// top-ups and the refund.
	var previous int64 = 10000
	var count int
	for rows.Next() {
		var kind string
		var amount, before, after int64
		if err := rows.Scan(&kind, &amount, &before, &after); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		if before != previous {
			t.Fatalf("row %d (%s): balance_before = %d, want the previous after-balance %d", count, kind, before, previous)
		}
		if after != before+amount {
			t.Fatalf("row %d (%s): after %d != before %d + amount %d", count, kind, after, before, amount)
		}
		previous = after
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	if count != 5 {
		t.Fatalf("ledger rows = %d, want 5 (one CHARGE, three top-ups, one REFUND)", count)
	}

	var balance int64
	if err := db.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1`, userA).Scan(&balance); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != previous {
		t.Fatalf("wallet balance = %d, want the ledger's last after-balance %d", balance, previous)
	}
}

// TestB07LedgerBalanceNeverNegative checks the last line of defence: the
// column's CHECK constraint rejects a negative balance even when a statement
// bypasses the adapter.
//
// The removal of the constraint is applied inside a transaction that is rolled
// back, so the test both proves the guard exists and proves this test would
// notice if it disappeared.
func TestB07LedgerBalanceNeverNegative(t *testing.T) {
	db, ctx := integrationDB(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	constraint, err := b07CheckConstraintOn(t, db, ctx, "wallet_accounts", "balance_cents")
	if err != nil {
		t.Fatalf("look up the balance CHECK constraint: %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE wallet_accounts SET balance_cents = -1 WHERE user_id = $1`, userA); err == nil {
		t.Fatal("a negative balance was accepted; the CHECK constraint is not enforced")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE wallet_accounts DROP CONSTRAINT %q`, constraint)); err != nil {
		t.Fatalf("drop constraint inside the rolled-back transaction: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE wallet_accounts SET balance_cents = -1 WHERE user_id = $1`, userA); err != nil {
		t.Fatalf("without the constraint the update should have succeeded, got %v", err)
	}
	if _, err := b07CheckConstraintOnTx(t, ctx, tx, "wallet_accounts", "balance_cents"); err == nil {
		t.Fatal("the contract check still reports the dropped constraint; it cannot detect its removal")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := b07CheckConstraintOn(t, db, ctx, "wallet_accounts", "balance_cents"); err != nil {
		t.Fatalf("constraint not restored by the rollback: %v", err)
	}
}

// TestB07LedgerPaginationAndTypeFilter checks the read path the API exposes:
// pages do not overlap or drop rows, the count is the total and not the page,
// and the type filter selects exactly one ledger kind.
func TestB07LedgerPaginationAndTypeFilter(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, _ := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	for i := 0; i < 5; i++ {
		if _, err := store.TopUp(ctx, wallet.TopUpCommand{
			UserID: userA, AmountCent: int64(10 * (i + 1)), IdempotencyKey: fmt.Sprintf("b07-page-%d-%s", i, suffix),
			RequestHash: "h", TraceID: "t"}); err != nil {
			t.Fatalf("top-up %d: %v", i, err)
		}
	}

	first, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	second, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 2, PageSize: 2})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	third, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 3, PageSize: 2})
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if len(first.Items) != 2 || len(second.Items) != 2 || len(third.Items) != 1 {
		t.Fatalf("page sizes = %d/%d/%d, want 2/2/1", len(first.Items), len(second.Items), len(third.Items))
	}
	if first.Meta.Total != 5 || second.Meta.Total != 5 || third.Meta.Total != 5 {
		t.Fatalf("totals = %d/%d/%d, want 5 (the total, not the page)", first.Meta.Total, second.Meta.Total, third.Meta.Total)
	}
	seen := map[int64]bool{}
	for _, page := range []wallet.EntryPage{first, second, third} {
		for _, entry := range page.Items {
			if seen[entry.ID] {
				t.Fatalf("entry %d appears on two pages", entry.ID)
			}
			seen[entry.ID] = true
		}
	}
	if len(seen) != 5 {
		t.Fatalf("distinct entries across the pages = %d, want 5", len(seen))
	}

	topUps, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 1, PageSize: 100, Type: wallet.TypeTopUp})
	if err != nil || len(topUps.Items) != 5 {
		t.Fatalf("TOP_UP filter = %d entries, %v; want 5", len(topUps.Items), err)
	}
	refunds, err := store.ListTransactions(ctx, wallet.EntryFilter{UserID: userA, Page: 1, PageSize: 100, Type: wallet.TypeRefund})
	if err != nil || len(refunds.Items) != 0 || refunds.Meta.Total != 0 {
		t.Fatalf("REFUND filter = %#v, %v; want an empty page", refunds, err)
	}
}

// TestB07IdempotencyScopeIsPerUser: one key used by two users is two requests.
// A shared scope would let the second user's credit be answered with the first
// user's balance.
func TestB07IdempotencyScopeIsPerUser(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	digits := b07UniqueDigits(t)
	userA := b07SeedUser(t, db, ctx, "136"+digits[:9])
	userB := b07SeedUser(t, db, ctx, "139"+digits[6:15])

	key := "b07-shared-key-" + digits
	for _, userID := range []int64{userA, userB} {
		view, err := store.TopUp(ctx, wallet.TopUpCommand{
			UserID: userID, AmountCent: 400, IdempotencyKey: key, RequestHash: "h", TraceID: "t"})
		if err != nil {
			t.Fatalf("TopUp() for user %d error = %v", userID, err)
		}
		if view.BalanceCent != 400 {
			t.Fatalf("user %d balance = %d, want 400", userID, view.BalanceCent)
		}
	}
}

// TestB07RefundUnsettledOrderIsNotRefundable covers the other half of "not
// refundable": an order that exists but has no settled amount. Nothing may be
// credited, no ledger row may appear, and no idempotency record may survive the
// failure.
func TestB07RefundUnsettledOrderIsNotRefundable(t *testing.T) {
	db, ctx := integrationDB(t)
	walletStore, err := NewWalletStore(db)
	if err != nil {
		t.Fatalf("NewWalletStore() error = %v", err)
	}
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := b07UniqueDigits(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, uniqueSuffix(t))

	created, err := orderStore.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userA, ChargerID: chargerA, IdempotencyKey: "b07-unsettled-" + suffix, RequestHash: "h", TraceID: "t"})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	before, err := walletStore.Wallet(ctx, userA)
	if err != nil {
		t.Fatalf("wallet before: %v", err)
	}
	key := "b07-refund-unsettled-" + suffix
	_, err = walletStore.RefundOrder(ctx, wallet.RefundCommand{
		AdminID: 9, OrderNo: created.OrderNo, IdempotencyKey: key, RequestHash: "h", TraceID: "t"})
	if !errors.Is(err, wallet.ErrOrderNotRefundable) {
		t.Fatalf("refund of an unsettled order = %v, want ErrOrderNotRefundable", err)
	}

	after, err := walletStore.Wallet(ctx, userA)
	if err != nil {
		t.Fatalf("wallet after: %v", err)
	}
	if after.BalanceCent != before.BalanceCent {
		t.Fatalf("balance changed from %d to %d on a rejected refund", before.BalanceCent, after.BalanceCent)
	}
	var refunds, claims int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_transactions WHERE user_id = $1 AND transaction_type = 'REFUND'`, userA).Scan(&refunds); err != nil {
		t.Fatalf("refund rows: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		"wallet:refund:"+created.OrderNo, key).Scan(&claims); err != nil {
		t.Fatalf("idempotency rows: %v", err)
	}
	if refunds != 0 || claims != 0 {
		t.Fatalf("refunds/claims = %d/%d, want 0/0", refunds, claims)
	}
}

// b07SettledOrder drives a fresh order to COMPLETED with 120 cents settled, the
// state a refund requires.
func b07SettledOrder(t *testing.T, db *sql.DB, ctx context.Context, store *OrderStore, userID, chargerID int64, suffix string) string {
	t.Helper()
	created, err := store.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userID, ChargerID: chargerID, IdempotencyKey: "b07-create-" + suffix, RequestHash: "h", TraceID: "t"})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if _, err := store.StartCharging(ctx, order.TransitionCommand{
		UserID: userID, OrderNo: created.OrderNo, IdempotencyKey: "b07-start-" + suffix, RequestHash: "h", TraceID: "t"}); err != nil {
		t.Fatalf("StartCharging() error = %v", err)
	}
	if _, err := store.ConfirmStart(ctx, order.ConfirmStartCommand{
		OrderNo: created.OrderNo, ChargerID: chargerID, OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatalf("ConfirmStart() error = %v", err)
	}
	if _, err := store.StopCharging(ctx, order.TransitionCommand{
		UserID: userID, OrderNo: created.OrderNo, IdempotencyKey: "b07-stop-" + suffix, RequestHash: "h", TraceID: "t"}); err != nil {
		t.Fatalf("StopCharging() error = %v", err)
	}
	if _, err := store.ConfirmStop(ctx, order.ConfirmStopCommand{
		OrderNo: created.OrderNo, ChargerID: chargerID, EnergyWh: 1000, OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatalf("ConfirmStop() error = %v", err)
	}
	if _, err := store.SettleOrder(ctx, order.SettleCommand{
		TransitionCommand: order.TransitionCommand{
			UserID: userID, OrderNo: created.OrderNo, IdempotencyKey: "b07-confirm-" + suffix, RequestHash: "h", TraceID: "t"}}); err != nil {
		t.Fatalf("SettleOrder() error = %v", err)
	}
	return created.OrderNo
}
