package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/wallet"
)

// WalletStore implements wallet.Store. Delivered by A-04 as the B-02
// handoff: the port lives in internal/wallet, the PostgreSQL adapter here.
// Every mutation runs in one transaction covering the wallet row, the
// ledger row and (for refunds) the order payment state.
type WalletStore struct {
	db    *sql.DB
	clock func() time.Time
}

// NewWalletStore binds the store to a connection pool.
func NewWalletStore(db *sql.DB) (*WalletStore, error) {
	if db == nil {
		return nil, errors.New("postgres: wallet store requires a database")
	}
	return &WalletStore{db: db, clock: time.Now}, nil
}

// claimWalletIdempotency mirrors the order store's claim semantics (fresh
// slot, hash-checked replay, expired-record supersede) with a wallet scope.
func (s *WalletStore) claimWalletIdempotency(tx *sql.Tx, ctx context.Context, scope, key, requestHash string) ([]byte, bool, error) {
	tag, err := tx.ExecContext(ctx, `INSERT INTO idempotency_records (scope, idempotency_key, request_hash, status, expires_at)
VALUES ($1, $2, $3, 'IN_PROGRESS', $4)
ON CONFLICT (scope, idempotency_key) DO NOTHING`,
		scope, key, requestHash, s.clock().Add(idempotencyTTL))
	if err != nil {
		return nil, false, err
	}
	if affected, err := tag.RowsAffected(); err == nil && affected == 1 {
		return nil, false, nil
	}

	var storedHash, status string
	var body []byte
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx, `SELECT request_hash, status, response_body, expires_at
FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2 FOR UPDATE`, scope, key).Scan(&storedHash, &status, &body, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, wallet.ErrOrderNotRefundable
	}
	if err != nil {
		return nil, false, err
	}
	if s.clock().After(expiresAt) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM idempotency_records WHERE scope = $1 AND idempotency_key = $2`, scope, key); err != nil {
			return nil, false, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO idempotency_records (scope, idempotency_key, request_hash, status, expires_at)
VALUES ($1, $2, $3, 'IN_PROGRESS', $4)`, scope, key, requestHash, s.clock().Add(idempotencyTTL)); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if storedHash != requestHash {
		return nil, false, wallet.ErrIdempotencyConflict
	}
	switch status {
	case "SUCCEEDED":
		return body, true, nil
	case "FAILED":
		if _, err := tx.ExecContext(ctx, `UPDATE idempotency_records SET status = 'IN_PROGRESS', updated_at = CURRENT_TIMESTAMP
WHERE scope = $1 AND idempotency_key = $2`, scope, key); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	default:
		return nil, false, wallet.ErrIdempotencyInProgress
	}
}

func (s *WalletStore) finalizeWalletIdempotency(tx *sql.Tx, ctx context.Context, scope, key string, body []byte) error {
	_, err := tx.ExecContext(ctx, `UPDATE idempotency_records
SET status = 'SUCCEEDED', response_code = 0, response_body = $3, updated_at = CURRENT_TIMESTAMP
WHERE scope = $1 AND idempotency_key = $2`, scope, key, body)
	return err
}

// ensureWalletRow creates the zero wallet when missing and returns the
// balance under a row lock. The re-read after the upsert matters: a
// concurrent transaction can create the row between the empty SELECT and
// the INSERT, and its committed balance — not zero — is the truth the
// caller's absolute balance update must build on.
func (s *WalletStore) ensureWalletRow(tx *sql.Tx, ctx context.Context, userID int64) (int64, error) {
	var balance int64
	err := tx.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1 FOR UPDATE`, userID).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO wallet_accounts (user_id, balance_cents) VALUES ($1, 0) ON CONFLICT DO NOTHING`, userID); err != nil {
			return 0, err
		}
		if err := tx.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1 FOR UPDATE`, userID).Scan(&balance); err != nil {
			return 0, err
		}
	}
	return balance, nil
}

// Wallet returns the balance view, auto-creating a zero wallet when the
// user has none.
func (s *WalletStore) Wallet(ctx context.Context, userID int64) (wallet.WalletView, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wallet.WalletView{}, err
	}
	defer func() { _ = tx.Rollback() }()

	balance, err := s.ensureWalletRow(tx, ctx, userID)
	if err != nil {
		return wallet.WalletView{}, err
	}
	if err := tx.Commit(); err != nil {
		return wallet.WalletView{}, err
	}
	return wallet.WalletView{BalanceCent: balance}, nil
}

// TopUp credits the amount and writes the TOP_UP ledger row inside one
// transaction. A replayed key returns the unchanged wallet view.
func (s *WalletStore) TopUp(ctx context.Context, command wallet.TopUpCommand) (wallet.WalletView, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wallet.WalletView{}, err
	}
	defer func() { _ = tx.Rollback() }()

	scope := fmt.Sprintf("wallet:%d:top-up", command.UserID)
	replay, replayed, err := s.claimWalletIdempotency(tx, ctx, scope, command.IdempotencyKey, command.RequestHash)
	if err != nil {
		return wallet.WalletView{}, err
	}
	if replayed {
		var view wallet.WalletView
		if err := json.Unmarshal(replay, &view); err != nil {
			return wallet.WalletView{}, fmt.Errorf("decode idempotency replay: %w", err)
		}
		return view, nil
	}

	balance, err := s.ensureWalletRow(tx, ctx, command.UserID)
	if err != nil {
		return wallet.WalletView{}, err
	}
	after := balance + command.AmountCent
	if _, err := tx.ExecContext(ctx, `UPDATE wallet_accounts
SET balance_cents = $2, version = version + 1, updated_at = CURRENT_TIMESTAMP
WHERE user_id = $1`, command.UserID, after); err != nil {
		return wallet.WalletView{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wallet_transactions
    (user_id, transaction_type, amount_cents, balance_before_cents, balance_after_cents, idempotency_key)
VALUES ($1, 'TOP_UP', $2, $3, $4, $5)`,
		command.UserID, command.AmountCent, balance, after,
		fmt.Sprintf("topup:%d:%s", command.UserID, command.IdempotencyKey)); err != nil {
		return wallet.WalletView{}, err
	}

	view := wallet.WalletView{BalanceCent: after}
	body, err := json.Marshal(view)
	if err != nil {
		return wallet.WalletView{}, err
	}
	if err := s.finalizeWalletIdempotency(tx, ctx, scope, command.IdempotencyKey, body); err != nil {
		return wallet.WalletView{}, err
	}
	if err := tx.Commit(); err != nil {
		return wallet.WalletView{}, err
	}
	return view, nil
}

// ListTransactions returns one page of the user's ledger, newest first.
func (s *WalletStore) ListTransactions(ctx context.Context, filter wallet.EntryFilter) (wallet.EntryPage, error) {
	const filterSQL = `user_id = $1
  AND ($2 = '' OR transaction_type = $2)`
	const pageQuery = `SELECT id, transaction_type, amount_cents, balance_before_cents, balance_after_cents,
COALESCE((SELECT order_no FROM charging_orders o WHERE o.id = t.order_id), '') AS order_no,
idempotency_key, created_at
FROM wallet_transactions t
WHERE ` + filterSQL + `
ORDER BY created_at DESC
LIMIT $3 OFFSET $4`
	const countQuery = `SELECT count(*) FROM wallet_transactions WHERE ` + filterSQL

	offset := (filter.Page - 1) * filter.PageSize
	args := []any{filter.UserID, filter.Type}

	rows, err := s.db.QueryContext(ctx, pageQuery, append(args, filter.PageSize, offset)...)
	if err != nil {
		return wallet.EntryPage{}, err
	}
	defer rows.Close()

	page := wallet.EntryPage{Meta: wallet.PageMeta{Page: filter.Page, PageSize: filter.PageSize}}
	for rows.Next() {
		var entry wallet.Entry
		if err := rows.Scan(&entry.ID, &entry.TransactionType, &entry.AmountCent,
			&entry.BalanceBeforeCent, &entry.BalanceAfterCent, &entry.OrderNo,
			&entry.IdempotencyKey, &entry.CreatedAt); err != nil {
			return wallet.EntryPage{}, err
		}
		entry.CreatedAt = entry.CreatedAt.UTC()
		page.Items = append(page.Items, entry)
	}
	if err := rows.Err(); err != nil {
		return wallet.EntryPage{}, err
	}
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&page.Meta.Total); err != nil {
		return wallet.EntryPage{}, err
	}
	return page, nil
}

// RefundOrder returns a completed order's settled amount to the wallet
// (admin action, BR-11): the REFUND ledger row, the wallet credit and the
// order's payment-state revert (paid_cents cleared, status PENDING) commit
// together, with the audit trail in the same transaction.
func (s *WalletStore) RefundOrder(ctx context.Context, command wallet.RefundCommand) (wallet.WalletView, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wallet.WalletView{}, err
	}
	defer func() { _ = tx.Rollback() }()

	scope := fmt.Sprintf("wallet:refund:%s", command.OrderNo)
	replay, replayed, err := s.claimWalletIdempotency(tx, ctx, scope, command.IdempotencyKey, command.RequestHash)
	if err != nil {
		return wallet.WalletView{}, err
	}
	if replayed {
		var view wallet.WalletView
		if err := json.Unmarshal(replay, &view); err != nil {
			return wallet.WalletView{}, fmt.Errorf("decode idempotency replay: %w", err)
		}
		return view, nil
	}

	var orderID, userID, paidCents int64
	var orderStatus, paymentStatus string
	err = tx.QueryRowContext(ctx, `SELECT id, user_id, paid_cents, status, payment_status FROM charging_orders
WHERE order_no = $1 FOR UPDATE`, command.OrderNo).Scan(&orderID, &userID, &paidCents, &orderStatus, &paymentStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return wallet.WalletView{}, wallet.ErrOrderNotRefundable
	}
	if err != nil {
		return wallet.WalletView{}, err
	}
	if orderStatus != "COMPLETED" || paidCents <= 0 || paymentStatus == "PENDING" {
		return wallet.WalletView{}, wallet.ErrOrderNotRefundable
	}

	balance, err := s.ensureWalletRow(tx, ctx, userID)
	if err != nil {
		return wallet.WalletView{}, err
	}
	after := balance + paidCents
	if _, err := tx.ExecContext(ctx, `UPDATE wallet_accounts
SET balance_cents = $2, version = version + 1, updated_at = CURRENT_TIMESTAMP
WHERE user_id = $1`, userID, after); err != nil {
		return wallet.WalletView{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wallet_transactions
    (user_id, order_id, transaction_type, amount_cents, balance_before_cents, balance_after_cents, idempotency_key)
VALUES ($1, $2, 'REFUND', $3, $4, $5, $6)`,
		userID, orderID, paidCents, balance, after,
		fmt.Sprintf("refund:%s", command.OrderNo)); err != nil {
		return wallet.WalletView{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE charging_orders
SET paid_cents = 0, payment_status = 'PENDING', version = version + 1, updated_at = CURRENT_TIMESTAMP
WHERE id = $1`, orderID); err != nil {
		return wallet.WalletView{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operation_logs
    (actor_type, actor_id, action, resource_type, resource_id, request_id, payload)
VALUES ('ADMIN', $1, 'wallet.refund', 'order', $2, $3, $4::jsonb)`,
		fmt.Sprintf("%d", command.AdminID), command.OrderNo, command.TraceID,
		fmt.Sprintf(`{"refundCent":%d,"orderNo":%q}`, paidCents, command.OrderNo)); err != nil {
		return wallet.WalletView{}, err
	}

	view := wallet.WalletView{BalanceCent: after}
	body, err := json.Marshal(view)
	if err != nil {
		return wallet.WalletView{}, err
	}
	if err := s.finalizeWalletIdempotency(tx, ctx, scope, command.IdempotencyKey, body); err != nil {
		return wallet.WalletView{}, err
	}
	if err := tx.Commit(); err != nil {
		return wallet.WalletView{}, err
	}
	return view, nil
}
