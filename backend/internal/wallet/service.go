// Package wallet implements the A-04 wallet domain: top-up, the auditable
// ledger, refunds of settled orders and the settlement queries.
//
// Money is integer cents (BR-09). Every mutation runs in one transaction on
// the store side so the wallet row, the ledger row and any order payment
// state commit atomically; idempotency keys make replays inert.
package wallet

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Ledger transaction types (shared registry: wallet_transactions.type).
const (
	TypeTopUp      = "TOP_UP"
	TypeCharge     = "CHARGE"
	TypeRefund     = "REFUND"
	TypeAdjustment = "ADJUSTMENT"
)

// Payment states mirrored from the order domain for the ledger filters.
const (
	PaymentPending     = "PENDING"
	PaymentPaid        = "PAID"
	PaymentPartialPaid = "PARTIAL_PAID"
)

// Top-up bounds from UC-U-05: 0.01..10000 元.
const (
	MinTopUpCent = 1
	MaxTopUpCent = 1_000_000
)

var (
	// ErrIdempotencyConflict maps to 409 IDEMPOTENCY_CONFLICT.
	ErrIdempotencyConflict = errors.New("wallet: idempotency key reused for a different request")
	// ErrIdempotencyInProgress maps to 409 IDEMPOTENCY_CONFLICT as well.
	ErrIdempotencyInProgress = errors.New("wallet: identical request is still in progress")
	// ErrInvalidTopUpAmount maps to 400 INVALID_ARGUMENT.
	ErrInvalidTopUpAmount = errors.New("wallet: top-up amount is outside 0.01..10000 yuan")
	// ErrWalletNotFound reports a missing wallet (auto-created on demand by
	// the store, so this surfaces only for unknown users).
	ErrWalletNotFound = errors.New("wallet: wallet not found")
	// ErrOrderNotRefundable maps to 409: refund requires a completed order
	// with a positive settled amount.
	ErrOrderNotRefundable = errors.New("wallet: order has no settled amount to refund")
	// ErrOrderNotFound maps to 404: the order a refund names does not exist.
	// It is deliberately distinct from ErrOrderNotRefundable, because the
	// caller cannot fix a missing order by looking at the order's state.
	ErrOrderNotFound = errors.New("wallet: order not found")
	// ErrInvalidLedgerFilter reports a malformed ledger query.
	ErrInvalidLedgerFilter = errors.New("wallet: invalid ledger filter")
)

// WalletView is the balance summary.
type WalletView struct {
	BalanceCent int64 `json:"balanceCent"`
}

// Entry is one auditable ledger row (wallet_transactions).
type Entry struct {
	ID                int64     `json:"id"`
	TransactionType   string    `json:"transactionType"`
	AmountCent        int64     `json:"amountCent"`
	BalanceBeforeCent int64     `json:"balanceBeforeCent"`
	BalanceAfterCent  int64     `json:"balanceAfterCent"`
	OrderNo           string    `json:"orderNo,omitempty"`
	IdempotencyKey    string    `json:"-"`
	CreatedAt         time.Time `json:"createdAt"`
}

// EntryPage is one page of the ledger.
type EntryPage struct {
	Items []Entry  `json:"items"`
	Meta  PageMeta `json:"meta"`
}

// PageMeta is the contract pagination metadata.
type PageMeta struct {
	Page     int64 `json:"page"`
	PageSize int64 `json:"pageSize"`
	Total    int64 `json:"total"`
}

// EntryFilter carries validated ledger query parameters.
type EntryFilter struct {
	UserID   int64
	Page     int64
	PageSize int64
	Type     string // "", TOP_UP, CHARGE, REFUND, ADJUSTMENT
}

// TopUpCommand carries a validated top-up request.
type TopUpCommand struct {
	UserID         int64
	AmountCent     int64
	IdempotencyKey string
	RequestHash    string
	TraceID        string
}

// RefundCommand carries a validated admin refund request.
type RefundCommand struct {
	AdminID        int64
	OrderNo        string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
}

// Store persists wallet state. Mutating methods own their transactions:
// wallet row, ledger row and order payment state commit together.
type Store interface {
	// Wallet returns the balance view, auto-creating a zero wallet when the
	// user has none (registration guarantees one, so missing rows are only
	// possible for legacy rows).
	Wallet(ctx context.Context, userID int64) (WalletView, error)
	// TopUp credits the amount and writes the TOP_UP ledger row. A replayed
	// idempotency key returns the unchanged wallet view without a second
	// credit.
	TopUp(ctx context.Context, command TopUpCommand) (WalletView, error)
	// ListTransactions returns one page of the user's ledger.
	ListTransactions(ctx context.Context, filter EntryFilter) (EntryPage, error)
	// RefundOrder returns a completed order's settled amount to the wallet
	// (admin action, BR-11 audit inside the transaction).
	RefundOrder(ctx context.Context, command RefundCommand) (WalletView, error)
}

// Service validates commands and delegates persistence to a Store.
type Service struct {
	store Store
	clock func() time.Time
}

// NewService wires the service to its store.
func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("wallet: store is required")
	}
	return &Service{store: store, clock: time.Now}, nil
}

// View returns the caller's wallet balance.
func (s *Service) View(ctx context.Context, userID int64) (WalletView, error) {
	if userID < 1 {
		return WalletView{}, ErrWalletNotFound
	}
	return s.store.Wallet(ctx, userID)
}

// TopUp credits the wallet through the simulated payment channel
// (UC-U-05 余额充值).
func (s *Service) TopUp(ctx context.Context, command TopUpCommand) (WalletView, error) {
	if command.UserID < 1 {
		return WalletView{}, ErrWalletNotFound
	}
	if command.AmountCent < MinTopUpCent || command.AmountCent > MaxTopUpCent {
		return WalletView{}, ErrInvalidTopUpAmount
	}
	return s.store.TopUp(ctx, command)
}

// Transactions returns one page of the caller's ledger.
func (s *Service) Transactions(ctx context.Context, filter EntryFilter) (EntryPage, error) {
	if filter.UserID < 1 {
		return EntryPage{}, ErrWalletNotFound
	}
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > 100 {
		return EntryPage{}, ErrInvalidLedgerFilter
	}
	switch filter.Type {
	case "", TypeTopUp, TypeCharge, TypeRefund, TypeAdjustment:
	default:
		return EntryPage{}, ErrInvalidLedgerFilter
	}
	return s.store.ListTransactions(ctx, filter)
}

// RefundOrder returns a completed order's settled amount to the wallet
// (admin action). The order's payment state reverts to PENDING with the
// settled amount cleared, so the debt semantics of the order domain apply
// unchanged.
func (s *Service) RefundOrder(ctx context.Context, command RefundCommand) (WalletView, error) {
	if command.AdminID < 1 {
		return WalletView{}, ErrOrderNotRefundable
	}
	if command.OrderNo == "" {
		return WalletView{}, ErrOrderNotRefundable
	}
	return s.store.RefundOrder(ctx, command)
}

// ValidateTopUpAmount is exported for the HTTP layer's early bound checks.
func ValidateTopUpAmount(amountCent int64) error {
	if amountCent < MinTopUpCent || amountCent > MaxTopUpCent {
		return fmt.Errorf("%w: %d", ErrInvalidTopUpAmount, amountCent)
	}
	return nil
}
