package wallet

import (
	"context"
	"errors"
	"testing"
)

type fakeStore struct {
	view       WalletView
	topUp      WalletView
	topUpErr   error
	entries    EntryPage
	entriesErr error
	refund     WalletView
	refundErr  error
	topUps     []TopUpCommand
}

func (f *fakeStore) Wallet(context.Context, int64) (WalletView, error) {
	return f.view, nil
}

func (f *fakeStore) TopUp(_ context.Context, command TopUpCommand) (WalletView, error) {
	f.topUps = append(f.topUps, command)
	return f.topUp, f.topUpErr
}

func (f *fakeStore) ListTransactions(context.Context, EntryFilter) (EntryPage, error) {
	return f.entries, f.entriesErr
}

func (f *fakeStore) RefundOrder(context.Context, RefundCommand) (WalletView, error) {
	return f.refund, f.refundErr
}

func TestTopUpValidatesAmount(t *testing.T) {
	service, _ := NewService(&fakeStore{})
	ctx := context.Background()

	for _, amount := range []int64{0, -100, MaxTopUpCent + 1} {
		if _, err := service.TopUp(ctx, TopUpCommand{UserID: 1, AmountCent: amount}); !errors.Is(err, ErrInvalidTopUpAmount) {
			t.Fatalf("amount %d error = %v, want ErrInvalidTopUpAmount", amount, err)
		}
	}
	if err := ValidateTopUpAmount(MinTopUpCent); err != nil {
		t.Fatalf("minimum amount rejected: %v", err)
	}
}

func TestTopUpRequiresUserAndStore(t *testing.T) {
	if _, err := NewService(nil); err == nil {
		t.Fatal("nil store accepted")
	}
	service, _ := NewService(&fakeStore{})
	if _, err := service.TopUp(ctxOf(), TopUpCommand{UserID: 0, AmountCent: 100}); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("unknown user error = %v", err)
	}
}

func TestTransactionsValidatesFilter(t *testing.T) {
	service, _ := NewService(&fakeStore{})
	ctx := context.Background()

	if _, err := service.Transactions(ctx, EntryFilter{UserID: 0}); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("unknown user error = %v", err)
	}
	if _, err := service.Transactions(ctx, EntryFilter{UserID: 1, Page: 0, PageSize: 20}); !errors.Is(err, ErrInvalidLedgerFilter) {
		t.Fatalf("bad page error = %v", err)
	}
	if _, err := service.Transactions(ctx, EntryFilter{UserID: 1, Page: 1, PageSize: 101}); !errors.Is(err, ErrInvalidLedgerFilter) {
		t.Fatalf("oversized page error = %v", err)
	}
	if _, err := service.Transactions(ctx, EntryFilter{UserID: 1, Page: 1, PageSize: 20, Type: "SOMETHING"}); !errors.Is(err, ErrInvalidLedgerFilter) {
		t.Fatalf("bad type error = %v", err)
	}
	for _, valid := range []string{"", TypeTopUp, TypeCharge, TypeRefund, TypeAdjustment} {
		if _, err := service.Transactions(ctx, EntryFilter{UserID: 1, Page: 1, PageSize: 20, Type: valid}); err != nil {
			t.Fatalf("type %q rejected: %v", valid, err)
		}
	}
}

func TestRefundOrderRequiresAdminAndOrderNo(t *testing.T) {
	service, _ := NewService(&fakeStore{})
	ctx := context.Background()

	if _, err := service.RefundOrder(ctx, RefundCommand{AdminID: 0, OrderNo: "ORD20260915120000aaaa"}); !errors.Is(err, ErrOrderNotRefundable) {
		t.Fatalf("no admin error = %v", err)
	}
	if _, err := service.RefundOrder(ctx, RefundCommand{AdminID: 1, OrderNo: ""}); !errors.Is(err, ErrOrderNotRefundable) {
		t.Fatalf("no order error = %v", err)
	}
}

func TestViewRequiresUser(t *testing.T) {
	service, _ := NewService(&fakeStore{})
	if _, err := service.View(ctxOf(), 0); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("error = %v, want ErrWalletNotFound", err)
	}
}

func ctxOf() context.Context { return context.Background() }
