package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/auth"
	"github.com/heguangV/charging-station-platform/backend/internal/order"
)

// A-01 persistence, against a real PostgreSQL.
//
// The A-01 module ships the auth port and an in-memory double; the PostgreSQL adapter is B-line work,
// so these tests are what make its statements true: the profile round trip, the anonymization the
// deletion contract promises, the freeze semantics BR-07 relies on, and the invariants migration 0008
// adds to the table. The business consequences are asserted too (a frozen user cannot start charging,
// a deleted phone number becomes available again), because an adapter that only returns the right rows
// can still break the flow it exists for.

// uniquePhone returns an 11-digit phone number that is unique per run.
//
// The tests below provision real accounts, and user_accounts.phone is UNIQUE: a fixed number would
// make the second run of the suite fail on a duplicate key, which is a test defect rather than a
// finding about the code.
func uniquePhone(t *testing.T) string {
	t.Helper()
	digits := strings.NewReplacer(".", "", "-", "").Replace(uniqueSuffix(t))
	if len(digits) > 8 {
		digits = digits[len(digits)-8:]
	}
	for len(digits) < 8 {
		digits = "0" + digits
	}
	return "137" + digits
}

// newAccountFixture inserts a user account and returns its id.
func newAccountFixture(t *testing.T, db *sql.DB, ctx context.Context, phone string) int64 {
	t.Helper()
	var userID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO user_accounts (phone, display_name, password_hash) VALUES ($1, $2, $3) RETURNING id`,
		phone, "开发用户", "pbkdf2-sha256$1000$salt$hash").Scan(&userID); err != nil {
		t.Fatalf("seed user account: %v", err)
	}
	return userID
}

func newMutationAdapter(t *testing.T, db *sql.DB) *AccountMutationAdapter {
	t.Helper()
	adapter, err := NewAccountMutationAdapter(db)
	if err != nil {
		t.Fatalf("NewAccountMutationAdapter() error = %v", err)
	}
	return adapter
}

// accountRow reads the raw row, which is what the deletion invariants are about: the returned view
// could be right while the stored row is not.
type accountRow struct {
	phone        sql.NullString
	displayName  string
	avatarURL    string
	passwordHash string
	status       string
	deletedAt    sql.NullTime
}

func readAccountRow(t *testing.T, db *sql.DB, ctx context.Context, userID int64) accountRow {
	t.Helper()
	var row accountRow
	if err := db.QueryRowContext(ctx, `SELECT phone, display_name, avatar_url, password_hash, status, deleted_at
FROM user_accounts WHERE id = $1`, userID).
		Scan(&row.phone, &row.displayName, &row.avatarURL, &row.passwordHash, &row.status, &row.deletedAt); err != nil {
		t.Fatalf("read account row: %v", err)
	}
	return row
}

func TestAccountMutationProfileRoundTrip(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	phone := uniquePhone(t)
	userID := newAccountFixture(t, db, ctx, phone)

	view, err := adapter.GetProfile(ctx, userID)
	if err != nil {
		t.Fatalf("GetProfile() error = %v", err)
	}
	if view.ID != userID || view.DisplayName != "开发用户" || view.Status != "ACTIVE" {
		t.Fatalf("unexpected view %+v", view)
	}
	if view.RegisteredAt.IsZero() {
		t.Fatal("registeredAt must be the row's created_at")
	}
	// The store returns the raw phone and the service masks it: the two halves of that chain are
	// asserted here, so a change on either side shows up.
	if view.PhoneMasked != phone {
		t.Fatalf("the store must return the raw phone for the service to mask, got %q", view.PhoneMasked)
	}
	wantMasked := phone[:3] + "****" + phone[7:]
	if masked := auth.MaskPhone(view.PhoneMasked); masked != wantMasked {
		t.Fatalf("auth.MaskPhone(raw) = %q, want %q", masked, wantMasked)
	}

	nickname := "新昵称"
	view, err = adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{DisplayName: &nickname})
	if err != nil {
		t.Fatalf("UpdateProfile(nickname) error = %v", err)
	}
	if view.DisplayName != nickname {
		t.Fatalf("display name = %q, want %q", view.DisplayName, nickname)
	}
	if got := readAccountRow(t, db, ctx, userID); got.displayName != nickname {
		t.Fatalf("the nickname was not persisted: %q", got.displayName)
	}

	avatar := "https://cdn.example.com/avatar/1.png"
	if _, err := adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{AvatarURL: &avatar}); err != nil {
		t.Fatalf("UpdateProfile(avatar) error = %v", err)
	}
	if got := readAccountRow(t, db, ctx, userID); got.avatarURL != avatar {
		t.Fatalf("the avatar was not persisted: %q", got.avatarURL)
	}
	// The nickname set earlier must survive an avatar-only update: that is what the COALESCE form is
	// for, and a three-branch implementation could have overwritten it.
	if got := readAccountRow(t, db, ctx, userID); got.displayName != nickname {
		t.Fatalf("an avatar-only update cleared the nickname: %q", got.displayName)
	}

	// Clearing the avatar is an explicit empty string, and both fields at once must land together.
	cleared := ""
	both := "两个字段"
	view, err = adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{DisplayName: &both, AvatarURL: &cleared})
	if err != nil {
		t.Fatalf("UpdateProfile(both) error = %v", err)
	}
	if view.DisplayName != both || view.AvatarURL != "" {
		t.Fatalf("unexpected view after a combined update: %+v", view)
	}

	if _, err := adapter.GetProfile(ctx, userID+999); !errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("unknown account error = %v, want ErrProfileNotFound", err)
	}
}

// The store must not depend on a caller to avoid a nil dereference. The handler rejects a request with
// no fields, so this path was unreachable from HTTP - and a panic waiting for the next caller.
func TestAccountMutationUpdateRequiresAField(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	userID := newAccountFixture(t, db, ctx, uniquePhone(t))

	_, err := adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{})
	if err == nil {
		t.Fatal("expected an update with no fields to be refused")
	}
	if !strings.Contains(err.Error(), "at least one field") {
		t.Fatalf("the refusal should say what is missing, got %q", err.Error())
	}
	if got := readAccountRow(t, db, ctx, userID); got.displayName != "开发用户" {
		t.Fatalf("a refused update changed the row: %q", got.displayName)
	}
}

// UC-U-05 deletion: the account is anonymized in place, disappears from every read, and the phone
// number it held becomes usable again.
func TestAccountMutationDeletionAnonymizesAndFreesThePhone(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	phone := uniquePhone(t)
	userID := newAccountFixture(t, db, ctx, phone)

	nickname := "改个名字再注销"
	if _, err := adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{DisplayName: &nickname}); err != nil {
		t.Fatalf("UpdateProfile() error = %v", err)
	}

	deleted, err := adapter.DeleteAccount(ctx, userID)
	if err != nil || !deleted {
		t.Fatalf("DeleteAccount() = %v, %v; want true", deleted, err)
	}

	row := readAccountRow(t, db, ctx, userID)
	if row.phone.String != "deleted-"+itoa(userID)+"@invalid" {
		t.Fatalf("phone = %q, want the anonymized placeholder", row.phone.String)
	}
	if row.displayName != "已注销用户" {
		t.Fatalf("display name = %q, want the anonymized placeholder", row.displayName)
	}
	if row.passwordHash != "" {
		t.Fatalf("credentials were kept: %q", row.passwordHash)
	}
	if row.avatarURL != "" {
		t.Fatalf("avatar was kept: %q", row.avatarURL)
	}
	if row.status != "DISABLED" {
		t.Fatalf("status = %q, want DISABLED", row.status)
	}
	if !row.deletedAt.Valid {
		t.Fatal("deleted_at was not recorded")
	}

	// Every read path filters the account out, and a second deletion is not an application.
	if _, err := adapter.GetProfile(ctx, userID); !errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("GetProfile() on a deleted account error = %v, want ErrProfileNotFound", err)
	}
	nickname = "again"
	if _, err := adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{DisplayName: &nickname}); !errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("UpdateProfile() on a deleted account error = %v, want ErrProfileNotFound", err)
	}
	if found, err := adapter.SetFrozen(ctx, userID, true); err != nil || found {
		t.Fatalf("SetFrozen() on a deleted account = %v, %v; want false", found, err)
	}
	if deleted, err := adapter.DeleteAccount(ctx, userID); err != nil || deleted {
		t.Fatalf("a second DeleteAccount() = %v, %v; want false", deleted, err)
	}

	// The deleted phone number is free again: registering with it creates a NEW account rather than
	// resurrecting the anonymized row, which is the behaviour a user who deleted and came back needs.
	accountStore, err := NewAccountStore(db)
	if err != nil {
		t.Fatalf("NewAccountStore() error = %v", err)
	}
	account, err := accountStore.EnsureUserWithWallet(ctx, phone)
	if err != nil {
		t.Fatalf("EnsureUserWithWallet() after deletion error = %v", err)
	}
	if account.ID == userID {
		t.Fatal("re-registration resurrected the deleted account")
	}
	fresh := readAccountRow(t, db, ctx, account.ID)
	if fresh.status != "ACTIVE" || fresh.deletedAt.Valid || fresh.phone.String != phone {
		t.Fatalf("unexpected fresh account row %+v", fresh)
	}
}

// BR-07: a frozen account cannot log in and cannot start charging. The login half is A-line logic
// reading this status; the charging half is asserted here through the order store, because that path
// re-reads the account and is the one a frozen user would otherwise keep using.
func TestAccountMutationFreezeBlocksCharging(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	orderStore, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userID, _, _, _, chargerID := orderFlowFixture(t, db, ctx, suffix)
	if userID < 1 {
		t.Fatal("fixture did not create the user")
	}

	created, err := orderStore.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userID, ChargerID: chargerID,
		IdempotencyKey: "freeze-create-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	frozen, err := adapter.SetFrozen(ctx, userID, true)
	if err != nil || !frozen {
		t.Fatalf("SetFrozen(true) = %v, %v; want true", frozen, err)
	}
	if got := readAccountRow(t, db, ctx, userID); got.status != "DISABLED" {
		t.Fatalf("status = %q, want DISABLED", got.status)
	}

	// Starting a charge re-reads the account, so the freeze holds even while a stale session token is
	// still alive in Redis - which is exactly the window the two-store boundary leaves open.
	if _, err := orderStore.StartCharging(ctx, order.TransitionCommand{
		UserID: userID, OrderNo: created.OrderNo,
		IdempotencyKey: "freeze-start-" + suffix, RequestHash: "h", TraceID: "t",
	}); !errors.Is(err, order.ErrUserFrozen) {
		t.Fatalf("StartCharging() for a frozen user error = %v, want ErrUserFrozen", err)
	}
	if got := orderStatus(t, db, ctx, created.OrderNo); got != order.StatusCreated {
		t.Fatalf("the order moved to %s while its user was frozen", got)
	}

	if unfrozen, err := adapter.SetFrozen(ctx, userID, false); err != nil || !unfrozen {
		t.Fatalf("SetFrozen(false) = %v, %v; want true", unfrozen, err)
	}
	if _, err := orderStore.StartCharging(ctx, order.TransitionCommand{
		UserID: userID, OrderNo: created.OrderNo,
		IdempotencyKey: "freeze-start-again-" + suffix, RequestHash: "h", TraceID: "t",
	}); err != nil {
		t.Fatalf("StartCharging() after unfreezing error = %v", err)
	}
	if got := orderStatus(t, db, ctx, created.OrderNo); got != order.StatusStarting {
		t.Fatalf("status = %s, want STARTING after unfreezing", got)
	}
}

// Migration 0008 is what keeps a half-deleted account from existing: the invariants are only worth
// documenting if the database refuses to break them, so this drives the statements directly.
func TestDeletedAccountInvariantsAreEnforcedByTheDatabase(t *testing.T) {
	db, ctx := integrationDB(t)
	userID := newAccountFixture(t, db, ctx, uniquePhone(t))
	adapter := newMutationAdapter(t, db)
	if deleted, err := adapter.DeleteAccount(ctx, userID); err != nil || !deleted {
		t.Fatalf("DeleteAccount() = %v, %v", deleted, err)
	}

	// Deleted but active: the account would be able to log in again.
	if _, err := db.ExecContext(ctx, `UPDATE user_accounts SET status = 'ACTIVE' WHERE id = $1`, userID); err == nil {
		t.Fatal("the database accepted a deleted account in status ACTIVE")
	}
	// Deleted but with credentials: a rolled-back-looking account that still authenticates.
	if _, err := db.ExecContext(ctx, `UPDATE user_accounts SET password_hash = 'restored-hash' WHERE id = $1`, userID); err == nil {
		t.Fatal("the database accepted credentials on a deleted account")
	}
	// An unbounded avatar column would let any size of content into the user table.
	if _, err := db.ExecContext(ctx, `UPDATE user_accounts SET avatar_url = repeat('a', 513) WHERE id = $1`, userID); err == nil {
		t.Fatal("the database accepted an avatar URL beyond the storage bound")
	}

	// The anonymized phone stays unique, so two deletions cannot collide on the placeholder.
	other := newAccountFixture(t, db, ctx, uniquePhone(t))
	if deleted, err := adapter.DeleteAccount(ctx, other); err != nil || !deleted {
		t.Fatalf("DeleteAccount(second) = %v, %v", deleted, err)
	}
	if row := readAccountRow(t, db, ctx, other); row.phone.String != "deleted-"+itoa(other)+"@invalid" {
		t.Fatalf("second placeholder = %q", row.phone.String)
	}
}

func itoa(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	digits := make([]byte, 0, 20)
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	if negative {
		return "-" + string(digits)
	}
	return string(digits)
}

// A registered account must keep its profile fields when the rest of the flow runs: the fixture is
// reused across suites, so this guards against a later change silently nulling them.
func TestProfileColumnsSurviveAWalletProvisioning(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewAccountStore(db)
	if err != nil {
		t.Fatalf("NewAccountStore() error = %v", err)
	}
	phone := uniquePhone(t)
	account, err := store.EnsureUserWithWallet(ctx, phone)
	if err != nil {
		t.Fatalf("EnsureUserWithWallet() error = %v", err)
	}
	adapter := newMutationAdapter(t, db)
	nickname := "保留昵称"
	if _, err := adapter.UpdateProfile(ctx, account.ID, auth.ProfileUpdate{DisplayName: &nickname}); err != nil {
		t.Fatalf("UpdateProfile() error = %v", err)
	}
	again, err := store.EnsureUserWithWallet(ctx, phone)
	if err != nil {
		t.Fatalf("EnsureUserWithWallet() second call error = %v", err)
	}
	if again.ID != account.ID {
		t.Fatalf("the existing account was replaced: %d -> %d", account.ID, again.ID)
	}
	row := readAccountRow(t, db, ctx, account.ID)
	if row.displayName != nickname {
		t.Fatalf("provisioning cleared the nickname: %q", row.displayName)
	}
	if row.deletedAt.Valid {
		t.Fatal("an active account must not carry deleted_at")
	}
	// A regenerated wallet is not part of this assertion; only that the profile survived the pass.
	_ = time.Now
}
