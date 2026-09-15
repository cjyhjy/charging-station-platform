package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/heguangV/charging-station-platform/backend/internal/auth"
)

// AccountStore implements auth.AccountReader against user_accounts and
// admin_accounts. Every query is parameterized.
type AccountStore struct {
	db *sql.DB
}

// NewAccountStore binds the store to a connection pool.
func NewAccountStore(db *sql.DB) (*AccountStore, error) {
	if db == nil {
		return nil, errors.New("postgres: account store requires a database")
	}
	return &AccountStore{db: db}, nil
}

// FindUserByAccount resolves a user by exact phone or email match. Unknown
// accounts return (nil, nil) so login timing is equalized by the service.
func (s *AccountStore) FindUserByAccount(ctx context.Context, account string) (*auth.UserAccount, error) {
	const query = `SELECT id, phone, email, display_name, password_hash, status
FROM user_accounts
WHERE phone = $1 OR email = $1
LIMIT 1`

	var user auth.UserAccount
	var phone, email sql.NullString
	err := s.db.QueryRowContext(ctx, query, account).Scan(
		&user.ID, &phone, &email, &user.DisplayName, &user.PasswordHash, &user.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	user.Phone = phone.String
	user.Email = email.String
	return &user, nil
}

// FindAdminByUsername resolves an administrator by exact username match.
func (s *AccountStore) FindAdminByUsername(ctx context.Context, username string) (*auth.AdminAccount, error) {
	const query = `SELECT id, username, role, password_hash, status
FROM admin_accounts
WHERE username = $1
LIMIT 1`

	var admin auth.AdminAccount
	err := s.db.QueryRowContext(ctx, query, username).Scan(
		&admin.ID, &admin.Username, &admin.Role, &admin.PasswordHash, &admin.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &admin, nil
}

// AccountMutationAdapter implements the A-line auth.AccountMutation port
// over user_accounts. Delivered by A-01 per the two-track split: the port
// lives in internal/auth, the PostgreSQL adapter lives here (B-02).
type AccountMutationAdapter struct {
	db *sql.DB
}

// NewAccountMutationAdapter binds the adapter to a connection pool.
func NewAccountMutationAdapter(db *sql.DB) (*AccountMutationAdapter, error) {
	if db == nil {
		return nil, errors.New("postgres: mutation adapter requires a database")
	}
	return &AccountMutationAdapter{db: db}, nil
}

// GetProfile returns the profile view; deleted accounts are not found.
func (s *AccountMutationAdapter) GetProfile(ctx context.Context, userID int64) (auth.ProfileView, error) {
	const query = `SELECT id, phone, display_name, avatar_url, status, created_at
FROM user_accounts WHERE id = $1 AND deleted_at IS NULL`
	var view auth.ProfileView
	var phone, avatar sql.NullString
	if err := s.db.QueryRowContext(ctx, query, userID).Scan(
		&view.ID, &phone, &view.DisplayName, &avatar, &view.Status, &view.RegisteredAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return auth.ProfileView{}, auth.ErrProfileNotFound
		}
		return auth.ProfileView{}, err
	}
	view.PhoneMasked = phone.String
	view.AvatarURL = avatar.String
	return view, nil
}

// UpdateProfile applies a nickname and/or avatar change.
func (s *AccountMutationAdapter) UpdateProfile(ctx context.Context, userID int64, update auth.ProfileUpdate) (auth.ProfileView, error) {
	var view auth.ProfileView
	var phone, avatar sql.NullString
	var row *sql.Row
	if update.DisplayName != nil && update.AvatarURL != nil {
		row = s.db.QueryRowContext(ctx, `UPDATE user_accounts
SET display_name = $2, avatar_url = $3, updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND deleted_at IS NULL
RETURNING id, phone, display_name, avatar_url, status, created_at`,
			userID, *update.DisplayName, *update.AvatarURL)
	} else if update.DisplayName != nil {
		row = s.db.QueryRowContext(ctx, `UPDATE user_accounts
SET display_name = $2, updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND deleted_at IS NULL
RETURNING id, phone, display_name, avatar_url, status, created_at`,
			userID, *update.DisplayName)
	} else {
		row = s.db.QueryRowContext(ctx, `UPDATE user_accounts
SET avatar_url = $2, updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND deleted_at IS NULL
RETURNING id, phone, display_name, avatar_url, status, created_at`,
			userID, *update.AvatarURL)
	}
	if err := row.Scan(&view.ID, &phone, &view.DisplayName, &avatar, &view.Status, &view.RegisteredAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return auth.ProfileView{}, auth.ErrProfileNotFound
		}
		return auth.ProfileView{}, err
	}
	view.PhoneMasked = phone.String
	view.AvatarURL = avatar.String
	return view, nil
}

// DeleteAccount anonymizes the account in place (UC-U-05 申请注销): the
// phone and display name are replaced with irreversible placeholders, the
// password is dropped and the account is disabled. Returns false when the
// account was already deleted or never existed.
func (s *AccountMutationAdapter) DeleteAccount(ctx context.Context, userID int64) (bool, error) {
	tag, err := s.db.ExecContext(ctx, `UPDATE user_accounts
SET phone = 'deleted-' || id::text || '@invalid',
    display_name = '已注销用户',
    password_hash = '',
    avatar_url = '',
    status = 'DISABLED',
    deleted_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND deleted_at IS NULL`, userID)
	if err != nil {
		return false, err
	}
	affected, err := tag.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// SetFrozen sets or clears the DISABLED status (BR-07). Returns false when
// the account is missing or already deleted.
func (s *AccountMutationAdapter) SetFrozen(ctx context.Context, userID int64, frozen bool) (bool, error) {
	status := "ACTIVE"
	if frozen {
		status = "DISABLED"
	}
	tag, err := s.db.ExecContext(ctx, `UPDATE user_accounts
SET status = $2, updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND deleted_at IS NULL`, userID, status)
	if err != nil {
		return false, err
	}
	affected, err := tag.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

var _ auth.AccountMutation = (*AccountMutationAdapter)(nil)

// EnsureUserWithWallet registers the user and the wallet when missing
// (UC-U-01) and returns the account either way. Both paths execute the same
// statements — an INSERT ... ON CONFLICT DO NOTHING for the user and for the
// wallet, then a SELECT — so a registered phone is not enumerable through
// response timing. Display name defaults to 用户 + the last four phone
// digits, balance starts at zero, and no password is set until the user
// creates one.
func (s *AccountStore) EnsureUserWithWallet(ctx context.Context, phone string) (auth.UserAccount, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return auth.UserAccount{}, err
	}
	defer func() { _ = tx.Rollback() }()

	displayName := "用户" + phone[len(phone)-4:]
	if _, err := tx.ExecContext(ctx, `INSERT INTO user_accounts (phone, display_name, password_hash, status)
VALUES ($1, $2, '', 'ACTIVE') ON CONFLICT (phone) DO NOTHING`, phone, displayName); err != nil {
		return auth.UserAccount{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wallet_accounts (user_id, balance_cents)
SELECT id, 0 FROM user_accounts WHERE phone = $1 ON CONFLICT (user_id) DO NOTHING`, phone); err != nil {
		return auth.UserAccount{}, err
	}

	var user auth.UserAccount
	var email sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT id, phone, email, display_name, password_hash, status
FROM user_accounts WHERE phone = $1`, phone).Scan(
		&user.ID, &user.Phone, &email, &user.DisplayName, &user.PasswordHash, &user.Status)
	if err != nil {
		return auth.UserAccount{}, err
	}
	user.Email = email.String
	if err := tx.Commit(); err != nil {
		return auth.UserAccount{}, err
	}
	return user, nil
}
