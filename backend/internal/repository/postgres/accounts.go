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
