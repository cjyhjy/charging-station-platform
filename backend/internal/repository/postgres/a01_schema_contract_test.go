package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/auth"
)

// A-01 持久化收口：迁移结论必须是可执行的，不能只是"看起来够用"。
//
// 第一步要回答的问题是"0008 是否完整覆盖 A-01，是否需要 0009"。回答方式不是读一遍迁移文件，而是
// 把适配器与 A-01 契约需要的每一个结构断言出来：列（含可空性与默认值）、约束、索引，以及**与既有
// 结构不冲突**（phone/email 唯一性、phone-or-email 约束、status 枚举都还在）。全部通过即说明数据库
// 侧没有缺口，因此不需要 0009；任何一条缺失都会在这里失败，并直接指出缺什么。

type columnSpec struct {
	name     string
	dataType string
	nullable bool
	// defaultValue is checked as a substring of column_default when not empty.
	defaultHint string
}

func assertColumns(t *testing.T, db *sql.DB, ctx context.Context, table string, specs []columnSpec) {
	t.Helper()
	for _, spec := range specs {
		var dataType, nullable, defaultValue string
		err := db.QueryRowContext(ctx, `SELECT data_type, is_nullable, coalesce(column_default, '')
  FROM information_schema.columns
 WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`, table, spec.name).
			Scan(&dataType, &nullable, &defaultValue)
		if errors.Is(err, sql.ErrNoRows) {
			t.Errorf("%s.%s is missing from the schema", table, spec.name)
			continue
		}
		if err != nil {
			t.Fatalf("read column %s.%s: %v", table, spec.name, err)
		}
		if dataType != spec.dataType {
			t.Errorf("%s.%s type = %s, want %s", table, spec.name, dataType, spec.dataType)
		}
		if isNullable := nullable == "YES"; isNullable != spec.nullable {
			t.Errorf("%s.%s nullable = %v, want %v", table, spec.name, isNullable, spec.nullable)
		}
		if spec.defaultHint != "" && !strings.Contains(defaultValue, spec.defaultHint) {
			t.Errorf("%s.%s default = %q, want it to contain %q", table, spec.name, defaultValue, spec.defaultHint)
		}
	}
}

func TestA01SchemaSatisfiesTheAdapterAndTheContract(t *testing.T) {
	db, ctx := integrationDB(t)

	// 1. The columns A-01 reads and writes. 0008 adds the last two; the rest predate it (0001), which
	// is what makes "no migration 0009" a fact rather than an assumption.
	assertColumns(t, db, ctx, "user_accounts", []columnSpec{
		{name: "id", dataType: "bigint", nullable: false},
		{name: "phone", dataType: "text", nullable: true},
		{name: "email", dataType: "text", nullable: true},
		{name: "display_name", dataType: "text", nullable: false, defaultHint: "''"},
		{name: "password_hash", dataType: "text", nullable: false},
		{name: "status", dataType: "text", nullable: false, defaultHint: "ACTIVE"},
		{name: "created_at", dataType: "timestamp with time zone", nullable: false},
		{name: "updated_at", dataType: "timestamp with time zone", nullable: false},
		// 0008:
		{name: "avatar_url", dataType: "text", nullable: false, defaultHint: "''"},
		{name: "deleted_at", dataType: "timestamp with time zone", nullable: true},
	})

	// 2. The constraints. The first three come from 0008 and are what make a half-deleted account
	// impossible; the last three are the pre-existing structure that A-01 must not have broken.
	for _, name := range []string{
		"user_accounts_deleted_is_disabled",
		"user_accounts_deleted_has_no_credentials",
		"user_accounts_avatar_url_length",
		"user_accounts_check",        // phone IS NOT NULL OR email IS NOT NULL
		"user_accounts_status_check", // ACTIVE | DISABLED
	} {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint
 WHERE conrelid = 'user_accounts'::regclass AND conname = $1)`, name).Scan(&exists); err != nil {
			t.Fatalf("read constraint %s: %v", name, err)
		}
		if !exists {
			t.Errorf("constraint %s is missing", name)
		}
	}
	// Uniqueness of phone and email must survive: they are what makes registration idempotent and what
	// the anonymization scheme relies on when it releases a deleted number.
	for _, column := range []string{"phone", "email"} {
		var unique bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (
    SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
     WHERE i.indrelid = 'user_accounts'::regclass AND i.indisunique AND a.attname = $1)`, column).Scan(&unique); err != nil {
			t.Fatalf("read unique index for %s: %v", column, err)
		}
		if !unique {
			t.Errorf("user_accounts.%s lost its unique constraint", column)
		}
	}

	// 3. The index the read paths depend on exists and is partial: every profile query filters
	// deleted_at IS NULL, and the partial form only pays for the rows that are actually deleted.
	var indexDef string
	if err := db.QueryRowContext(ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'user_accounts' AND indexname = 'idx_user_accounts_deleted_at'`).
		Scan(&indexDef); err != nil {
		t.Fatalf("the deleted_at index is missing: %v", err)
	}
	if !strings.Contains(indexDef, "deleted_at IS NOT NULL") {
		t.Errorf("the deleted_at index is not partial: %s", indexDef)
	}

	// 4. Everything the wallet registration path needs still exists: A-01's EnsureUserWithWallet writes
	// user_accounts and wallet_accounts in one transaction, so a missing wallet column would break
	// registration, not the wallet module.
	assertColumns(t, db, ctx, "wallet_accounts", []columnSpec{
		{name: "user_id", dataType: "bigint", nullable: false},
		{name: "balance_cents", dataType: "bigint", nullable: false, defaultHint: "0"},
	})

	// 5. With all of the above present, the schema satisfies A-01 without a further migration. The
	// assertion is the conclusion of step one: no 0009 is needed while this test passes.
	var highest int
	if err := db.QueryRowContext(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&highest); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if highest < 8 {
		t.Fatalf("highest applied migration = %d, want at least 8", highest)
	}
}

// 错误映射复核：未知账号映射为"找不到"，但**上下文取消不能被误报成"找不到"**——把取消当成 404 会让
// 客户端以为账号不存在，而实际上请求根本没有到达数据库。
func TestAccountMutationErrorMapping(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	userID := newAccountFixture(t, db, ctx, uniquePhone(t))

	// Unknown account → not found (the mapping the HTTP layer turns into 404).
	if _, err := adapter.GetProfile(ctx, userID+987654); !errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("unknown account error = %v, want ErrProfileNotFound", err)
	}
	if _, err := adapter.UpdateProfile(ctx, userID+987654, auth.ProfileUpdate{DisplayName: new(string)}); !errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("unknown account update error = %v, want ErrProfileNotFound", err)
	}
	// A canceled context is an infrastructure failure, not a missing account.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := adapter.GetProfile(canceledCtx, userID)
	if err == nil {
		t.Fatal("expected an error from a canceled context")
	}
	if errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("a canceled context was reported as a missing account: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancellation was not preserved in the error: %v", err)
	}
	// A malformed update (no fields) is a caller error, not "not found", and must not touch the row.
	if _, err := adapter.UpdateProfile(ctx, userID, auth.ProfileUpdate{}); err == nil {
		t.Fatal("expected an empty update to be refused")
	} else if errors.Is(err, auth.ErrProfileNotFound) {
		t.Fatalf("an empty update was reported as a missing account: %v", err)
	}
}

// 跨存储边界里可以被数据库这一侧证明的一半：冻结之后，登录路径读到的状态就是 DISABLED。
// 这解释了为什么 BR-07 的两条硬要求不依赖 Redis 撤销成功；撤销决定的是"已签发的会话还能用多久"。
func TestFrozenAccountIsDisabledForTheLoginPath(t *testing.T) {
	db, ctx := integrationDB(t)
	adapter := newMutationAdapter(t, db)
	accountStore, err := NewAccountStore(db)
	if err != nil {
		t.Fatalf("NewAccountStore() error = %v", err)
	}
	phone := uniquePhone(t)
	userID := newAccountFixture(t, db, ctx, phone)

	if frozen, err := adapter.SetFrozen(ctx, userID, true); err != nil || !frozen {
		t.Fatalf("SetFrozen(true) = %v, %v", frozen, err)
	}

	// This is the read the login path performs; its status is what the service checks before issuing a
	// session, so a frozen account cannot log in even if session revocation failed.
	account, err := accountStore.FindUserByAccount(ctx, phone)
	if err != nil {
		t.Fatalf("FindUserByAccount() error = %v", err)
	}
	if account.Status != auth.StatusDisable {
		t.Fatalf("the login lookup returned status %q, want %s", account.Status, auth.StatusDisable)
	}

	if unfrozen, err := adapter.SetFrozen(ctx, userID, false); err != nil || !unfrozen {
		t.Fatalf("SetFrozen(false) = %v, %v", unfrozen, err)
	}
	account, err = accountStore.FindUserByAccount(ctx, phone)
	if err != nil {
		t.Fatalf("FindUserByAccount() after unfreeze error = %v", err)
	}
	if account.Status != auth.StatusActive {
		t.Fatalf("status after unfreezing = %q, want %s", account.Status, auth.StatusActive)
	}
}
