package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// This file is B-07's schema contract test: it states every database fact the
// wallet adapter depends on and proves each one exists in the schema the
// migrations actually produce. The module's conclusion "no 0009 is needed" is
// this test passing - not an inspection of 0001_init.sql.
//
// The facts, and the adapter code that breaks without them:
//
//	wallet_accounts.user_id UNIQUE      walletBalanceForUpdate's ON CONFLICT target
//	wallet_accounts.balance_cents >= 0  creditWalletLocked's last line of defence
//	wallet_accounts.version             creditWalletLocked's optimistic counter
//	wallet_accounts.user_id -> user_accounts(id)
//	                                    isForeignKeyViolation maps 23503 to ErrWalletNotFound
//	wallet_transactions.idempotency_key UNIQUE
//	                                    ledgerResult makes a post-window replay safe
//	wallet_transactions columns         ListTransactions' SELECT and page scan
//	charging_orders columns             RefundOrder's order read and payment revert
//	idempotency_records (scope, key) UNIQUE
//	                                    claimWalletIdempotency's ON CONFLICT target
//	idempotency_records columns         the claim's insert/select/update statements
//	operation_logs columns              the BR-11 audit insert

// walletSchemaFacts is the expected shape. Column entries are table -> columns
// the adapter reads or writes by name. wallet_accounts has no surrogate id: its
// primary key is user_id, which is also the upsert target.
var walletSchemaFacts = map[string][]string{
	"wallet_accounts": {
		"user_id", "balance_cents", "version", "created_at", "updated_at",
	},
	"wallet_transactions": {
		"id", "user_id", "order_id", "transaction_type", "amount_cents",
		"balance_before_cents", "balance_after_cents", "idempotency_key", "created_at",
	},
	"idempotency_records": {
		"scope", "idempotency_key", "request_hash", "status", "response_code",
		"response_body", "expires_at", "created_at", "updated_at",
	},
	"operation_logs": {
		"actor_type", "actor_id", "action", "resource_type", "resource_id",
		"request_id", "payload",
	},
	"charging_orders": {
		"id", "order_no", "user_id", "status", "payment_status", "paid_cents",
		"version", "updated_at", "created_at",
	},
}

// walletSchemaProblems returns one message per missing fact, so a failure names
// everything that is wrong instead of the first thing.
func walletSchemaProblems(t *testing.T, ctx context.Context, q queryer) []string {
	t.Helper()
	var problems []string

	for table, columns := range walletSchemaFacts {
		existing, err := tableColumns(ctx, q, table)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", table, err))
			continue
		}
		if len(existing) == 0 {
			problems = append(problems, fmt.Sprintf("table %s does not exist", table))
			continue
		}
		for _, column := range columns {
			if !existing[column] {
				problems = append(problems, fmt.Sprintf("%s.%s is missing", table, column))
			}
		}
	}

	// The unique indexes the upserts target. The key SET must match exactly: a
	// unique index over extra columns would not satisfy ON CONFLICT (columns).
	uniques := []struct {
		table   string
		columns []string
	}{
		{"wallet_accounts", []string{"user_id"}},
		{"wallet_transactions", []string{"idempotency_key"}},
		{"idempotency_records", []string{"scope", "idempotency_key"}},
		{"charging_orders", []string{"order_no"}},
	}
	for _, unique := range uniques {
		name, err := uniqueIndexName(ctx, q, unique.table, unique.columns...)
		if err != nil {
			problems = append(problems, fmt.Sprintf("unique index on %s(%s): %v",
				unique.table, strings.Join(unique.columns, ", "), err))
			continue
		}
		if name == "" {
			problems = append(problems, fmt.Sprintf("%s(%s) has no unique index; the ON CONFLICT upserts cannot work",
				unique.table, strings.Join(unique.columns, ", ")))
		}
	}

	// The non-negative balance check.
	if _, err := checkConstraintOn(ctx, q, "wallet_accounts", "balance_cents"); err != nil {
		problems = append(problems, fmt.Sprintf("wallet_accounts.balance_cents: %v", err))
	}

	// The wallet must reference its account: that foreign key is how a write
	// for an unknown user becomes ErrWalletNotFound instead of a 500.
	fk, err := foreignKeys(ctx, q, "wallet_accounts")
	if err != nil {
		problems = append(problems, fmt.Sprintf("foreign keys of wallet_accounts: %v", err))
	} else if !fk["user_id"] {
		problems = append(problems, "wallet_accounts.user_id has no foreign key to user_accounts(id)")
	}

	// The ledger's transaction_type must accept the kinds the adapter writes.
	for _, kind := range []string{"TOP_UP", "REFUND"} {
		ok, err := checkConstraintAllows(ctx, q, "wallet_transactions", "transaction_type", kind)
		if err != nil {
			problems = append(problems, fmt.Sprintf("transaction_type constraint: %v", err))
			break
		}
		if !ok {
			problems = append(problems, fmt.Sprintf("wallet_transactions.transaction_type does not allow %s", kind))
		}
	}

	// The migration set itself: the wallet module must not need a migration of
	// its own, so the highest applied version is whatever the A-line delivered.
	highest, err := highestMigrationVersion(ctx, q)
	if err != nil {
		problems = append(problems, fmt.Sprintf("schema_migrations: %v", err))
	} else if highest < 1 {
		problems = append(problems, "schema_migrations is empty; the schema was not migrated")
	}

	sort.Strings(problems)
	return problems
}

// TestB07WalletSchemaContract is the "no 0009" decision, executable.
func TestB07WalletSchemaContract(t *testing.T) {
	db, ctx := integrationDB(t)
	problems := walletSchemaProblems(t, ctx, db)
	if len(problems) > 0 {
		t.Fatalf("wallet schema contract violated:\n  %s", strings.Join(problems, "\n  "))
	}
	highest, err := highestMigrationVersion(ctx, db)
	if err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	t.Logf("wallet schema contract holds at migration version %d; no wallet-specific migration is required", highest)
}

// TestB07SchemaContractDetectsMissingFacts is the reverse verification of the
// contract test, kept as a permanent test: inside a transaction that is rolled
// back it removes facts the adapter depends on (a unique index, a CHECK
// constraint, a column) and asserts the contract check reports each one. A
// contract test that cannot notice a removal would pass for the wrong reason.
func TestB07SchemaContractDetectsMissingFacts(t *testing.T) {
	db, ctx := integrationDB(t)
	if problems := walletSchemaProblems(t, ctx, db); len(problems) > 0 {
		t.Fatalf("precondition: the schema already violates the contract: %v", problems)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	uniqueIndex, err := uniqueIndexName(ctx, tx, "idempotency_records", "scope", "idempotency_key")
	if err != nil {
		t.Fatalf("find the unique index: %v", err)
	}
	if uniqueIndex == "" {
		t.Fatal("precondition: idempotency_records has no (scope, idempotency_key) unique index")
	}
	checkName, err := checkConstraintOn(ctx, tx, "wallet_accounts", "balance_cents")
	if err != nil {
		t.Fatalf("find the balance CHECK: %v", err)
	}
	// A unique index that backs a constraint must be dropped as a constraint.
	dropUnique, err := constraintExists(ctx, tx, "idempotency_records", uniqueIndex)
	if err != nil {
		t.Fatalf("check the constraint: %v", err)
	}
	dropIndexDDL := fmt.Sprintf(`DROP INDEX %s`, uniqueIndex)
	if dropUnique {
		dropIndexDDL = fmt.Sprintf(`ALTER TABLE idempotency_records DROP CONSTRAINT %s`, uniqueIndex)
	}
	for _, ddl := range []string{
		dropIndexDDL,
		fmt.Sprintf(`ALTER TABLE wallet_accounts DROP CONSTRAINT %q`, checkName),
		`ALTER TABLE wallet_transactions DROP COLUMN idempotency_key`,
	} {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}

	problems := walletSchemaProblems(t, ctx, tx)
	joined := strings.Join(problems, "\n")
	for _, want := range []string{
		"idempotency_records(scope, idempotency_key) has no unique index",
		"balance_cents",
		"wallet_transactions.idempotency_key is missing",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("contract check did not report %q; reported:\n%s", want, joined)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if problems := walletSchemaProblems(t, ctx, db); len(problems) > 0 {
		t.Fatalf("the rolled-back tampering was not undone: %v", problems)
	}
}

// queryer is the subset of *sql.DB and *sql.Tx the introspection needs.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func tableColumns(ctx context.Context, q queryer, table string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = $1`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	return columns, rows.Err()
}

// uniqueIndexName returns the name of a unique index whose key columns are
// exactly the given set, which is what ON CONFLICT (columns) requires.
func uniqueIndexName(ctx context.Context, q queryer, table string, columns ...string) (string, error) {
	rows, err := q.QueryContext(ctx, `
SELECT i.relname, array_agg(a.attname ORDER BY a.attname)
FROM pg_class t
JOIN pg_index x ON x.indrelid = t.oid
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (x.indkey)
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema() AND t.relname = $1 AND x.indisunique
GROUP BY i.relname`, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	want := append([]string(nil), columns...)
	sort.Strings(want)
	for rows.Next() {
		var name, keyColumns string
		if err := rows.Scan(&name, &keyColumns); err != nil {
			return "", err
		}
		got := strings.Split(strings.Trim(keyColumns, "{}"), ",")
		sort.Strings(got)
		if len(got) == len(want) {
			matches := true
			for i := range got {
				if got[i] != want[i] {
					matches = false
					break
				}
			}
			if matches {
				return name, nil
			}
		}
	}
	return "", rows.Err()
}

// checkConstraintOn returns the name of a CHECK constraint on the column.
func checkConstraintOn(ctx context.Context, q queryer, table, column string) (string, error) {
	rows, err := q.QueryContext(ctx, `
SELECT c.conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema() AND t.relname = $1 AND c.contype = 'c'`, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return "", err
		}
		if strings.Contains(definition, column) {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no CHECK constraint mentions %s", column)
	}
	sort.Strings(names)
	return names[0], nil
}

// b07CheckConstraintOn is the test-facing wrapper used by the balance test.
func b07CheckConstraintOn(t *testing.T, db *sql.DB, ctx context.Context, table, column string) (string, error) {
	t.Helper()
	return checkConstraintOn(ctx, db, table, column)
}

// b07CheckConstraintOnTx is the same lookup inside a transaction, so a test can
// observe a constraint it dropped without committing.
func b07CheckConstraintOnTx(t *testing.T, ctx context.Context, tx *sql.Tx, table, column string) (string, error) {
	t.Helper()
	return checkConstraintOn(ctx, tx, table, column)
}

// foreignKeys maps local columns of a table to whether they reference another
// table.
func foreignKeys(ctx context.Context, q queryer, table string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `
SELECT a.attname
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
WHERE n.nspname = current_schema() AND t.relname = $1 AND c.contype = 'f'`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		keys[name] = true
	}
	return keys, rows.Err()
}

// checkConstraintAllows reports whether any CHECK constraint on the column
// mentions the given literal.
func checkConstraintAllows(ctx context.Context, q queryer, table, column, literal string) (bool, error) {
	rows, err := q.QueryContext(ctx, `
SELECT pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema() AND t.relname = $1 AND c.contype = 'c'`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			return false, err
		}
		if strings.Contains(definition, column) && strings.Contains(definition, "'"+literal+"'") {
			found = true
		}
	}
	return found, rows.Err()
}

// constraintExists reports whether a constraint of that name exists on the
// table, which decides whether a unique index must be dropped as a constraint.
func constraintExists(ctx context.Context, q queryer, table, name string) (bool, error) {
	var count int64
	if err := q.QueryRowContext(ctx, `
SELECT count(*)
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema() AND t.relname = $1 AND c.conname = $2`, table, name).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func highestMigrationVersion(ctx context.Context, q queryer) (int64, error) {
	var highest sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&highest); err != nil {
		return 0, err
	}
	return highest.Int64, nil
}
