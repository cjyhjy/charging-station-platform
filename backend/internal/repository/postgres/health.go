package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
)

// Operational probes and the migration gate.
//
// The API reports what its dependencies are doing, and the processes that do not run migrations -
// the worker and the publisher - refuse to start against a database older than the binary. Both
// exist for the same reason: a process that starts against an out-of-date schema, or one that keeps
// answering while its database is gone, is worse than one that says so. The worker writes device
// outcomes into tables a migration introduces, so "it started and mostly works" is not a state this
// platform may run in.

// HighestMigrationVersion reports the newest version in an embedded migration set.
//
// It is derived from the same files the runner applies rather than from a constant, so a new
// migration cannot be added without the expected version moving with it.
func HighestMigrationVersion(migrationFS fs.FS) (int, error) {
	if migrationFS == nil {
		return 0, fmt.Errorf("postgres: migration filesystem is nil")
	}
	migrations, err := loadMigrations(migrationFS)
	if err != nil {
		return 0, err
	}
	if len(migrations) == 0 {
		return 0, fmt.Errorf("postgres: migration set is empty")
	}
	// loadMigrations returns the files in ascending version order.
	return migrations[len(migrations)-1].Version, nil
}

// SchemaVersion reports the highest applied migration version, or 0 when the database has never
// been migrated. A missing version table is reported as 0 rather than as an error: "not migrated
// yet" is exactly the state a deployment gate has to detect.
func SchemaVersion(ctx context.Context, db *sql.DB) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("postgres: database is nil")
	}
	var version sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&version); err != nil {
		if isUndefinedTable(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return int(version.Int64), nil
}

// AssertSchemaVersion returns an error when the database is behind the binary that is starting.
//
// The message names both numbers, because the operator's next action depends on which direction the
// gap is: behind means run the API (which migrates on startup) before this process, and ahead means
// this binary is older than the schema it was pointed at.
func AssertSchemaVersion(ctx context.Context, db *sql.DB, expected int) error {
	applied, err := SchemaVersion(ctx, db)
	if err != nil {
		return err
	}
	if applied < expected {
		return fmt.Errorf("postgres: schema version %d is behind the expected %d; start the API first (it applies migrations on startup) or run the migration gate before this process", applied, expected)
	}
	return nil
}

// OutboxBacklog counts the outbox rows that have not been published yet.
//
// It is the gap between a committed business transaction and the stream, and therefore the first
// number that grows when the publisher is down. The readiness probe is deliberately not built on
// it: a growing backlog is an alert, not an outage, because the rows are durable and will be
// published when the publisher returns.
func OutboxBacklog(ctx context.Context, db *sql.DB) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("postgres: database is nil")
	}
	var backlog int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&backlog); err != nil {
		return 0, fmt.Errorf("count unpublished outbox rows: %w", err)
	}
	return backlog, nil
}

// isUndefinedTable reports whether an error is PostgreSQL's undefined_table (SQLSTATE 42P01),
// which is what a query against schema_migrations returns before the first migration runs.
//
// The driver's error type is matched through a one-method interface instead of an import, so this
// package keeps its single direct dependency on the pgx stdlib driver registration.
func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		return state.SQLState() == "42P01"
	}
	return strings.Contains(err.Error(), "42P01")
}
