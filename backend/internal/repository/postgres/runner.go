// Package postgres contains the PostgreSQL persistence boundary and schema
// migration runner used by the backend services.
package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const migrationTable = "schema_migrations"

// migrationAdvisoryLockKey namespaces the advisory lock so independent API
// replicas serialize migration runs instead of racing on the same files.
var migrationAdvisoryLockKey = func() int64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte("ncs:postgres:migrations"))
	return int64(hasher.Sum64())
}()

// MigrationSession is one dedicated connection carrying a whole migration
// run: the session-level advisory lock, the version table and every per-file
// transaction execute on it. Doing everything on a single session removes the
// runner's hidden requirement for a second pooled connection — pools with
// NCS_POSTGRES_MAX_CONNS=1 cannot deadlock during startup.
type MigrationSession interface {
	// AcquireLock blocks until this session owns the migration advisory
	// lock. The lock lives as long as the session.
	AcquireLock(ctx context.Context) error
	AppliedMigrations(ctx context.Context) (map[int]AppliedMigration, error)
	Exec(ctx context.Context, query string, args ...any) error
	// BeginTransaction, CommitTransaction and RollbackTransaction drive one
	// per-file transaction on the session.
	BeginTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
	// Close ends the session; the advisory lock is released with it.
	Close(ctx context.Context) error
}

// DB provides a migration session.
type DB interface {
	Session(ctx context.Context) (MigrationSession, error)
}

// AppliedMigration identifies the immutable contents of an applied file.
type AppliedMigration struct {
	Version  int
	Name     string
	Checksum string
}

// Report describes what a runner invocation applied. An empty Applied slice
// means the database was already up to date.
type Report struct {
	Applied []AppliedMigration
}

// Run applies all numbered .sql files in migrationFS in ascending order.
// Each file runs in its own transaction (BEGIN/COMMIT on the session), so a
// retry is safe and a changed historical migration is rejected. The
// session-level advisory lock serializes concurrent runners for the whole
// invocation.
func Run(ctx context.Context, db DB, migrationFS fs.FS) (Report, error) {
	if db == nil {
		return Report{}, errors.New("postgres: database is nil")
	}
	if migrationFS == nil {
		return Report{}, errors.New("postgres: migration filesystem is nil")
	}

	session, err := db.Session(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("open migration session: %w", err)
	}
	defer func() { _ = session.Close(ctx) }()

	if err := session.AcquireLock(ctx); err != nil {
		return Report{}, fmt.Errorf("lock migrations: %w", err)
	}
	if err := ensureMigrationTable(ctx, session); err != nil {
		return Report{}, err
	}
	applied, err := session.AppliedMigrations(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("load applied migrations: %w", err)
	}
	migrations, err := loadMigrations(migrationFS)
	if err != nil {
		return Report{}, err
	}

	var report Report
	for _, migration := range migrations {
		if current, ok := applied[migration.Version]; ok {
			if current.Name != migration.Name || current.Checksum != migration.Checksum {
				return Report{}, fmt.Errorf("migration %03d changed: database has %s/%s, filesystem has %s/%s", migration.Version, current.Name, current.Checksum, migration.Name, migration.Checksum)
			}
			continue
		}

		if err := applyMigration(ctx, session, migration); err != nil {
			return Report{}, err
		}
		applied[migration.Version] = migration.AppliedMigration
		report.Applied = append(report.Applied, migration.AppliedMigration)
	}
	return report, nil
}

func applyMigration(ctx context.Context, session MigrationSession, migration migrationFile) error {
	if err := session.BeginTransaction(ctx); err != nil {
		return fmt.Errorf("begin migration %03d (%s): %w", migration.Version, migration.Name, err)
	}
	if err := session.Exec(ctx, migration.SQL); err != nil {
		_ = session.RollbackTransaction(ctx)
		return fmt.Errorf("execute migration %03d (%s): %w", migration.Version, migration.Name, err)
	}
	if err := session.Exec(ctx, "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3) ON CONFLICT (version) DO NOTHING", migration.Version, migration.Name, migration.Checksum); err != nil {
		_ = session.RollbackTransaction(ctx)
		return fmt.Errorf("record migration %03d (%s): %w", migration.Version, migration.Name, err)
	}
	if err := session.CommitTransaction(ctx); err != nil {
		_ = session.RollbackTransaction(ctx)
		return fmt.Errorf("commit migration %03d (%s): %w", migration.Version, migration.Name, err)
	}
	return nil
}

// RunDirectory applies migrations from a host directory using database/sql.
// It is retained for operational tooling; the API binary uses the embedded FS.
func RunDirectory(ctx context.Context, db *sql.DB, directory string) (Report, error) {
	if strings.TrimSpace(directory) == "" {
		return Report{}, errors.New("postgres: migration directory is empty")
	}
	adapted, err := NewSQLDB(db)
	if err != nil {
		return Report{}, err
	}
	return Run(ctx, adapted, os.DirFS(filepath.Clean(directory)))
}

// SQLDB adapts database/sql to the DB boundary.
type SQLDB struct {
	db *sql.DB
}

// NewSQLDB wraps a database/sql connection for use with Run.
func NewSQLDB(db *sql.DB) (*SQLDB, error) {
	if db == nil {
		return nil, errors.New("postgres: database connection is nil")
	}
	return &SQLDB{db: db}, nil
}

// Session reserves one pooled connection for the whole migration run. The
// session-level advisory lock dies with the connection, so Close releases it
// even after a crash.
func (db *SQLDB) Session(ctx context.Context) (MigrationSession, error) {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve migration connection: %w", err)
	}
	return &sqlSession{conn: conn}, nil
}

type sqlSession struct {
	conn *sql.Conn
}

// AcquireLock takes the session-level advisory lock. The caller's context
// bounds the wait: a cancelled context aborts with an error instead of
// blocking startup forever.
func (s *sqlSession) AcquireLock(ctx context.Context) error {
	if _, err := s.conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	return nil
}

func (s *sqlSession) AppliedMigrations(ctx context.Context) (map[int]AppliedMigration, error) {
	rows, err := s.conn.QueryContext(ctx, "SELECT version, name, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int]AppliedMigration)
	for rows.Next() {
		var migration AppliedMigration
		if err := rows.Scan(&migration.Version, &migration.Name, &migration.Checksum); err != nil {
			return nil, fmt.Errorf("scan schema migration: %w", err)
		}
		result[migration.Version] = migration
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema migrations: %w", err)
	}
	return result, nil
}

func (s *sqlSession) Exec(ctx context.Context, query string, args ...any) error {
	_, err := s.conn.ExecContext(ctx, query, args...)
	return err
}

// The transaction helpers drive BEGIN/COMMIT/ROLLBACK on the reserved
// connection. database/sql does not manage transaction state on a raw
// *sql.Conn, so the explicit statements are the transaction boundary here.

func (s *sqlSession) BeginTransaction(ctx context.Context) error {
	_, err := s.conn.ExecContext(ctx, "BEGIN")
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	return nil
}

func (s *sqlSession) CommitTransaction(ctx context.Context) error {
	_, err := s.conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *sqlSession) RollbackTransaction(ctx context.Context) error {
	_, err := s.conn.ExecContext(ctx, "ROLLBACK")
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}
	return nil
}

// Close releases the advisory lock explicitly (best effort) and returns the
// connection to the pool. If the unlock fails the pool close still ends the
// session and with it the lock.
func (s *sqlSession) Close(ctx context.Context) error {
	_, _ = s.conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockKey)
	return s.conn.Close()
}

func ensureMigrationTable(ctx context.Context, session MigrationSession) error {
	const query = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)`
	if err := session.Exec(ctx, query); err != nil {
		return fmt.Errorf("create schema version table: %w", err)
	}
	return nil
}

type migrationFile struct {
	AppliedMigration
	SQL string
}

func loadMigrations(migrationFS fs.FS) ([]migrationFile, error) {
	entries, err := fs.ReadDir(migrationFS, ".")
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}
	result := make([]migrationFile, 0, len(entries))
	seen := make(map[int]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, name, ok := parseMigrationName(entry.Name())
		if !ok {
			return nil, fmt.Errorf("invalid migration filename %q: expected NNN_name.sql", entry.Name())
		}
		if previous, exists := seen[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %03d: %s and %s", version, previous, entry.Name())
		}
		contents, err := fs.ReadFile(migrationFS, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if strings.TrimSpace(string(contents)) == "" {
			return nil, fmt.Errorf("migration %q is empty", entry.Name())
		}
		digest := sha256.Sum256(contents)
		seen[version] = entry.Name()
		result = append(result, migrationFile{
			AppliedMigration: AppliedMigration{Version: version, Name: name, Checksum: hex.EncodeToString(digest[:])},
			SQL:              string(contents),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })
	return result, nil
}

func parseMigrationName(filename string) (version int, name string, ok bool) {
	if !strings.HasSuffix(filename, ".sql") {
		return 0, "", false
	}
	base := strings.TrimSuffix(filename, ".sql")
	separator := strings.IndexByte(base, '_')
	if separator <= 0 || separator == len(base)-1 {
		return 0, "", false
	}
	version, err := strconv.Atoi(base[:separator])
	if err != nil || version <= 0 {
		return 0, "", false
	}
	return version, base[separator+1:] + ".sql", true
}
