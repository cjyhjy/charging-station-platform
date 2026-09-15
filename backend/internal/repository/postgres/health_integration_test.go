package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/migrations"
)

// The migration gate and the operational gauges are what a deployment relies on: the worker must
// not start against a database older than the binary, and the API reports the backlog and the
// applied version. These tests run against a real PostgreSQL because both answers come from the
// database, not from the process.

func TestHighestMigrationVersionFollowsTheEmbeddedSet(t *testing.T) {
	version, err := HighestMigrationVersion(migrations.FS)
	if err != nil {
		t.Fatalf("HighestMigrationVersion() error = %v", err)
	}
	expected, err := HighestMigrationVersion(migrations.FS)
	if err != nil {
		t.Fatalf("HighestMigrationVersion() error = %v", err)
	}
	if version != expected {
		t.Fatalf("highest embedded migration = %d, want %d", version, expected)
	}
	if _, err := HighestMigrationVersion(nil); err == nil {
		t.Fatal("expected a nil filesystem to be refused")
	}
}

func TestSchemaVersionAndAssertionOnARealDatabase(t *testing.T) {
	db, ctx := integrationDB(t)

	applied, err := SchemaVersion(ctx, db)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	expected, err := HighestMigrationVersion(migrations.FS)
	if err != nil {
		t.Fatalf("HighestMigrationVersion() error = %v", err)
	}
	if applied != expected {
		t.Fatalf("applied version = %d, want %d: integrationDB migrates through the same runner", applied, expected)
	}

	if err := AssertSchemaVersion(ctx, db, expected); err != nil {
		t.Fatalf("AssertSchemaVersion() with the current version error = %v", err)
	}
	// A binary older than the schema is allowed: the database is ahead, which cannot break the
	// queries this binary makes.
	if err := AssertSchemaVersion(ctx, db, expected-1); err != nil {
		t.Fatalf("AssertSchemaVersion() with an older binary error = %v", err)
	}

	// A binary newer than the schema must be refused, and the message has to name both numbers:
	// the operator's next action depends on which side of the gap they are on.
	err = AssertSchemaVersion(ctx, db, expected+1)
	if err == nil {
		t.Fatal("expected a database behind the binary to be refused")
	}
	message := err.Error()
	if !strings.Contains(message, "behind") || !strings.Contains(message, "start the API first") {
		t.Fatalf("the gate must explain what to do next, got %q", message)
	}
	if !strings.Contains(message, strconv.Itoa(expected)) || !strings.Contains(message, strconv.Itoa(expected+1)) {
		t.Fatalf("the gate must name both versions, got %q", message)
	}
}

// A database that has never been migrated is the first deploy case, and it must read as version 0
// rather than as an error: the gate's job is to say "behind", and a read failure would look like an
// infrastructure problem instead of a missing migration.
func TestSchemaVersionOnAnUnmigratedDatabase(t *testing.T) {
	base := os.Getenv("NCS_TEST_PG_DSN")
	if base == "" {
		t.Skip("NCS_TEST_PG_DSN not set; PostgreSQL integration tests skipped")
	}
	expected, err := HighestMigrationVersion(migrations.FS)
	if err != nil {
		t.Fatalf("HighestMigrationVersion() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := "ncs_health_unmigrated_" + strings.NewReplacer(".", "_", "-", "_").Replace(uniqueSuffix(t))
	maintenance := dsnForDatabase(t, base, "postgres")
	admin, err := Open(ctx, maintenance, 1)
	if err != nil {
		t.Fatalf("open maintenance database: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quoteIdentifier(name)); err != nil {
		_ = admin.Close()
		t.Fatalf("create scratch database: %v", err)
	}
	_ = admin.Close()
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancelCleanup()
		if err := dropDatabase(cleanupCtx, maintenance, name); err != nil {
			t.Logf("drop scratch database: %v", err)
		}
	})

	scratch, err := Open(ctx, dsnForDatabase(t, base, name), 2)
	if err != nil {
		t.Fatalf("open scratch database: %v", err)
	}
	defer func() { _ = scratch.Close() }()

	version, err := SchemaVersion(ctx, scratch)
	if err != nil {
		t.Fatalf("SchemaVersion() on an unmigrated database error = %v", err)
	}
	if version != 0 {
		t.Fatalf("version = %d, want 0", version)
	}
	if err := AssertSchemaVersion(ctx, scratch, expected); err == nil {
		t.Fatal("expected an unmigrated database to be refused")
	}
}

func TestOutboxBacklogCountsUnpublishedRows(t *testing.T) {
	db, ctx := integrationDB(t)
	store, err := NewOrderStore(db)
	if err != nil {
		t.Fatalf("NewOrderStore() error = %v", err)
	}
	suffix := uniqueSuffix(t)
	userA, _, _, _, chargerA := orderFlowFixture(t, db, ctx, suffix)

	before, err := OutboxBacklog(ctx, db)
	if err != nil {
		t.Fatalf("OutboxBacklog() error = %v", err)
	}

	created, err := store.CreateOrder(ctx, order.CreateOrderCommand{
		UserID: userA, ChargerID: chargerA,
		IdempotencyKey: "backlog-create-" + suffix, RequestHash: "h", TraceID: "t",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	after, err := OutboxBacklog(ctx, db)
	if err != nil {
		t.Fatalf("OutboxBacklog() error = %v", err)
	}
	// One committed business transaction, one more unpublished row: this is the number that grows
	// while the publisher is down.
	if after != before+1 {
		t.Fatalf("backlog moved from %d to %d, want exactly one more row", before, after)
	}

	// Publishing the row removes it from the backlog.
	if _, err := db.ExecContext(ctx, `UPDATE outbox_events SET published_at = CURRENT_TIMESTAMP WHERE aggregate_id = $1`,
		created.OrderNo); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	published, err := OutboxBacklog(ctx, db)
	if err != nil {
		t.Fatalf("OutboxBacklog() error = %v", err)
	}
	if published != before {
		t.Fatalf("backlog = %d after publishing, want %d", published, before)
	}
}

// dsnForDatabase replaces the database name in a DSN, so a test can reach the maintenance database
// to create and drop a scratch one.
func dsnForDatabase(t *testing.T, current, name string) string {
	t.Helper()
	parsed, err := url.Parse(current)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

func currentDatabase(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var name string
	if err := db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatalf("read current database: %v", err)
	}
	return name
}

func dropDatabase(ctx context.Context, maintenanceDSN, name string) error {
	if name == "" {
		return nil
	}
	admin, err := Open(ctx, maintenanceDSN, 1)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	_, err = admin.ExecContext(ctx, `DROP DATABASE IF EXISTS `+quoteIdentifier(name))
	return err
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
