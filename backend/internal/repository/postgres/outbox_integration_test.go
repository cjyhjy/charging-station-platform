package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
	"github.com/heguangV/charging-station-platform/backend/internal/worker"
)

// These tests exercise the outbox source and the single-active publisher lock against a real
// PostgreSQL, because both are statements about concurrency and about what the database does
// with a partially applied write - neither is provable against a fake.

// insertOutboxRow writes one unpublished row and returns its identity.
func insertOutboxRow(t *testing.T, db *sql.DB, ctx context.Context, eventID string, eventType event.Type, secondsUntilDue int) string {
	t.Helper()
	var id int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, payload,
		                           trace_id, stream, occurred_at, next_attempt_at)
		VALUES ($1, $2, 'order', 'order_01', '{"charger_id":"ch_01"}'::JSONB, 'trace-1',
		        'ncs:stream:charge-event', CURRENT_TIMESTAMP,
		        CURRENT_TIMESTAMP + ($3 || ' seconds')::INTERVAL)
		RETURNING id`, eventID, string(eventType), fmt.Sprintf("%d", secondsUntilDue)).Scan(&id); err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, id)
	})
	return strconvFormatInt64(id)
}

// ListUnpublished returns only due, unpublished rows, in a stable order, bounded by the limit.
func TestIntegrationOutboxSourceListsDueRows(t *testing.T) {
	db, ctx := integrationDB(t)
	source, err := NewOutboxSource(db)
	if err != nil {
		t.Fatalf("NewOutboxSource() error = %v", err)
	}

	first := insertOutboxRow(t, db, ctx, uniqueEventID(t, "outbox-first"), event.ChargeStarted, 0)
	second := insertOutboxRow(t, db, ctx, uniqueEventID(t, "outbox-second"), event.ChargeStarted, 0)
	// Not due yet: a row scheduled for the future must not be listed.
	later := insertOutboxRow(t, db, ctx, uniqueEventID(t, "outbox-later"), event.ChargeStarted, 3600)

	records, err := source.ListUnpublished(ctx, 500)
	if err != nil {
		t.Fatalf("ListUnpublished() error = %v", err)
	}
	seen := map[string]event.OutboxRecord{}
	for _, record := range records {
		seen[record.ID] = record
	}
	if _, ok := seen[first]; !ok {
		t.Fatalf("expected the first due row in the batch, got %v", seen)
	}
	if _, ok := seen[second]; !ok {
		t.Fatalf("expected the second due row in the batch, got %v", seen)
	}
	if _, ok := seen[later]; ok {
		t.Fatal("a row whose next_attempt_at is in the future must not be listed")
	}

	// The batch is ordered by (next_attempt_at, id), so the two rows we just wrote keep their
	// relative identity order. The database is shared with the other integration suites, so
	// this asserts the relation between OUR rows rather than an absolute position.
	mine := []string{first, second}
	if indexOf(records, mine[0]) > indexOf(records, mine[1]) {
		t.Fatalf("expected row %s before %s", first, second)
	}

	// The limit is honoured. Which row it returns depends on the rows other suites left
	// unpublished, so only the size is asserted here.
	limited, err := source.ListUnpublished(ctx, 1)
	if err != nil {
		t.Fatalf("ListUnpublished(1) error = %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 record, got %d", len(limited))
	}
	// The envelope fields are mapped, not defaulted.
	record := seen[first]
	if record.EventID == "" || record.EventType != event.ChargeStarted || record.Stream != "ncs:stream:charge-event" {
		t.Fatalf("unexpected record mapping: %+v", record)
	}
	if len(record.Payload) == 0 {
		t.Fatal("expected the payload to be carried")
	}
	if _, err := record.ToEvent(); err != nil {
		t.Fatalf("the record must convert into a valid envelope: %v", err)
	}

	if _, err := source.ListUnpublished(ctx, 0); err == nil {
		t.Fatal("expected a zero limit to be rejected")
	}
}

func indexOf(records []event.OutboxRecord, id string) int {
	for i, record := range records {
		if record.ID == id {
			return i
		}
	}
	return -1
}

// Marking is conditional on the row still being unpublished, so the duplicate publish that the
// crash window allows stays idempotent instead of becoming an error.
func TestIntegrationOutboxSourceMarkPublishedIsIdempotent(t *testing.T) {
	db, ctx := integrationDB(t)
	source, err := NewOutboxSource(db)
	if err != nil {
		t.Fatalf("NewOutboxSource() error = %v", err)
	}
	id := insertOutboxRow(t, db, ctx, uniqueEventID(t, "outbox-mark"), event.ChargeStarted, 0)

	publishedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := source.MarkPublished(ctx, id, publishedAt); err != nil {
		t.Fatalf("MarkPublished() error = %v", err)
	}
	if err := source.MarkPublished(ctx, id, publishedAt.Add(time.Second)); err != nil {
		t.Fatalf("a second MarkPublished must be a no-op, got %v", err)
	}

	var stored sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT published_at FROM outbox_events WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatalf("read published_at: %v", err)
	}
	if !stored.Valid {
		t.Fatal("expected the row to be marked published")
	}
	if !stored.Time.Equal(publishedAt) {
		t.Fatalf("the first mark must be kept, got %s want %s", stored.Time, publishedAt)
	}

	records, err := source.ListUnpublished(ctx, 500)
	if err != nil {
		t.Fatalf("ListUnpublished() error = %v", err)
	}
	if indexOf(records, id) != -1 {
		t.Fatal("a published row must not be listed again")
	}

	if err := source.MarkPublished(ctx, "not-a-row-id", publishedAt); err == nil {
		t.Fatal("expected a non-numeric row identity to be rejected")
	}
}

// The publishing loop against the real source and the real publisher: unpublished rows reach the
// stream and are marked, and a second pass has nothing left to do.
func TestIntegrationOutboxPublisherPublishesRowsOnce(t *testing.T) {
	db, ctx := integrationDB(t)
	source, err := NewOutboxSource(db)
	if err != nil {
		t.Fatalf("NewOutboxSource() error = %v", err)
	}
	writer := &recordingStreamWriter{}
	// A large batch, because the database is shared: the rows other suites left unpublished are
	// due as well and would otherwise fill a small batch before this test's row is reached.
	publisher, err := worker.NewPublisher(source, writer, worker.PublisherConfig{BatchSize: 1000})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	eventID := uniqueEventID(t, "outbox-publish")
	id := insertOutboxRow(t, db, ctx, eventID, event.ChargeStarted, 0)

	result, err := publisher.PublishOnce(ctx)
	if err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}
	if result.Published == 0 {
		t.Fatal("expected the due rows to be published")
	}
	// The published entry carries the event identity, which is what makes a duplicate publish
	// harmless: the consumer deduplicates by it.
	published, ok := writer.find(eventID)
	if !ok {
		t.Fatalf("expected the event to reach the stream, got %d entries", len(writer.entries()))
	}
	if published.stream != "ncs:stream:charge-event" || published.fields["event_id"] != eventID {
		t.Fatalf("unexpected stream entry: %+v", published)
	}

	// The row is marked, so a second pass does not publish this event again.
	before := writer.countFor(eventID)
	if _, err := publisher.PublishOnce(ctx); err != nil {
		t.Fatalf("second PublishOnce() error = %v", err)
	}
	if after := writer.countFor(eventID); after != before {
		t.Fatalf("expected no further publish of this event, got %d", after)
	}
	var publishedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT published_at FROM outbox_events WHERE id = $1`, id).Scan(&publishedAt); err != nil {
		t.Fatalf("read published_at: %v", err)
	}
	if !publishedAt.Valid {
		t.Fatal("expected the row to be marked published")
	}
}

// A write that fails leaves the row unpublished, and the next pass retries it: that is the
// retry half of "publish first, mark after".
func TestIntegrationOutboxPublisherKeepsTheRowOnAStreamFailure(t *testing.T) {
	db, ctx := integrationDB(t)
	source, err := NewOutboxSource(db)
	if err != nil {
		t.Fatalf("NewOutboxSource() error = %v", err)
	}
	writer := &recordingStreamWriter{failUntil: 1}
	publisher, err := worker.NewPublisher(source, writer, worker.PublisherConfig{BatchSize: 1000})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	eventID := uniqueEventID(t, "outbox-retry")
	id := insertOutboxRow(t, db, ctx, eventID, event.ChargeStarted, 0)

	if _, err := publisher.PublishOnce(ctx); err == nil {
		t.Fatal("expected the stream failure to surface")
	}
	// The publisher stops at the first failure, so this test's row may not even have been
	// attempted. What matters is that a failed pass marks nothing it did not publish.
	var publishedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT published_at FROM outbox_events WHERE id = $1`, id).Scan(&publishedAt); err != nil {
		t.Fatalf("read published_at: %v", err)
	}
	if publishedAt.Valid {
		if _, ok := writer.find(eventID); !ok {
			t.Fatal("a row must not be marked when its append did not happen")
		}
	}

	// The next passes retry, and this event eventually reaches the stream exactly once.
	for pass := 0; pass < 5; pass++ {
		if _, err := publisher.PublishOnce(ctx); err != nil {
			t.Fatalf("retry PublishOnce() error = %v", err)
		}
		if _, ok := writer.find(eventID); ok {
			break
		}
	}
	if _, ok := writer.find(eventID); !ok {
		t.Fatal("expected the retry to publish the event")
	}
	if count := writer.countFor(eventID); count != 1 {
		t.Fatalf("expected the event to be published once, got %d", count)
	}
}

// The single-active guarantee: a second publisher cannot take the lock while the first holds it,
// and the lock is released when the holder gives it back.
func TestIntegrationOutboxPublisherLockIsSingleActive(t *testing.T) {
	db, ctx := integrationDB(t)

	first, err := AcquireOutboxPublisherLock(ctx, db)
	if err != nil {
		t.Fatalf("AcquireOutboxPublisherLock() error = %v", err)
	}
	if !first.Held() {
		t.Fatal("expected the first acquire to hold the lock")
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })

	second, err := AcquireOutboxPublisherLock(ctx, db)
	if err != nil {
		t.Fatalf("second AcquireOutboxPublisherLock() error = %v", err)
	}
	if second != nil {
		_ = second.Release(ctx)
		t.Fatal("a second publisher must not acquire the lock while the first holds it")
	}

	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	third, err := AcquireOutboxPublisherLock(ctx, db)
	if err != nil {
		t.Fatalf("AcquireOutboxPublisherLock() after release error = %v", err)
	}
	if third == nil {
		t.Fatal("expected the lock to be available after it was released")
	}
	if err := third.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	// Releasing twice, and releasing a nil lock, must be safe: a caller defers it
	// unconditionally.
	if err := third.Release(ctx); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	var nilLock *OutboxPublisherLock
	if err := nilLock.Release(ctx); err != nil {
		t.Fatalf("releasing an unheld lock must be safe, got %v", err)
	}
}

// The lock's liveness check is what stops a publisher from continuing on a lock it no longer
// holds: a session advisory lock ends with its session, so "the dedicated connection still
// answers" is the same statement as "this process still owns the lock".
func TestIntegrationOutboxPublisherLockReportsLiveness(t *testing.T) {
	db, ctx := integrationDB(t)

	lock, err := AcquireOutboxPublisherLock(ctx, db)
	if err != nil {
		t.Fatalf("AcquireOutboxPublisherLock() error = %v", err)
	}
	if lock == nil {
		t.Fatal("expected the lock to be acquired")
	}
	if !lock.Alive(ctx) {
		t.Fatal("a held lock must report itself alive")
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	// After the release the handle no longer owns a session, so it must not claim to be alive.
	if lock.Alive(ctx) {
		t.Fatal("a released lock must not report itself alive")
	}

	var nilLock *OutboxPublisherLock
	if nilLock.Alive(ctx) {
		t.Fatal("an unheld lock must not report itself alive")
	}
}

// recordingStreamWriter captures what the publisher appends and can fail the first attempts.
type recordingStreamWriter struct {
	mu        sync.Mutex
	writes    []recordedEntry
	failUntil int
}

type recordedEntry struct {
	stream string
	fields map[string]string
}

func (w *recordingStreamWriter) Add(_ context.Context, stream string, fields map[string]string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failUntil > 0 {
		w.failUntil--
		return "", fmt.Errorf("stream unavailable")
	}
	copied := make(map[string]string, len(fields))
	for key, value := range fields {
		copied[key] = value
	}
	w.writes = append(w.writes, recordedEntry{stream: stream, fields: copied})
	return fmt.Sprintf("%d-0", len(w.writes)), nil
}

func (w *recordingStreamWriter) entries() []recordedEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]recordedEntry, len(w.writes))
	copy(out, w.writes)
	return out
}

func (w *recordingStreamWriter) countFor(eventID string) int {
	count := 0
	for _, entry := range w.entries() {
		if entry.fields["event_id"] == eventID {
			count++
		}
	}
	return count
}

func (w *recordingStreamWriter) find(eventID string) (recordedEntry, bool) {
	for _, entry := range w.entries() {
		if entry.fields["event_id"] == eventID {
			return entry, true
		}
	}
	return recordedEntry{}, false
}

// The Redis client is the production writer; this keeps that relationship checked.
var _ worker.StreamWriter = (redisrepo.StreamClient)(nil)
