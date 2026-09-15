package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// This file implements the outbox half of the closed loop: reading unpublished rows and
// marking them published after the Redis append succeeded.
//
// The publish order is fixed and must not be reversed: append first, mark after. Marking
// first would lose events whenever the crash lands between the two, while a duplicate
// append is removed downstream by the event id and the consumption record. A row that was
// published but not marked is therefore published again on the next pass, which is the
// direction this design is allowed to fail in.

// OutboxSource reads unpublished outbox rows from PostgreSQL.
//
// It implements event.OutboxSource, the contract B-03 froze. That contract is deliberately
// narrow - list and mark - and it is NOT a claim protocol: the rows it returns are not
// locked, so it cannot by itself stop two publishers from taking the same row. The
// single-active guarantee comes from the advisory lock below, held by the publishing
// process for as long as it runs.
type OutboxSource struct {
	db *sql.DB
	// clock is injectable so tests can assert the timestamps written back.
	clock func() time.Time
}

// NewOutboxSource validates its inputs.
func NewOutboxSource(db *sql.DB) (*OutboxSource, error) {
	if db == nil {
		return nil, errors.New("postgres: outbox source needs a database")
	}
	return &OutboxSource{db: db, clock: func() time.Time { return time.Now().UTC() }}, nil
}

var _ event.OutboxSource = (*OutboxSource)(nil)

// ListUnpublished returns up to limit rows that are due to be published.
//
// Ordering is by (next_attempt_at, id) and the query only considers rows whose
// next_attempt_at has arrived, so a pass is deterministic and a row that failed does not
// starve the ones behind it. published_at IS NULL is the only state that means "to do";
// the partial index added in migration 0006 serves this query.
func (s *OutboxSource) ListUnpublished(ctx context.Context, limit int) ([]event.OutboxRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("postgres: outbox limit must be greater than zero")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, payload, trace_id,
		       stream, occurred_at
		  FROM outbox_events
		 WHERE published_at IS NULL
		   AND next_attempt_at <= CURRENT_TIMESTAMP
		 ORDER BY next_attempt_at, id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list unpublished outbox rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	records := make([]event.OutboxRecord, 0, limit)
	for rows.Next() {
		var (
			id            int64
			eventID       string
			eventType     string
			aggregateType string
			aggregateID   string
			payload       []byte
			traceID       string
			stream        string
			occurredAt    time.Time
		)
		if err := rows.Scan(&id, &eventID, &eventType, &aggregateType, &aggregateID, &payload,
			&traceID, &stream, &occurredAt); err != nil {
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		record := event.OutboxRecord{
			ID:            formatRowID(id),
			EventID:       eventID,
			EventType:     event.Type(eventType),
			AggregateType: aggregateType,
			AggregateID:   aggregateID,
			OccurredAt:    occurredAt.UTC(),
			TraceID:       traceID,
			Stream:        stream,
		}
		if len(payload) > 0 {
			// json.RawMessage must stay valid JSON; an empty default is normalised to an
			// empty object so the event envelope validates.
			if !json.Valid(payload) {
				return nil, fmt.Errorf("postgres: outbox row %d carries invalid JSON payload", id)
			}
			record.Payload = json.RawMessage(payload)
		} else {
			record.Payload = json.RawMessage(`{}`)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate outbox rows: %w", err)
	}
	return records, nil
}

// MarkPublished records that a row reached the stream.
//
// The update is conditional on the row still being unpublished, so publishing the same row
// twice (which the crash window between append and mark allows) is idempotent rather than
// an error: the second call changes nothing and reports success. A row that another
// publisher already marked is therefore not an error either - the event is in the stream,
// which is all this call promises.
func (s *OutboxSource) MarkPublished(ctx context.Context, id string, publishedAt time.Time) error {
	rowID, err := parseRowID(id)
	if err != nil {
		return err
	}
	if publishedAt.IsZero() {
		publishedAt = s.clock()
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE outbox_events
		   SET published_at = $2
		 WHERE id = $1
		   AND published_at IS NULL`, rowID, publishedAt.UTC()); err != nil {
		return fmt.Errorf("postgres: mark outbox row %s published: %w", id, err)
	}
	return nil
}

// OutboxPublisherLockKey is the PostgreSQL advisory lock the publishing process takes.
//
// The value is a fixed constant so every process competes for the same lock: 0x6E63736F75746231
// spells "ncsoutb1". It is a *session* lock, so the guarantee lasts exactly as long as the
// connection that took it; the publisher keeps one dedicated connection open for its
// lifetime and releases the lock when it stops.
const OutboxPublisherLockKey int64 = 0x6E63736F75746231

// OutboxPublisherLock is a held advisory lock.
type OutboxPublisherLock struct {
	conn *sql.Conn
}

// AcquireOutboxPublisherLock takes the single-active publisher lock without blocking.
//
// It returns (nil, nil) when another process already holds it, which is a normal result and
// not a failure: a second publisher must not publish, and the caller decides whether to
// wait or to exit. The lock is taken on a dedicated connection, because a pooled
// connection could hand the lock's session to somebody else.
func AcquireOutboxPublisherLock(ctx context.Context, db *sql.DB) (*OutboxPublisherLock, error) {
	if db == nil {
		return nil, errors.New("postgres: advisory lock needs a database")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: acquire dedicated connection for the publisher lock: %w", err)
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, OutboxPublisherLockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("postgres: take publisher advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, nil
	}
	return &OutboxPublisherLock{conn: conn}, nil
}

// Release gives the lock back and returns the connection to the pool.
//
// It is safe to call on a nil lock, so a caller can defer it unconditionally. Closing the
// connection would release a session lock anyway, which is the backstop if the explicit
// unlock fails: the guarantee must not depend on this call succeeding.
func (l *OutboxPublisherLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	_, unlockErr := l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, OutboxPublisherLockKey)
	closeErr := l.conn.Close()
	l.conn = nil
	if unlockErr != nil {
		return fmt.Errorf("postgres: release publisher advisory lock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("postgres: close publisher lock connection: %w", closeErr)
	}
	return nil
}

// Held reports whether the lock object is still the holder, for tests and diagnostics.
func (l *OutboxPublisherLock) Held() bool { return l != nil && l.conn != nil }

// Alive reports whether the session that holds the lock is still there.
//
// A PostgreSQL session advisory lock lives exactly as long as its session, so "the dedicated
// connection still answers" is the same statement as "this process still holds the lock". That
// matters because the alternative - remembering that the lock was acquired once - is wrong in the
// one case that costs correctness: the connection drops (network, server restart, idle timeout),
// the server releases the lock, a second publisher acquires it, and the first process keeps
// publishing because a release function is still non-nil. It uses its own short timeout so a
// stalled connection cannot hold the publishing loop hostage.
func (l *OutboxPublisherLock) Alive(ctx context.Context) bool {
	if l == nil || l.conn == nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := l.conn.PingContext(probeCtx); err != nil {
		return false
	}
	var one int
	if err := l.conn.QueryRowContext(probeCtx, `SELECT 1`).Scan(&one); err != nil {
		return false
	}
	return one == 1
}

// formatRowID renders the bigint identity as the opaque string the event package uses.
func formatRowID(id int64) string { return strconvFormatInt64(id) }

func parseRowID(id string) (int64, error) {
	var rowID int64
	if _, err := fmt.Sscanf(id, "%d", &rowID); err != nil || rowID <= 0 {
		return 0, fmt.Errorf("postgres: outbox record id %q is not a row identity", id)
	}
	return rowID, nil
}
