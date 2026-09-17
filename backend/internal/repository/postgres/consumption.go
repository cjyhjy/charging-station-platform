package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// This file implements the authoritative consumption record in PostgreSQL.
//
// It replaces the in-memory store that the worker used until now, which means "a restart
// does not apply the event twice" becomes a real guarantee rather than a property of a
// process that happened not to restart. The semantics are the ones established over six
// review rounds of B-04, and they are not negotiable in the implementation:
//
//   - Begin answers three states, not two. "Somebody is handling this right now" and "this
//     is finished" demand opposite actions, and conflating them loses events.
//   - A live lease covers both a normal reservation and a dead-letter claim. Fail clears
//     the lease before a claim is taken, so the claim has to establish one of its own or
//     it excludes nobody.
//   - Release does not advance the attempt count; Fail does. Waiting for another
//     consumer's claim is not an attempt, and charging it spends the retry budget before
//     the first real failure.
//   - The dead-letter decision is two phases: an atomic claim that only the current
//     attempt can take, and a compare-and-set finalisation whose refusal means the caller
//     must stay completely silent.

// ConsumptionStore is the PostgreSQL consumption record.
type ConsumptionStore struct {
	db *sql.DB
	// clock is injectable so tests can drive lease expiry without sleeping.
	clock func() time.Time
	// lease is how long a reservation or a dead-letter claim holds the event.
	lease time.Duration
	// newOwner mints the owner of a reservation. It must be unique per reservation:
	// ownership is what stops a stale holder from finalising an attempt that took over.
	newOwner func() (string, error)
}

// NewConsumptionStore validates its inputs and applies the default lease.
func NewConsumptionStore(db *sql.DB) (*ConsumptionStore, error) {
	if db == nil {
		return nil, errors.New("postgres: consumption store needs a database")
	}
	return &ConsumptionStore{
		db:       db,
		clock:    func() time.Time { return time.Now().UTC() },
		lease:    event.DefaultConsumptionLease,
		newOwner: newReservationOwner,
	}, nil
}

var _ event.ConsumptionStore = (*ConsumptionStore)(nil)

// SetLeaseTTL overrides how long a reservation or dead-letter claim survives.
func (s *ConsumptionStore) SetLeaseTTL(ttl time.Duration) {
	if ttl > 0 {
		s.lease = ttl
	}
}

// SetClock replaces the clock, so tests do not depend on wall time.
func (s *ConsumptionStore) SetClock(clock func() time.Time) {
	if clock != nil {
		s.clock = clock
	}
}

// Begin reserves the event and reports what this delivery may do.
//
// The whole decision is one statement pair inside a transaction, with the row locked for
// the second step, so two deliveries racing for the same event cannot both be told they
// may apply it.
func (s *ConsumptionStore) Begin(ctx context.Context, record event.ConsumptionRecord) (event.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return event.Reservation{}, err
	}
	if record.EventID == "" {
		return event.Reservation{}, errors.New("postgres: consumption record needs an event id")
	}
	owner, err := s.newOwner()
	if err != nil {
		return event.Reservation{}, fmt.Errorf("postgres: mint reservation owner: %w", err)
	}
	now := s.clock()
	leaseUntil := now.Add(s.lease)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return event.Reservation{}, fmt.Errorf("postgres: begin reservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// The first reservation of an event is attempt one. ON CONFLICT DO NOTHING makes the
	// insert safe against a concurrent delivery doing the same thing at the same instant.
	inserted, err := s.insertReservation(ctx, tx, record, owner, now, leaseUntil)
	if err != nil {
		return event.Reservation{}, err
	}
	if inserted {
		if err := tx.Commit(); err != nil {
			return event.Reservation{}, fmt.Errorf("postgres: commit reservation: %w", err)
		}
		return event.Reservation{
			EventID:        record.EventID,
			State:          event.ReservationReserved,
			Owner:          owner,
			Attempt:        1,
			LeaseExpiresAt: leaseUntil,
		}, nil
	}

	// The row exists: lock it and decide.
	current, err := lockConsumption(ctx, tx, record.EventID)
	if err != nil {
		return event.Reservation{}, err
	}
	if current.outcome.Terminal() {
		if err := tx.Commit(); err != nil {
			return event.Reservation{}, fmt.Errorf("postgres: commit reservation read: %w", err)
		}
		return event.Reservation{
			EventID: record.EventID,
			State:   event.ReservationAlreadyConsumed,
			Owner:   current.owner,
			Attempt: current.attempts,
		}, nil
	}
	if current.inProgress(now) {
		if err := tx.Commit(); err != nil {
			return event.Reservation{}, fmt.Errorf("postgres: commit reservation read: %w", err)
		}
		return event.Reservation{
			EventID:        record.EventID,
			State:          event.ReservationInProgress,
			Owner:          current.owner,
			Attempt:        current.attempts,
			LeaseExpiresAt: current.leaseUntil.Time,
		}, nil
	}

	// The previous attempt failed and released, the lease expired because its holder died,
	// or the reservation was abandoned before the event was applied. An abandoned attempt
	// keeps its number: it never reached the handler, so the delivery that takes it over is
	// the same attempt the abandoned one would have been.
	attempt := current.attempts
	if current.outcome != event.OutcomeAbandoned {
		attempt++
	}
	if attempt < 1 {
		attempt = 1
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET event_type = $2, aggregate_type = $3, aggregate_id = $4, trace_id = $5,
		       stream = $6, stream_id = $7, consumer = $8, delivery_count = $9,
		       attempts = $10, outcome = '', detail = '', owner = $11,
		       reserved_at = $12, completed_at = NULL, lease_until = $13,
		       updated_at = $12
		 WHERE event_id = $1`,
		record.EventID, string(record.EventType), record.AggregateType, record.AggregateID,
		record.TraceID, record.Stream, record.StreamID, record.Consumer, record.Attempt,
		attempt, owner, now, leaseUntil); err != nil {
		return event.Reservation{}, fmt.Errorf("postgres: take over reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return event.Reservation{}, fmt.Errorf("postgres: commit reservation takeover: %w", err)
	}
	return event.Reservation{
		EventID:        record.EventID,
		State:          event.ReservationReserved,
		Owner:          owner,
		Attempt:        attempt,
		LeaseExpiresAt: leaseUntil,
	}, nil
}

func (s *ConsumptionStore) insertReservation(ctx context.Context, tx *sql.Tx, record event.ConsumptionRecord, owner string, now, leaseUntil time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO event_consumptions
		    (event_id, event_type, aggregate_type, aggregate_id, trace_id, stream, stream_id,
		     consumer, delivery_count, attempts, outcome, detail, owner, reserved_at, lease_until, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, '', '', $10, $11, $12, $11)
		ON CONFLICT (event_id) DO NOTHING`,
		record.EventID, string(record.EventType), record.AggregateType, record.AggregateID,
		record.TraceID, record.Stream, record.StreamID, record.Consumer, record.Attempt,
		owner, now, leaseUntil)
	if err != nil {
		return false, fmt.Errorf("postgres: insert reservation: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: insert reservation rows: %w", err)
	}
	return affected == 1, nil
}

// Complete records a terminal outcome for a reservation this caller owns.
func (s *ConsumptionStore) Complete(ctx context.Context, reservation event.Reservation, outcome event.ConsumptionOutcome, detail string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reservation.EventID == "" {
		return errors.New("postgres: complete needs an event id")
	}
	if outcome != event.OutcomeSucceeded && outcome != event.OutcomeFailed && outcome != event.OutcomeDeadLettered {
		return fmt.Errorf("postgres: complete %s: outcome %q is not a terminal write", reservation.EventID, outcome)
	}
	now := s.clock()
	result, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = $3, detail = $4, completed_at = $5, lease_until = NULL, updated_at = $5
		 WHERE event_id = $1
		   AND owner = $2
		   AND outcome NOT IN ('SUCCEEDED', 'DEAD_LETTERED')`,
		reservation.EventID, reservation.Owner, string(outcome), detail, now)
	if err != nil {
		return fmt.Errorf("postgres: complete consumption of %s: %w", reservation.EventID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: complete consumption rows: %w", err)
	}
	if affected == 0 {
		// Either the attempt was taken over (a newer owner holds the row) or the event is
		// already finished. Both mean this caller must not write: the record belongs to
		// somebody else now.
		return fmt.Errorf("postgres: complete %s: reservation is no longer owned by this attempt", reservation.EventID)
	}
	return nil
}

// Fail releases the reservation and records that the attempt happened, so the next Begin
// grants the following attempt number.
func (s *ConsumptionStore) Fail(ctx context.Context, reservation event.Reservation, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reservation.EventID == "" {
		return nil
	}
	now := s.clock()
	// A missing row is a no-op rather than an error: releasing a reservation that is
	// already gone must not turn a bookkeeping step into a failure. A dead-letter claim is
	// left alone - it owns the event until it is finalised or aborted.
	if _, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = 'FAILED', detail = $3, completed_at = $4, lease_until = NULL, updated_at = $4
		 WHERE event_id = $1
		   AND (owner = $2 OR $2 = '')
		   AND outcome NOT IN ('SUCCEEDED', 'DEAD_LETTERED', 'DEAD_LETTERING')`,
		reservation.EventID, reservation.Owner, reason, now); err != nil {
		return fmt.Errorf("postgres: fail consumption of %s: %w", reservation.EventID, err)
	}
	return nil
}

// Release gives a reservation back without recording an attempt.
//
// It is what stops contention from spending the retry budget: the next Begin keeps this
// attempt's number rather than advancing it.
func (s *ConsumptionStore) Release(ctx context.Context, reservation event.Reservation, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reservation.EventID == "" {
		return nil
	}
	now := s.clock()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = 'ABANDONED', detail = $3, completed_at = $4, lease_until = NULL, updated_at = $4
		 WHERE event_id = $1
		   AND (owner = $2 OR $2 = '')
		   AND outcome NOT IN ('SUCCEEDED', 'DEAD_LETTERED', 'DEAD_LETTERING')`,
		reservation.EventID, reservation.Owner, reason, now); err != nil {
		return fmt.Errorf("postgres: release consumption of %s: %w", reservation.EventID, err)
	}
	return nil
}

// BeginDeadLetter atomically claims the decision to park the event.
//
// Only the current attempt may be granted. The claim then holds the event: it establishes
// its own lease, because Fail cleared the one it inherited, and Begin treats it as
// in-progress until that lease expires.
func (s *ConsumptionStore) BeginDeadLetter(ctx context.Context, reservation event.Reservation, reason string) (event.DeadLetterClaim, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if reservation.EventID == "" {
		// An entry that cannot be decoded has no record to claim against, and no other
		// attempt can hold it either: the parked entry is its only record.
		return event.DeadLetterClaimGranted, nil
	}
	now := s.clock()
	result, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = 'DEAD_LETTERING', detail = $3, reserved_at = $4, lease_until = $5, updated_at = $4
		 WHERE event_id = $1
		   AND owner = $2
		   AND $2 <> ''
		   AND outcome NOT IN ('SUCCEEDED', 'DEAD_LETTERED')`,
		reservation.EventID, reservation.Owner, reason, now, now.Add(s.lease))
	if err != nil {
		return "", fmt.Errorf("postgres: claim dead letter for %s: %w", reservation.EventID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("postgres: claim dead letter rows: %w", err)
	}
	if affected == 0 {
		// The record is gone, belongs to a newer attempt, or already finished. Staying
		// silent is recoverable; a terminal record for the wrong outcome is not.
		return event.DeadLetterClaimSuperseded, nil
	}
	return event.DeadLetterClaimGranted, nil
}

// FinalizeDeadLetter writes the terminal outcome when this delivery still owns the claim.
//
// The parked entry was written outside the store, so the event can have moved on in
// between: a compare-and-set is the only correct write here. A false result means the
// caller must not acknowledge the source entry and must not report the event as parked.
func (s *ConsumptionStore) FinalizeDeadLetter(ctx context.Context, reservation event.Reservation, record event.ConsumptionRecord, reason string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if reservation.EventID == "" && record.EventID == "" {
		// An undecodable entry: the parked entry is the only record there can be.
		return true, nil
	}
	key := reservation.EventID
	owned := key != "" && reservation.Owner != ""
	if key == "" {
		key = record.EventID
	}
	now := s.clock()

	if owned {
		result, err := s.db.ExecContext(ctx, `
			UPDATE event_consumptions
			   SET event_type = $3, aggregate_type = $4, aggregate_id = $5, trace_id = $6,
			       stream = $7, stream_id = $8, consumer = $9, delivery_count = $10,
			       outcome = 'DEAD_LETTERED', detail = $11, completed_at = $12,
			       lease_until = NULL, updated_at = $12
			 WHERE event_id = $1
			   AND owner = $2
			   AND outcome = 'DEAD_LETTERING'`,
			key, reservation.Owner, string(record.EventType), record.AggregateType, record.AggregateID,
			record.TraceID, record.Stream, record.StreamID, record.Consumer, record.Attempt,
			reason, now)
		if err != nil {
			return false, fmt.Errorf("postgres: finalize dead letter for %s: %w", key, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("postgres: finalize dead letter rows: %w", err)
		}
		return affected == 1, nil
	}

	// A caller that reports no reservation - a handler outside the pipeline - is judged by
	// the record instead: a park is refused for an event that already finished or is being
	// processed right now. That is weaker than the ownership check, but it still stops a
	// worker whose routing changed from parking an event another worker applied.
	inserted, err := s.insertDeadLettered(ctx, record, key, reason, now)
	if err != nil {
		return false, err
	}
	if inserted {
		return true, nil
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = 'DEAD_LETTERED', detail = $2, completed_at = $3,
		       lease_until = NULL, updated_at = $3
		 WHERE event_id = $1
		   AND outcome NOT IN ('SUCCEEDED', 'DEAD_LETTERED')
		   AND (lease_until IS NULL OR lease_until <= $3)`,
		key, reason, now)
	if err != nil {
		return false, fmt.Errorf("postgres: finalize dead letter for %s: %w", key, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: finalize dead letter rows: %w", err)
	}
	return affected == 1, nil
}

func (s *ConsumptionStore) insertDeadLettered(ctx context.Context, record event.ConsumptionRecord, key, reason string, now time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO event_consumptions
		    (event_id, event_type, aggregate_type, aggregate_id, trace_id, stream, stream_id,
		     consumer, delivery_count, attempts, outcome, detail, owner, reserved_at,
		     completed_at, lease_until, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, 'DEAD_LETTERED', $10, '', $11, $11, NULL, $11)
		ON CONFLICT (event_id) DO NOTHING`,
		key, string(record.EventType), record.AggregateType, record.AggregateID, record.TraceID,
		record.Stream, record.StreamID, record.Consumer, record.Attempt, reason, now)
	if err != nil {
		return false, fmt.Errorf("postgres: insert dead-lettered record for %s: %w", key, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: insert dead-lettered rows: %w", err)
	}
	return affected == 1, nil
}

// AbortDeadLetter gives a claim back after the parked entry could not be written.
//
// The claim itself did happen - the handler ran and failed - so the reservation is
// released as failed: the next delivery is a new attempt, and it can claim again
// immediately instead of waiting for the claim's lease to expire.
func (s *ConsumptionStore) AbortDeadLetter(ctx context.Context, reservation event.Reservation, reason string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if reservation.EventID == "" {
		return true, nil
	}
	now := s.clock()
	result, err := s.db.ExecContext(ctx, `
		UPDATE event_consumptions
		   SET outcome = 'FAILED', detail = $3, completed_at = $4, lease_until = NULL, updated_at = $4
		 WHERE event_id = $1
		   AND owner = $2
		   AND outcome = 'DEAD_LETTERING'`,
		reservation.EventID, reservation.Owner, reason, now)
	if err != nil {
		return false, fmt.Errorf("postgres: abort dead letter for %s: %w", reservation.EventID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: abort dead letter rows: %w", err)
	}
	return affected == 1, nil
}

// Entry returns the stored state of one event, for tests and diagnostics.
func (s *ConsumptionStore) Entry(ctx context.Context, eventID string) (event.ConsumptionEntry, bool, error) {
	if eventID == "" {
		return event.ConsumptionEntry{}, false, nil
	}
	current, err := queryConsumption(ctx, s.db, eventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return event.ConsumptionEntry{}, false, nil
		}
		return event.ConsumptionEntry{}, false, err
	}
	return current.entry(), true, nil
}

// consumptionRow is the stored row in the shape the contract describes.
type consumptionRow struct {
	record     event.ConsumptionRecord
	outcome    event.ConsumptionOutcome
	detail     string
	attempts   int
	owner      string
	reservedAt time.Time
	leaseUntil sql.NullTime
	completed  sql.NullTime
}

func (r consumptionRow) entry() event.ConsumptionEntry {
	entry := event.ConsumptionEntry{
		ConsumptionRecord: r.record,
		Outcome:           r.outcome,
		Detail:            r.detail,
		Attempts:          r.attempts,
		Owner:             r.owner,
		ReservedAt:        r.reservedAt,
	}
	if r.leaseUntil.Valid {
		entry.LeaseUntil = r.leaseUntil.Time
	}
	if r.completed.Valid {
		entry.CompletedAt = r.completed.Time
	}
	return entry
}

// inProgress reports whether the row is held by a delivery right now: a live reservation,
// or a dead-letter claim whose parked entry is still being written.
func (r consumptionRow) inProgress(now time.Time) bool {
	if r.outcome != "" && r.outcome != event.OutcomeDeadLettering {
		return false
	}
	return r.leaseUntil.Valid && now.Before(r.leaseUntil.Time)
}

func lockConsumption(ctx context.Context, tx *sql.Tx, eventID string) (consumptionRow, error) {
	row := tx.QueryRowContext(ctx, consumptionSelect+` WHERE event_id = $1 FOR UPDATE`, eventID)
	return scanConsumption(row)
}

func queryConsumption(ctx context.Context, db *sql.DB, eventID string) (consumptionRow, error) {
	row := db.QueryRowContext(ctx, consumptionSelect+` WHERE event_id = $1`, eventID)
	return scanConsumption(row)
}

const consumptionSelect = `
	SELECT event_id, event_type, aggregate_type, aggregate_id, trace_id, stream, stream_id,
	       consumer, delivery_count, attempts, outcome, detail, owner, reserved_at,
	       lease_until, completed_at
	  FROM event_consumptions`

func scanConsumption(row interface {
	Scan(...any) error
}) (consumptionRow, error) {
	var (
		record      event.ConsumptionRecord
		eventType   string
		outcome     string
		deliveryCnt int
		current     consumptionRow
	)
	if err := row.Scan(&record.EventID, &eventType, &record.AggregateType, &record.AggregateID,
		&record.TraceID, &record.Stream, &record.StreamID, &record.Consumer, &deliveryCnt,
		&current.attempts, &outcome, &current.detail, &current.owner, &current.reservedAt,
		&current.leaseUntil, &current.completed); err != nil {
		return consumptionRow{}, err
	}
	record.EventType = event.Type(eventType)
	record.Attempt = deliveryCnt
	current.record = record
	current.outcome = event.ConsumptionOutcome(outcome)
	return current, nil
}

// newReservationOwner mints a unique owner for one reservation.
//
// Uniqueness is what makes an ownership check meaningful: a stale holder must never be able
// to finalise, release or park an attempt that has since been taken over.
func newReservationOwner() (string, error) {
	var buffer [16]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return "", fmt.Errorf("generate reservation owner: %w", err)
	}
	return "owner-" + hex.EncodeToString(buffer[:]), nil
}
