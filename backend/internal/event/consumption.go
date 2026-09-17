package event

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// ConsumptionOutcome is the terminal state of one consumption attempt.
type ConsumptionOutcome string

const (
	// OutcomeSucceeded means the handler applied the event.
	OutcomeSucceeded ConsumptionOutcome = "SUCCEEDED"
	// OutcomeFailed means an attempt failed and the event may be retried.
	OutcomeFailed ConsumptionOutcome = "FAILED"
	// OutcomeDeadLettered means the event exhausted its retry budget or failed
	// permanently, and was moved to the dead-letter stream.
	OutcomeDeadLettered ConsumptionOutcome = "DEAD_LETTERED"
	// OutcomeDeadLettering means the current attempt has claimed the decision to park the
	// event, and the parked entry does not exist yet.
	//
	// It is deliberately NOT terminal. The write can still fail, so a redelivery must be able
	// to finish the job; and treating it as finished would make a later delivery skip an event
	// that is in neither stream.
	OutcomeDeadLettering ConsumptionOutcome = "DEAD_LETTERING"
	// OutcomeAbandoned means the reservation was given back before the event reached the
	// domain: the delivery was refused - another consumer held the duplicate guard, or the
	// guard itself could not be reached - and no attempt was made.
	//
	// It exists so waiting does not consume the retry budget. A reservation that was
	// abandoned is not an attempt, so the next Begin grants the SAME attempt number rather
	// than the following one; otherwise a delivery that waited through a burst of contention
	// would arrive at its first real failure with the budget already spent, and a perfectly
	// good event would be dead-lettered for having queued.
	OutcomeAbandoned ConsumptionOutcome = "ABANDONED"
)

// Terminal reports whether an outcome ends the event's life. A terminal event must never
// be applied again; a non-terminal one may be retried.
func (o ConsumptionOutcome) Terminal() bool {
	return o == OutcomeSucceeded || o == OutcomeDeadLettered
}

// ConsumptionRecord identifies one consumption attempt.
//
// It carries the stream coordinates as well as the event identity so an operator can move
// from a record back to the exact stream entry, which is what makes a dead letter
// investigable.
type ConsumptionRecord struct {
	EventID       string
	EventType     Type
	AggregateType string
	AggregateID   string
	TraceID       string
	Stream        string
	StreamID      string
	Consumer      string
	// Attempt is the transport's delivery count, starting at one.
	//
	// It is recorded for diagnostics: it shows how often Redis redelivered the entry, which is
	// useful when investigating a stuck consumer. It is NOT the retry budget; see
	// Reservation.Attempt for why.
	Attempt int
}

// ReservationState tells the caller what it is allowed to do with a delivery.
//
// The three states exist because "the event is already handled" and "somebody else is
// handling it right now" demand opposite actions, and conflating them loses events: a
// process that dies between reserving an event and applying it leaves a reservation
// behind, and a later delivery that treats that reservation as "already consumed" would
// acknowledge an event nobody ever applied.
type ReservationState string

const (
	// ReservationReserved means this caller owns the attempt and must apply the event.
	ReservationReserved ReservationState = "reserved"
	// ReservationAlreadyConsumed means the event reached a terminal outcome. The delivery
	// is acknowledged without applying the event again.
	ReservationAlreadyConsumed ReservationState = "already_consumed"
	// ReservationInProgress means a live lease owns the attempt. The delivery must NOT be
	// acknowledged: if that lease holder has died, this delivery is the only thing that can
	// still apply the event, and acknowledging it would lose the event permanently. The
	// correct action is to leave the entry pending until the lease expires or the holder
	// completes.
	ReservationInProgress ReservationState = "in_progress"
)

// Reservation is the result of Begin.
type Reservation struct {
	EventID string
	State   ReservationState
	// Owner identifies this reservation attempt. Complete and Fail require it, so a stale
	// holder cannot finalise an attempt that was taken over in the meantime.
	Owner string
	// Attempt is the number of this attempt, counted by the store.
	//
	// It is deliberately the store's own count and not the transport's delivery count. Redis
	// increments its delivery counter every time an entry is reclaimed, including the reclaims
	// that happen while a reservation is held by another live lease and no attempt is made at all,
	// so a lease wait can add hundreds of deliveries. A retry budget measured against that number
	// would be spent before the first real attempt failed. This counter only advances when the
	// store actually grants an attempt.
	Attempt int
	// LeaseExpiresAt is when another delivery may take the reservation over. It protects
	// against a holder that died without releasing: without an expiry, the event would be
	// stuck in progress forever and never retried.
	LeaseExpiresAt time.Time
}

// DeadLetterClaim is what the store decided about a caller's request to park an event.
//
// It exists because "may I park this?" is a question only the store can answer: the caller
// holds a reservation it believes is current, but a lease that expired while its handler was
// still running means somebody else owns the event now, and that somebody may be applying it
// at this very moment.
type DeadLetterClaim string

const (
	// DeadLetterClaimGranted means the caller is still the current attempt and now owns the
	// dead-letter decision: it must write the parked entry, record the terminal outcome and
	// then acknowledge.
	DeadLetterClaimGranted DeadLetterClaim = "granted"
	// DeadLetterClaimSuperseded means the caller is no longer the current attempt, or the
	// event already reached a terminal outcome.
	//
	// The caller must then stay completely silent: no parked entry, no record, no
	// acknowledgement, and nothing reported as a delivery outcome. Writing would park an event
	// another consumer has already applied - a dead letter for work that succeeded - and
	// acknowledging would delete the only copy of work that consumer may still be doing.
	DeadLetterClaimSuperseded DeadLetterClaim = "superseded"
)

// ConsumptionStore records that an event was consumed.
//
// PostgreSQL is the authoritative store, because contract section 4.3 keeps consumption
// results and audit records there, and section 4.2 requires duplicate deliveries to be
// neutralised by a consumption record or a business unique key. The B line owns this
// contract and the call order; the PostgreSQL implementation is the A line's, since
// backend/internal/repository/postgres is not B-owned.
//
// Two implementations of the same guarantee are acceptable, and the author of the
// PostgreSQL store should pick one deliberately:
//
//   - a lease, as described on Reservation, where an expired lease is taken over by the
//     next delivery; or
//   - stronger, putting the consumption record and the domain mutation in one PostgreSQL
//     transaction, so there is no window in which a reservation exists without the domain
//     change. That removes the crash window this contract has to describe rather than
//     closing it.
type ConsumptionStore interface {
	// Begin reserves the event for processing and reports what the caller may do.
	Begin(ctx context.Context, record ConsumptionRecord) (Reservation, error)
	// Complete records a terminal outcome for a reservation this caller owns.
	Complete(ctx context.Context, reservation Reservation, outcome ConsumptionOutcome, detail string) error
	// Fail releases a reservation so a redelivery can retry it, recording that the attempt
	// happened. Use it when the event reached the handler and the handler failed.
	Fail(ctx context.Context, reservation Reservation, reason string) error
	// Release gives a reservation back without recording an attempt, for a delivery that was
	// refused before it could apply the event.
	//
	// The distinction from Fail is the retry budget, and it is the whole point of the method:
	// the next Begin after a Release must grant the same attempt number, so contention never
	// counts against the event. Releasing must not finalise the reservation, and a stale
	// holder must not be able to release an attempt that has since been taken over.
	Release(ctx context.Context, reservation Reservation, reason string) error
	// BeginDeadLetter atomically claims the decision to park an event, for the reservation
	// that is asking.
	//
	// It marks the event as being parked, which is NOT the terminal state: the parked entry
	// does not exist yet, and the write can still fail.
	//
	// Only the current attempt may be granted. A reservation whose lease has expired and been
	// taken over must be refused, because the newer attempt may be applying the event right
	// now or may already have applied it, and a stale generation that parks and acknowledges
	// anyway overwrites a successful terminal outcome and reports work as dead-lettered that
	// in fact completed.
	BeginDeadLetter(ctx context.Context, reservation Reservation, reason string) (DeadLetterClaim, error)
	// FinalizeDeadLetter records the terminal outcome for a dead-letter claim this caller
	// still owns, and reports whether it was still the owner.
	//
	// A false result means the event moved on while the caller was writing the parked entry -
	// a newer attempt took it over, or it already reached a terminal outcome - so the caller
	// must not acknowledge the source entry and must not report the event as parked. Anything
	// it wrote is a duplicate, which is the direction this design is allowed to fail in.
	//
	// The write must be idempotent: a worker that crashes after parking the entry and before
	// acknowledging it will call this again on the retry.
	FinalizeDeadLetter(ctx context.Context, reservation Reservation, record ConsumptionRecord, reason string) (bool, error)
	// AbortDeadLetter gives a claim back when the parked entry could not be written, so the retry
	// may park it immediately instead of waiting for the claim's lease to expire.
	//
	// A false result means the claim was no longer this reservation's to give up, which is not an
	// error: the claim's lease expires on its own and the event is recovered either way.
	AbortDeadLetter(ctx context.Context, reservation Reservation, reason string) (bool, error)
}

// DefaultConsumptionLease bounds how long a reservation survives without its holder
// finishing. It must exceed the longest a handler may legitimately take.
const DefaultConsumptionLease = 5 * time.Minute

// ConsumptionEntry is the stored state of one event, exposed by the in-memory store so
// tests can assert on it.
type ConsumptionEntry struct {
	ConsumptionRecord
	Outcome     ConsumptionOutcome
	Detail      string
	Attempts    int
	Owner       string
	ReservedAt  time.Time
	LeaseUntil  time.Time
	CompletedAt time.Time
}

// MemoryConsumptionStore is an in-process ConsumptionStore for unit tests and for a local
// worker run before the PostgreSQL store exists.
//
// It is NOT production safe: state is lost on restart, so a redelivered event would be
// applied a second time. A process using it must say so loudly at startup.
type MemoryConsumptionStore struct {
	mu       sync.Mutex
	entries  map[string]*ConsumptionEntry
	clock    func() time.Time
	leaseTTL time.Duration
	// newOwner mints reservation owners. Tests inject a deterministic source.
	newOwner func() string
}

// NewMemoryConsumptionStore returns an empty store using the wall clock and the default
// lease.
func NewMemoryConsumptionStore() *MemoryConsumptionStore {
	return &MemoryConsumptionStore{
		entries:  make(map[string]*ConsumptionEntry),
		clock:    func() time.Time { return time.Now().UTC() },
		leaseTTL: DefaultConsumptionLease,
		newOwner: newReservationOwner,
	}
}

var _ ConsumptionStore = (*MemoryConsumptionStore)(nil)

// SetClock replaces the clock so tests do not depend on wall time.
func (m *MemoryConsumptionStore) SetClock(clock func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if clock == nil {
		m.clock = func() time.Time { return time.Now().UTC() }
		return
	}
	m.clock = clock
}

// SetLeaseTTL overrides how long a reservation survives without its holder finishing.
func (m *MemoryConsumptionStore) SetLeaseTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ttl > 0 {
		m.leaseTTL = ttl
	}
}

// SetOwnerSource overrides the reservation owner generator, for deterministic tests.
func (m *MemoryConsumptionStore) SetOwnerSource(source func() string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if source != nil {
		m.newOwner = source
	}
}

// Begin reserves an event and reports which of the three states the caller is in.
//
// A previous attempt that failed, or a reservation whose lease has expired, is taken over
// rather than reported as consumed: that is what makes a crash between reserving and
// applying recoverable instead of silently losing the event.
func (m *MemoryConsumptionStore) Begin(ctx context.Context, record ConsumptionRecord) (Reservation, error) {
	if err := ctx.Err(); err != nil {
		return Reservation{}, err
	}
	if record.EventID == "" {
		return Reservation{}, fmt.Errorf("consumption record: event id is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock()

	entry, exists := m.entries[record.EventID]
	if !exists {
		owner := m.newOwner()
		m.entries[record.EventID] = &ConsumptionEntry{
			ConsumptionRecord: record,
			// The first reservation of an event is attempt one, whatever the transport reports.
			Attempts:   1,
			Owner:      owner,
			ReservedAt: now,
			LeaseUntil: now.Add(m.leaseTTL),
		}
		return Reservation{
			EventID:        record.EventID,
			State:          ReservationReserved,
			Owner:          owner,
			Attempt:        1,
			LeaseExpiresAt: now.Add(m.leaseTTL),
		}, nil
	}

	if entry.Outcome.Terminal() {
		return Reservation{EventID: record.EventID, State: ReservationAlreadyConsumed, Attempt: entry.Attempts}, nil
	}

	// A live lease belongs to another delivery, so this one must not apply the event and
	// must not acknowledge it.
	//
	// A dead-letter claim counts as live for the same reason, and it needs the check explicitly:
	// Fail has already cleared the lease by the time the claim is taken, so without this a second
	// consumer would reserve the event immediately and run the handler again while the first
	// delivery was still writing and finalising the parked entry.
	if inProgressLocked(entry) && now.Before(entry.LeaseUntil) {
		return Reservation{
			EventID:        record.EventID,
			State:          ReservationInProgress,
			Owner:          entry.Owner,
			Attempt:        entry.Attempts,
			LeaseExpiresAt: entry.LeaseUntil,
		}, nil
	}

	// Either the previous attempt failed and released the reservation, the lease expired because
	// its holder died, or the reservation was abandoned before the event was applied. All three
	// are recoverable, so this delivery takes the reservation.
	//
	// Only the first two are attempts. A reservation that was abandoned never reached the
	// handler, so it keeps its number: the delivery that takes it over is the same attempt the
	// abandoned one would have been. Advancing the counter here is what made waiting on the
	// duplicate guard spend the retry budget.
	owner := m.newOwner()
	entry.ConsumptionRecord = record
	abandoned := entry.Outcome == OutcomeAbandoned
	entry.Outcome = ""
	entry.Detail = ""
	entry.Owner = owner
	entry.ReservedAt = now
	entry.LeaseUntil = now.Add(m.leaseTTL)
	if !abandoned {
		entry.Attempts++
	}
	if entry.Attempts < 1 {
		entry.Attempts = 1
	}
	return Reservation{
		EventID:        record.EventID,
		State:          ReservationReserved,
		Owner:          owner,
		Attempt:        entry.Attempts,
		LeaseExpiresAt: now.Add(m.leaseTTL),
	}, nil
}

// Complete records a terminal outcome.
//
// An owner that no longer holds the reservation is refused rather than silently writing:
// after a takeover, the stale holder must not be able to mark an event it never applied as
// consumed.
func (m *MemoryConsumptionStore) Complete(ctx context.Context, reservation Reservation, outcome ConsumptionOutcome, detail string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[reservation.EventID]
	if !ok {
		return fmt.Errorf("complete %s: no consumption record", reservation.EventID)
	}
	if reservation.Owner != "" && entry.Owner != "" && entry.Owner != reservation.Owner {
		return fmt.Errorf("complete %s: reservation is owned by another attempt", reservation.EventID)
	}
	entry.Outcome = outcome
	entry.Detail = detail
	entry.CompletedAt = m.clock()
	return nil
}

// Fail releases the reservation so a redelivery can retry, and records that the attempt
// happened: the next Begin grants the following attempt number.
func (m *MemoryConsumptionStore) Fail(ctx context.Context, reservation Reservation, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[reservation.EventID]
	if !ok {
		// Releasing a reservation that is already gone must not fail, or a retry path would
		// turn a bookkeeping no-op into an error.
		return nil
	}
	if reservation.Owner != "" && entry.Owner != "" && entry.Owner != reservation.Owner {
		// A stale holder must not disturb the attempt that took over.
		return nil
	}
	if entry.Outcome == OutcomeDeadLettering {
		// A dead-letter claim owns the event until it is finalised or aborted. Releasing it as a
		// failed attempt would clear the claim's lease, which is the only thing stopping another
		// consumer from running the handler again while the parked entry is still being written.
		return nil
	}
	entry.Outcome = OutcomeFailed
	entry.Detail = reason
	entry.CompletedAt = m.clock()
	// The lease ends now so the next delivery may take over immediately.
	entry.LeaseUntil = time.Time{}
	return nil
}

// Release gives the reservation back without recording an attempt.
//
// See ConsumptionStore.Release: it is called when a delivery was refused before the event was
// applied, so the retry budget must not be charged for the wait. The attempt number is left as
// it was, which is what makes the next Begin grant the same attempt rather than the next one.
func (m *MemoryConsumptionStore) Release(ctx context.Context, reservation Reservation, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[reservation.EventID]
	if !ok {
		// Releasing a reservation that is already gone must not fail, or a retry path would
		// turn a bookkeeping no-op into an error.
		return nil
	}
	if reservation.Owner != "" && entry.Owner != "" && entry.Owner != reservation.Owner {
		// A stale holder must not disturb the attempt that took over.
		return nil
	}
	if entry.Outcome.Terminal() {
		// An event that reached a terminal outcome must not be sent back for another attempt.
		return nil
	}
	if entry.Outcome == OutcomeDeadLettering {
		// See Fail: a claim is given up through AbortDeadLetter, which also releases the lease the
		// claim exists to hold.
		return nil
	}
	entry.Outcome = OutcomeAbandoned
	entry.Detail = reason
	entry.CompletedAt = m.clock()
	// The lease ends now so the redelivery may take the reservation immediately.
	entry.LeaseUntil = time.Time{}
	return nil
}

// BeginDeadLetter claims the dead-letter decision for a reservation.
//
// The ownership check is strict, and deliberately stricter than the one Complete and Fail use:
// a reservation that cannot name the current owner cannot demonstrate that it is the current
// attempt, and the cost of guessing wrong here is a parked entry and a terminal record for an
// event a newer attempt has already applied.
func (m *MemoryConsumptionStore) BeginDeadLetter(ctx context.Context, reservation Reservation, reason string) (DeadLetterClaim, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if reservation.EventID == "" {
		// An undecodable entry has no event id, so it never had a reservation to claim against.
		// The caller writes it and accepts that a retry may repeat it.
		return DeadLetterClaimGranted, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock()
	entry, ok := m.entries[reservation.EventID]
	if !ok {
		// The record is gone, so this caller cannot prove it is still the current attempt.
		// Staying silent is recoverable - the source entry stays pending and a later delivery
		// reserves the event again - while a terminal record for the wrong outcome is not.
		return DeadLetterClaimSuperseded, nil
	}
	if !m.ownsDeadLetterLocked(entry, reservation) {
		return DeadLetterClaimSuperseded, nil
	}
	if entry.Outcome.Terminal() {
		// Somebody finished the event: the newer attempt succeeded, or already parked it.
		return DeadLetterClaimSuperseded, nil
	}

	entry.Outcome = OutcomeDeadLettering
	entry.Detail = reason
	// The claim gets a lease of its own. Fail cleared the previous one, so without this the claim
	// would be invisible to Begin - which only skipped a live *reservation* - and another consumer
	// could take the event over and re-run the handler while this delivery was still writing the
	// parked entry. The lease is what makes the claim mutually exclusive; an expired one is what
	// makes a crashed park recoverable.
	entry.ReservedAt = now
	entry.LeaseUntil = now.Add(m.leaseTTL)
	return DeadLetterClaimGranted, nil
}

// FinalizeDeadLetter records the terminal outcome for a claim this caller still owns.
//
// It is a compare-and-set rather than a write, because the parked entry is written outside the
// store: between BeginDeadLetter and this call the lease can expire, a newer attempt can take
// the event over, and that attempt can succeed. Overwriting the record then would report
// applied work as dead-lettered and make an operator chase an event that completed.
//
// A caller that reports no reservation - a handler that does not run through the pipeline, and
// therefore has no attempt to prove - is judged by the record instead: a park is refused for an
// event that already finished or is being processed right now. That is weaker than the
// reservation check, because the caller's own generation is unknown, but it still closes the
// case that matters most: a worker whose routing changed must not park an event another worker
// has already applied.
func (m *MemoryConsumptionStore) FinalizeDeadLetter(ctx context.Context, reservation Reservation, record ConsumptionRecord, reason string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	key := reservation.EventID
	owned := key != "" && reservation.Owner != ""
	if key == "" {
		// Keyed on the record: the caller has no reservation, but the event still has an identity.
		key = record.EventID
	}
	if key == "" {
		// An undecodable entry has no event id at all: the parked entry is the only record there
		// can be, and there is nothing to order against.
		return true, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock()
	entry, ok := m.entries[key]
	if !ok {
		if owned {
			// The record is gone, so this caller cannot prove it is the current attempt. Staying
			// silent is recoverable - the source entry stays pending and a later delivery reserves
			// the event again - while a terminal record for the wrong outcome is not.
			return false, nil
		}
		// A handler outside the pipeline never created a record. The parked entry is the first
		// thing this store learns about the event, so the record is created from it.
		m.entries[key] = &ConsumptionEntry{
			ConsumptionRecord: record,
			Outcome:           OutcomeDeadLettered,
			Detail:            reason,
			Attempts:          1,
			ReservedAt:        now,
			CompletedAt:       now,
		}
		return true, nil
	}

	if owned {
		if !m.ownsDeadLetterLocked(entry, reservation) {
			return false, nil
		}
		if entry.Outcome != OutcomeDeadLettering {
			// The event moved on while the parked entry was being written.
			return false, nil
		}
	} else if entry.Outcome.Terminal() || (!entry.LeaseUntil.IsZero() && now.Before(entry.LeaseUntil)) {
		// Finished, or owned by an attempt that is working right now: either way this caller has
		// no business parking or recording the event.
		return false, nil
	}

	if record.EventID != "" {
		entry.ConsumptionRecord = record
	}
	entry.Outcome = OutcomeDeadLettered
	entry.Detail = reason
	entry.CompletedAt = now
	// A terminal outcome means no lease should remain that another delivery could take over.
	entry.LeaseUntil = time.Time{}
	return true, nil
}

// AbortDeadLetter gives a dead-letter claim back after its parked entry could not be written.
//
// The claim itself did happen - the handler ran and failed - so the reservation is released as a
// failed attempt rather than an abandoned one: the next delivery is a new attempt. Releasing it
// here is what lets the retry write the parked entry immediately instead of waiting for the claim's
// lease to expire, and it is safe because only the claim holder can release it.
func (m *MemoryConsumptionStore) AbortDeadLetter(ctx context.Context, reservation Reservation, reason string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if reservation.EventID == "" {
		return true, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[reservation.EventID]
	if !ok {
		return false, nil
	}
	if !m.ownsDeadLetterLocked(entry, reservation) || entry.Outcome != OutcomeDeadLettering {
		// The claim is gone or belongs to a newer attempt, which must not be disturbed.
		return false, nil
	}

	entry.Outcome = OutcomeFailed
	entry.Detail = reason
	entry.CompletedAt = m.clock()
	// The claim is given up, so the lease goes with it and the retry may reserve immediately.
	entry.LeaseUntil = time.Time{}
	return true, nil
}

// inProgressLocked reports whether the entry is currently held by a delivery: either a normal
// reservation whose holder is still working, or a dead-letter claim whose parked entry is still
// being written.
func inProgressLocked(entry *ConsumptionEntry) bool {
	return entry.Outcome == "" || entry.Outcome == OutcomeDeadLettering
}

// ownsDeadLetterLocked reports whether the reservation is the attempt that currently owns the
// event. A reservation without an owner is refused: Begin always mints one, so an empty owner
// means the caller did not get its reservation from this store.
func (m *MemoryConsumptionStore) ownsDeadLetterLocked(entry *ConsumptionEntry, reservation Reservation) bool {
	return reservation.Owner != "" && entry.Owner == reservation.Owner
}

// Entry returns the stored state of one event.
func (m *MemoryConsumptionStore) Entry(eventID string) (ConsumptionEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[eventID]
	if !ok {
		return ConsumptionEntry{}, false
	}
	return *entry, true
}

// Len reports how many events the store knows about, for assertions.
func (m *MemoryConsumptionStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// newReservationOwner mints a unique reservation owner.
//
// Uniqueness matters for the same reason the lock token does: after a takeover, a stale
// holder must not be able to finalise or release the new attempt.
func newReservationOwner() string {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Sprintf("owner-%d", time.Now().UnixNano())
	}
	return "owner-" + hex.EncodeToString(random[:])
}
