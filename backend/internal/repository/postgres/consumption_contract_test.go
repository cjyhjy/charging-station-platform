package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
)

// This file holds the consumption-record contract test.
//
// It exists because the PostgreSQL store replaces the in-memory one, and the semantics it has
// to reproduce were established over six review rounds: three reservation states, a lease
// that covers dead-letter claims too, an attempt counter that Release must not advance, and a
// two-phase dead-letter decision whose refusal means "stay completely silent". A store that
// differs from the contract in any of those would silently lose or duplicate events, so the
// same scenarios are executed against BOTH implementations and a difference fails the test.

// contractStore is the smallest surface the contract test needs from a store, so the
// in-memory and PostgreSQL implementations can be driven by the same assertions.
type contractStore interface {
	event.ConsumptionStore
	entry(ctx context.Context, eventID string) (event.ConsumptionEntry, bool)
}

// memoryStoreAdapter adapts the in-memory store's clock and entry access to the contract.
type memoryStoreAdapter struct {
	*event.MemoryConsumptionStore
}

func (m memoryStoreAdapter) entry(_ context.Context, eventID string) (event.ConsumptionEntry, bool) {
	return m.MemoryConsumptionStore.Entry(eventID)
}

// pgStoreAdapter adapts the PostgreSQL store the same way.
type pgStoreAdapter struct {
	*ConsumptionStore
}

func (p pgStoreAdapter) entry(ctx context.Context, eventID string) (event.ConsumptionEntry, bool) {
	entry, ok, err := p.ConsumptionStore.Entry(ctx, eventID)
	if err != nil {
		panic(fmt.Sprintf("read consumption entry: %v", err))
	}
	return entry, ok
}

// contractEntry is one store under test together with its clock, so a scenario can advance
// time for exactly the implementation it is driving.
type contractEntry struct {
	store contractStore
	clock *contractClock
}

// newContractStores returns the in-memory store and, when a test DSN is configured, the
// PostgreSQL store. Each gets its own clock so the two subtests cannot influence each other.
func newContractStores(t *testing.T, leaseTTL time.Duration) map[string]contractEntry {
	t.Helper()
	entries := map[string]contractEntry{}

	memoryClock := newContractClock()
	memory := event.NewMemoryConsumptionStore()
	memory.SetClock(memoryClock.nowFunc)
	memory.SetLeaseTTL(leaseTTL)
	entries["memory"] = contractEntry{store: memoryStoreAdapter{MemoryConsumptionStore: memory}, clock: memoryClock}

	dsn, ok := testDSN()
	if !ok {
		return entries
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db, err := Open(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewConsumptionStore(db)
	if err != nil {
		t.Fatalf("NewConsumptionStore() error = %v", err)
	}
	pgClock := newContractClock()
	store.SetClock(pgClock.nowFunc)
	store.SetLeaseTTL(leaseTTL)
	entries["postgres"] = contractEntry{store: pgStoreAdapter{ConsumptionStore: store}, clock: pgClock}
	return entries
}

// testDSN reports the disposable database the PostgreSQL half of the contract runs against.
func testDSN() (string, bool) {
	dsn := strings.TrimSpace(os.Getenv("NCS_TEST_PG_DSN"))
	return dsn, dsn != ""
}

// contractClock is a mutable clock the contract scenarios own, so lease expiry is asserted
// without sleeping.
type contractClock struct {
	mu  sync.Mutex
	now time.Time
}

func newContractClock() *contractClock {
	return &contractClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
}

func (c *contractClock) nowFunc() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *contractClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// uniqueEventID keeps the shared test database isolated between runs and between cases.
func uniqueEventID(t *testing.T, name string) string {
	t.Helper()
	return fmt.Sprintf("evt_%s_%d_%s", name, time.Now().UnixNano(), testNameSuffix(t))
}

func testNameSuffix(t *testing.T) string {
	t.Helper()
	suffix := t.Name()
	if len(suffix) > 40 {
		suffix = suffix[:40]
	}
	return suffix
}

const contractLease = time.Minute

// The three states Begin answers, and the actions each one demands.
func TestConsumptionContractBeginStates(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "begin")
			record := event.ConsumptionRecord{EventID: eventID, Stream: "ncs:stream:charge-event", StreamID: "1-1", Consumer: "c1"}

			first, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if first.State != event.ReservationReserved || first.Attempt != 1 || first.Owner == "" {
				t.Fatalf("expected a reserved attempt 1 with an owner, got %+v", first)
			}

			// A second delivery while the lease is live must not be told to apply the event.
			second, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("second begin: %v", err)
			}
			if second.State != event.ReservationInProgress {
				t.Fatalf("expected in_progress, got %s", second.State)
			}
			if second.Attempt != first.Attempt {
				t.Fatalf("a wait must not advance the attempt, got %d", second.Attempt)
			}

			// Terminal: already consumed, and never applied twice.
			if err := store.Complete(ctx, first, event.OutcomeSucceeded, ""); err != nil {
				t.Fatalf("complete: %v", err)
			}
			third, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("third begin: %v", err)
			}
			if third.State != event.ReservationAlreadyConsumed {
				t.Fatalf("expected already_consumed, got %s", third.State)
			}
		})
	}
}

// Release gives the reservation back without recording an attempt, which is what stops
// contention from spending the retry budget; Fail records the attempt.
func TestConsumptionContractReleaseDoesNotSpendTheBudget(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "release")
			record := event.ConsumptionRecord{EventID: eventID}

			first, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := store.Release(ctx, first, "duplicate guard is held by another claim"); err != nil {
				t.Fatalf("release: %v", err)
			}
			entry, ok := store.entry(ctx, eventID)
			if !ok {
				t.Fatal("expected a consumption record")
			}
			if entry.Outcome != event.OutcomeAbandoned {
				t.Fatalf("expected an abandoned outcome, got %q", entry.Outcome)
			}

			// Ten waits later the attempt number is unchanged.
			for i := 0; i < 10; i++ {
				waiting, err := store.Begin(ctx, record)
				if err != nil {
					t.Fatalf("begin %d: %v", i, err)
				}
				if waiting.State != event.ReservationReserved {
					t.Fatalf("expected the redelivery to be granted, got %s", waiting.State)
				}
				if waiting.Attempt != 1 {
					t.Fatalf("waiting advanced the attempt to %d", waiting.Attempt)
				}
				if err := store.Release(ctx, waiting, "still held"); err != nil {
					t.Fatalf("release %d: %v", i, err)
				}
			}

			// The first real failure does advance it.
			real, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := store.Fail(ctx, real, "device unavailable"); err != nil {
				t.Fatalf("fail: %v", err)
			}
			next, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if next.Attempt != 2 {
				t.Fatalf("expected a failed attempt to be followed by attempt 2, got %d", next.Attempt)
			}
		})
	}
}

// A lease that expires lets another delivery take the event over, and that is a new attempt.
func TestConsumptionContractTakeoverAfterLeaseExpiry(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "takeover")
			record := event.ConsumptionRecord{EventID: eventID}

			stale, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			entry.clock.advance(2 * contractLease)

			current, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("takeover: %v", err)
			}
			if current.State != event.ReservationReserved {
				t.Fatalf("expected the takeover to be granted, got %s", current.State)
			}
			if current.Attempt != stale.Attempt+1 {
				t.Fatalf("expected attempt %d, got %d", stale.Attempt+1, current.Attempt)
			}
			if current.Owner == stale.Owner {
				t.Fatal("expected the takeover to mint a new owner")
			}

			// The stale holder must not be able to finalise the attempt that took over.
			if err := store.Complete(ctx, stale, event.OutcomeSucceeded, ""); err == nil {
				t.Fatal("expected a stale completion to be refused")
			}
			entry, _ := store.entry(ctx, eventID)
			if entry.Owner != current.Owner {
				t.Fatalf("a stale completion disturbed the current attempt: owner %q", entry.Owner)
			}
		})
	}
}

// The dead-letter decision is two phases, and the two phases are what stop a stale delivery
// from overwriting the outcome of the attempt that took the event over.
func TestConsumptionContractDeadLetterOwnership(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "deadletter")
			record := event.ConsumptionRecord{EventID: eventID}

			// The old generation reserves the event, fails, and its lease expires.
			stale, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := store.Fail(ctx, stale, "bad payload"); err != nil {
				t.Fatalf("fail: %v", err)
			}
			entry.clock.advance(2 * contractLease)

			// A newer attempt takes the event over and applies it.
			current, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("takeover: %v", err)
			}
			if err := store.Complete(ctx, current, event.OutcomeSucceeded, ""); err != nil {
				t.Fatalf("complete: %v", err)
			}

			// The old generation now tries to park the event. This is the finding that made the
			// whole check exist: granting it would write a dead letter for applied work.
			claim, err := store.BeginDeadLetter(ctx, stale, "permanent_failure")
			if err != nil {
				t.Fatalf("begin dead letter: %v", err)
			}
			if claim != event.DeadLetterClaimSuperseded {
				t.Fatalf("expected a superseded claim, got %s", claim)
			}
			finalized, err := store.FinalizeDeadLetter(ctx, stale, record, "permanent_failure")
			if err != nil {
				t.Fatalf("finalize: %v", err)
			}
			if finalized {
				t.Fatal("a superseded finalisation must be refused")
			}
			entry, _ := store.entry(ctx, eventID)
			if entry.Outcome != event.OutcomeSucceeded {
				t.Fatalf("the applied outcome must survive, got %s", entry.Outcome)
			}
		})
	}
}

// The claim holds the event while the parked entry is written, so a second consumer cannot
// run the handler again in that window; and only the claim holder may finalise or abort it.
func TestConsumptionContractDeadLetterClaimHoldsTheEvent(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "claimholds")
			record := event.ConsumptionRecord{EventID: eventID}

			reservation, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := store.Fail(ctx, reservation, "bad payload"); err != nil {
				t.Fatalf("fail: %v", err)
			}
			claim, err := store.BeginDeadLetter(ctx, reservation, "permanent_failure")
			if err != nil || claim != event.DeadLetterClaimGranted {
				t.Fatalf("expected the claim to be granted, got %s and %v", claim, err)
			}

			held, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if held.State != event.ReservationInProgress {
				t.Fatalf("expected the claim to hold the event, got %s", held.State)
			}
			if held.Attempt != reservation.Attempt {
				t.Fatalf("a held claim must not create a new attempt, got %d", held.Attempt)
			}

			// Neither Fail nor Release may hand the event over while the park is being written.
			if err := store.Fail(ctx, reservation, "late failure"); err != nil {
				t.Fatalf("fail: %v", err)
			}
			if err := store.Release(ctx, reservation, "late release"); err != nil {
				t.Fatalf("release: %v", err)
			}
			stillHeld, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if stillHeld.State != event.ReservationInProgress {
				t.Fatalf("Fail and Release must not release a claim, got %s", stillHeld.State)
			}

			// A stale reservation cannot finalise or abort somebody else's claim.
			gave, err := store.AbortDeadLetter(ctx, event.Reservation{EventID: eventID, Owner: "someone-else"}, "stale")
			if err != nil {
				t.Fatalf("abort: %v", err)
			}
			if gave {
				t.Fatal("a foreign owner must not abort the claim")
			}

			finalized, err := store.FinalizeDeadLetter(ctx, reservation, record, "permanent_failure")
			if err != nil {
				t.Fatalf("finalize: %v", err)
			}
			if !finalized {
				t.Fatal("expected the claim holder to finalise the park")
			}
			entry, _ := store.entry(ctx, eventID)
			if entry.Outcome != event.OutcomeDeadLettered {
				t.Fatalf("expected the terminal dead-letter outcome, got %s", entry.Outcome)
			}

			// A parked event is terminal: it is never applied again.
			next, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if next.State != event.ReservationAlreadyConsumed {
				t.Fatalf("a parked event must stay terminal, got %s", next.State)
			}
		})
	}
}

// A failed write gives the claim back, so the retry parks immediately instead of waiting for
// the claim's lease to expire.
func TestConsumptionContractAbortDeadLetter(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "abort")
			record := event.ConsumptionRecord{EventID: eventID}

			reservation, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if _, err := store.BeginDeadLetter(ctx, reservation, "permanent_failure"); err != nil {
				t.Fatalf("begin dead letter: %v", err)
			}
			gave, err := store.AbortDeadLetter(ctx, reservation, "write failed")
			if err != nil {
				t.Fatalf("abort: %v", err)
			}
			if !gave {
				t.Fatal("expected the claim holder to give the claim back")
			}
			entry, _ := store.entry(ctx, eventID)
			if entry.Outcome != event.OutcomeFailed {
				t.Fatalf("expected the reservation released as failed, got %s", entry.Outcome)
			}

			// Nothing holds the event, so the retry may reserve and claim again right away.
			retry, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if retry.State != event.ReservationReserved {
				t.Fatalf("expected the retry to be granted, got %s", retry.State)
			}
			claim, err := store.BeginDeadLetter(ctx, retry, "permanent_failure")
			if err != nil || claim != event.DeadLetterClaimGranted {
				t.Fatalf("expected the retry to claim the park, got %s and %v", claim, err)
			}
		})
	}
}

// A claim that died mid-write is recoverable: once its lease expires, the event can be taken
// over and the handler runs again.
func TestConsumptionContractClaimLeaseExpires(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "claimexpiry")
			record := event.ConsumptionRecord{EventID: eventID}

			reservation, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if _, err := store.BeginDeadLetter(ctx, reservation, "permanent_failure"); err != nil {
				t.Fatalf("begin dead letter: %v", err)
			}
			entry.clock.advance(2 * contractLease)
			recovered, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if recovered.State != event.ReservationReserved {
				t.Fatalf("expected an expired claim to be recoverable, got %s", recovered.State)
			}
			if recovered.Attempt != reservation.Attempt+1 {
				t.Fatalf("expected a new attempt, got %d", recovered.Attempt)
			}
		})
	}
}

// A caller that reports no reservation - a handler outside the pipeline - is still refused a
// park for an event that already finished.
func TestConsumptionContractDeadLetterWithoutAReservation(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "unreserved")
			record := event.ConsumptionRecord{EventID: eventID}

			reservation, err := store.Begin(ctx, record)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := store.Complete(ctx, reservation, event.OutcomeSucceeded, ""); err != nil {
				t.Fatalf("complete: %v", err)
			}
			claim, err := store.BeginDeadLetter(ctx, event.Reservation{}, "permanent_failure")
			if err != nil || claim != event.DeadLetterClaimGranted {
				t.Fatalf("expected the unkeyed claim to be granted, got %s and %v", claim, err)
			}
			finalized, err := store.FinalizeDeadLetter(ctx, event.Reservation{}, record, "permanent_failure")
			if err != nil {
				t.Fatalf("finalize: %v", err)
			}
			if finalized {
				t.Fatal("expected the park to be refused for an event that already finished")
			}
			entry, _ := store.entry(ctx, eventID)
			if entry.Outcome != event.OutcomeSucceeded {
				t.Fatalf("expected the success to survive, got %s", entry.Outcome)
			}
		})
	}
}

// Two deliveries racing for the same fresh event: exactly one is granted the attempt.
func TestConsumptionContractConcurrentBeginGrantsOneAttempt(t *testing.T) {
	for name, entry := range newContractStores(t, contractLease) {
		store := entry.store
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			eventID := uniqueEventID(t, "race")
			record := event.ConsumptionRecord{EventID: eventID}

			const racers = 8
			type outcome struct {
				reservation event.Reservation
				err         error
			}
			results := make(chan outcome, racers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < racers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					reservation, err := store.Begin(ctx, record)
					results <- outcome{reservation: reservation, err: err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)

			reserved := 0
			for result := range results {
				if result.err != nil {
					t.Fatalf("begin: %v", result.err)
				}
				switch result.reservation.State {
				case event.ReservationReserved:
					reserved++
				case event.ReservationInProgress:
					// Fine: another racer took it first.
				default:
					t.Fatalf("unexpected state %s", result.reservation.State)
				}
			}
			if reserved != 1 {
				t.Fatalf("expected exactly one racer to be granted the attempt, got %d", reserved)
			}
		})
	}
}
