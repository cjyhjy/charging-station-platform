package event

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryConsumptionStoreReservesOnce(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_01", EventType: ChargeStarted, AggregateID: "order_01", Attempt: 1}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if reservation.State != ReservationReserved {
		t.Fatalf("expected the first reservation to be granted, got %s", reservation.State)
	}
	if reservation.Owner == "" {
		t.Fatal("expected an owner token")
	}

	// A second delivery of the same event while the first is still in flight must be told to
	// wait, not told the event is finished.
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != ReservationInProgress {
		t.Fatalf("expected in_progress for a live lease, got %s", second.State)
	}
	if second.Owner != reservation.Owner {
		t.Fatal("expected the live lease owner to be reported")
	}
	if !second.LeaseExpiresAt.After(time.Now().UTC().Add(-time.Second)) {
		t.Fatalf("expected a future lease expiry, got %s", second.LeaseExpiresAt)
	}
}

// This is the state the previous revision conflated with "already consumed", and the
// conflation was a data-loss bug: a process that died between reserving and applying left a
// reservation behind, and the next delivery acknowledged the event as a duplicate without
// anyone having applied it.
func TestMemoryConsumptionStoreInProgressIsNotConsumed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_crash"}

	// A first attempt reserves the event and then "dies" without completing or failing.
	if _, err := store.Begin(ctx, record); err != nil {
		t.Fatalf("begin: %v", err)
	}

	// A redelivery inside the lease window must be able to tell that apart from a finished
	// event.
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State == ReservationAlreadyConsumed {
		t.Fatal("an event held by a live lease must not be reported as already consumed")
	}
	if second.State != ReservationInProgress {
		t.Fatalf("expected in_progress, got %s", second.State)
	}
}

// Once the lease expires the event becomes recoverable again, which is what bounds the
// damage a crashed holder can do.
func TestMemoryConsumptionStoreTakesOverAnExpiredLease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := ConsumptionRecord{EventID: "evt_expired", Attempt: 1}

	first, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	clock.advance(2 * time.Minute)

	record.Attempt = 2
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != ReservationReserved {
		t.Fatalf("expected the expired lease to be taken over, got %s", second.State)
	}
	if second.Owner == first.Owner {
		t.Fatal("expected a fresh owner token for the takeover")
	}
	if second.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", second.Attempt)
	}

	entry, _ := store.Entry(record.EventID)
	if entry.Owner != second.Owner {
		t.Fatalf("expected the store to record the new owner, got %q", entry.Owner)
	}
}

// A stale holder must not be able to finalise an attempt that was taken over.
func TestMemoryConsumptionStoreRefusesAStaleOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := ConsumptionRecord{EventID: "evt_stale"}

	stale, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	clock.advance(2 * time.Minute)

	fresh, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if fresh.State != ReservationReserved {
		t.Fatalf("expected a takeover, got %s", fresh.State)
	}

	if err := store.Complete(ctx, stale, OutcomeSucceeded, ""); err == nil {
		t.Fatal("expected the stale owner's completion to be refused")
	}
	// A stale release must not disturb the new attempt either.
	if err := store.Fail(ctx, stale, "stale release"); err != nil {
		t.Fatalf("expected a stale release to be a no-op, got %v", err)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome == OutcomeFailed {
		t.Fatal("a stale release must not clear the new attempt's reservation")
	}

	if err := store.Complete(ctx, fresh, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("expected the current owner to complete, got %v", err)
	}
}

// A completed event is terminal: this is the property that makes a redelivery a no-op.
func TestMemoryConsumptionStoreCompletedEventIsTerminal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_02"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Complete(ctx, reservation, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != ReservationAlreadyConsumed {
		t.Fatalf("a completed event must never be reserved again, got %s", second.State)
	}

	entry, ok := store.Entry(record.EventID)
	if !ok {
		t.Fatal("expected the entry to exist")
	}
	if entry.Outcome != OutcomeSucceeded {
		t.Fatalf("expected %s, got %s", OutcomeSucceeded, entry.Outcome)
	}
	if entry.CompletedAt.IsZero() {
		t.Fatal("expected a completion timestamp")
	}
}

// A failed attempt must be retryable, or an at-least-once redelivery would be blocked
// forever and the event lost.
func TestMemoryConsumptionStoreFailedEventCanBeRetried(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_03", Attempt: 1}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Fail(ctx, reservation, "gateway timeout"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	// The lease must be cleared as part of the release, or the next delivery would be told
	// the event is still held by a live attempt.
	entry, _ := store.Entry(record.EventID)
	if !entry.LeaseUntil.IsZero() {
		t.Fatal("expected the released lease to be cleared so the retry is not blocked")
	}
	if entry.Outcome != OutcomeFailed {
		t.Fatalf("expected the failure recorded, got %q", entry.Outcome)
	}

	record.Attempt = 2
	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != ReservationReserved {
		t.Fatalf("expected a failed event to be re-reservable, got %s", second.State)
	}

	entry, _ = store.Entry(record.EventID)
	if entry.Attempts != 2 {
		t.Fatalf("expected 2 attempts recorded, got %d", entry.Attempts)
	}
	if entry.Outcome != "" {
		t.Fatalf("expected the failed outcome to be cleared for the retry, got %q", entry.Outcome)
	}
}

// A dead-lettered event is terminal: re-applying it would duplicate whatever partial effect
// it already had.
func TestMemoryConsumptionStoreDeadLetteredEventIsTerminal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_04"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	describeDeadLetter(t, ctx, store, reservation, record, "unsupported charger")

	second, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if second.State != ReservationAlreadyConsumed {
		t.Fatalf("a dead-lettered event must never be reserved again, got %s", second.State)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != OutcomeDeadLettered {
		t.Fatalf("expected %s, got %s", OutcomeDeadLettered, entry.Outcome)
	}
}

// The dead-letter write must be idempotent, because the worker calls it again after a crash
// between parking the entry and acknowledging it.
//
// Idempotence now rests on the two phases: a second attempt at the same reservation is refused
// the claim (the event already reached a terminal outcome), and the record is left alone.
func TestMemoryConsumptionStoreDeadLetterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_dlq_idem", Stream: "s", StreamID: "1-1"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	describeDeadLetter(t, ctx, store, reservation, record, "retry_exhausted")

	for i := 0; i < 3; i++ {
		claim, err := store.BeginDeadLetter(ctx, reservation, "retry_exhausted")
		if err != nil {
			t.Fatalf("begin dead letter %d: %v", i, err)
		}
		if claim != DeadLetterClaimSuperseded {
			t.Fatalf("expected the repeated claim to be refused, got %s", claim)
		}
	}
	if store.Len() != 1 {
		t.Fatalf("expected one record, got %d", store.Len())
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != OutcomeDeadLettered {
		t.Fatalf("expected the terminal outcome, got %s", entry.Outcome)
	}
	if entry.Stream != "s" || entry.StreamID != "1-1" {
		t.Fatalf("expected the stream coordinates recorded, got %+v", entry.ConsumptionRecord)
	}
}

// An undecodable entry has no event id to key a record on, so nothing is created: the parked
// entry itself is the record. The claim is still granted, because the caller has nothing to
// check against and refusing it would leave the entry pending forever.
func TestMemoryConsumptionStoreDeadLetterWithoutAnEventIDCreatesNoRecord(t *testing.T) {
	store := NewMemoryConsumptionStore()
	ctx := context.Background()
	claim, err := store.BeginDeadLetter(ctx, Reservation{}, "invalid_event")
	if err != nil || claim != DeadLetterClaimGranted {
		t.Fatalf("expected the claim to be granted, got %s and %v", claim, err)
	}
	if _, err := store.FinalizeDeadLetter(ctx, Reservation{}, ConsumptionRecord{}, "invalid_event"); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("expected no record, got %d", store.Len())
	}
}

func TestMemoryConsumptionStoreRequiresAnEventID(t *testing.T) {
	store := NewMemoryConsumptionStore()
	if _, err := store.Begin(context.Background(), ConsumptionRecord{}); err == nil {
		t.Fatal("expected a missing event id to be rejected")
	}
}

func TestMemoryConsumptionStoreRecordsStreamCoordinates(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{
		EventID: "evt_05", Stream: "ncs:stream:charge-event", StreamID: "1700000000000-0",
		Consumer: "worker-1", Attempt: 1, TraceID: "trace_01",
	}
	if _, err := store.Begin(ctx, record); err != nil {
		t.Fatalf("begin: %v", err)
	}

	entry, ok := store.Entry(record.EventID)
	if !ok {
		t.Fatal("expected the entry")
	}
	// The stream coordinates are what let an operator move from a record back to the
	// exact stream entry, which is what makes a dead letter investigable.
	if entry.Stream != record.Stream || entry.StreamID != record.StreamID || entry.Consumer != record.Consumer {
		t.Fatalf("expected the coordinates preserved, got %+v", entry.ConsumptionRecord)
	}
	if entry.TraceID != "trace_01" {
		t.Fatalf("expected the trace id preserved, got %q", entry.TraceID)
	}
}

func TestMemoryConsumptionStoreDefaultsAttemptToOne(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	reservation, err := store.Begin(ctx, ConsumptionRecord{EventID: "evt_06"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if reservation.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", reservation.Attempt)
	}
}

func TestMemoryConsumptionStoreHonoursContextCancellation(t *testing.T) {
	store := NewMemoryConsumptionStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.Begin(ctx, ConsumptionRecord{EventID: "evt_07"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if err := store.Complete(ctx, Reservation{EventID: "evt_07"}, OutcomeSucceeded, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if err := store.Fail(ctx, Reservation{EventID: "evt_07"}, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if _, err := store.BeginDeadLetter(ctx, Reservation{EventID: "evt_07"}, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if _, err := store.FinalizeDeadLetter(ctx, Reservation{EventID: "evt_07"}, ConsumptionRecord{EventID: "evt_07"}, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestMemoryConsumptionStoreFailOnUnknownEventIsNotAnError(t *testing.T) {
	// Releasing a reservation that is already gone must not fail, or a retry path would
	// turn a bookkeeping no-op into an error.
	if err := NewMemoryConsumptionStore().Fail(context.Background(), Reservation{EventID: "evt_unknown"}, "x"); err != nil {
		t.Fatalf("expected a no-op, got %v", err)
	}
}

func TestMemoryConsumptionStoreCompleteOnUnknownEventIsAnError(t *testing.T) {
	// Completing an attempt that was never reserved means the caller has lost the
	// reservation, so it must surface rather than silently record a terminal state.
	err := NewMemoryConsumptionStore().Complete(context.Background(), Reservation{EventID: "evt_unknown"}, OutcomeSucceeded, "")
	if err == nil {
		t.Fatal("expected an error for a completion without a reservation")
	}
}

func TestMemoryConsumptionStoreUsesInjectedClock(t *testing.T) {
	store := NewMemoryConsumptionStore()
	base := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return base })

	ctx := context.Background()
	reservation, err := store.Begin(ctx, ConsumptionRecord{EventID: "evt_08"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Complete(ctx, reservation, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	entry, _ := store.Entry("evt_08")
	if !entry.ReservedAt.Equal(base) || !entry.CompletedAt.Equal(base) {
		t.Fatalf("expected the injected clock to be used, got %+v", entry)
	}
	if !reservation.LeaseExpiresAt.Equal(base.Add(DefaultConsumptionLease)) {
		t.Fatalf("expected the default lease, got %s", reservation.LeaseExpiresAt)
	}
}

// Owners must be unique per reservation, or a stale holder could finalise or release an
// attempt it no longer owns.
func TestMemoryConsumptionStoreMintsUniqueOwners(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	seen := map[string]bool{}
	for i := 0; i < 32; i++ {
		record := ConsumptionRecord{EventID: "evt_owner_" + string(rune('a'+i%26)) + string(rune('0'+i/26))}
		reservation, err := store.Begin(ctx, record)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if reservation.Owner == "" {
			t.Fatal("expected an owner token")
		}
		if seen[reservation.Owner] {
			t.Fatalf("owner %q was reused", reservation.Owner)
		}
		seen[reservation.Owner] = true
	}
}

func TestConsumptionOutcomeTerminal(t *testing.T) {
	if !OutcomeSucceeded.Terminal() || !OutcomeDeadLettered.Terminal() {
		t.Fatal("succeeded and dead-lettered are terminal")
	}
	if OutcomeFailed.Terminal() {
		t.Fatal("a failed attempt must stay retryable")
	}
	if ConsumptionOutcome("").Terminal() {
		t.Fatal("an unset outcome must not be terminal")
	}
}

func TestMemoryConsumptionStoreLen(t *testing.T) {
	store := NewMemoryConsumptionStore()
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := store.Begin(ctx, ConsumptionRecord{EventID: id}); err != nil {
			t.Fatalf("begin: %v", err)
		}
	}
	if store.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", store.Len())
	}
}

// A released reservation is not an attempt. This is what stops waiting for another consumer's
// duplicate guard from spending the retry budget: the delivery that eventually takes the
// reservation over is the same attempt the abandoned one would have been.
func TestMemoryConsumptionStoreReleaseDoesNotSpendAnAttempt(t *testing.T) {
	store := NewMemoryConsumptionStore()
	ctx := context.Background()
	record := ConsumptionRecord{EventID: "evt_release"}

	first, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if first.Attempt != 1 {
		t.Fatalf("expected the first reservation to be attempt 1, got %d", first.Attempt)
	}
	if err := store.Release(ctx, first, "duplicate guard is held by another claim"); err != nil {
		t.Fatalf("release: %v", err)
	}

	entry, ok := store.Entry("evt_release")
	if !ok {
		t.Fatal("expected a consumption record")
	}
	if entry.Outcome != OutcomeAbandoned {
		t.Fatalf("expected the reservation to be abandoned, got %q", entry.Outcome)
	}
	if entry.Outcome.Terminal() {
		t.Fatal("an abandoned reservation must not be terminal, or the event could never be processed")
	}

	// Ten waits later the attempt number is still the one the first delivery was granted.
	for i := 0; i < 10; i++ {
		reservation, err := store.Begin(ctx, record)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if reservation.State != ReservationReserved {
			t.Fatalf("expected the redelivery to be granted the reservation, got %s", reservation.State)
		}
		if reservation.Attempt != 1 {
			t.Fatalf("expected waiting to leave the attempt count at 1, got %d after %d waits", reservation.Attempt, i+1)
		}
		if err := store.Release(ctx, reservation, "still held"); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}

	// The first real failure, by contrast, does advance the budget.
	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Fail(ctx, reservation, "device unavailable"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	next, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if next.Attempt != 2 {
		t.Fatalf("expected a failed attempt to be followed by attempt 2, got %d", next.Attempt)
	}
}

// A stale holder must not release an attempt that has since been taken over, for the same reason
// it must not be able to complete or fail one.
func TestMemoryConsumptionStoreReleaseRefusesAStaleOwner(t *testing.T) {
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	ctx := context.Background()
	record := ConsumptionRecord{EventID: "evt_release_owner"}

	stale, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if current.State != ReservationReserved || current.Owner == stale.Owner {
		t.Fatalf("expected the expired reservation to be taken over, got %s", current.State)
	}

	if err := store.Release(ctx, stale, "stale holder"); err != nil {
		t.Fatalf("release: %v", err)
	}
	entry, _ := store.Entry("evt_release_owner")
	if entry.Outcome == OutcomeAbandoned {
		t.Fatal("a stale holder must not abandon the attempt that took the reservation over")
	}
	if entry.Owner != current.Owner {
		t.Fatal("a stale holder must not change who owns the attempt")
	}
}

// A terminal event must never be sent back for another attempt, whatever a late delivery says.
func TestMemoryConsumptionStoreReleaseIgnoresATerminalEvent(t *testing.T) {
	store := NewMemoryConsumptionStore()
	ctx := context.Background()
	record := ConsumptionRecord{EventID: "evt_release_terminal"}

	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Complete(ctx, reservation, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := store.Release(ctx, reservation, "late release"); err != nil {
		t.Fatalf("release: %v", err)
	}
	entry, _ := store.Entry("evt_release_terminal")
	if entry.Outcome != OutcomeSucceeded {
		t.Fatalf("expected the terminal outcome to survive, got %q", entry.Outcome)
	}
	if next, err := store.Begin(ctx, record); err != nil || next.State != ReservationAlreadyConsumed {
		t.Fatalf("expected the event to stay consumed, got %s and %v", next.State, err)
	}
}

// describeDeadLetter runs the two phases the worker runs, for tests that only care about the
// resulting state.
func describeDeadLetter(t *testing.T, ctx context.Context, store *MemoryConsumptionStore, reservation Reservation, record ConsumptionRecord, reason string) {
	t.Helper()
	claim, err := store.BeginDeadLetter(ctx, reservation, reason)
	if err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	if claim != DeadLetterClaimGranted {
		t.Fatalf("expected the claim to be granted, got %s", claim)
	}
	finalized, err := store.FinalizeDeadLetter(ctx, reservation, record, reason)
	if err != nil {
		t.Fatalf("finalize dead letter: %v", err)
	}
	if !finalized {
		t.Fatal("expected the finalisation to be accepted for the current attempt")
	}
}

// The finding this pair of operations exists for: a delivery whose lease expired while its
// handler was still running must not be able to park an event that a newer attempt has already
// applied, and must not be able to overwrite that success in the record.
func TestMemoryConsumptionStoreRefusesADeadLetterFromASupersededAttempt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := ConsumptionRecord{EventID: "evt_superseded"}

	// The old generation reserves the event and then stalls.
	stale, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Its lease expires and a newer attempt takes the event over and applies it.
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if current.Owner == stale.Owner {
		t.Fatal("expected the takeover to mint a new owner")
	}
	if err := store.Complete(ctx, current, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The old generation now fails permanently and tries to park the event.
	claim, err := store.BeginDeadLetter(ctx, stale, "bad payload")
	if err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	if claim != DeadLetterClaimSuperseded {
		t.Fatalf("expected a stale attempt to be refused the dead-letter claim, got %s", claim)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != OutcomeSucceeded {
		t.Fatalf("expected the success to survive, got %s", entry.Outcome)
	}
}

// The same refusal while the newer attempt is still working: nothing may be written, and the
// event must not be finalised as dead-lettered behind the attempt that owns it.
func TestMemoryConsumptionStoreRefusesADeadLetterWhileANewerAttemptIsInProgress(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := ConsumptionRecord{EventID: "evt_in_progress"}

	stale, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	claim, err := store.BeginDeadLetter(ctx, stale, "bad payload")
	if err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	if claim != DeadLetterClaimSuperseded {
		t.Fatalf("expected the stale attempt to be refused, got %s", claim)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome == OutcomeDeadLettering {
		t.Fatal("a refused claim must not leave the event marked as being parked")
	}
	if entry.Owner != current.Owner {
		t.Fatal("a refused claim must not disturb the attempt that owns the event")
	}
	// The event is still retryable by the attempt that owns it.
	if err := store.Complete(ctx, current, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// Between the claim and the finalisation the parked entry is written outside the store, so the
// event can move on in that window. The finalisation must then be refused rather than
// overwriting whatever the newer attempt recorded.
func TestMemoryConsumptionStoreRefusesToFinaliseAfterTheEventMovedOn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	clock := newTestClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)
	record := ConsumptionRecord{EventID: "evt_moved_on"}

	dead, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	claim, err := store.BeginDeadLetter(ctx, dead, "bad payload")
	if err != nil || claim != DeadLetterClaimGranted {
		t.Fatalf("expected the claim to be granted, got %s and %v", claim, err)
	}

	// The write takes longer than the lease, another consumer takes the event over and applies it.
	clock.advance(2 * time.Minute)
	current, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.Complete(ctx, current, OutcomeSucceeded, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	finalized, err := store.FinalizeDeadLetter(ctx, dead, record, "bad payload")
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if finalized {
		t.Fatal("expected the finalisation to be refused after the event moved on")
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != OutcomeSucceeded {
		t.Fatalf("expected the applied outcome to survive, got %s", entry.Outcome)
	}
}

// A reservation without an owner cannot demonstrate that it is the current attempt, so it is
// refused rather than trusted.
func TestMemoryConsumptionStoreRefusesADeadLetterWithoutAnOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_no_owner"}
	if _, err := store.Begin(ctx, record); err != nil {
		t.Fatalf("begin: %v", err)
	}

	claim, err := store.BeginDeadLetter(ctx, Reservation{EventID: record.EventID}, "bad payload")
	if err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	if claim != DeadLetterClaimSuperseded {
		t.Fatalf("expected a reservation without an owner to be refused, got %s", claim)
	}
}

// An entry that cannot be decoded never had a reservation, so there is nothing to claim against
// and the write is granted - the parked entry is the only record such an event can have.
func TestMemoryConsumptionStoreGrantsAnUnkeyableDeadLetter(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	claim, err := store.BeginDeadLetter(ctx, Reservation{}, "invalid_event: missing event_id")
	if err != nil {
		t.Fatalf("begin dead letter: %v", err)
	}
	if claim != DeadLetterClaimGranted {
		t.Fatalf("expected an entry without an event id to be granted, got %s", claim)
	}
	finalized, err := store.FinalizeDeadLetter(ctx, Reservation{}, ConsumptionRecord{}, "invalid_event")
	if err != nil || !finalized {
		t.Fatalf("expected finalisation without an event id to succeed, got %v and %v", finalized, err)
	}
}

// Being parked is a claim in progress, not a terminal outcome: the write can still fail, so the
// event must remain reservable afterwards.
func TestMemoryConsumptionStoreDeadLetteringIsNotTerminal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryConsumptionStore()
	record := ConsumptionRecord{EventID: "evt_claiming"}
	reservation, err := store.Begin(ctx, record)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	claim, err := store.BeginDeadLetter(ctx, reservation, "bad payload")
	if err != nil || claim != DeadLetterClaimGranted {
		t.Fatalf("expected the claim to be granted, got %s and %v", claim, err)
	}
	entry, _ := store.Entry(record.EventID)
	if entry.Outcome != OutcomeDeadLettering {
		t.Fatalf("expected the claiming outcome, got %s", entry.Outcome)
	}
	if entry.Outcome.Terminal() {
		t.Fatal("a dead-letter claim must not be terminal: the parked entry does not exist yet")
	}
}

// newTestClock is a mutable clock the tests own, so lease behaviour is asserted without
// sleeping.
type testClock struct {
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) nowFunc() time.Time { return c.now }

func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }
