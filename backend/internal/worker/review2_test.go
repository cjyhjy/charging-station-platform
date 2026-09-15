package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests pin the second review of BE-B-04. Each one corresponds to a finding that could
// destroy an event, so they assert on the transport state (pending, acknowledged, parked)
// rather than only on the handler's return value.

// fakeDeadLetterGuard mirrors the two facts the real claim store keeps: who is writing an
// event's parked entry right now, and whether a completed write for it is published.
//
// The two are separate on purpose. A guard that only knew "claimed" could not tell a retry
// whether the write it is looking at already happened or is still in progress, and treating the
// second as the first is what loses an event.
type fakeDeadLetterGuard struct {
	mu      sync.Mutex
	writing map[string]string // event id -> the token currently writing it
	written map[string]string // event id -> the token that published the completed write

	next int
	err  error
	// releaseErr forces the release path to fail, so a caller can assert it is handled.
	releaseErr error
	// markErr forces the publish step to fail, which must not fail the park itself.
	markErr  error
	releases int
	marks    int
}

func newFakeDeadLetterGuard() *fakeDeadLetterGuard {
	return &fakeDeadLetterGuard{writing: map[string]string{}, written: map[string]string{}}
}

func (g *fakeDeadLetterGuard) ClaimDeadLetter(_ context.Context, eventID string) (string, DeadLetterClaimState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return "", "", g.err
	}
	if eventID == "" {
		// An undecodable entry has no event id to key a claim on.
		return "", DeadLetterClaimTaken, nil
	}
	if _, ok := g.written[eventID]; ok {
		return "", DeadLetterClaimWritten, nil
	}
	if _, ok := g.writing[eventID]; ok {
		return "", DeadLetterClaimWriting, nil
	}
	g.next++
	token := fmt.Sprintf("dlq-owner-%d", g.next)
	g.writing[eventID] = token
	return token, DeadLetterClaimTaken, nil
}

func (g *fakeDeadLetterGuard) MarkDeadLetterWritten(_ context.Context, eventID string, token string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.marks++
	if g.markErr != nil {
		return g.markErr
	}
	if eventID == "" {
		return nil
	}
	// The first completed write is the one that is kept, and the claim is dropped only when the
	// caller still owns it - the same discipline the Redis store applies with SetNX and a
	// compare-and-delete.
	if _, ok := g.written[eventID]; !ok {
		g.written[eventID] = token
	}
	if g.writing[eventID] == token {
		delete(g.writing, eventID)
	}
	return nil
}

func (g *fakeDeadLetterGuard) ReleaseDeadLetter(_ context.Context, eventID string, token string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.releases++
	if g.releaseErr != nil {
		return g.releaseErr
	}
	// Only the owning token may release, which is the same invariant the Redis guard enforces.
	if g.writing[eventID] != token {
		return nil
	}
	delete(g.writing, eventID)
	return nil
}

// isWriting reports whether a write claim is currently held.
func (g *fakeDeadLetterGuard) isWriting(eventID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.writing[eventID]
	return ok
}

// isWritten reports whether a completed write is on record.
func (g *fakeDeadLetterGuard) isWritten(eventID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.written[eventID]
	return ok
}

// holdWrite simulates another consumer that took the claim and has not finished writing.
func (g *fakeDeadLetterGuard) holdWrite(eventID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writing[eventID] = "another-consumer"
}

// ackFailingStream fails the first N acknowledgements, which is how a crash between parking a
// dead letter and acknowledging it is modelled.
type ackFailingStream struct {
	*redisrepo.MemoryStream

	mu           sync.Mutex
	failuresLeft int
	ackCalls     int
}

func (s *ackFailingStream) Ack(ctx context.Context, stream, group string, ids ...string) (int, error) {
	s.mu.Lock()
	s.ackCalls++
	fail := s.failuresLeft > 0
	if fail {
		s.failuresLeft--
	}
	s.mu.Unlock()
	if fail {
		return 0, errors.New("simulated ack failure")
	}
	return s.MemoryStream.Ack(ctx, stream, group, ids...)
}

func (s *ackFailingStream) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackCalls
}

// fixtureWithStream builds a worker over an explicitly supplied stream client, with the
// underlying memory stream supplied separately so assertions can read transport state.
func fixtureWithStream(t *testing.T, client redisrepo.StreamClient, memory *redisrepo.MemoryStream, handler Handler, mutate func(*Config)) *workerFixture {
	t.Helper()
	config := Config{
		Stream:          fixtureStream,
		Group:           fixtureGroup,
		Consumer:        fixtureConsume,
		Count:           10,
		Block:           time.Millisecond,
		PendingInterval: time.Millisecond,
		RetryAfter:      0,
		MaxAttempts:     3,
		StartID:         "0-0",
	}
	if mutate != nil {
		mutate(&config)
	}
	worker, err := New(client, handler, config)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if memory == nil {
		t.Fatal("fixtureWithStream requires the underlying memory stream")
	}
	return &workerFixture{worker: worker, stream: memory, config: config}
}

// localClock is a mutable clock the worker tests own, so lease behaviour is asserted without
// sleeping.
type localClock struct {
	now time.Time
}

func newLocalClock() *localClock {
	return &localClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
}

func (c *localClock) nowFunc() time.Time      { return c.now }
func (c *localClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// Finding 3: an event held by a live reservation lease must not be acknowledged. The previous
// revision reported such a delivery as a duplicate and acknowledged it, so an event whose
// holder had died between reserving and applying it was lost with nothing having applied it.
func TestWorkerDoesNotAcknowledgeAnEventHeldByALiveLease(t *testing.T) {
	store := event.NewMemoryConsumptionStore()

	// The worker's handler must share the store the lease was created in, so the reservation
	// it sees is the one a crashed process left behind.
	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new charge handler: %v", err)
	}
	fixture := newWorkerFixture(t, handler, nil)

	e := newChargeStartedEvent(t, "evt_held")
	if _, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID, Attempt: 1}); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}

	fixture.publish(t, e)
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err != nil {
		t.Fatalf("process: %v", err)
	}

	if applier.calls != 0 {
		t.Fatalf("the handler must not run for a held event, ran %d times", applier.calls)
	}
	// The decisive assertion: the entry is still pending, so the event is not lost.
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending, got %d pending", len(pending))
	}
	if len(fixture.deadLetters(t)) != 0 {
		t.Fatal("a held event must not be dead-lettered")
	}
}

// The pipeline itself must report "held" as a distinct outcome from "duplicate", because the
// worker's reaction is the opposite one.
func TestPipelineReportsAHeldEventDistinctlyFromADuplicate(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_held_pipeline")

	// Reserve the event and leave the lease live, as a crashed holder would.
	if _, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	applied := 0
	runErr := pipe.run(context.Background(), e, DeliveryInfo{Attempt: 2}, func(context.Context, int) error {
		applied++
		return nil
	})
	if !errors.Is(runErr, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", runErr)
	}
	if errors.Is(runErr, ErrDuplicate) {
		t.Fatal("a held event must not be reported as a duplicate")
	}
	if applied != 0 {
		t.Fatalf("the domain must not see a held event, applied %d times", applied)
	}
}

// Once the lease expires the event becomes recoverable, so the worker must then apply it.
func TestWorkerRecoversAnEventWhoseLeaseExpired(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	clock := newLocalClock()
	store.SetClock(clock.nowFunc)
	store.SetLeaseTTL(time.Minute)

	applier := &stubApplier{}
	handler, err := NewChargeHandler(applier, store, nil, ChargeHandlerConfig{Consumer: "c1", Scope: "charge-event"})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	e := newChargeStartedEvent(t, "evt_expired_lease")
	if _, err := store.Begin(context.Background(), event.ConsumptionRecord{EventID: e.EventID}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fixture := newWorkerFixture(t, handler, nil)
	fixture.publish(t, e)
	delivery := fixture.delivery(t)

	// Still inside the lease: the entry must stay pending.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process: %v", err)
	}
	if pending := fixture.pending(t); len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending inside the lease, got %d", len(pending))
	}
	if applier.calls != 0 {
		t.Fatalf("expected no application inside the lease, got %d", applier.calls)
	}

	// Past the lease: the worker takes the reservation over and applies the event.
	clock.advance(2 * time.Minute)
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("process after expiry: %v", err)
	}
	if applier.calls != 1 {
		t.Fatalf("expected the event to be applied after the lease expired, got %d calls", applier.calls)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged, got %d pending", len(pending))
	}
}

// Finding 4: a worker that parked an entry and then failed to acknowledge it must not park a
// second copy on the retry. The previous order was DLQ -> ACK -> REC, so an ACK failure meant
// the next attempt wrote the dead letter again.
func TestDeadLetterIsIdempotentAcrossAnAckFailure(t *testing.T) {
	// Fail the first acknowledgement only.
	stream := &ackFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failuresLeft: 1}
	t.Cleanup(func() { _ = stream.Close() })

	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("unknown charger"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	guard := newFakeDeadLetterGuard()
	fixture.worker.SetDeadLetterGuard(guard)

	fixture.publish(t, newChargeStartedEvent(t, "evt_dlq_idem"))
	delivery := fixture.delivery(t)

	// First attempt: the dead letter is parked and recorded, then the acknowledgement fails.
	if err := fixture.worker.process(context.Background(), delivery); err == nil {
		t.Fatal("expected the acknowledgement failure to surface")
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected one dead letter after the first attempt, got %d", got)
	}

	// The retry must not park a second copy.
	if err := fixture.worker.process(context.Background(), delivery); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := len(fixture.deadLetters(t)); got != 1 {
		t.Fatalf("expected the retry to reuse the existing dead letter, got %d", got)
	}
	if pending := fixture.pending(t); len(pending) != 0 {
		t.Fatalf("expected the entry to be acknowledged on the retry, got %d pending", len(pending))
	}
	if stream.calls() < 2 {
		t.Fatalf("expected two acknowledgement attempts, got %d", stream.calls())
	}
}

// The record must be durable before the acknowledgement, because acknowledging is the only
// irreversible step: once the source entry is gone nothing can rediscover it.
func TestDeadLetterRecordsBeforeItAcknowledges(t *testing.T) {
	stream := &ackFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failuresLeft: 1}
	t.Cleanup(func() { _ = stream.Close() })

	store := event.NewMemoryConsumptionStore()
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("malformed payload"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)

	fixture.publish(t, newChargeStartedEvent(t, "evt_rec_before_ack"))
	// The acknowledgement fails, so process returns an error, but the record must already
	// exist.
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err == nil {
		t.Fatal("expected the acknowledgement failure to surface")
	}

	entry, ok := store.Entry("evt_rec_before_ack")
	if !ok {
		t.Fatal("expected the consumption record to exist even though the acknowledgement failed")
	}
	if entry.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("expected %s, got %s", event.OutcomeDeadLettered, entry.Outcome)
	}
	if entry.Stream != fixture.config.Stream {
		t.Fatalf("expected the stream coordinates recorded, got %+v", entry.ConsumptionRecord)
	}
}

// A parking failure must not record a terminal outcome that did not happen.
func TestDeadLetterDoesNotRecordWhenTheParkFails(t *testing.T) {
	stream := &addFailingStream{MemoryStream: redisrepo.NewMemoryStream(), failing: true}
	t.Cleanup(func() { _ = stream.Close() })

	store := event.NewMemoryConsumptionStore()
	fixture := fixtureWithStream(t, stream, stream.MemoryStream, HandlerFunc(func(context.Context, event.Event) error {
		return Permanent(errors.New("bad payload"))
	}), func(config *Config) { config.MaxAttempts = 5 })

	recorder, err := NewDeadLetterRecorder(store)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	fixture.worker.SetDeadLetterRecorder(recorder)

	fixture.publish(t, newChargeStartedEvent(t, "evt_park_failed"))
	if err := fixture.worker.process(context.Background(), fixture.delivery(t)); err == nil {
		t.Fatal("expected the parking failure to surface")
	}
	if _, ok := store.Entry("evt_park_failed"); ok {
		t.Fatal("a failed park must not record a dead-letter outcome")
	}
}

// addFailingStream fails writes to the dead-letter stream while setFailing is true, so a test can
// model a parking failure and then let the retry succeed.
type addFailingStream struct {
	*redisrepo.MemoryStream

	mu      sync.Mutex
	failing bool
}

func (s *addFailingStream) Add(ctx context.Context, stream string, values map[string]string) (string, error) {
	if stream == event.StreamDeadLetter {
		s.mu.Lock()
		failing := s.failing
		s.mu.Unlock()
		if failing {
			return "", errors.New("simulated dead-letter write failure")
		}
	}
	return s.MemoryStream.Add(ctx, stream, values)
}

// setFailing controls whether dead-letter writes fail.
func (s *addFailingStream) setFailing(failing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = failing
}

// The pipeline must never report success for work it did not do, which is what finding 1 was
// about at the process level. This is the pipeline-level statement of the same rule: only a nil
// error from apply may lead to completion.
func TestPipelineDoesNotCompleteWhenApplyFails(t *testing.T) {
	store := event.NewMemoryConsumptionStore()
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_apply_failed")

	applyErr := errors.New("domain write failed")
	if err := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error {
		return applyErr
	}); !errors.Is(err, applyErr) {
		t.Fatalf("expected the apply error, got %v", err)
	}

	entry, ok := store.Entry(e.EventID)
	if !ok {
		t.Fatal("expected a record")
	}
	if entry.Outcome == event.OutcomeSucceeded {
		t.Fatal("a failed apply must never be recorded as succeeded")
	}
	if entry.Outcome != event.OutcomeFailed {
		t.Fatalf("expected the attempt recorded as failed, got %q", entry.Outcome)
	}
}

// A permissive store that reports success for everything must not let the worker acknowledge:
// the check is that the pipeline reads the reservation state rather than assuming one.
func TestPipelineRejectsAnUnknownReservationState(t *testing.T) {
	store := &unknownStateStore{}
	pipe := newPipelineForTest(t, store, nil)
	e := newChargeStartedEvent(t, "evt_unknown_state")

	applied := 0
	err := pipe.run(context.Background(), e, DeliveryInfo{}, func(context.Context, int) error {
		applied++
		return nil
	})
	if err == nil {
		t.Fatal("expected an unknown reservation state to be refused")
	}
	if applied != 0 {
		t.Fatalf("the domain must not see an event whose reservation state is unknown, applied %d", applied)
	}
}

// unknownStateStore reports a state the pipeline does not understand.
type unknownStateStore struct{}

func (unknownStateStore) Begin(context.Context, event.ConsumptionRecord) (event.Reservation, error) {
	return event.Reservation{State: event.ReservationState("nonsense")}, nil
}

func (unknownStateStore) Complete(context.Context, event.Reservation, event.ConsumptionOutcome, string) error {
	return nil
}

func (unknownStateStore) Fail(context.Context, event.Reservation, string) error { return nil }

func (unknownStateStore) Release(context.Context, event.Reservation, string) error { return nil }

func (unknownStateStore) BeginDeadLetter(context.Context, event.Reservation, string) (event.DeadLetterClaim, error) {
	return event.DeadLetterClaimGranted, nil
}

func (unknownStateStore) FinalizeDeadLetter(context.Context, event.Reservation, event.ConsumptionRecord, string) (bool, error) {
	return true, nil
}

func (unknownStateStore) AbortDeadLetter(context.Context, event.Reservation, string) (bool, error) {
	return true, nil
}
