package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
)

// These tests cover the parts of the publishing process that are not PostgreSQL: the loop's
// willingness to publish only while it holds the lock, the standby behaviour, and the retry
// after a failed pass. The advisory lock itself is tested against the real database in
// repository/postgres.

// stubLocker hands out the lock according to a script, so a test can act as the active or the
// standby publisher - and can make a held lock stop being held.
type stubLocker struct {
	mu        sync.Mutex
	held      bool
	alive     bool
	acquires  int
	releases  int
	lastError error
}

func newStubLocker(held bool) *stubLocker {
	return &stubLocker{held: held, alive: held}
}

func (l *stubLocker) Acquire(context.Context) (publisherLock, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquires++
	if l.lastError != nil {
		return nil, l.lastError
	}
	if !l.held {
		return nil, nil
	}
	l.alive = true
	return stubLock{locker: l}, nil
}

func (l *stubLocker) setHeld(held bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = held
	l.alive = held
}

// kill drops the lock without telling the holder, which is what a dropped connection does.
func (l *stubLocker) kill() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.alive = false
}

func (l *stubLocker) counts() (acquires, releases int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquires, l.releases
}

// stubLock is one held lock.
type stubLock struct {
	locker *stubLocker
}

func (l stubLock) Alive(context.Context) bool {
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	return l.locker.alive
}

func (l stubLock) Release(context.Context) error {
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	l.locker.releases++
	l.locker.alive = false
	// Releasing gives the lock back: another process may now take it.
	l.locker.held = false
	return nil
}

// scriptedSource serves a fixed number of records and records how often it was asked.
type scriptedSource struct {
	mu       sync.Mutex
	records  []event.OutboxRecord
	lists    int
	marked   map[string]time.Time
	listErr  error
	markErr  error
	wantMore chan struct{}
}

func (s *scriptedSource) ListUnpublished(_ context.Context, limit int) ([]event.OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]event.OutboxRecord, 0, limit)
	for _, record := range s.records {
		if _, done := s.marked[record.ID]; done {
			continue
		}
		out = append(out, record)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *scriptedSource) MarkPublished(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil {
		return s.markErr
	}
	s.marked[id] = at
	return nil
}

func (s *scriptedSource) listed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func (s *scriptedSource) published() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.marked)
}

// countingWriter records appended events and can fail for a while.
type countingWriter struct {
	mu       sync.Mutex
	count    int
	failures int
}

func (w *countingWriter) Add(context.Context, string, map[string]string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failures > 0 {
		w.failures--
		return "", errors.New("stream unavailable")
	}
	w.count++
	return fmt.Sprintf("%d-0", w.count), nil
}

func (w *countingWriter) added() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

func newScriptedDeps(source *scriptedSource, writer *countingWriter, locker *stubLocker) loopDeps {
	return loopDeps{
		source:   source,
		writer:   writer,
		locker:   locker,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval: time.Millisecond,
		batch:    10,
	}
}

func outboxRecord(id string) event.OutboxRecord {
	return event.OutboxRecord{
		ID:            id,
		EventID:       "evt_" + id,
		EventType:     event.ChargeStarted,
		AggregateType: "order",
		AggregateID:   "order_01",
		OccurredAt:    time.Now().UTC(),
		Payload:       []byte(`{"charger_id":"ch_01"}`),
	}
}

// The loop publishes only while it holds the lock.
func TestLoopPublishesWhileHoldingTheLock(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1"), outboxRecord("2")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, newScriptedDeps(source, writer, locker)) }()

	deadline := time.Now().Add(3 * time.Second)
	for source.published() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runLoop() error = %v", err)
	}

	if writer.added() != 2 {
		t.Fatalf("expected both records to reach the stream, got %d", writer.added())
	}
	if source.published() != 2 {
		t.Fatalf("expected both records to be marked, got %d", source.published())
	}
	acquires, releases := locker.counts()
	if acquires != 1 || releases != 1 {
		t.Fatalf("expected the lock to be taken once and released once, got %d/%d", acquires, releases)
	}
}

// A standby must not publish, and it must take over once the lock becomes available.
func TestLoopStandsByWithoutTheLockAndTakesOver(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, newScriptedDeps(source, writer, locker)) }()

	// While another publisher holds the lock nothing may be published.
	time.Sleep(20 * time.Millisecond)
	if writer.added() != 0 || source.published() != 0 {
		t.Fatalf("a standby publisher must not publish, got %d appends and %d marks", writer.added(), source.published())
	}
	if acquires, _ := locker.counts(); acquires < 2 {
		t.Fatalf("expected the standby to keep asking for the lock, got %d attempts", acquires)
	}

	// The active publisher stops; the standby takes over.
	locker.setHeld(true)
	deadline := time.Now().Add(3 * time.Second)
	for source.published() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runLoop() error = %v", err)
	}
	if source.published() != 1 {
		t.Fatal("expected the standby to publish once it acquired the lock")
	}
}

// A failed pass is not fatal: the row stays unpublished and the next pass retries it.
func TestLoopRetriesAfterAFailedPass(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1")}, marked: map[string]time.Time{}}
	writer := &countingWriter{failures: 1}
	locker := newStubLocker(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, newScriptedDeps(source, writer, locker)) }()

	deadline := time.Now().Add(3 * time.Second)
	for source.published() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("a failed pass must not stop the loop: %v", err)
	}
	if source.published() != 1 {
		t.Fatalf("expected the retry to mark the row, got %d", source.published())
	}
	if source.listed() < 2 {
		t.Fatalf("expected at least two passes, got %d", source.listed())
	}
}

// A locking failure is fatal: publishing without the lock is exactly what the lock exists to
// prevent, so the process must stop rather than guess.
func TestLoopStopsWhenTheLockCannotBeEvaluated(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(true)
	locker.lastError = errors.New("database unreachable")

	err := runLoop(context.Background(), newScriptedDeps(source, writer, locker))
	if err == nil {
		t.Fatal("expected an unevaluable lock to stop the loop")
	}
	if writer.added() != 0 {
		t.Fatal("nothing may be published when the lock state is unknown")
	}
}

func TestRunLoopValidatesItsDependencies(t *testing.T) {
	if err := runLoop(context.Background(), loopDeps{}); err == nil {
		t.Fatal("expected missing dependencies to be rejected")
	}
	if err := runLoop(context.Background(), loopDeps{source: &scriptedSource{}, writer: &countingWriter{}}); err == nil {
		t.Fatal("expected a missing lock to be rejected")
	}
}

// The review finding: a lock that was acquired once was treated as still held forever. When the
// session behind it ends, PostgreSQL releases the lock and another publisher may take it, so this
// process must stop publishing instead of continuing on a lock it no longer owns.
func TestLoopStopsPublishingWhenTheLockIsLost(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1"), outboxRecord("2")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, newScriptedDeps(source, writer, locker)) }()

	// Let it publish the first record, then drop the lock behind its back.
	deadline := time.Now().Add(3 * time.Second)
	for source.published() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	locker.kill()
	publishedAtLoss := source.published()

	// The loop must notice and give the lock back rather than keep publishing.
	for {
		_, releases := locker.counts()
		if releases >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the loop kept publishing on a lock it no longer held")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if after := source.published(); after != publishedAtLoss {
		t.Fatalf("publication continued after the lock was lost: %d -> %d", publishedAtLoss, after)
	}

	// Once the lock is available again it is re-acquired and publishing resumes.
	locker.setHeld(true)
	for source.published() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runLoop() error = %v", err)
	}
	if source.published() < 2 {
		t.Fatal("expected the loop to resume publishing after re-acquiring the lock")
	}
	acquires, _ := locker.counts()
	if acquires < 2 {
		t.Fatalf("expected the lock to be re-acquired, acquisitions = %d", acquires)
	}
}

func TestDurationAndIntParsing(t *testing.T) {
	if got := durationOr("", time.Second); got != time.Second {
		t.Fatalf("expected the fallback, got %s", got)
	}
	if got := durationOr("2s", time.Second); got != 2*time.Second {
		t.Fatalf("expected 2s, got %s", got)
	}
	if got := durationOr("nonsense", time.Second); got != time.Second {
		t.Fatalf("expected the fallback for an invalid duration, got %s", got)
	}
	if got := durationOr("-5s", time.Second); got != time.Second {
		t.Fatalf("expected the fallback for a non-positive duration, got %s", got)
	}
	if got := intOr("", 7); got != 7 {
		t.Fatalf("expected the fallback, got %d", got)
	}
	if got := intOr("25", 7); got != 25 {
		t.Fatalf("expected 25, got %d", got)
	}
	if got := intOr("0", 7); got != 7 {
		t.Fatalf("expected the fallback for a zero batch, got %d", got)
	}
}

// recordingObserver captures what the loop reports, so the process metrics can be asserted without
// a registry or an HTTP endpoint.
//
// It is called from the loop goroutine and read from the test goroutine, so every field is behind a
// mutex: a metrics observer that is not concurrency safe would be a race in the process it observes.
type recordingObserver struct {
	mu         sync.Mutex
	acquired   int
	lost       int
	standby    int
	passFailed int
	published  int
}

func (o *recordingObserver) LockAcquired() { o.mu.Lock(); o.acquired++; o.mu.Unlock() }
func (o *recordingObserver) LockLost()     { o.mu.Lock(); o.lost++; o.mu.Unlock() }
func (o *recordingObserver) Standby()      { o.mu.Lock(); o.standby++; o.mu.Unlock() }
func (o *recordingObserver) PassFailed()   { o.mu.Lock(); o.passFailed++; o.mu.Unlock() }
func (o *recordingObserver) Published(count int) {
	o.mu.Lock()
	o.published += count
	o.mu.Unlock()
}
func (o *recordingObserver) LocksHeld(bool) {}

// counts returns a consistent snapshot of everything the observer recorded.
func (o *recordingObserver) counts() (acquired, lost, standby, passFailed, published int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.acquired, o.lost, o.standby, o.passFailed, o.published
}

// The publisher's state is only visible from the publisher: holding the lock, standing by, losing it,
// and when it last managed to publish. These are the facts the ruling asks the endpoint to expose, so
// the loop's reporting is pinned here.
func TestLoopReportsLockStateAndPublishes(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1"), outboxRecord("2")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &recordingObserver{}
	deps := newScriptedDeps(source, writer, locker)
	deps.observer = observer
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, deps) }()

	deadline := time.Now().Add(3 * time.Second)
	for source.published() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if acquired, _, _, _, published := observer.counts(); acquired == 0 || published == 0 {
		t.Fatalf("lock acquisition and publishing must both be reported: acquired=%d published=%d", acquired, published)
	}

	// Losing the lock is an event, not a routine counter: it is the window in which another
	// publisher may have become active.
	locker.kill()
	for {
		if _, lost, _, _, _ := observer.counts(); lost > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("losing the lock was not reported")
		}
		time.Sleep(time.Millisecond)
	}
	if _, lost, _, _, _ := observer.counts(); lost != 1 {
		t.Fatalf("lock losses reported = %d, want 1", lost)
	}
	cancel()
	<-done
}

// A process that never gets the lock must report standing by rather than looking idle-but-healthy.
func TestLoopReportsStandbyWithoutTheLock(t *testing.T) {
	source := &scriptedSource{records: []event.OutboxRecord{outboxRecord("1")}, marked: map[string]time.Time{}}
	writer := &countingWriter{}
	locker := newStubLocker(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &recordingObserver{}
	deps := newScriptedDeps(source, writer, locker)
	deps.observer = observer
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, deps) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, _, standby, _, _ := observer.counts(); standby > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("standing by was not reported while another publisher holds the lock")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, _, _, published := observer.counts(); published != 0 {
		t.Fatal("a standby publisher must not report published rows")
	}
	cancel()
	<-done
}

// The registry observer is what the endpoint actually serves, so it is exercised directly: counters
// rise, the standby gauge follows the lock, and the last-publish timestamp moves only on success.
func TestRegistryObserverWritesProcessMetrics(t *testing.T) {
	registry := observability.NewRegistry()
	clock := observability.NewSuccessClock(registry, observability.MetricPublisherLastPublish)
	pinned := time.Date(2026, 9, 15, 8, 30, 0, 0, time.UTC)
	clock.SetNow(func() time.Time { return pinned })
	observer := &registryObserver{registry: registry, clock: clock}

	observer.Standby()
	if value, _ := registry.Gauge(observability.MetricPublisherStandby, nil); value != 1 {
		t.Fatalf("standby gauge = %v, want 1", value)
	}
	observer.LockAcquired()
	if value, _ := registry.Gauge(observability.MetricPublisherStandby, nil); value != 0 {
		t.Fatalf("standby gauge = %v, want 0 after acquiring the lock", value)
	}
	observer.Published(3)
	if count, _ := registry.Counter(observability.MetricPublisherPublishedTotal, nil); count != 3 {
		t.Fatalf("published counter = %v, want 3", count)
	}
	if value, present := registry.Gauge(observability.MetricPublisherLastPublish, nil); !present || value != float64(pinned.Unix()) {
		t.Fatalf("last publish = %v (present %v), want %d", value, present, pinned.Unix())
	}
	observer.PassFailed()
	if count, _ := registry.Counter(observability.MetricPublisherPassFailuresTotal, nil); count != 1 {
		t.Fatalf("pass failure counter = %v, want 1", count)
	}
	observer.LockLost()
	observer.LockLost()
	if count, _ := registry.Counter(observability.MetricPublisherLockLossesTotal, nil); count != 2 {
		t.Fatalf("lock loss counter = %v, want 2", count)
	}
}
