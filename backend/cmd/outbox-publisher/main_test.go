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
