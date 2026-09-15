package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

func testEvent(t *testing.T) event.Event {
	t.Helper()
	e, err := event.New(event.OrderCreated, "order", "order_01", "trace_01", map[string]string{"amount_cent": "100"})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func addEvent(t *testing.T, stream *redisrepo.MemoryStream, name string, e event.Event) string {
	t.Helper()
	fields, err := e.Fields()
	if err != nil {
		t.Fatal(err)
	}
	id, err := stream.Add(context.Background(), name, fields)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func workerConfig() Config {
	config := DefaultConfig()
	config.Stream = "events"
	config.Group = "workers"
	config.Consumer = "worker-1"
	config.Block = 2 * time.Millisecond
	config.PendingInterval = 1 * time.Millisecond
	config.RetryAfter = 0
	config.MaxAttempts = 2
	return config
}

func runUntil(t *testing.T, stream *redisrepo.MemoryStream, handler Handler, config Config, ready func() bool) {
	t.Helper()
	worker, err := New(stream, handler, config)
	if err != nil {
		t.Fatal(err)
	}
	requireRecorder(t, worker, event.NewMemoryConsumptionStore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !ready() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestWorkerACKsSuccessfulDelivery(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(context.Background(), "events", "workers", "$"); err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int32
	addEvent(t, stream, "events", testEvent(t))
	runUntil(t, stream, HandlerFunc(func(ctx context.Context, e event.Event) error {
		handled.Add(1)
		return nil
	}), workerConfig(), func() bool { return handled.Load() == 1 })
	pending, err := stream.Pending(context.Background(), "events", "workers")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after successful processing = %#v, error=%v", pending, err)
	}
}

func TestWorkerRetriesAndDeadLettersAfterMaxAttempts(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(context.Background(), "events", "workers", "$"); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	addEvent(t, stream, "events", testEvent(t))
	runUntil(t, stream, HandlerFunc(func(ctx context.Context, e event.Event) error {
		attempts.Add(1)
		return errors.New("device unavailable")
	}), workerConfig(), func() bool {
		records, _ := stream.Records(context.Background(), event.StreamDeadLetter)
		return len(records) == 1
	})
	if attempts.Load() != 2 {
		t.Fatalf("handler attempts = %d, want 2", attempts.Load())
	}
	records, err := stream.Records(context.Background(), event.StreamDeadLetter)
	if err != nil || len(records) != 1 {
		t.Fatalf("dead-letter records = %#v, error=%v", records, err)
	}
	// The reason is the bounded label an operator groups by; the free text moved to the detail field
	// so that a distinct error message cannot become a new metric series.
	if records[0].Values["dead_letter_reason"] != string(deadLetterReasonExhausted) {
		t.Fatalf("expected the retry_exhausted reason label: %#v", records[0].Values)
	}
	if records[0].Values["dead_letter_detail"] != "device unavailable" {
		t.Fatalf("expected the error text as the detail: %#v", records[0].Values)
	}
	if records[0].Values["dead_letter_attempts"] != "2" {
		t.Fatalf("expected 2 attempts recorded: %#v", records[0].Values)
	}
	pending, err := stream.Pending(context.Background(), "events", "workers")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after dead-letter = %#v, error=%v", pending, err)
	}
}

func TestWorkerClaimsPendingFromPreviousConsumer(t *testing.T) {
	ctx := context.Background()
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(ctx, "events", "workers", "$"); err != nil {
		t.Fatal(err)
	}
	addEvent(t, stream, "events", testEvent(t))
	if _, err := stream.ReadGroup(ctx, redisrepo.ReadGroupOptions{Stream: "events", Group: "workers", Consumer: "crashed-worker"}); err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int32
	runUntil(t, stream, HandlerFunc(func(ctx context.Context, e event.Event) error {
		handled.Add(1)
		return nil
	}), workerConfig(), func() bool { return handled.Load() == 1 })
	pending, err := stream.Pending(ctx, "events", "workers")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after recovery = %#v, error=%v", pending, err)
	}
}

func TestWorkerStopsGracefully(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	worker, err := New(stream, HandlerFunc(func(context.Context, event.Event) error { return nil }), workerConfig())
	if err != nil {
		t.Fatal(err)
	}
	requireRecorder(t, worker, event.NewMemoryConsumptionStore())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
