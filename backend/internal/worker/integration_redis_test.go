package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// These tests exercise the worker against a real Redis server, which is the only way to
// confirm the consumer-group creation semantics. They are skipped unless
// NCS_REDIS_TEST_ADDR is set:
//
//	NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test ./internal/worker/ -run IntegrationRedis -v

const (
	envRedisTestAddress = "NCS_REDIS_TEST_ADDR"
	envRedisTestDB      = "NCS_REDIS_TEST_DB"
)

// requireRedisStreams returns a live Streams client, or skips the test.
func requireRedisStreams(t *testing.T) (*redisrepo.StreamsClient, *redisrepo.Client) {
	t.Helper()

	address := os.Getenv(envRedisTestAddress)
	if address == "" {
		t.Skipf("set %s to run the Redis integration tests", envRedisTestAddress)
	}

	config := redisrepo.DefaultConnConfig()
	config.Address = address
	config.Database = 15
	if raw := os.Getenv(envRedisTestDB); raw != "" {
		var database int
		if _, err := fmt.Sscanf(raw, "%d", &database); err != nil {
			t.Fatalf("invalid %s=%q: %v", envRedisTestDB, raw, err)
		}
		config.Database = database
	}
	config.DialTimeout = 3 * time.Second
	config.ReadTimeout = 3 * time.Second
	config.WriteTimeout = 3 * time.Second

	client, err := redisrepo.NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		t.Skipf("redis at %s is not reachable: %v", address, err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// The Client satisfies the Streams command surface through StreamsClient.
	return redisrepo.NewStreamsClientOver(client), client
}

// streamName returns a stream unique to one test run, so a leftover from a failed run cannot
// make a later run pass or fail by accident.
func streamName(t *testing.T) string {
	name := t.Name()
	clean := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			clean = append(clean, r)
		default:
			clean = append(clean, '-')
		}
	}
	return fmt.Sprintf("ncs:test:worker:%s:%d", string(clean), time.Now().UnixNano())
}

// Finding 2, as a real Redis test: events published BEFORE the consumer group exists must still
// be consumed.
//
// The outbox publishes as soon as the API commits a transaction, so a worker deployed after the
// first publish sees a stream that already has entries. Creating the group at "$" would place
// it at the head and skip all of them permanently; the default start position is therefore the
// beginning of the stream.
func TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation(t *testing.T) {
	streams, client := requireRedisStreams(t)
	ctx := context.Background()

	stream := streamName(t)
	group := "backlog-workers"
	t.Cleanup(func() { _, _ = client.Del(context.Background(), stream) })

	// Publish first, exactly as the outbox would before any worker existed.
	const published = 5
	for i := 0; i < published; i++ {
		e, err := event.New(event.ChargeStarted, "order", fmt.Sprintf("order_%02d", i), fmt.Sprintf("trace_%02d", i), map[string]string{"charger_id": "ch_01"})
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		fields, err := e.Fields()
		if err != nil {
			t.Fatalf("fields: %v", err)
		}
		if _, err := streams.Add(ctx, stream, fields); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Confirm the group does not exist yet, so the worker is genuinely the one creating it.
	// Pending reports ErrGroupMissing for an absent group, which is the probe available on the
	// Streams boundary itself.
	if _, err := streams.Pending(ctx, stream, group); err == nil {
		t.Fatal("expected no consumer group before the worker starts")
	}

	// Now start the worker, which creates the group.
	seen := make(chan string, published*2)
	handler := HandlerFunc(func(_ context.Context, e event.Event) error {
		seen <- e.EventID
		return nil
	})

	config := Config{
		Stream:          stream,
		Group:           group,
		Consumer:        "worker-backlog",
		Count:           10,
		Block:           100 * time.Millisecond,
		PendingInterval: 100 * time.Millisecond,
		RetryAfter:      0,
		MaxAttempts:     3,
	}
	worker, err := New(streams, handler, config)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	requireRecorder(t, worker, event.NewMemoryConsumptionStore())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()

	consumed := map[string]bool{}
	deadline := time.After(15 * time.Second)
	for len(consumed) < published {
		select {
		case id := <-seen:
			consumed[id] = true
		case <-deadline:
			t.Fatalf("expected all %d pre-existing events to be consumed, got %d: %v", published, len(consumed), consumed)
		}
	}

	// Wait for the acknowledgements to land before cancelling. A handler returning is not the same
	// as its acknowledgement completing, so cancelling here races the last ack and leaves the entry
	// pending - a failure that shows up as "expected no pending entries, got 1" under load rather
	// than as a defect in the worker.
	ackDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(ackDeadline) {
		pending, err := streams.Pending(ctx, stream, group)
		if err == nil && len(pending) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	// Nothing may be left pending, and the backlog must be drained.
	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending entries, got %d", len(pending))
	}
	// Nothing new may be waiting either: the backlog was consumed, not merely delivered.
	remaining, err := streams.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: stream, Group: group, Consumer: "probe", Count: 10, Block: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("probe read: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected no undelivered entries, got %d", len(remaining))
	}
}

// The opposite configuration must actually skip pre-existing entries, so the documented
// tradeoff is real and not just a comment.
func TestIntegrationRedisStartIDAtHeadSkipsTheBacklog(t *testing.T) {
	streams, client := requireRedisStreams(t)
	ctx := context.Background()

	stream := streamName(t)
	group := "head-workers"
	t.Cleanup(func() { _, _ = client.Del(context.Background(), stream) })

	for i := 0; i < 3; i++ {
		e, err := event.New(event.ChargeStarted, "order", fmt.Sprintf("order_%02d", i), "trace", nil)
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		fields, _ := e.Fields()
		if _, err := streams.Add(ctx, stream, fields); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	seen := make(chan string, 8)
	handler := HandlerFunc(func(_ context.Context, e event.Event) error {
		seen <- e.EventID
		return nil
	})
	worker, err := New(streams, handler, Config{
		Stream: stream, Group: group, Consumer: "worker-head",
		Count: 10, Block: 100 * time.Millisecond, PendingInterval: 100 * time.Millisecond,
		MaxAttempts: 3,
		StartID:     "$",
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	requireRecorder(t, worker, event.NewMemoryConsumptionStore())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()

	// Give the worker time to create the group and read; nothing must arrive.
	select {
	case id := <-seen:
		cancel()
		<-done
		t.Fatalf("expected the pre-existing backlog to be skipped, consumed %s", id)
	case <-time.After(700 * time.Millisecond):
	}
	cancel()
	<-done

	// The entries are still in the stream, which is the whole point: they were skipped, not
	// consumed. A second group created at the beginning of the same stream still sees all three
	// of them, which is what proves they were never delivered to the first group.
	freshGroup := group + "-from-start"
	if err := streams.EnsureGroup(ctx, stream, freshGroup, "0-0"); err != nil {
		t.Fatalf("ensure fresh group: %v", err)
	}
	remaining, err := streams.ReadGroup(ctx, redisrepo.ReadGroupOptions{
		Stream: stream, Group: freshGroup, Consumer: "probe", Count: 10, Block: time.Second,
	})
	if err != nil {
		t.Fatalf("read fresh group: %v", err)
	}
	if len(remaining) != 3 {
		t.Fatalf("expected the 3 skipped entries to still be in the stream, got %d", len(remaining))
	}
}

// An event published after the group exists must be consumed on the normal path, so the
// backlog fix did not break steady-state consumption.
func TestIntegrationRedisConsumesNewEventsAfterGroupCreation(t *testing.T) {
	streams, client := requireRedisStreams(t)
	ctx := context.Background()

	stream := streamName(t)
	group := "steady-workers"
	t.Cleanup(func() { _, _ = client.Del(context.Background(), stream) })

	seen := make(chan string, 8)
	handler := HandlerFunc(func(_ context.Context, e event.Event) error {
		seen <- e.EventID
		return nil
	})
	worker, err := New(streams, handler, Config{
		Stream: stream, Group: group, Consumer: "worker-steady",
		Count: 10, Block: 100 * time.Millisecond, PendingInterval: 100 * time.Millisecond,
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	requireRecorder(t, worker, event.NewMemoryConsumptionStore())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()

	// Wait for the group to exist, then publish. Pending reports ErrGroupMissing until the
	// worker has created it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := streams.Pending(ctx, stream, group); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the worker never created its consumer group")
		}
		time.Sleep(10 * time.Millisecond)
	}

	e, err := event.New(event.ChargeStopped, "order", "order_new", "trace_new", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	fields, _ := e.Fields()
	if _, err := streams.Add(ctx, stream, fields); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case id := <-seen:
		if id != e.EventID {
			t.Fatalf("expected %s, got %s", e.EventID, id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the new event was not consumed")
	}
	cancel()
	<-done
}
