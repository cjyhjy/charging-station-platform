package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// requireRedisStreams returns a live Streams client, or skips the test. These
// tests are the only evidence that the Streams command encoding and reply decoding
// work against a real server.
func requireRedisStreams(t *testing.T) *StreamsClient {
	t.Helper()
	client := requireRedis(t)
	streams := NewStreamsClientOver(client)
	// The shared client is closed by requireRedis's cleanup.
	return streams
}

// streamFixture returns a stream and group unique to one test run, plus a cleanup
// that removes the stream. Leaving keys behind would make a later -count run
// observe another run's backlog.
func streamFixture(t *testing.T, streams *StreamsClient) (stream, group string) {
	t.Helper()
	suffix := sanitizeTestName(t.Name())
	stream = fmt.Sprintf("ncs:test:stream:%s", suffix)
	group = fmt.Sprintf("group-%s", suffix)

	ctx := context.Background()
	// A previous failed run may have left the stream behind.
	_, _ = streams.client.Del(ctx, stream)
	if err := streams.EnsureGroup(ctx, stream, group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = streams.client.Del(cleanupCtx, stream)
	})
	return stream, group
}

func TestIntegrationStreamsAddAndReadGroup(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	values := map[string]string{"event_id": "evt_01", "event_type": "CHARGE_STARTED", "aggregate_id": "order_01"}
	id, err := streams.Add(ctx, stream, values)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if id == "" {
		t.Fatal("expected a generated stream id")
	}

	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "c1", Count: 10, Block: time.Second})
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	got := deliveries[0]
	if got.ID != id {
		t.Fatalf("expected id %s, got %s", id, got.ID)
	}
	if got.Stream != stream {
		t.Fatalf("expected stream %s, got %s", stream, got.Stream)
	}
	if got.Consumer != "c1" {
		t.Fatalf("expected consumer c1, got %s", got.Consumer)
	}
	for field, want := range values {
		if got.Values[field] != want {
			t.Errorf("expected %s=%q, got %q", field, want, got.Values[field])
		}
	}
}

// A blocking read that finds nothing must report an empty batch rather than an error, because an
// idle stream is the normal case.
//
// There is deliberately no assertion here on how long the read blocked. One was tried and removed:
// the elapsed time was occasionally far below the requested block, but only while several test
// packages hammered the same Redis server, and it never reproduced in isolation (30 consecutive
// runs of this test alone passed; 20 more before removal). A bound close to the requested block
// therefore measures server scheduling under load rather than the behaviour under test.
//
// What actually matters is covered deterministically elsewhere: TestStreamsReadGroupFramesBlockAndNoAck
// asserts against a scripted server that "BLOCK <ms>" is sent with the requested value, which is
// precisely the failure this test was trying to catch - a blocking read that does not block at all.
func TestIntegrationStreamsBlockingReadTimesOutCleanly(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{
		Stream: stream, Group: group, Consumer: "c1", Count: 10, Block: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected no deliveries, got %d", len(deliveries))
	}

	// The read must not have been cut short by the configured read timeout, which would surface as
	// an availability failure rather than an empty batch. That part of the deadline interaction is
	// deterministic, so it is asserted.
	if err := streams.Ping(ctx); err != nil {
		t.Fatalf("expected the connection to remain usable after a block timeout: %v", err)
	}
}

// A block longer than the configured ReadTimeout must still work, which is why the
// read deadline is raised for blocking reads.
func TestIntegrationStreamsBlockOutlivesTheReadTimeout(t *testing.T) {
	client := requireRedis(t)
	config := client.pool.config
	// A short ReadTimeout would abort a 1500ms block without the override.
	client.pool.config.ReadTimeout = 300 * time.Millisecond

	streams := NewStreamsClientOver(client)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{
		Stream: stream, Group: group, Consumer: "c1", Count: 10, Block: 1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("a blocking read must not fail on the configured read timeout: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected no deliveries, got %d", len(deliveries))
	}
	client.pool.config.ReadTimeout = config.ReadTimeout
}

func TestIntegrationStreamsEnsureGroupIsIdempotent(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	// The fixture already created the group; BUSYGROUP must be treated as success so
	// a worker can start repeatedly without failing.
	if err := streams.EnsureGroup(ctx, stream, group, "0-0"); err != nil {
		t.Fatalf("expected the second EnsureGroup to succeed: %v", err)
	}
}

// EnsureGroup must create the stream too, so a worker can start before any event is
// published.
func TestIntegrationStreamsEnsureGroupCreatesTheStream(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream := fmt.Sprintf("ncs:test:stream:autocreate-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	if err := streams.EnsureGroup(ctx, stream, "g", "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	// A blocking read against the auto-created stream must not report NOGROUP.
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{
		Stream: stream, Group: "g", Consumer: "c1", Count: 1, Block: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("read group on an empty auto-created stream: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected no deliveries, got %d", len(deliveries))
	}
}

func TestIntegrationStreamsMissingGroupIsReported(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	stream := fmt.Sprintf("ncs:test:stream:nogroup-%d", time.Now().UnixNano())
	_, err := streams.Add(ctx, stream, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	_, err = streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: "absent", Consumer: "c1", Count: 1})
	if !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing, got %v", err)
	}
}

func TestIntegrationStreamsAckRemovesPending(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	if _, err := streams.Add(ctx, stream, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "c1", Count: 1, Block: time.Second})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d and %v", len(deliveries), err)
	}

	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending entry, got %d", len(pending))
	}
	if pending[0].Consumer != "c1" {
		t.Fatalf("expected consumer c1, got %s", pending[0].Consumer)
	}
	if pending[0].DeliveryCount != 1 {
		t.Fatalf("expected delivery count 1, got %d", pending[0].DeliveryCount)
	}

	acked, err := streams.Ack(ctx, stream, group, deliveries[0].ID)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if acked != 1 {
		t.Fatalf("expected 1 ack, got %d", acked)
	}
	pending, err = streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending entries after ack, got %d", len(pending))
	}
}

// The extended XPENDING form is what supplies the idle time and delivery count the
// retry budget needs, so an unacknowledged entry must report both.
func TestIntegrationStreamsPendingReportsIdleAndDeliveryCount(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	if _, err := streams.Add(ctx, stream, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "c1", Count: 1, Block: time.Second}); err != nil {
		t.Fatalf("read group: %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending entry, got %d", len(pending))
	}
	if pending[0].Idle < 100*time.Millisecond {
		t.Fatalf("expected an idle time near 120ms, got %s", pending[0].Idle)
	}
}

// Claim is what recovers work from a consumer that died, and it must report the
// incremented delivery count so a poison message eventually reaches the retry cap.
func TestIntegrationStreamsClaimTakesOverAndCountsDeliveries(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	if _, err := streams.Add(ctx, stream, map[string]string{"event_id": "evt_claim"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "dead-consumer", Count: 1, Block: time.Second})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d and %v", len(deliveries), err)
	}

	// A claim with a minimum idle time longer than the actual idle must not steal the
	// entry.
	none, err := streams.Claim(ctx, stream, group, "recovery", []string{deliveries[0].ID}, time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected the entry to stay with its owner, got %d", len(none))
	}

	time.Sleep(120 * time.Millisecond)
	claimed, err := streams.Claim(ctx, stream, group, "recovery", []string{deliveries[0].ID}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed entry, got %d", len(claimed))
	}
	if claimed[0].Values["event_id"] != "evt_claim" {
		t.Fatalf("expected the claimed body, got %#v", claimed[0].Values)
	}
	if claimed[0].Consumer != "recovery" {
		t.Fatalf("expected consumer recovery, got %s", claimed[0].Consumer)
	}
	// The second delivery must be counted, otherwise a poison message would retry
	// forever.
	if claimed[0].DeliveryCount != 2 {
		t.Fatalf("expected delivery count 2 after a claim, got %d", claimed[0].DeliveryCount)
	}

	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].Consumer != "recovery" {
		t.Fatalf("expected the entry to belong to recovery, got %+v", pending)
	}
}

func TestIntegrationStreamsReadGroupRejectsMissingArguments(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	if _, err := streams.ReadGroup(ctx, ReadGroupOptions{Group: "g", Consumer: "c"}); err == nil {
		t.Fatal("expected a missing stream to be rejected")
	}
	if _, err := streams.Add(ctx, "", map[string]string{"k": "v"}); err == nil {
		t.Fatal("expected a missing stream to be rejected")
	}
	if _, err := streams.Add(ctx, "s", nil); err == nil {
		t.Fatal("expected an empty field set to be rejected")
	}
	if err := streams.EnsureGroup(ctx, "", "g", "0-0"); err == nil {
		t.Fatal("expected a missing stream to be rejected")
	}
	if _, err := streams.Claim(ctx, "s", "g", "", []string{"1-1"}, 0); err == nil {
		t.Fatal("expected a missing consumer to be rejected")
	}
}

// The full worker path against a real server: publish, consume, fail once, recover
// the pending entry by claiming it, then acknowledge.
func TestIntegrationStreamsWorkerRecoveryPathAgainstRealRedis(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	// Publish three events.
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		id, err := streams.Add(ctx, stream, map[string]string{
			"event_id":   fmt.Sprintf("evt_%02d", i),
			"event_type": "CHARGE_STARTED",
		})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	// Consume them but do not acknowledge, simulating a crash after handling.
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "worker-a", Count: 10, Block: time.Second})
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	if len(deliveries) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(deliveries))
	}

	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending entries, got %d", len(pending))
	}

	// Recover them on a different consumer and acknowledge.
	time.Sleep(120 * time.Millisecond)
	claimed, err := streams.Claim(ctx, stream, group, "worker-b", ids, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("expected 3 claimed entries, got %d", len(claimed))
	}
	claimedIDs := make([]string, 0, len(claimed))
	for _, delivery := range claimed {
		if delivery.Values["event_id"] == "" {
			t.Fatalf("expected the event body, got %#v", delivery.Values)
		}
		claimedIDs = append(claimedIDs, delivery.ID)
	}
	acked, err := streams.Ack(ctx, stream, group, claimedIDs...)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if acked != 3 {
		t.Fatalf("expected 3 acks, got %d", acked)
	}

	pending, err = streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending entries, got %d", len(pending))
	}
}

func TestIntegrationStreamsNoAckDoesNotCreatePending(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	if _, err := streams.Add(ctx, stream, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{
		Stream: stream, Group: group, Consumer: "c1", Count: 1, Block: time.Second, NoAck: true,
	})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d and %v", len(deliveries), err)
	}
	pending, err := streams.Pending(ctx, stream, group)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("NOACK must not create pending entries, got %d", len(pending))
	}
}

func TestIntegrationStreamsFieldOrderIsDeterministic(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, group := streamFixture(t, streams)

	values := map[string]string{"zeta": "1", "alpha": "2", "mu": "3"}
	if _, err := streams.Add(ctx, stream, values); err != nil {
		t.Fatalf("add: %v", err)
	}
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "c1", Count: 1, Block: time.Second})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d and %v", len(deliveries), err)
	}
	if len(deliveries[0].Values) != len(values) {
		t.Fatalf("expected %d fields, got %#v", len(values), deliveries[0].Values)
	}
	for field, want := range values {
		if deliveries[0].Values[field] != want {
			t.Errorf("expected %s=%q, got %q", field, want, deliveries[0].Values[field])
		}
	}
}
