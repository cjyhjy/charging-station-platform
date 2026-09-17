package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// These tests read real stream state, which is the only way to confirm the XINFO parsing
// and the lag semantics the observability layer depends on.

func TestIntegrationStreamInfoOnARealStream(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, _ := streamFixture(t, streams)

	for i := 0; i < 3; i++ {
		if _, err := streams.Add(ctx, stream, map[string]string{"event_id": fmt.Sprintf("evt_%d", i)}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	info, err := streams.StreamInfo(ctx, stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Length != 3 {
		t.Fatalf("expected length 3, got %d", info.Length)
	}
	// entries-added is the total ever produced, which is what makes a derived lag stable
	// across trimming.
	if info.EntriesAdded != 3 {
		t.Fatalf("expected 3 entries added, got %d", info.EntriesAdded)
	}
	if info.LastGeneratedID == "" {
		t.Fatal("expected a last generated id")
	}
	if info.GroupCount != 1 {
		t.Fatalf("expected 1 group, got %d", info.GroupCount)
	}
}

// A stream that has never received an entry does not exist as a key. The worker subscribes
// to streams before traffic exists, so this must be reported as missing rather than as a
// failure.
func TestIntegrationStreamInfoOnAnAbsentStream(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	absent := fmt.Sprintf("ncs:test:stream:absent-%d", time.Now().UnixNano())
	_, err := streams.StreamInfo(ctx, absent)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// XLEN reports a missing key as zero, which keeps the dead-letter gauge meaningful
	// before the first dead letter exists.
	length, err := streams.StreamLen(ctx, absent)
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected 0 for an absent stream, got %d", length)
	}
}

func TestIntegrationGroupInfoReportsLagPendingAndConsumers(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	// A group created at 0-0 sees the whole backlog, which is what makes the lag observable.
	suffix := sanitizeTestName(t.Name())
	stream := fmt.Sprintf("ncs:test:stream:lag-%s", suffix)
	group := "lag-workers"
	_, _ = streams.client.Del(ctx, stream)
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	for i := 0; i < 5; i++ {
		if _, err := streams.Add(ctx, stream, map[string]string{"event_id": fmt.Sprintf("evt_%d", i)}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := streams.EnsureGroup(ctx, stream, group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	info, err := streams.GroupInfo(ctx, stream, group)
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.Pending != 0 {
		t.Fatalf("expected no pending entries, got %d", info.Pending)
	}
	if !info.LagKnown() {
		t.Skip("this Redis build does not report lag")
	}
	if info.Lag != 5 {
		t.Fatalf("expected lag 5 before any read, got %d", info.Lag)
	}

	// Consume two without acknowledging: lag drops, pending rises.
	deliveries, err := streams.ReadGroup(ctx, ReadGroupOptions{Stream: stream, Group: group, Consumer: "c1", Count: 2, Block: time.Second})
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("expected 2 deliveries, got %d and %v", len(deliveries), err)
	}

	info, err = streams.GroupInfo(ctx, stream, group)
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.Pending != 2 {
		t.Fatalf("expected 2 pending entries, got %d", info.Pending)
	}
	if info.Consumers != 1 {
		t.Fatalf("expected 1 consumer, got %d", info.Consumers)
	}
	if info.LagKnown() && info.Lag != 3 {
		t.Fatalf("expected lag 3 after reading 2 of 5, got %d", info.Lag)
	}

	// Acknowledging the two clears pending while lag stays at the unread remainder.
	if _, err := streams.Ack(ctx, stream, group, deliveries[0].ID, deliveries[1].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	info, err = streams.GroupInfo(ctx, stream, group)
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.Pending != 0 {
		t.Fatalf("expected pending to clear after ack, got %d", info.Pending)
	}
}

// EffectiveLag is what the collector uses, so it is asserted against real Redis values
// rather than only against the fake server.
func TestIntegrationEffectiveLagAgainstRealRedis(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	suffix := sanitizeTestName(t.Name())
	stream := fmt.Sprintf("ncs:test:stream:efflag-%s", suffix)
	group := "efflag-workers"
	_, _ = streams.client.Del(ctx, stream)
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	for i := 0; i < 4; i++ {
		if _, err := streams.Add(ctx, stream, map[string]string{"event_id": fmt.Sprintf("evt_%d", i)}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := streams.EnsureGroup(ctx, stream, group, "0-0"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	streamInfo, err := streams.StreamInfo(ctx, stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	groupInfo, err := streams.GroupInfo(ctx, stream, group)
	if err != nil {
		t.Fatalf("group info: %v", err)
	}

	lag, known := EffectiveLag(streamInfo, groupInfo)
	if !known {
		t.Fatalf("expected a known backlog, got stream=%+v group=%+v", streamInfo, groupInfo)
	}
	if lag != 4 {
		t.Fatalf("expected a backlog of 4, got %d", lag)
	}
}

// A group created at "$" is positioned at the head of an existing stream, so Redis reports no
// lag and pre-existing entries are never delivered to it.
//
// This pins a Redis semantic, not the worker's behaviour: a worker only creates its group at
// "$" if an operator explicitly configures that start position. The default is the beginning of
// the stream, because otherwise a worker deployed after the outbox had already published would
// skip that backlog permanently. See worker.Config.StartID.
func TestIntegrationGroupCreatedAtHeadReportsNoLag(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	suffix := sanitizeTestName(t.Name())
	stream := fmt.Sprintf("ncs:test:stream:head-%s", suffix)
	group := "head-workers"
	_, _ = streams.client.Del(ctx, stream)
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	for i := 0; i < 3; i++ {
		if _, err := streams.Add(ctx, stream, map[string]string{"event_id": fmt.Sprintf("evt_%d", i)}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	// "$" places the group at the head, so no historical entry is ever delivered.
	if err := streams.EnsureGroup(ctx, stream, group, "$"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	info, err := streams.GroupInfo(ctx, stream, group)
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.LagKnown() && info.Lag != 0 {
		t.Fatalf("expected no lag for a group positioned at the head, got %d", info.Lag)
	}

	// The stream still holds the entries, so length and entries-added report them even
	// though the group will never see them.
	streamInfo, err := streams.StreamInfo(ctx, stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if streamInfo.Length != 3 {
		t.Fatalf("expected length 3, got %d", streamInfo.Length)
	}
}

func TestIntegrationGroupInfoOnAMissingGroup(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()
	stream, _ := streamFixture(t, streams)

	if _, err := streams.GroupInfo(ctx, stream, "absent-group"); !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing, got %v", err)
	}
}

// The dead-letter length is the parked backlog an operator has to work through, so it is read
// from the real stream the worker writes to.
func TestIntegrationDeadLetterLength(t *testing.T) {
	streams := requireRedisStreams(t)
	ctx := context.Background()

	// A dedicated stream rather than the real dead-letter stream, so the test cannot collide
	// with another run.
	stream := fmt.Sprintf("ncs:test:stream:dlq-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = streams.client.Del(context.Background(), stream)
	})

	length, err := streams.StreamLen(ctx, stream)
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected 0 before any dead letter, got %d", length)
	}

	for i := 0; i < 2; i++ {
		if _, err := streams.Add(ctx, stream, map[string]string{"dead_letter_reason": "retry_exhausted"}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	length, err = streams.StreamLen(ctx, stream)
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if length != 2 {
		t.Fatalf("expected 2 parked entries, got %d", length)
	}
}
