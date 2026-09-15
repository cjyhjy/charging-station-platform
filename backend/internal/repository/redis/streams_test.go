package redis

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStreamAckAndPending(t *testing.T) {
	ctx := context.Background()
	stream := NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(ctx, "events", "workers", "$"); err != nil {
		t.Fatalf("EnsureGroup() error = %v", err)
	}
	id, err := stream.Add(ctx, "events", map[string]string{"event_id": "evt_01"})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	deliveries, err := stream.ReadGroup(ctx, ReadGroupOptions{Stream: "events", Group: "workers", Consumer: "worker-1", Count: 1})
	if err != nil {
		t.Fatalf("ReadGroup() error = %v", err)
	}
	if len(deliveries) != 1 || deliveries[0].ID != id || deliveries[0].DeliveryCount != 1 {
		t.Fatalf("unexpected deliveries: %#v", deliveries)
	}
	pending, err := stream.Pending(ctx, "events", "workers")
	if err != nil || len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("unexpected pending: %#v, error=%v", pending, err)
	}
	acked, err := stream.Ack(ctx, "events", "workers", id)
	if err != nil || acked != 1 {
		t.Fatalf("Ack() = %d, error=%v", acked, err)
	}
	pending, err = stream.Pending(ctx, "events", "workers")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after ack = %#v, error=%v", pending, err)
	}
}

func TestMemoryStreamClaimMovesPendingToAnotherConsumer(t *testing.T) {
	ctx := context.Background()
	stream := NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(ctx, "events", "workers", "$"); err != nil {
		t.Fatal(err)
	}
	id, err := stream.Add(ctx, "events", map[string]string{"value": "x"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.ReadGroup(ctx, ReadGroupOptions{Stream: "events", Group: "workers", Consumer: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := stream.Claim(ctx, "events", "workers", "worker-2", []string{id}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Consumer != "worker-2" || claimed[0].DeliveryCount != 2 {
		t.Fatalf("unexpected claimed delivery: %#v", claimed)
	}
	pending, err := stream.Pending(ctx, "events", "workers")
	if err != nil || len(pending) != 1 || pending[0].Consumer != "worker-2" {
		t.Fatalf("unexpected pending after claim: %#v, error=%v", pending, err)
	}
}

func TestMemoryStreamReadHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stream := NewMemoryStream()
	defer stream.Close()
	if err := stream.EnsureGroup(context.Background(), "events", "workers", "$"); err != nil {
		t.Fatal(err)
	}
	_, err := stream.ReadGroup(ctx, ReadGroupOptions{Stream: "events", Group: "workers", Consumer: "worker-1", Block: -1})
	if err != context.DeadlineExceeded {
		t.Fatalf("ReadGroup() error = %v, want context deadline", err)
	}
}
