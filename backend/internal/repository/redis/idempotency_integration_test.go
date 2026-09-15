package redis

import (
	"context"
	"testing"
	"time"
)

func TestIntegrationGuardClaimAndReleaseAgainstRealRedis(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	guard, err := NewGuard(client, DefaultPolicy(), NewDegradation(), GuardConfig{TTL: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}

	scope := "it-" + sanitizeTestName(t.Name())
	eventID := "evt_guard_01"
	key, err := IdempotencyKey(scope, eventID)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	cleanupKey(t, client, key)

	owner, err := guard.Claim(ctx, scope, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if owner == "" {
		t.Fatal("expected the first claim to win")
	}

	// The claim must be a real key under the frozen naming baseline, with a TTL so a
	// leaked claim self-heals.
	ttl, hasExpiry, err := client.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the claim to have a ttl")
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Fatalf("expected a ttl inside 30s, got %s", ttl)
	}

	// A second claim must be refused: this is the duplicate suppression.
	second, err := guard.Claim(ctx, scope, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if second != "" {
		t.Fatal("expected the second claim to be refused")
	}

	if err := guard.Release(ctx, scope, eventID, owner); err != nil {
		t.Fatalf("release: %v", err)
	}
	held, err := guard.Held(ctx, scope, eventID)
	if err != nil {
		t.Fatalf("held: %v", err)
	}
	if held {
		t.Fatal("expected the claim to be gone after release")
	}

	// A redelivery after a handling failure must be processable again.
	reclaimed, err := guard.Claim(ctx, scope, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if reclaimed == "" {
		t.Fatal("expected the released event to be reclaimable")
	}
}

// The release must be a compare-and-delete on the server, so a stale release cannot
// remove a claim another consumer holds.
func TestIntegrationGuardReleaseUsesAnAtomicCompareAndDelete(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	guard, err := NewGuard(client, DefaultPolicy(), NewDegradation(), GuardConfig{TTL: time.Minute}, nil)
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}

	scope := "it-" + sanitizeTestName(t.Name())
	eventID := "evt_guard_foreign"
	key, _ := IdempotencyKey(scope, eventID)
	cleanupKey(t, client, key)

	// Another consumer's claim.
	if err := client.Set(ctx, key, "claim:someone-else:"+eventID, time.Minute); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := guard.Release(ctx, scope, eventID, "someone-else"); err != nil {
		t.Fatalf("release: %v", err)
	}
	value, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected the foreign claim to survive: %v", err)
	}
	if value != "claim:someone-else:"+eventID {
		t.Fatalf("expected the foreign claim untouched, got %q", value)
	}
}

// A claim that outlives its TTL must be reclaimable, so an event is never blocked
// forever by a crashed consumer.
func TestIntegrationGuardClaimExpires(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	guard, err := NewGuard(client, DefaultPolicy(), NewDegradation(), GuardConfig{TTL: 200 * time.Millisecond}, nil)
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}

	scope := "it-" + sanitizeTestName(t.Name())
	eventID := "evt_guard_expiry"
	key, _ := IdempotencyKey(scope, eventID)
	cleanupKey(t, client, key)

	if owner, err := guard.Claim(ctx, scope, eventID); err != nil || owner == "" {
		t.Fatalf("expected the first claim to win, got %q and %v", owner, err)
	}
	time.Sleep(350 * time.Millisecond)

	reclaimed, err := guard.Claim(ctx, scope, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if reclaimed == "" {
		t.Fatal("expected the expired claim to be reclaimable")
	}
}
