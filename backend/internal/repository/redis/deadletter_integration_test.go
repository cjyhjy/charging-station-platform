package redis

import (
	"context"
	"testing"
	"time"
)

// The claim store's whole purpose is an ordering property that only a real server can confirm
// end to end: what a second consumer sees while the first is still writing, and what it sees
// afterwards.
func TestIntegrationDeadLetterClaimsAgainstRealRedis(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	config := DeadLetterClaimConfig{WriteTTL: 30 * time.Second, WrittenTTL: 10 * time.Minute}
	claims, err := NewDeadLetterClaims(client, DefaultPolicy(), NewDegradation(), config, nil)
	if err != nil {
		t.Fatalf("new dead-letter claims: %v", err)
	}

	suffix := sanitizeTestName(t.Name())
	eventID := "evt_" + suffix
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, eventID)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	cleanupKey(t, client, writeKey)
	cleanupKey(t, client, writtenKey)

	token, state, err := claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimTaken || token == "" {
		t.Fatalf("expected the first claim to be taken, got %s and token %q", state, token)
	}

	// Both markers live under the frozen naming baseline, and the claim self-heals.
	ttl, hasExpiry, err := client.TTL(ctx, writeKey)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl <= 0 || ttl > config.WriteTTL {
		t.Fatalf("expected a write claim inside %s, got %s (hasExpiry=%v)", config.WriteTTL, ttl, hasExpiry)
	}

	// A second consumer must see an unfinished write, not a completed one. This is the whole
	// finding: before this distinction existed, the second consumer recorded the event as
	// terminally dead-lettered and acknowledged it while the first was still writing.
	_, state, err = claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWriting {
		t.Fatalf("expected the concurrent claim to report an unfinished write, got %s", state)
	}
	written, err := claims.Written(ctx, eventID)
	if err != nil {
		t.Fatalf("written: %v", err)
	}
	if written {
		t.Fatal("no completed write exists yet, so none may be reported")
	}

	// The write completes.
	if err := claims.MarkWritten(ctx, eventID, token); err != nil {
		t.Fatalf("mark written: %v", err)
	}
	_, state, err = claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWritten {
		t.Fatalf("expected the completed write to be reported, got %s", state)
	}
	inFlight, err := claims.InFlight(ctx, eventID)
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	if inFlight {
		t.Fatal("expected the write claim to be gone once the write is published")
	}

	ttl, hasExpiry, err = client.TTL(ctx, writtenKey)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl <= config.WriteTTL {
		t.Fatalf("expected the evidence to outlive the claim, got %s (hasExpiry=%v)", ttl, hasExpiry)
	}
}

// A write that fails must give the claim back, so the retry writes immediately rather than waiting
// for the claim to expire - and only the owning token may do it.
func TestIntegrationDeadLetterClaimReleaseAgainstRealRedis(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	claims, err := NewDeadLetterClaims(client, DefaultPolicy(), NewDegradation(), DefaultDeadLetterClaimConfig(), nil)
	if err != nil {
		t.Fatalf("new dead-letter claims: %v", err)
	}

	suffix := sanitizeTestName(t.Name())
	eventID := "evt_" + suffix
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, eventID)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	cleanupKey(t, client, writeKey)
	cleanupKey(t, client, writtenKey)

	token, _, err := claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claims.Release(ctx, eventID, "someone-elses-token"); err != nil {
		t.Fatalf("release: %v", err)
	}
	_, state, err := claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWriting {
		t.Fatalf("a foreign token must not release the claim, got state %s", state)
	}

	if err := claims.Release(ctx, eventID, token); err != nil {
		t.Fatalf("release: %v", err)
	}
	reclaimed, state, err := claims.Claim(ctx, eventID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimTaken || reclaimed == "" {
		t.Fatalf("expected the release to let the retry write, got %s and token %q", state, reclaimed)
	}
}
