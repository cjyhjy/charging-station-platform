package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newDeadLetterClaimsFixture(t *testing.T, config DeadLetterClaimConfig) (*DeadLetterClaims, *MemoryCommands, *testClock) {
	t.Helper()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	claims, err := NewDeadLetterClaims(commands, DefaultPolicy(), NewDegradation(), config,
		sequenceTokens("owner-1", "owner-2", "owner-3", "owner-4"))
	if err != nil {
		t.Fatalf("new dead-letter claims: %v", err)
	}
	return claims, commands, clock
}

func TestNewDeadLetterClaimsValidatesInputs(t *testing.T) {
	config := DefaultDeadLetterClaimConfig()
	if _, err := NewDeadLetterClaims(nil, DefaultPolicy(), nil, config, nil); err == nil {
		t.Fatal("expected nil commands to be rejected")
	}
	if _, err := NewDeadLetterClaims(NewMemoryCommands(), Policy{Idempotency: FailMode(9)}, nil, config, nil); err == nil {
		t.Fatal("expected an invalid policy to be rejected")
	}
	if _, err := NewDeadLetterClaims(NewMemoryCommands(), DefaultPolicy(), nil, DeadLetterClaimConfig{}, nil); err == nil {
		t.Fatal("expected empty lifetimes to be rejected")
	}
	if _, err := NewDeadLetterClaims(NewMemoryCommands(), DefaultPolicy(), nil,
		DeadLetterClaimConfig{WriteTTL: time.Hour, WrittenTTL: time.Minute}, nil); err == nil {
		t.Fatal("expected evidence that expires before the claim protecting it to be rejected")
	}
}

// The finding this store exists for: a claim held by somebody else says nothing about the outcome
// of their write. Reporting it as "already written" is what let a delivery record a terminal
// outcome and acknowledge an event whose only write then failed.
func TestDeadLetterClaimsDistinguishAnUnfinishedWriteFromACompletedOne(t *testing.T) {
	claims, _, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	token, state, err := claims.Claim(ctx, "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimTaken || token == "" {
		t.Fatalf("expected the first claim to be taken, got %s and token %q", state, token)
	}

	// A second consumer arrives while the first is still writing.
	_, state, err = claims.Claim(ctx, "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWriting {
		t.Fatalf("expected an unfinished write to be reported as writing, got %s", state)
	}

	// The write completes and is published.
	if err := claims.MarkWritten(ctx, "evt_01", token); err != nil {
		t.Fatalf("mark written: %v", err)
	}
	_, state, err = claims.Claim(ctx, "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWritten {
		t.Fatalf("expected a completed write to be reported as written, got %s", state)
	}
}

// Publishing the write must happen before the claim is dropped, so a delivery that fails to take
// the claim can never see a finished writer as an unfinished one.
func TestMarkingWrittenReleasesTheClaimAndPublishesTheWrite(t *testing.T) {
	claims, _, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	token, _, err := claims.Claim(ctx, "evt_02")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claims.MarkWritten(ctx, "evt_02", token); err != nil {
		t.Fatalf("mark written: %v", err)
	}

	written, err := claims.Written(ctx, "evt_02")
	if err != nil {
		t.Fatalf("written: %v", err)
	}
	if !written {
		t.Fatal("expected the completed write to be on record")
	}
	inFlight, err := claims.InFlight(ctx, "evt_02")
	if err != nil {
		t.Fatalf("in flight: %v", err)
	}
	if inFlight {
		t.Fatal("expected the write claim to be released once the write is published")
	}
}

// A failed write gives the claim back, so the retry writes instead of waiting for the claim to
// expire - and only the owning token may do it.
func TestReleasingAWriteClaimRequiresTheOwningToken(t *testing.T) {
	claims, _, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	token, _, err := claims.Claim(ctx, "evt_03")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claims.Release(ctx, "evt_03", "someone-elses-token"); err != nil {
		t.Fatalf("release: %v", err)
	}
	_, state, err := claims.Claim(ctx, "evt_03")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimWriting {
		t.Fatalf("a foreign token must not release the claim, got state %s", state)
	}

	if err := claims.Release(ctx, "evt_03", token); err != nil {
		t.Fatalf("release: %v", err)
	}
	reclaimed, state, err := claims.Claim(ctx, "evt_03")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimTaken || reclaimed == "" {
		t.Fatalf("expected the release to let the retry write, got %s and token %q", state, reclaimed)
	}
}

// An outage must degrade towards writing again. Reporting "already written" while Redis is
// unreachable would acknowledge an entry that nothing had parked, which is the one direction this
// store is not allowed to fail in.
func TestDeadLetterClaimsPreferWritingAgainWhenRedisIsUnavailable(t *testing.T) {
	claims, commands, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	// A write really was completed before the outage.
	token, _, err := claims.Claim(ctx, "evt_04")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claims.MarkWritten(ctx, "evt_04", token); err != nil {
		t.Fatalf("mark written: %v", err)
	}

	commands.SetDown(ErrUnavailable)
	_, state, err := claims.Claim(ctx, "evt_04")
	if err != nil {
		t.Fatalf("a FailOpen capability must not surface the outage: %v", err)
	}
	if state != DeadLetterClaimTaken {
		t.Fatalf("expected an outage to be treated as \"write again\", got %s", state)
	}
}

// The claim must be a real key under the frozen naming baseline, and the two markers must have
// the lifetimes the configuration asks for.
func TestDeadLetterClaimMarkersCarryTheConfiguredLifetimes(t *testing.T) {
	config := DeadLetterClaimConfig{WriteTTL: 30 * time.Second, WrittenTTL: 48 * time.Hour}
	claims, commands, _ := newDeadLetterClaimsFixture(t, config)
	ctx := context.Background()

	token, _, err := claims.Claim(ctx, "evt_05")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	writeKey, err := IdempotencyKey(deadLetterWriteScope, "evt_05")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	ttl, hasExpiry, err := commands.TTL(ctx, writeKey)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl <= 0 || ttl > config.WriteTTL {
		t.Fatalf("expected a write claim inside %s, got %s (hasExpiry=%v)", config.WriteTTL, ttl, hasExpiry)
	}

	if err := claims.MarkWritten(ctx, "evt_05", token); err != nil {
		t.Fatalf("mark written: %v", err)
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, "evt_05")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	ttl, hasExpiry, err = commands.TTL(ctx, writtenKey)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl <= config.WriteTTL || ttl > config.WrittenTTL {
		t.Fatalf("expected the evidence to outlive the claim, got %s (hasExpiry=%v)", ttl, hasExpiry)
	}
}

// An undecodable entry has no event id to key a claim on, so it is always written and never
// deduplicated.
func TestDeadLetterClaimsTakeAnUnkeyableEntry(t *testing.T) {
	claims, _, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	token, state, err := claims.Claim(ctx, "")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if state != DeadLetterClaimTaken {
		t.Fatalf("expected an entry without an event id to be written, got %s", state)
	}
	if token != "" {
		t.Fatalf("expected no token for an entry with nothing to key on, got %q", token)
	}
	if err := claims.MarkWritten(ctx, "", token); err != nil {
		t.Fatalf("mark written: %v", err)
	}
	if err := claims.Release(ctx, "", token); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// A publish that cannot reach Redis must be reported rather than swallowed: the caller goes on to
// acknowledge either way, and the difference is whether a later retry knows the write happened.
func TestMarkWrittenReportsAnUnreachableRedis(t *testing.T) {
	claims, commands, _ := newDeadLetterClaimsFixture(t, DefaultDeadLetterClaimConfig())
	ctx := context.Background()

	token, _, err := claims.Claim(ctx, "evt_06")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	commands.SetDown(ErrUnavailable)
	if err := claims.MarkWritten(ctx, "evt_06", token); err == nil {
		t.Fatal("expected the failed publish to be reported")
	}
	// The caller's write is not undone by this, which is why the error is a warning and not a
	// failure of the park.
	if err := claims.Release(ctx, "evt_06", token); !errors.Is(err, ErrUnavailable) && err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}
}
