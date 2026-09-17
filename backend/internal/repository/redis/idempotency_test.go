package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newGuardFixture(t *testing.T, policy Policy, ttl time.Duration) (*Guard, *MemoryCommands, *testClock) {
	t.Helper()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	guard, err := NewGuard(commands, policy, NewDegradation(), GuardConfig{TTL: ttl}, sequenceTokens("owner-1", "owner-2", "owner-3", "owner-4"))
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}
	return guard, commands, clock
}

func TestNewGuardValidatesInputs(t *testing.T) {
	if _, err := NewGuard(nil, DefaultPolicy(), nil, DefaultGuardConfig(), nil); err == nil {
		t.Fatal("expected nil commands to be rejected")
	}
	if _, err := NewGuard(NewMemoryCommands(), Policy{Idempotency: FailMode(9)}, nil, DefaultGuardConfig(), nil); err == nil {
		t.Fatal("expected an invalid policy to be rejected")
	}
	if _, err := NewGuard(NewMemoryCommands(), DefaultPolicy(), nil, GuardConfig{TTL: 0}, nil); err == nil {
		t.Fatal("expected a zero ttl to be rejected")
	}
}

// Only the first claimer may process an event; the rest get an empty token and must skip it.
func TestGuardOnlyTheFirstClaimWins(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	token, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if token == "" {
		t.Fatal("expected the first claim to return an owner token")
	}

	second, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if second != "" {
		t.Fatalf("expected the second claim to be refused, got token %q", second)
	}
}

// Each claim must mint its own owner. A token derived from the key alone is identical for
// every consumer, which is what allowed a consumer whose processing outlived the claim TTL to
// release the claim a different consumer had since acquired.
func TestGuardMintsAUniqueOwnerPerClaim(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	first, err := guard.Claim(ctx, "charge-event", "evt_a")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := guard.Release(ctx, "charge-event", "evt_a", first); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, err := guard.Claim(ctx, "charge-event", "evt_a")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if second == first {
		t.Fatal("expected a different owner token for the second claim on the same key")
	}
	if second == "" {
		t.Fatal("expected a token for the reclaim")
	}
}

// This is the defect the previous revision had: a stale holder releasing a claim that a
// different consumer now owns.
func TestGuardStaleReleaseDoesNotDeleteTheCurrentClaim(t *testing.T) {
	guard, commands, clock := newGuardFixture(t, DefaultPolicy(), 30*time.Second)
	ctx := context.Background()
	key, _ := IdempotencyKey("charge-event", "evt_stale")

	stale, err := guard.Claim(ctx, "charge-event", "evt_stale")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// The first holder outlives the TTL, and another consumer takes the claim.
	clock.Advance(time.Minute)
	fresh, err := guard.Claim(ctx, "charge-event", "evt_stale")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if fresh == "" || fresh == stale {
		t.Fatalf("expected a new owner after the TTL, got %q", fresh)
	}

	// The stale holder now finishes and releases. It must not remove the new claim.
	if err := guard.Release(ctx, "charge-event", "evt_stale", stale); err != nil {
		t.Fatalf("release: %v", err)
	}
	value, err := commands.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected the current claim to survive: %v", err)
	}
	if value != fresh {
		t.Fatalf("expected the current owner %q to still hold the claim, got %q", fresh, value)
	}

	// And the current owner can still release its own claim.
	if err := guard.Release(ctx, "charge-event", "evt_stale", fresh); err != nil {
		t.Fatalf("release: %v", err)
	}
	held, err := guard.Held(ctx, "charge-event", "evt_stale")
	if err != nil {
		t.Fatalf("held: %v", err)
	}
	if held {
		t.Fatal("expected the owner's release to succeed")
	}
}

func TestGuardReleaseRequiresAnOwnerToken(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	if _, err := guard.Claim(ctx, "charge-event", "evt_01"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := guard.Release(ctx, "charge-event", "evt_01", ""); err == nil {
		t.Fatal("expected a release without an owner token to be rejected")
	}
}

// The claim must live under the frozen naming baseline so a running process and an
// operator inspecting Redis agree on where it is.
func TestGuardUsesTheIdempotencyNamingBaseline(t *testing.T) {
	guard, commands, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	owner, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, err := IdempotencyKey("charge-event", "evt_01")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	value, err := commands.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected the claim at %s: %v", key, err)
	}
	// The stored value is the owner token, which is what the compare-and-delete relies on.
	if value != owner {
		t.Fatalf("expected the owner token stored as the claim value, got %q", value)
	}

	ttl, hasExpiry, err := commands.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl != time.Minute {
		t.Fatalf("expected a one minute expiry, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}
}

// Releasing must let a redelivery be processed, which is what stops a failed event from
// being mistaken for a duplicate and lost.
func TestGuardReleaseAllowsReclaiming(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	owner, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := guard.Release(ctx, "charge-event", "evt_01", owner); err != nil {
		t.Fatalf("release: %v", err)
	}

	held, err := guard.Held(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("held: %v", err)
	}
	if held {
		t.Fatal("expected the claim to be gone after release")
	}

	reclaimed, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if reclaimed == "" {
		t.Fatal("expected the released event to be reclaimable")
	}
}

func TestGuardClaimExpiresWithItsTTL(t *testing.T) {
	guard, _, clock := newGuardFixture(t, DefaultPolicy(), 30*time.Second)
	ctx := context.Background()

	if _, err := guard.Claim(ctx, "charge-event", "evt_01"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	clock.Advance(31 * time.Second)

	// A leaked claim must self-heal, otherwise the event could never be processed again.
	token, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if token == "" {
		t.Fatal("expected the expired claim to be reclaimable")
	}
}

func TestGuardScopesAreIndependent(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	if _, err := guard.Claim(ctx, "charge-event", "evt_01"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// The same event id under a different scope is a different guard entry. The dead-letter
	// scope relies on this so it cannot collide with the processing claim.
	token, err := guard.Claim(ctx, "charger-command", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if token == "" {
		t.Fatal("expected a different scope to be independent")
	}
	if deadLetterToken, err := guard.Claim(ctx, "dead-letter", "evt_01"); err != nil || deadLetterToken == "" {
		t.Fatalf("expected the dead-letter scope to be independent, got %q and %v", deadLetterToken, err)
	}
}

func TestGuardRejectsInvalidKeys(t *testing.T) {
	guard, _, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	if _, err := guard.Claim(ctx, "", "evt_01"); err == nil {
		t.Fatal("expected an empty scope to be rejected")
	}
	if _, err := guard.Claim(ctx, "charge-event", ""); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
	if _, err := guard.Claim(ctx, "a:b", "evt_01"); err == nil {
		t.Fatal("expected an injectable scope to be rejected")
	}
}

// The default policy is FailOpen: the guard only suppresses rapid duplicates, and the
// authoritative protection is the consumption record, so refusing to process an event
// because the short circuit is down would lose work for no safety gain.
func TestGuardDegradesOpenOnOutage(t *testing.T) {
	guard, commands, _ := newGuardFixture(t, DefaultPolicy(), time.Minute)
	ctx := context.Background()

	owner, err := guard.Claim(ctx, "charge-event", "evt_01")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	commands.SetDown(errors.New("connection refused"))

	// The event must still be processed; the consumption record prevents a business
	// duplicate.
	token, err := guard.Claim(ctx, "charge-event", "evt_02")
	if err != nil {
		t.Fatalf("expected the outage to be hidden, got %v", err)
	}
	if token == "" {
		t.Fatal("expected the event to be processed while degraded")
	}

	// Release must not turn a degraded path into a failure either.
	if err := guard.Release(ctx, "charge-event", "evt_01", owner); err != nil {
		t.Fatalf("expected release to tolerate the outage, got %v", err)
	}
}

func TestGuardFailClosedSurfacesOutage(t *testing.T) {
	policy := DefaultPolicy()
	policy.Idempotency = FailClosed
	guard, commands, _ := newGuardFixture(t, policy, time.Minute)
	ctx := context.Background()

	commands.SetDown(errors.New("connection refused"))
	if _, err := guard.Claim(ctx, "charge-event", "evt_01"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestGuardSurfacesATokenSourceFailure(t *testing.T) {
	commands := NewMemoryCommands()
	guard, err := NewGuard(commands, DefaultPolicy(), NewDegradation(), DefaultGuardConfig(), func() (string, error) {
		return "", errors.New("no entropy")
	})
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}
	if _, err := guard.Claim(context.Background(), "charge-event", "evt_01"); err == nil {
		t.Fatal("expected the token failure to surface")
	}
}

func TestDefaultGuardConfigIsValid(t *testing.T) {
	config := DefaultGuardConfig()
	if err := config.validate(); err != nil {
		t.Fatalf("default guard config must be valid: %v", err)
	}
	if config.TTL <= 0 {
		t.Fatal("expected a positive default ttl")
	}
}
