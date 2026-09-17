package redis

import (
	"context"
	"testing"
	"time"
)

// These tests pin the behaviour of the optional constructor arguments: every
// collaborator that may be nil must fall back to a working production default
// rather than producing a type that panics or silently never expires.

func TestMemoryCommandsSetClockNilRestoresWallClock(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	// Anchoring far in the future keeps the entry live long enough that the
	// switch back to the wall clock cannot expire it by accident.
	future := newTestClockAt(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	commands.SetClock(future.Now)

	if err := commands.Set(ctx, "ncs:station:st_01", "v", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Detaching the injected clock must not detach expiry altogether.
	commands.SetClock(nil)
	if _, err := commands.Get(ctx, "ncs:station:st_01"); err != nil {
		t.Fatalf("expected the entry to survive a clock reset: %v", err)
	}

	// A fresh write must now be bounded by the wall clock, which is what proves
	// the default clock is really in use.
	if err := commands.Set(ctx, "ncs:station:st_02", "v", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	ttl, hasExpiry, err := commands.TTL(ctx, "ncs:station:st_02")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("expected a wall-clock ttl inside one minute, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}
}

func TestNewLimiterFallsBackToWallClock(t *testing.T) {
	ctx := context.Background()
	limiter, err := NewLimiter(NewMemoryCommands(), DefaultPolicy(), nil, nil)
	if err != nil {
		t.Fatalf("new limiter: %v", err)
	}

	result, err := limiter.Allow(ctx, "id_01", Limit{Requests: 2, Window: time.Minute})
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !result.Allowed {
		t.Fatal("expected the first attempt to be allowed")
	}
	// A wall-clock ResetAt must be in the future, which is what proves the
	// default clock was actually used.
	if !result.ResetAt.After(time.Now().UTC().Add(-time.Second)) {
		t.Fatalf("expected a future reset time, got %s", result.ResetAt)
	}
}

func TestNewLockerFallsBackToProductionTokensAndClock(t *testing.T) {
	ctx := context.Background()
	locker, err := NewLocker(NewMemoryCommands(), DefaultPolicy(), nil, nil, nil)
	if err != nil {
		t.Fatalf("new locker: %v", err)
	}
	key, _ := OrderLockKey("o_01")

	grant, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !grant.Acquired {
		t.Fatal("expected the acquire to succeed")
	}
	if len(grant.Lock.Token) != 32 {
		t.Fatalf("expected a 32 character production token, got %q", grant.Lock.Token)
	}
	if !grant.Lock.Until.After(time.Now().UTC().Add(-time.Second)) {
		t.Fatalf("expected a future deadline from the wall clock, got %s", grant.Lock.Until)
	}
}

func TestNewHealthFallsBackToWallClock(t *testing.T) {
	ctx := context.Background()
	health, err := NewHealth(NewMemoryCommands(), nil, 0, nil)
	if err != nil {
		t.Fatalf("new health: %v", err)
	}
	status := health.Status(ctx)
	if !status.OK {
		t.Fatalf("expected a healthy probe, got %q", status.Error)
	}
	if status.CheckedAt.IsZero() {
		t.Fatal("expected the check to be timestamped")
	}
}

func TestNewCacheAcceptsNilStatsRegistry(t *testing.T) {
	// A caller that does not care about counters must not have to build one.
	cache, err := NewCache(NewMemoryCommands(), DefaultPolicy(), nil, DefaultCacheConfig())
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if cache.observer.stats == nil {
		t.Fatal("expected an internal stats registry")
	}
}

func TestNewSessionsAcceptsNilStatsRegistry(t *testing.T) {
	sessions, err := NewSessions(NewMemoryCommands(), DefaultPolicy(), nil, DefaultSessionConfig(), nil)
	if err != nil {
		t.Fatalf("new sessions: %v", err)
	}
	if sessions.observer.stats == nil {
		t.Fatal("expected an internal stats registry")
	}
}

// A value that cannot be marshalled must be reported, not silently stored, so a
// caller cannot believe it cached something it did not.
func TestCacheSetJSONReportsMarshalFailure(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("st_01")

	if err := cache.SetJSON(ctx, key, make(chan int)); err == nil {
		t.Fatal("expected a marshal failure to surface")
	}
	if _, found, err := cache.Get(ctx, key); err != nil || found {
		t.Fatalf("a failed marshal must not store anything, got found=%v err=%v", found, err)
	}
}
