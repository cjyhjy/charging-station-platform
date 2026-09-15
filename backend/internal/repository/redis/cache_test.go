package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestCache(t *testing.T, commands Commands, policy Policy, stats *Degradation) *Cache {
	t.Helper()
	cache, err := NewCache(commands, policy, stats, DefaultCacheConfig())
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	return cache
}

func TestNewCacheValidatesInputs(t *testing.T) {
	tests := []struct {
		name     string
		commands Commands
		policy   Policy
		config   CacheConfig
	}{
		{"nil commands", nil, DefaultPolicy(), DefaultCacheConfig()},
		{"invalid policy", NewMemoryCommands(), Policy{Cache: FailMode(9)}, DefaultCacheConfig()},
		{"zero ttl", NewMemoryCommands(), DefaultPolicy(), CacheConfig{DefaultTTL: 0}},
		{"negative ttl", NewMemoryCommands(), DefaultPolicy(), CacheConfig{DefaultTTL: -time.Second}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewCache(test.commands, test.policy, nil, test.config); err == nil {
				t.Fatal("expected the configuration to be rejected")
			}
		})
	}
}

func TestCacheSetGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, err := StationKey("st_01")
	if err != nil {
		t.Fatalf("station key: %v", err)
	}

	if err := cache.Set(ctx, key, "station-payload"); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, found, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found || value != "station-payload" {
		t.Fatalf("expected a hit, got found=%v value=%q", found, value)
	}
}

// A miss is a normal outcome. Callers must be able to fall through to PostgreSQL
// without treating absence as an error.
func TestCacheGetMissIsNotAnError(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("absent")

	value, found, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected no error for a miss, got %v", err)
	}
	if found {
		t.Fatalf("expected a miss, got %q", value)
	}
}

func TestCacheEntriesExpire(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	cache, err := NewCache(commands, DefaultPolicy(), nil, CacheConfig{DefaultTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	key, _ := ChargerKey("ch_01")

	if err := cache.Set(ctx, key, "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	clock.Advance(31 * time.Second)

	if _, found, err := cache.Get(ctx, key); err != nil || found {
		t.Fatalf("expected the entry to expire, got found=%v err=%v", found, err)
	}
}

func TestCacheRejectsUnboundedEntry(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("st_01")

	// An entry that never expires would let Redis become a second source of
	// truth, which the contract forbids.
	if err := cache.SetWithTTL(ctx, key, "v", 0); err == nil {
		t.Fatal("expected a zero ttl to be rejected")
	}
	if err := cache.SetWithTTL(ctx, key, "v", -time.Second); err == nil {
		t.Fatal("expected a negative ttl to be rejected")
	}
}

func TestCacheRejectsEmptyKey(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)

	if _, _, err := cache.Get(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty key")
	}
	if err := cache.Set(ctx, "", "v"); err == nil {
		t.Fatal("expected an error for an empty key")
	}
	if _, err := cache.Delete(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}

func TestCacheDelete(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()
	cache := newTestCache(t, commands, DefaultPolicy(), nil)
	stationKey, _ := StationKey("st_01")
	chargerKey, _ := ChargerKey("ch_01")

	if err := cache.Set(ctx, stationKey, "a"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := cache.Set(ctx, chargerKey, "b"); err != nil {
		t.Fatalf("set: %v", err)
	}

	removed, err := cache.Delete(ctx, stationKey, chargerKey)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 removals, got %d", removed)
	}
	if got, err := cache.Delete(ctx); err != nil || got != 0 {
		t.Fatalf("expected deleting nothing to be a no-op, got %d and %v", got, err)
	}
}

type stationSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestCacheJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("st_01")
	want := stationSummary{ID: "st_01", Name: "North Plaza"}

	if err := cache.SetJSON(ctx, key, want); err != nil {
		t.Fatalf("set json: %v", err)
	}
	var got stationSummary
	found, err := cache.GetJSON(ctx, key, &got)
	if err != nil {
		t.Fatalf("get json: %v", err)
	}
	if !found {
		t.Fatal("expected a hit")
	}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestCacheGetJSONReportsMissWithoutDecoding(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("absent")

	var target stationSummary
	found, err := cache.GetJSON(ctx, key, &target)
	if err != nil {
		t.Fatalf("expected no error on a miss, got %v", err)
	}
	if found {
		t.Fatal("expected a miss")
	}
}

func TestCacheReportsDecodeFailure(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, NewMemoryCommands(), DefaultPolicy(), nil)
	key, _ := StationKey("st_01")
	if err := cache.Set(ctx, key, "not-json"); err != nil {
		t.Fatalf("set: %v", err)
	}

	var target stationSummary
	found, err := cache.GetJSON(ctx, key, &target)
	if err == nil {
		t.Fatal("expected a decode error")
	}
	if found {
		t.Fatal("a decode failure must not report a hit")
	}
}

// The default cache policy is FailOpen: a Redis outage must cost latency, not
// correctness, because PostgreSQL remains authoritative.
func TestCacheDegradesToMissWhenRedisIsDown(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands := NewMemoryCommands()
	cache := newTestCache(t, commands, DefaultPolicy(), stats)
	key, _ := StationKey("st_01")

	if err := cache.Set(ctx, key, "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	commands.SetDown(errors.New("connection refused"))

	value, found, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected a degraded read to look like a miss, got %v", err)
	}
	if found || value != "" {
		t.Fatalf("expected a miss, got found=%v value=%q", found, value)
	}
	if got := stats.Failures(CapabilityCache); got != 1 {
		t.Fatalf("expected the hidden failure to be counted, got %d", got)
	}
}

func TestCacheSwallowsWriteFailureWhenDegraded(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands := NewMemoryCommands()
	cache := newTestCache(t, commands, DefaultPolicy(), stats)
	key, _ := StationKey("st_01")

	commands.SetDown(errors.New("connection refused"))
	if err := cache.Set(ctx, key, "v"); err != nil {
		t.Fatalf("a dropped write must not fail the request: %v", err)
	}
	if got := stats.Failures(CapabilityCache); got != 1 {
		t.Fatalf("expected the dropped write to be counted, got %d", got)
	}
}

func TestCacheFailClosedSurfacesOutage(t *testing.T) {
	ctx := context.Background()
	policy := DefaultPolicy()
	policy.Cache = FailClosed
	commands := NewMemoryCommands()
	cache := newTestCache(t, commands, policy, nil)
	key, _ := StationKey("st_01")

	commands.SetDown(errors.New("connection refused"))
	if _, _, err := cache.Get(ctx, key); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable under FailClosed, got %v", err)
	}
}
