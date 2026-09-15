package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryCommandsGetReportsMissingKeyAsNotFound(t *testing.T) {
	commands := NewMemoryCommands()
	_, err := commands.Get(context.Background(), "ncs:session:absent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryCommandsSetGetDelete(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	if err := commands.Set(ctx, "ncs:station:st_01", "value", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, err := commands.Get(ctx, "ncs:station:st_01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "value" {
		t.Fatalf("expected value %q, got %q", "value", value)
	}

	removed, err := commands.Del(ctx, "ncs:station:st_01", "ncs:station:missing")
	if err != nil {
		t.Fatalf("del: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removal, got %d", removed)
	}
	if commands.Len() != 0 {
		t.Fatalf("expected an empty store, got %d entries", commands.Len())
	}
}

func TestMemoryCommandsRejectsNegativeTTLAndEmptyKey(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	if err := commands.Set(ctx, "ncs:charger:ch_01", "v", -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL from set, got %v", err)
	}
	if _, err := commands.Expire(ctx, "ncs:charger:ch_01", -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL from expire, got %v", err)
	}
	if err := commands.Set(ctx, "", "v", time.Minute); err == nil {
		t.Fatal("expected an error for an empty key")
	}
	if _, err := commands.Get(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}

func TestMemoryCommandsExpiryFollowsInjectedClock(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)

	if err := commands.Set(ctx, "ncs:station:st_01", "v", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	clock.Advance(59 * time.Second)
	if _, err := commands.Get(ctx, "ncs:station:st_01"); err != nil {
		t.Fatalf("value should still be present before the ttl elapses: %v", err)
	}

	// Exactly at the deadline the key is gone: expiry is not inclusive.
	clock.Advance(time.Second)
	if _, err := commands.Get(ctx, "ncs:station:st_01"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the key to expire, got %v", err)
	}
	if commands.Len() != 0 {
		t.Fatalf("expected the expired entry to be dropped, got %d entries", commands.Len())
	}
}

func TestMemoryCommandsSetWithoutTTLPersists(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)

	if err := commands.Set(ctx, "ncs:lock:order:o_01", "token", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	clock.Advance(24 * time.Hour)
	if _, err := commands.Get(ctx, "ncs:lock:order:o_01"); err != nil {
		t.Fatalf("a zero ttl must not expire the key: %v", err)
	}

	ttl, hasExpiry, err := commands.TTL(ctx, "ncs:lock:order:o_01")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if hasExpiry {
		t.Fatalf("expected no expiration, got ttl %s", ttl)
	}
}

func TestMemoryCommandsIncrCreatesCounterAndKeepsWindow(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)

	for expected := int64(1); expected <= 3; expected++ {
		got, err := commands.Incr(ctx, "ncs:auth:rate-limit:id_01")
		if err != nil {
			t.Fatalf("incr: %v", err)
		}
		if got != expected {
			t.Fatalf("expected counter %d, got %d", expected, got)
		}
	}

	// The window is attached after the first increment; a later increment must
	// not reset it, otherwise a busy attacker would never hit the limit.
	if _, err := commands.Expire(ctx, "ncs:auth:rate-limit:id_01", time.Minute); err != nil {
		t.Fatalf("expire: %v", err)
	}
	clock.Advance(10 * time.Second)
	if _, err := commands.Incr(ctx, "ncs:auth:rate-limit:id_01"); err != nil {
		t.Fatalf("incr: %v", err)
	}
	ttl, hasExpiry, err := commands.TTL(ctx, "ncs:auth:rate-limit:id_01")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the counter to keep its window")
	}
	if ttl != 50*time.Second {
		t.Fatalf("expected the window to keep counting down (50s), got %s", ttl)
	}
}

func TestMemoryCommandsIncrRejectsNonIntegerValue(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()
	if err := commands.Set(ctx, "ncs:auth:rate-limit:id_01", "not-a-number", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := commands.Incr(ctx, "ncs:auth:rate-limit:id_01"); err == nil {
		t.Fatal("expected an error when the stored value is not an integer")
	}
}

func TestMemoryCommandsExpireOnMissingKeyReportsFalse(t *testing.T) {
	commands := NewMemoryCommands()
	changed, err := commands.Expire(context.Background(), "ncs:session:absent", time.Minute)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if changed {
		t.Fatal("expected expire to report false for a missing key")
	}
}

func TestMemoryCommandsTTLDistinguishesMissingFromNoExpiry(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)

	if _, _, err := commands.TTL(ctx, "ncs:session:absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := commands.Set(ctx, "ncs:session:s_01", "v", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	ttl, hasExpiry, err := commands.TTL(ctx, "ncs:session:s_01")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl != time.Minute {
		t.Fatalf("expected a 1m expiry, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}

	// A never-expiring key must report hasExpiry=false with no error, so callers
	// never confuse it with a missing key.
	if err := commands.Set(ctx, "ncs:session:s_02", "v", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	ttl, hasExpiry, err = commands.TTL(ctx, "ncs:session:s_02")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if hasExpiry || ttl != 0 {
		t.Fatalf("expected no expiry reported, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}
}

func TestMemoryCommandsSetNXOnlyFirstWriterWins(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	acquired, err := commands.SetNX(ctx, "ncs:lock:order:o_01", "token-a", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if !acquired {
		t.Fatal("expected the first writer to acquire the key")
	}

	acquired, err = commands.SetNX(ctx, "ncs:lock:order:o_01", "token-b", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if acquired {
		t.Fatal("expected the second writer to be refused")
	}

	value, err := commands.Get(ctx, "ncs:lock:order:o_01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "token-a" {
		t.Fatalf("expected the first token to survive, got %q", value)
	}
}

func TestMemoryCommandsSetNXTreatsExpiredKeyAsFree(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)

	if _, err := commands.SetNX(ctx, "ncs:lock:order:o_01", "token-a", time.Minute); err != nil {
		t.Fatalf("setnx: %v", err)
	}
	clock.Advance(2 * time.Minute)

	acquired, err := commands.SetNX(ctx, "ncs:lock:order:o_01", "token-b", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if !acquired {
		t.Fatal("expected an expired lock to be reacquirable")
	}
}

func TestMemoryCommandsCompareAndDeleteRequiresMatchingToken(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()
	if err := commands.Set(ctx, "ncs:lock:order:o_01", "token-a", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	deleted, err := commands.CompareAndDelete(ctx, "ncs:lock:order:o_01", "token-b")
	if err != nil {
		t.Fatalf("compare and delete: %v", err)
	}
	if deleted {
		t.Fatal("a foreign token must not delete the key")
	}

	deleted, err = commands.CompareAndDelete(ctx, "ncs:lock:order:o_01", "token-a")
	if err != nil {
		t.Fatalf("compare and delete: %v", err)
	}
	if !deleted {
		t.Fatal("the owning token must delete the key")
	}
}

func TestMemoryCommandsUnavailableAndClosed(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	commands.SetDown(errors.New("connection refused"))
	_, err := commands.Get(ctx, "ncs:station:st_01")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	if err := commands.Ping(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ping to report ErrUnavailable, got %v", err)
	}

	commands.SetDown(nil)
	if err := commands.Ping(ctx); err != nil {
		t.Fatalf("expected ping to recover: %v", err)
	}

	if err := commands.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := commands.Ping(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after close, got %v", err)
	}
}

func TestMemoryCommandsHonoursContextCancellation(t *testing.T) {
	commands := NewMemoryCommands()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := commands.Get(ctx, "ncs:station:st_01"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if err := commands.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from ping, got %v", err)
	}
}

// The two concurrency tests below are the reason this store guards every command
// with a mutex: the limiter and the locker both depend on INCR and SetNX being
// atomic. Run with -race.
func TestMemoryCommandsConcurrentIncrIsAtomic(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := commands.Incr(ctx, "ncs:auth:rate-limit:shared"); err != nil {
				t.Errorf("incr: %v", err)
			}
		}()
	}
	wg.Wait()

	value, err := commands.Get(ctx, "ncs:auth:rate-limit:shared")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != fmt.Sprintf("%d", workers) {
		t.Fatalf("expected counter %d, got %s", workers, value)
	}
}

func TestMemoryCommandsConcurrentSetNXHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()

	const workers = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wg.Done()
			acquired, err := commands.SetNX(ctx, "ncs:lock:order:o_01", fmt.Sprintf("token-%d", index), time.Minute)
			if err != nil {
				t.Errorf("setnx: %v", err)
				return
			}
			if acquired {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly one lock winner, got %d", winners)
	}
}
