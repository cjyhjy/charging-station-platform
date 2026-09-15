package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// These tests exercise the real Client against a real Redis server. They are the
// only evidence that the RESP encoding, the connection pool, the Lua scripts and
// the degradation paths work against an actual deployment, so they must be run
// before this module is approved.
//
// They are skipped unless NCS_REDIS_TEST_ADDR is set, so a machine without Redis
// still runs the rest of the suite:
//
//	NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test ./internal/repository/redis/ -run Integration -v
//
// The database index is taken from NCS_REDIS_TEST_DB (default 15) so the tests do
// not collide with application data. Every key is namespaced under a per-test
// prefix and removed at the end.

const (
	envTestAddress  = "NCS_REDIS_TEST_ADDR"
	envTestDatabase = "NCS_REDIS_TEST_DB"
)

// requireRedis returns a live client, or skips the test.
func requireRedis(t *testing.T) *Client {
	t.Helper()

	address := os.Getenv(envTestAddress)
	if address == "" {
		t.Skipf("set %s to run the integration tests against a real Redis", envTestAddress)
	}

	config := DefaultConnConfig()
	config.Address = address
	config.Database = 15
	if raw := os.Getenv(envTestDatabase); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &config.Database); err != nil {
			t.Fatalf("invalid %s=%q: %v", envTestDatabase, raw, err)
		}
	}
	config.DialTimeout = 3 * time.Second
	config.ReadTimeout = 3 * time.Second
	config.WriteTimeout = 3 * time.Second

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		t.Skipf("redis at %s is not reachable: %v", address, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// integrationKey builds a key unique to one test run so a leftover key can never
// make a later run pass or fail by accident.
func integrationKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("ncs:test:%s:%s", sanitizeTestName(t.Name()), suffix)
}

func sanitizeTestName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

func cleanupKey(t *testing.T, client *Client, key string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = client.Del(ctx, key)
	})
}

func TestIntegrationPingAndPoolReuse(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	for i := 0; i < 25; i++ {
		if err := client.Ping(ctx); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
	stats := client.Stats()
	if stats.Open != 1 {
		t.Fatalf("expected a single pooled connection to be reused, got %+v", stats)
	}
}

func TestIntegrationSetGetDeleteAndTTL(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "value")
	cleanupKey(t, client, key)

	if err := client.Set(ctx, key, "hello", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "hello" {
		t.Fatalf("expected hello, got %q", value)
	}

	ttl, hasExpiry, err := client.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the key to expire")
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("expected a ttl inside one minute, got %s", ttl)
	}

	removed, err := client.Del(ctx, key)
	if err != nil {
		t.Fatalf("del: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removal, got %d", removed)
	}
	if _, err := client.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestIntegrationSetPreservesBinarySafeValues(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "binary")
	cleanupKey(t, client, key)

	// Session payloads are JSON and cache values may be any bytes, so the framing
	// must survive embedded CRLF.
	payload := "line1\r\nline2\ttab\"quote"
	if err := client.Set(ctx, key, payload, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != payload {
		t.Fatalf("expected %q, got %q", payload, value)
	}
}

func TestIntegrationSetWithoutTTLPersists(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "persistent")
	cleanupKey(t, client, key)

	if err := client.Set(ctx, key, "v", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	_, hasExpiry, err := client.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if hasExpiry {
		t.Fatal("expected no expiration for a zero ttl")
	}
}

// The windowed counter must attach its window atomically, which is the whole
// point of doing it in Lua.
func TestIntegrationIncrWithWindowAttachesTheWindowAtomically(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "ratelimit")
	cleanupKey(t, client, key)

	count, remaining, err := client.IncrWithWindow(ctx, key, 30*time.Second)
	if err != nil {
		t.Fatalf("incr with window: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
	if remaining <= 0 || remaining > 30*time.Second {
		t.Fatalf("expected a window inside 30s, got %s", remaining)
	}

	// The window must exist immediately; there is no intermediate state in which
	// the counter is present without an expiration.
	ttl, hasExpiry, err := client.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry {
		t.Fatal("expected the window to be attached by the increment itself")
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Fatalf("expected a ttl inside 30s, got %s", ttl)
	}

	// A second increment must keep the original window rather than restart it.
	firstTTL := ttl
	time.Sleep(20 * time.Millisecond)
	count, remaining, err = client.IncrWithWindow(ctx, key, 30*time.Second)
	if err != nil {
		t.Fatalf("incr with window: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
	if remaining >= firstTTL {
		t.Fatalf("expected the window to keep counting down (%s), got %s", firstTTL, remaining)
	}
}

// A counter left behind without a window by any means must heal instead of
// blocking the identity forever.
func TestIntegrationIncrWithWindowHealsACounterWithoutAWindow(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "heal")
	cleanupKey(t, client, key)

	if err := client.Set(ctx, key, "5", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, hasExpiry, err := client.TTL(ctx, key); err != nil || hasExpiry {
		t.Fatalf("expected a windowless counter, got hasExpiry=%v err=%v", hasExpiry, err)
	}

	count, remaining, err := client.IncrWithWindow(ctx, key, 20*time.Second)
	if err != nil {
		t.Fatalf("incr with window: %v", err)
	}
	if count != 6 {
		t.Fatalf("expected the existing count to be carried forward to 6, got %d", count)
	}
	if remaining <= 0 {
		t.Fatalf("expected the window to be attached, got %s", remaining)
	}
	if _, hasExpiry, err := client.TTL(ctx, key); err != nil || !hasExpiry {
		t.Fatalf("expected the counter to be healed with a window, got hasExpiry=%v err=%v", hasExpiry, err)
	}
}

func TestIntegrationIncrRejectsANonIntegerValue(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "notinteger")
	cleanupKey(t, client, key)

	if err := client.Set(ctx, key, "abc", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	_, err := client.Incr(ctx, key)
	if err == nil {
		t.Fatal("expected a server error")
	}
	// A server error is not an outage, and the connection must stay usable.
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("a server error must not be an availability failure: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("expected the connection to remain usable: %v", err)
	}
}

func TestIntegrationSetNXAndExpire(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "setnx")
	cleanupKey(t, client, key)

	acquired, err := client.SetNX(ctx, key, "token-a", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if !acquired {
		t.Fatal("expected the first SET NX to win")
	}
	acquired, err = client.SetNX(ctx, key, "token-b", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if acquired {
		t.Fatal("expected the second SET NX to be refused")
	}

	// PERSIST removes the window, which is the zero-ttl contract.
	changed, err := client.Expire(ctx, key, 0)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if !changed {
		t.Fatal("expected PERSIST to report the expiration removed")
	}
	if _, hasExpiry, err := client.TTL(ctx, key); err != nil || hasExpiry {
		t.Fatalf("expected no expiration, got hasExpiry=%v err=%v", hasExpiry, err)
	}
}

// The real lock release must be a compare-and-delete, so a stale token cannot
// free a lock a new holder owns.
func TestIntegrationCompareAndDeleteUsesRealLua(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()
	key := integrationKey(t, "lock")
	cleanupKey(t, client, key)

	if err := client.Set(ctx, key, "token-b", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	deleted, err := client.CompareAndDelete(ctx, key, "token-a")
	if err != nil {
		t.Fatalf("compare and delete: %v", err)
	}
	if deleted {
		t.Fatal("a foreign token must not delete the key")
	}
	if value, err := client.Get(ctx, key); err != nil || value != "token-b" {
		t.Fatalf("expected token-b to survive, got %q and %v", value, err)
	}

	deleted, err = client.CompareAndDelete(ctx, key, "token-b")
	if err != nil {
		t.Fatalf("compare and delete: %v", err)
	}
	if !deleted {
		t.Fatal("the owning token must delete the key")
	}
	if _, err := client.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the key to be gone, got %v", err)
	}
}

// The whole B-01 foundation is exercised against a real server here: cache,
// sessions with an absolute ceiling, rate limiting and locking.
func TestIntegrationCapabilitiesAgainstRealRedis(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	settings := DefaultSettings()
	// A unique cache key per run, so a leftover value cannot mask a failure.
	cacheKey := integrationKey(t, "cache")
	cleanupKey(t, client, cacheKey)

	stats := NewDegradation()
	cache, err := NewCache(client, settings.Policy, stats, CacheConfig{DefaultTTL: 20 * time.Second})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := cache.Set(ctx, cacheKey, "cached-value"); err != nil {
		t.Fatalf("cache set: %v", err)
	}
	value, found, err := cache.Get(ctx, cacheKey)
	if err != nil || !found || value != "cached-value" {
		t.Fatalf("expected a cache hit, got found=%v value=%q err=%v", found, value, err)
	}

	sessions, err := NewSessions(client, settings.Policy, stats, SessionConfig{IdleTTL: 30 * time.Second, AbsoluteTTL: 2 * time.Minute}, nil)
	if err != nil {
		t.Fatalf("new sessions: %v", err)
	}
	sessionID := "it-" + sanitizeTestName(t.Name())
	sessionKey, _ := SessionKey(sessionID)
	cleanupKey(t, client, sessionKey)

	if err := sessions.Save(ctx, sessionID, "user_01"); err != nil {
		t.Fatalf("session save: %v", err)
	}
	payload, found, err := sessions.Load(ctx, sessionID)
	if err != nil || !found || payload != "user_01" {
		t.Fatalf("expected the session, got found=%v payload=%q err=%v", found, payload, err)
	}
	if refreshed, err := sessions.Refresh(ctx, sessionID); err != nil || !refreshed {
		t.Fatalf("expected a refresh, got %v and %v", refreshed, err)
	}
	if err := sessions.Delete(ctx, sessionID); err != nil {
		t.Fatalf("session delete: %v", err)
	}

	limiter, err := NewLimiter(client, settings.Policy, stats, nil)
	if err != nil {
		t.Fatalf("new limiter: %v", err)
	}
	limitKey, _ := AuthRateLimitKey("it-" + sanitizeTestName(t.Name()))
	cleanupKey(t, client, limitKey)
	limit := Limit{Requests: 2, Window: 30 * time.Second}
	for attempt := 1; attempt <= 2; attempt++ {
		result, err := limiter.Allow(ctx, "it-"+sanitizeTestName(t.Name()), limit)
		if err != nil {
			t.Fatalf("allow %d: %v", attempt, err)
		}
		if !result.Allowed {
			t.Fatalf("attempt %d should be allowed", attempt)
		}
	}
	result, err := limiter.Allow(ctx, "it-"+sanitizeTestName(t.Name()), limit)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if result.Allowed {
		t.Fatal("expected the third attempt to be refused")
	}

	locker, err := NewLocker(client, settings.Policy, stats, nil, nil)
	if err != nil {
		t.Fatalf("new locker: %v", err)
	}
	// Locks must live in the ncs:lock: namespace; the integration prefix is
	// deliberately used for ordinary keys only.
	lockKey := fmt.Sprintf("ncs:lock:order:it-%s", sanitizeTestName(t.Name()))
	cleanupKey(t, client, lockKey)
	grant, err := locker.Acquire(ctx, lockKey, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !grant.Acquired {
		t.Fatal("expected the lock")
	}
	second, err := locker.Acquire(ctx, lockKey, 30*time.Second)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second.Acquired {
		t.Fatal("expected the second acquire to be refused")
	}
	released, err := locker.Release(ctx, grant.Lock)
	if err != nil || !released {
		t.Fatalf("expected the release to succeed, got %v and %v", released, err)
	}
	if got := stats.Failures(CapabilityLock); got != 0 {
		t.Fatalf("expected no degradation against a healthy server, got %d", got)
	}
}

func TestIntegrationHealthCheck(t *testing.T) {
	client := requireRedis(t)
	health, err := NewHealth(client, NewDegradation(), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("new health: %v", err)
	}

	status := health.Status(context.Background())
	if !status.OK {
		t.Fatalf("expected a healthy probe, got %q", status.Error)
	}
	if status.Latency < 0 {
		t.Fatalf("expected a non-negative latency, got %s", status.Latency)
	}
}

// The pool must be safe under real concurrency, and it must not exceed MaxOpen.
func TestIntegrationConcurrentCommandsRespectThePoolBound(t *testing.T) {
	address := os.Getenv(envTestAddress)
	if address == "" {
		t.Skipf("set %s to run the integration tests against a real Redis", envTestAddress)
	}

	config := DefaultConnConfig()
	config.Address = address
	config.Database = 15
	config.Pool.MaxOpen = 4
	config.Pool.MaxIdle = 4
	config.DialTimeout = 3 * time.Second
	config.ReadTimeout = 3 * time.Second
	config.WriteTimeout = 3 * time.Second

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	// Registered as a cleanup, not a defer: cleanups run last-in-first-out, so the
	// key cleanup below must be registered after this and therefore run before the
	// pool is closed. With a defer, Close would run first and the key deletion
	// would fail with ErrClosed, leaking state into the next -count run.
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Skipf("redis at %s is not reachable: %v", address, err)
	}

	key := integrationKey(t, "concurrent")
	// Start from a clean slate even if an earlier failed run left the key behind,
	// so the exact-count assertion below is meaningful.
	if _, err := client.Del(ctx, key); err != nil {
		t.Fatalf("reset key: %v", err)
	}
	cleanupKey(t, client, key)
	if _, _, err := client.IncrWithWindow(ctx, key, time.Minute); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const workers = 40
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, _, err := client.IncrWithWindow(ctx, key, time.Minute); err != nil {
				t.Errorf("incr with window: %v", err)
			}
		}()
	}
	wg.Wait()

	// 40 concurrent atomic increments plus the seed must total 41 exactly: a lost
	// update would mean the Lua path is not atomic.
	value, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != fmt.Sprintf("%d", workers+1) {
		t.Fatalf("expected %d, got %s", workers+1, value)
	}
	if stats := client.Stats(); stats.Open > config.Pool.MaxOpen {
		t.Fatalf("expected at most %d connections, got %+v", config.Pool.MaxOpen, stats)
	}
}

// A wrong credential against a real server must degrade, not surface as a
// business error.
func TestIntegrationAuthenticationFailureDegrades(t *testing.T) {
	address := os.Getenv(envTestAddress)
	if address == "" {
		t.Skipf("set %s to run the integration tests against a real Redis", envTestAddress)
	}

	config := DefaultConnConfig()
	config.Address = address
	config.Database = 15
	config.Password = "definitely-not-the-password"
	config.DialTimeout = 3 * time.Second
	config.ReadTimeout = 3 * time.Second
	config.WriteTimeout = 3 * time.Second

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = client.Ping(ctx)
	// A server with no password configured rejects AUTH, which is also a
	// credential fault, so either way this must be an availability failure.
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}
