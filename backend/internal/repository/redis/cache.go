package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CacheConfig tunes the read-through cache.
type CacheConfig struct {
	// DefaultTTL bounds how long a cached value may be stale. It is required:
	// an unbounded cache entry would let Redis drift into being a second source
	// of truth, which section 4.3 of the parallel-development contract forbids.
	DefaultTTL time.Duration
}

// DefaultCacheConfig returns a short TTL suitable for station and charger
// listings that change rarely but must not stay stale for long.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{DefaultTTL: 30 * time.Second}
}

func (c CacheConfig) validate() error {
	if c.DefaultTTL <= 0 {
		return fmt.Errorf("cache default ttl must be greater than zero")
	}
	return nil
}

// Cache is the B-line cache abstraction that the A-line domain modules consume
// instead of talking to Redis directly (approval rule for BE-A-03 in
// docs/migration/backend-parallel-development.md section 5).
//
// Values are opaque strings so the cache never owns a domain schema; use
// SetJSON and GetJSON for structured payloads. Keys must come from the key
// builders in keys.go so the frozen naming baseline stays enforceable in one
// place.
//
// A miss is not an error. Under the default FailOpen policy a cache read that
// cannot reach Redis also reports a miss, so the caller falls through to
// PostgreSQL and never fails a request because the cache is down.
type Cache struct {
	commands Commands
	observer observer
	config   CacheConfig
}

// NewCache validates its inputs. A nil stats registry is replaced with a fresh
// one so callers only pass what they need.
func NewCache(commands Commands, policy Policy, stats *Degradation, config CacheConfig) (*Cache, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Cache{commands: commands, observer: newObserver(policy, stats), config: config}, nil
}

type cacheValue struct {
	value string
	found bool
}

// Get reads a key. found is false for both a genuine miss and a degraded read;
// ctx and application errors are still returned.
func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	if key == "" {
		return "", false, fmt.Errorf("cache get: key is required")
	}
	result, err := run(ctx, c.observer, CapabilityCache, cacheValue{}, func() (cacheValue, error) {
		raw, err := c.commands.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return cacheValue{}, nil
		}
		if err != nil {
			return cacheValue{}, err
		}
		return cacheValue{value: raw, found: true}, nil
	})
	if err != nil {
		return "", false, err
	}
	return result.value, result.found, nil
}

// Set writes a key with the configured default TTL.
func (c *Cache) Set(ctx context.Context, key, value string) error {
	return c.SetWithTTL(ctx, key, value, c.config.DefaultTTL)
}

// SetWithTTL writes a key with an explicit TTL. A zero TTL is rejected: every
// cache entry must expire.
func (c *Cache) SetWithTTL(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("cache set: key is required")
	}
	if ttl <= 0 {
		return fmt.Errorf("cache set: ttl must be greater than zero")
	}
	_, err := run(ctx, c.observer, CapabilityCache, struct{}{}, func() (struct{}, error) {
		return struct{}{}, c.commands.Set(ctx, key, value, ttl)
	})
	return err
}

// Delete removes keys. Under FailOpen a failure is reported as zero deletions
// rather than an error: a stale entry expires on its own.
func (c *Cache) Delete(ctx context.Context, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if key == "" {
			return 0, fmt.Errorf("cache delete: key is required")
		}
	}
	return run(ctx, c.observer, CapabilityCache, 0, func() (int, error) {
		return c.commands.Del(ctx, keys...)
	})
}

// SetJSON marshals a value and stores it with the default TTL.
func (c *Cache) SetJSON(ctx context.Context, key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache set json %s: %w", key, err)
	}
	return c.Set(ctx, key, string(encoded))
}

// GetJSON reads a key and unmarshals it into target. It reports found=false when
// the key is absent, so a caller can distinguish a miss from a decode failure.
func (c *Cache) GetJSON(ctx context.Context, key string, target any) (bool, error) {
	raw, found, err := c.Get(ctx, key)
	if err != nil || !found {
		return false, err
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return false, fmt.Errorf("cache get json %s: %w", key, err)
	}
	return true, nil
}
