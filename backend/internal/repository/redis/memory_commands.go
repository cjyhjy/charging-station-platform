package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// MemoryCommands is a deterministic, concurrency-safe Commands implementation
// for unit tests and local smoke runs. It models the semantics the B line
// depends on: lazy expiry against an injectable clock, atomic INCR, SetNX,
// compare-and-delete, and an injectable availability failure so degradation
// paths can be tested without stopping a real server.
//
// It intentionally does not simulate a network, a pool or serialization: those
// are adapter concerns. Tests that need a real Redis server belong to the
// integration suite, not to this package.
type MemoryCommands struct {
	mu      sync.Mutex
	entries map[string]*memoryEntry
	clock   func() time.Time
	down    error
	closed  bool
}

type memoryEntry struct {
	value     string
	expiresAt time.Time // zero means no expiration
}

// NewMemoryCommands returns an empty store using the wall clock.
func NewMemoryCommands() *MemoryCommands {
	return &MemoryCommands{
		entries: make(map[string]*memoryEntry),
		clock:   func() time.Time { return time.Now().UTC() },
	}
}

var _ Commands = (*MemoryCommands)(nil)

// SetClock replaces the clock so tests can advance TTLs without sleeping. A nil
// clock restores the wall clock.
func (m *MemoryCommands) SetClock(clock func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if clock == nil {
		m.clock = func() time.Time { return time.Now().UTC() }
		return
	}
	m.clock = clock
}

// Time control belongs to the caller: a test injects a clock it owns through
// SetClock and moves that clock itself, so this store never guesses a time
// base.

// SetDown makes every subsequent command fail as if Redis were unreachable.
// Passing nil restores normal operation.
func (m *MemoryCommands) SetDown(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = err
}

// Len reports how many unexpired keys are stored. It exists for assertions.
func (m *MemoryCommands) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for key := range m.entries {
		if _, ok := m.lookup(key); ok {
			count++
		}
	}
	return count
}

// now returns the current time. The caller must hold the lock.
func (m *MemoryCommands) now() time.Time { return m.clock() }

// lookup returns the live entry for key, dropping it when expired. The caller
// must hold the lock.
func (m *MemoryCommands) lookup(key string) (*memoryEntry, bool) {
	entry, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && !m.now().Before(entry.expiresAt) {
		delete(m.entries, key)
		return nil, false
	}
	return entry, true
}

// guard enforces availability and liveness. The caller must hold the lock.
func (m *MemoryCommands) guard(operation string) error {
	if m.closed {
		return fmt.Errorf("%s: %w", operation, ErrClosed)
	}
	if m.down != nil {
		return fmt.Errorf("%s: %w: %w", operation, ErrUnavailable, m.down)
	}
	return nil
}

func (m *MemoryCommands) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.guard("ping")
}

func (m *MemoryCommands) Get(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if key == "" {
		return "", fmt.Errorf("get: key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("get"); err != nil {
		return "", err
	}
	entry, ok := m.lookup(key)
	if !ok {
		return "", fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	return entry.value, nil
}

func (m *MemoryCommands) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("set: key is required")
	}
	if ttl < 0 {
		return fmt.Errorf("set %s: %w", key, ErrInvalidTTL)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("set"); err != nil {
		return err
	}
	m.store(key, value, ttl)
	return nil
}

// store writes an entry. The caller must hold the lock.
func (m *MemoryCommands) store(key, value string, ttl time.Duration) {
	entry := &memoryEntry{value: value}
	if ttl > 0 {
		entry.expiresAt = m.now().Add(ttl)
	}
	m.entries[key] = entry
}

func (m *MemoryCommands) Del(ctx context.Context, keys ...string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("del"); err != nil {
		return 0, err
	}
	removed := 0
	for _, key := range keys {
		if _, ok := m.lookup(key); ok {
			delete(m.entries, key)
			removed++
		}
	}
	return removed, nil
}

func (m *MemoryCommands) Incr(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if key == "" {
		return 0, fmt.Errorf("incr: key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("incr"); err != nil {
		return 0, err
	}
	entry, ok := m.lookup(key)
	if !ok {
		// A fresh counter keeps no TTL. Any counter that must expire inside a
		// window has to use IncrWithWindow; this primitive is for callers that
		// manage the lifetime themselves.
		m.entries[key] = &memoryEntry{value: "1"}
		return 1, nil
	}
	current, err := parseCounter(entry.value)
	if err != nil {
		return 0, fmt.Errorf("incr %s: %w", key, err)
	}
	next := current + 1
	entry.value = formatCounter(next)
	return next, nil
}

// IncrWithWindow increments a counter and attaches its window in one step, so no
// interleaving or failure can leave a counter without an expiration. The whole
// operation runs under the store mutex, which is what makes it atomic here.
func (m *MemoryCommands) IncrWithWindow(ctx context.Context, key string, window time.Duration) (int64, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if key == "" {
		return 0, 0, fmt.Errorf("incr with window: key is required")
	}
	if window <= 0 {
		return 0, 0, fmt.Errorf("incr with window: window must be greater than zero")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("incr with window"); err != nil {
		return 0, 0, err
	}

	entry, ok := m.lookup(key)
	if !ok {
		m.entries[key] = &memoryEntry{value: "1", expiresAt: m.now().Add(window)}
		return 1, window, nil
	}
	current, err := parseCounter(entry.value)
	if err != nil {
		return 0, 0, fmt.Errorf("incr with window %s: %w", key, err)
	}
	next := current + 1
	entry.value = formatCounter(next)

	// Self-heal a counter that exists without a window, so an identity can never
	// be blocked forever by a key left behind by an older client.
	if entry.expiresAt.IsZero() {
		entry.expiresAt = m.now().Add(window)
		return next, window, nil
	}
	remaining := entry.expiresAt.Sub(m.now())
	if remaining < 0 {
		remaining = 0
	}
	return next, remaining, nil
}

func (m *MemoryCommands) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl < 0 {
		return false, fmt.Errorf("expire %s: %w", key, ErrInvalidTTL)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("expire"); err != nil {
		return false, err
	}
	entry, ok := m.lookup(key)
	if !ok {
		return false, nil
	}
	if ttl == 0 {
		entry.expiresAt = time.Time{}
		return true, nil
	}
	entry.expiresAt = m.now().Add(ttl)
	return true, nil
}

func (m *MemoryCommands) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key == "" {
		return false, fmt.Errorf("setnx: key is required")
	}
	if ttl < 0 {
		return false, fmt.Errorf("setnx %s: %w", key, ErrInvalidTTL)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("setnx"); err != nil {
		return false, err
	}
	if _, ok := m.lookup(key); ok {
		return false, nil
	}
	m.store(key, value, ttl)
	return true, nil
}

func (m *MemoryCommands) CompareAndDelete(ctx context.Context, key, expected string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key == "" {
		return false, fmt.Errorf("compare and delete: key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("compare and delete"); err != nil {
		return false, err
	}
	entry, ok := m.lookup(key)
	if !ok || entry.value != expected {
		return false, nil
	}
	delete(m.entries, key)
	return true, nil
}

// RunScript is not supported by the in-memory commands: the scripted
// semantics (atomic multi-key verify/void) are exercised against a real
// Redis in integration tests. In-memory tests use capability-specific fakes.
func (m *MemoryCommands) RunScript(context.Context, string, []string, []string) (any, error) {
	return nil, fmt.Errorf("memory commands do not support RunScript")
}

func (m *MemoryCommands) TTL(ctx context.Context, key string) (time.Duration, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if key == "" {
		return 0, false, fmt.Errorf("ttl: key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.guard("ttl"); err != nil {
		return 0, false, err
	}
	entry, ok := m.lookup(key)
	if !ok {
		return 0, false, fmt.Errorf("ttl %s: %w", key, ErrNotFound)
	}
	if entry.expiresAt.IsZero() {
		return 0, false, nil
	}
	remaining := entry.expiresAt.Sub(m.now())
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true, nil
}

func (m *MemoryCommands) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func parseCounter(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value %q is not an integer", raw)
	}
	return value, nil
}

func formatCounter(value int64) string { return strconv.FormatInt(value, 10) }
