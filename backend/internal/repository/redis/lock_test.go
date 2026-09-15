package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestLocker(t *testing.T, commands Commands, policy Policy, stats *Degradation, tokens TokenSource, clock *testClock) *Locker {
	t.Helper()
	locker, err := NewLocker(commands, policy, stats, tokens, clock.Now)
	if err != nil {
		t.Fatalf("new locker: %v", err)
	}
	return locker
}

// newLockerFixture shares one clock between the store and the locker, so a test
// that advances time expires the lock exactly as production would.
func newLockerFixture(t *testing.T, policy Policy, stats *Degradation, tokens TokenSource) (*MemoryCommands, *Locker, *testClock) {
	t.Helper()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	return commands, newTestLocker(t, commands, policy, stats, tokens, clock), clock
}

// sequenceTokens returns deterministic tokens so a test can assert on lock
// ownership without depending on randomness.
func sequenceTokens(values ...string) TokenSource {
	var (
		mu    sync.Mutex
		index int
	)
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(values) {
			return "", errors.New("token sequence exhausted")
		}
		token := values[index]
		index++
		return token, nil
	}
}

func TestNewLockerValidatesInputs(t *testing.T) {
	if _, err := NewLocker(nil, DefaultPolicy(), nil, nil, nil); err == nil {
		t.Fatal("expected nil commands to be rejected")
	}
	if _, err := NewLocker(NewMemoryCommands(), Policy{Lock: FailMode(9)}, nil, nil, nil); err == nil {
		t.Fatal("expected an invalid policy to be rejected")
	}
}

func TestNewTokenSourceProducesDistinctUnguessableTokens(t *testing.T) {
	tokens := NewTokenSource()
	seen := make(map[string]bool)
	for i := 0; i < 64; i++ {
		token, err := tokens()
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		// 16 bytes rendered as hex.
		if len(token) != 32 {
			t.Fatalf("expected a 32 character hex token, got %q", token)
		}
		if seen[token] {
			t.Fatalf("token %q was repeated", token)
		}
		seen[token] = true
	}
}

func TestLockerAcquireAndRelease(t *testing.T) {
	ctx := context.Background()
	commands, locker, clock := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a"))
	key, _ := OrderLockKey("o_01")

	grant, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !grant.Acquired {
		t.Fatal("expected the first acquire to succeed")
	}
	if grant.Degraded {
		t.Fatal("a real lock must not be marked degraded")
	}
	if grant.Lock.Token != "token-a" {
		t.Fatalf("expected token-a, got %q", grant.Lock.Token)
	}
	if !grant.Lock.Until.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("expected the lock to run until %s, got %s", clock.Now().Add(time.Minute), grant.Lock.Until)
	}

	released, err := locker.Release(ctx, grant.Lock)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !released {
		t.Fatal("expected the owning token to release the lock")
	}
	if commands.Len() != 0 {
		t.Fatal("expected the lock key to be gone")
	}
}

func TestLockerRefusesASecondHolder(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a", "token-b"))
	key, _ := OrderLockKey("o_01")

	first, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !first.Acquired {
		t.Fatal("expected the first acquire to succeed")
	}

	second, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second.Acquired {
		t.Fatal("expected the second acquire to be refused while the lock is held")
	}
	if second.Degraded {
		t.Fatal("a plain refusal is not a degradation")
	}
}

// This is the invariant that makes a late release safe: a holder whose lock
// expired must not delete the lock a new holder now owns.
func TestLockerReleaseByAStaleTokenDoesNotFreeTheNewHolder(t *testing.T) {
	ctx := context.Background()
	commands, locker, clock := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a", "token-b"))
	key, _ := OrderLockKey("o_01")

	stale, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("stale acquire: %v", err)
	}
	clock.Advance(2 * time.Minute)

	fresh, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("fresh acquire: %v", err)
	}
	if !fresh.Acquired {
		t.Fatal("expected the expired lock to be reacquirable")
	}

	released, err := locker.Release(ctx, stale.Lock)
	if err != nil {
		t.Fatalf("stale release: %v", err)
	}
	if released {
		t.Fatal("a stale token must not release the lock")
	}

	// The new holder still owns the lock.
	value, err := commands.Get(ctx, key)
	if err != nil {
		t.Fatalf("expected the lock to survive: %v", err)
	}
	if value != "token-b" {
		t.Fatalf("expected token-b to still own the lock, got %q", value)
	}
}

func TestLockerReleaseRequiresAToken(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, nil)
	key, _ := OrderLockKey("o_01")

	if _, err := locker.Release(ctx, Lock{Key: key}); err == nil {
		t.Fatal("expected a release without a token to be rejected")
	}
}

// Acquire on a key outside ncs:lock: would let a caller bug delete a session or
// cache entry on release, so the namespace is enforced.
func TestLockerRejectsKeysOutsideTheLockNamespace(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a"))

	sessionKey, _ := SessionKey("sess_01")
	if _, err := locker.Acquire(ctx, sessionKey, time.Minute); err == nil {
		t.Fatal("expected a session key to be rejected by Acquire")
	}
	if _, err := locker.Acquire(ctx, "", time.Minute); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
	if _, err := locker.Release(ctx, Lock{Key: sessionKey, Token: "token-a"}); err == nil {
		t.Fatal("expected a session key to be rejected by Release")
	}
}

func TestLockerRejectsNonPositiveTTL(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a"))
	key, _ := OrderLockKey("o_01")

	if _, err := locker.Acquire(ctx, key, 0); err == nil {
		t.Fatal("expected a zero ttl to be rejected")
	}
	// A lock with no expiry could never be recovered from a crashed holder.
	if _, err := locker.Acquire(ctx, key, -time.Minute); err == nil {
		t.Fatal("expected a negative ttl to be rejected")
	}
}

func TestLockerSurfacesTokenSourceFailure(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, func() (string, error) {
		return "", errors.New("no entropy")
	})
	key, _ := OrderLockKey("o_01")

	if _, err := locker.Acquire(ctx, key, time.Minute); err == nil {
		t.Fatal("expected the token failure to surface")
	}
}

// The default lock policy is FailClosed, and FailOpen must still never invent a
// lock: it reports Degraded with Acquired=false so the caller decides whether to
// continue unprotected.
func TestLockerDegradationNeverReportsAnUnheldLock(t *testing.T) {
	ctx := context.Background()
	key, _ := OrderLockKey("o_01")

	t.Run("fail closed surfaces the outage", func(t *testing.T) {
		stats := NewDegradation()
		commands, locker, _ := newLockerFixture(t, DefaultPolicy(), stats, sequenceTokens("token-a"))
		commands.SetDown(errors.New("connection refused"))

		if _, err := locker.Acquire(ctx, key, time.Minute); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("expected ErrUnavailable, got %v", err)
		}
		if got := stats.Failures(CapabilityLock); got != 1 {
			t.Fatalf("expected the outage to be counted, got %d", got)
		}
	})

	t.Run("fail open reports degraded without a lock", func(t *testing.T) {
		policy := DefaultPolicy()
		policy.Lock = FailOpen
		commands, locker, _ := newLockerFixture(t, policy, nil, sequenceTokens("token-a"))
		commands.SetDown(errors.New("connection refused"))

		grant, err := locker.Acquire(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("expected the outage to be hidden, got %v", err)
		}
		if grant.Acquired {
			t.Fatal("a degraded acquire must never report a held lock")
		}
		if !grant.Degraded {
			t.Fatal("expected the grant to be flagged degraded")
		}
		if grant.Lock.Token != "" {
			t.Fatalf("expected no token, got %q", grant.Lock.Token)
		}
	})
}

func TestLockerReleaseSurfacesOutageUnderFailClosed(t *testing.T) {
	ctx := context.Background()
	commands, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, sequenceTokens("token-a"))
	key, _ := OrderLockKey("o_01")

	grant, err := locker.Acquire(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	commands.SetDown(errors.New("connection refused"))

	if _, err := locker.Release(ctx, grant.Lock); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

// Order-scoped mutual exclusion is the reason this type exists: concurrent
// charge-start requests for one order must produce exactly one winner.
func TestLockerGrantsExactlyOneWinnerUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	_, locker, _ := newLockerFixture(t, DefaultPolicy(), nil, NewTokenSource())
	key, _ := OrderLockKey("o_01")

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
			grant, err := locker.Acquire(ctx, key, time.Minute)
			if err != nil {
				t.Errorf("worker %d: %v", index, err)
				return
			}
			if grant.Acquired {
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

func TestTokenSourcesAreIndependent(t *testing.T) {
	// Two independently constructed sources must not hand out the same token, or
	// one holder could release the other's lock.
	first, err := NewTokenSource()()
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	second, err := NewTokenSource()()
	if err != nil {
		t.Fatalf("second token: %v", err)
	}
	if first == second {
		t.Fatalf("expected independent tokens, both were %q", first)
	}
}
