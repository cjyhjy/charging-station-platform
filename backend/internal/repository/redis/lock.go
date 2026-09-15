package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// TokenSource produces lock tokens. The default is 128 bits from crypto/rand;
// tests inject a deterministic source. A token must be unguessable, because it
// is the only thing that stops one holder from releasing another's lock.
type TokenSource func() (string, error)

// NewTokenSource returns the production token source.
func NewTokenSource() TokenSource {
	return func() (string, error) {
		var buffer [16]byte
		if _, err := rand.Read(buffer[:]); err != nil {
			return "", fmt.Errorf("generate lock token: %w", err)
		}
		return hex.EncodeToString(buffer[:]), nil
	}
}

// Lock identifies a held lock. Release needs the token, so a caller must keep
// the value returned by Acquire rather than reconstructing it from the key.
type Lock struct {
	Key   string
	Token string
	Until time.Time
}

// LockGrant is the outcome of one acquire attempt.
type LockGrant struct {
	Lock     Lock
	Acquired bool
	// Degraded is true when Redis was unreachable and the FailOpen policy let
	// the caller continue without a lock. Acquired is false in that case: the
	// repository reports the situation and the caller decides whether to
	// proceed unprotected. It never reports a lock that is not actually held.
	Degraded bool
}

// Locker is the distributed lock over the ncs:lock: namespace, used for
// order-scoped mutual exclusion (ncs:lock:order:{order_id}).
//
// # The lock is an optimisation, never the correctness guarantee
//
// This lock reduces contention and gives a fast, friendly rejection to a second
// concurrent charge-start request. It must NOT be the only thing standing
// between an order and a duplicate charge. A Redis lock cannot be authoritative:
// it is lost on failover, it expires while a holder is still running, and it is
// unavailable whenever Redis is. Final correctness for order and money state
// must come from PostgreSQL:
//
//   - a unique constraint that makes the second insert fail outright;
//   - an idempotency record keyed by the caller's request key;
//   - a transactional state transition that only moves an order out of a
//     terminal state once.
//
// Reviewers should reject any order path whose only protection is this lock. The
// approval criterion is in docs/migration/backend-parallel-development.md
// section 8.
//
// # Default mode
//
// The default Lock policy is FailClosed, so an unavailable Redis refuses the
// operation rather than proceeding unprotected. That is the approved default:
// until the PostgreSQL uniqueness, idempotency and state-machine protections
// pass integration approval, refusing to start charging is the safer failure.
//
// FailOpen is only acceptable for a caller that detects the reported
// Degraded && !Acquired result and then relies on the PostgreSQL backstop, never
// for a caller that simply proceeds.
//
// The lock is not reentrant and has no watchdog: its TTL must cover the whole
// critical section, and a holder that overruns the TTL loses the lock and must
// not commit. Extending a lock in place is deliberately absent, because a
// correct implementation needs compare-and-expire and the caller should size the
// TTL instead.
type Locker struct {
	commands Commands
	observer observer
	tokens   TokenSource
	clock    func() time.Time
}

// NewLocker validates its inputs. Nil tokens and clock fall back to the
// production defaults.
func NewLocker(commands Commands, policy Policy, stats *Degradation, tokens TokenSource, clock func() time.Time) (*Locker, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if tokens == nil {
		tokens = NewTokenSource()
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Locker{commands: commands, observer: newObserver(policy, stats), tokens: tokens, clock: clock}, nil
}

// Acquire tries to take the lock for ttl.
//
// The key must be inside the ncs:lock: namespace. Release deletes the key, so
// accepting an arbitrary key would let a caller bug delete a session or cache
// entry.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (LockGrant, error) {
	if err := validateLockKey(key); err != nil {
		return LockGrant{}, err
	}
	if ttl <= 0 {
		return LockGrant{}, fmt.Errorf("lock ttl must be greater than zero")
	}

	// FailOpen cannot invent a lock: it reports Degraded and not acquired so the
	// caller makes the availability decision explicitly.
	fallback := LockGrant{Degraded: true}

	return run(ctx, l.observer, CapabilityLock, fallback, func() (LockGrant, error) {
		token, err := l.tokens()
		if err != nil {
			return LockGrant{}, err
		}
		acquired, err := l.commands.SetNX(ctx, key, token, ttl)
		if err != nil {
			return LockGrant{}, err
		}
		if !acquired {
			return LockGrant{}, nil
		}
		return LockGrant{
			Lock:     Lock{Key: key, Token: token, Until: l.clock().Add(ttl)},
			Acquired: true,
		}, nil
	})
}

// Release frees a lock. released is false when this holder no longer owns it,
// which happens after the TTL expired and someone else took the lock. The
// compare-and-delete makes that safe: a late release never removes another
// holder's lock.
func (l *Locker) Release(ctx context.Context, lock Lock) (bool, error) {
	if err := validateLockKey(lock.Key); err != nil {
		return false, err
	}
	if lock.Token == "" {
		return false, fmt.Errorf("lock token is required")
	}
	return run(ctx, l.observer, CapabilityLock, false, func() (bool, error) {
		return l.commands.CompareAndDelete(ctx, lock.Key, lock.Token)
	})
}

func validateLockKey(key string) error {
	if key == "" {
		return fmt.Errorf("lock key is required")
	}
	if !IsLockKey(key) {
		return fmt.Errorf("lock key %q is outside the %s namespace", key, keyPrefixLock)
	}
	return nil
}
