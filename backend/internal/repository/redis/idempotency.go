package redis

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GuardConfig tunes the duplicate-event short circuit.
type GuardConfig struct {
	// TTL bounds how long a claim suppresses duplicates. It only needs to cover the
	// window in which a redelivery or a second consumer could race the same event,
	// because the authoritative record lives in PostgreSQL for as long as the
	// business requires.
	//
	// It must NOT exceed the consumption store's reservation lease. A claim outlives
	// nothing useful, and one that outlives the lease delays recovery: if a process dies
	// holding both, the lease expires first, the next delivery legitimately takes it over,
	// and the still-held claim then makes that delivery wait until the claim expires too.
	// The delay is bounded and loses nothing, because claim contention is reported as
	// "held" rather than as "already consumed", but a longer claim only makes the wait
	// longer for no gain.
	TTL time.Duration
}

// DefaultGuardConfig returns a window that matches the default consumption lease.
//
// Matching rather than exceeding is deliberate: the claim is created after the reservation
// starts, so equal durations mean the claim expires at about the same moment as the lease and
// never blocks a takeover for long.
func DefaultGuardConfig() GuardConfig {
	return GuardConfig{TTL: 5 * time.Minute}
}

func (g GuardConfig) validate() error {
	if g.TTL <= 0 {
		return fmt.Errorf("idempotency ttl must be greater than zero")
	}
	return nil
}

// Guard is the Redis-backed duplicate-event short circuit over
// ncs:idempotency:{scope}:{key}.
//
// It is deliberately NOT the correctness guarantee. Contract section 4.3 keeps the
// authoritative consumption record in PostgreSQL, because Redis may lose the key or
// be unavailable, and section 4.2 requires duplicate deliveries to be neutralised by
// a consumption record or a business unique key. What this guard buys is a cheap
// rejection of the common races: a redelivery inside the retry window, or two
// consumers processing the same event after a rebalance.
//
// The lifecycle is claim, act, and release only on failure:
//
//   - Claim reserves the event and returns the owner token. An empty token means another
//     consumer holds it, so this delivery must be skipped.
//   - Release returns the reservation and must be given the token Claim returned. The
//     token is unique per claim, so a release can never delete a claim that another
//     consumer has since acquired - which is what makes the TTL safe.
//   - A successful handler keeps the claim; its TTL expires it.
//
// The default Idempotency policy is FailOpen, so an unavailable Redis lets the event
// through and the consumption record still protects correctness.
type Guard struct {
	commands Commands
	observer observer
	config   GuardConfig
	tokens   TokenSource
}

// NewGuard validates its inputs. A nil token source uses crypto/rand, which is what makes
// each claim's owner unique.
func NewGuard(commands Commands, policy Policy, stats *Degradation, config GuardConfig, tokens TokenSource) (*Guard, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if tokens == nil {
		tokens = NewTokenSource()
	}
	return &Guard{commands: commands, observer: newObserver(policy, stats), config: config, tokens: tokens}, nil
}

// Claim reserves an event for processing and returns the owner token.
//
// A non-empty token means this caller owns the claim. An empty token with a nil error means
// another consumer holds it, so this delivery is a duplicate. Under the default FailOpen
// policy an unavailable Redis yields a token anyway, because skipping the event would lose
// work while the consumption record still prevents a business duplicate; the failure is
// recorded so an operator can alert on it.
func (g *Guard) Claim(ctx context.Context, scope, key string) (string, error) {
	name, err := IdempotencyKey(scope, key)
	if err != nil {
		return "", err
	}
	// The token identifies this specific claim, so a later Release can only ever delete its
	// own claim. A token derived from the key alone - which is what this used to be - is
	// identical for every consumer, so a consumer whose processing outlived the TTL could
	// release the claim a different consumer had since acquired.
	owner, err := g.tokens()
	if err != nil {
		return "", fmt.Errorf("generate claim owner for %s: %w", key, err)
	}

	claimed, err := run(ctx, g.observer, CapabilityIdempotency, true, func() (bool, error) {
		return g.commands.SetNX(ctx, name, owner, g.config.TTL)
	})
	if err != nil {
		return "", err
	}
	if !claimed {
		return "", nil
	}
	return owner, nil
}

// Release returns a reservation so a redelivery can be processed.
//
// The owner token is required: the delete is a compare-and-delete against it, so a stale
// holder cannot remove a claim that a different consumer now owns.
func (g *Guard) Release(ctx context.Context, scope, key, owner string) error {
	if owner == "" {
		return fmt.Errorf("release claim for %s: owner token is required", key)
	}
	name, err := IdempotencyKey(scope, key)
	if err != nil {
		return err
	}
	_, err = run(ctx, g.observer, CapabilityIdempotency, struct{}{}, func() (struct{}, error) {
		if _, err := g.commands.CompareAndDelete(ctx, name, owner); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// Held reports whether a claim is currently present, for diagnostics and tests.
func (g *Guard) Held(ctx context.Context, scope, key string) (bool, error) {
	name, err := IdempotencyKey(scope, key)
	if err != nil {
		return false, err
	}
	_, err = g.commands.Get(ctx, name)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// ownerToken was removed deliberately: a token derived from the key is identical for every
// consumer, so a consumer whose processing outlived the claim TTL could release the claim a
// different consumer had since acquired. Claim now mints a unique owner per claim and
// returns it, and Release requires it.
