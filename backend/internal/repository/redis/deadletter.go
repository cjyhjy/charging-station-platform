package redis

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// This file holds the dead-letter write claim, which is the Redis side of the worker's
// guard -> DLQ -> REC -> ACK sequence.
//
// It exists because a claim that only says "somebody is writing" is not enough to act on.
// A delivery that finds the claim held must not conclude that a parked entry exists: the
// holder may still be writing, and if its write then fails, an entry acknowledged on that
// assumption is gone from both streams. So the state is carried explicitly, in two keys:
//
//	in-flight claim  ncs:idempotency:dead-letter-write:{event_id}
//	completed write  ncs:idempotency:dead-letter-written:{event_id}
//
// Only the second one means "a parked entry exists". The first one only serialises writers,
// and it is short-lived because it is held for the duration of a single XADD.

// Dead-letter claim scopes, placed under the idempotency prefix so they reuse the frozen
// key naming rather than adding a prefix that the contract does not define.
const (
	deadLetterWriteScope   = "dead-letter-write"
	deadLetterWrittenScope = "dead-letter-written"
)

// DeadLetterClaimConfig bounds the two keys.
type DeadLetterClaimConfig struct {
	// WriteTTL bounds how long one delivery may hold the right to write. It only has to
	// outlast a single XADD attempt, including the pool's own command timeout, because a
	// claim that lives longer than the write it protects only delays the recovery of a
	// writer that died while holding it.
	WriteTTL time.Duration
	// WrittenTTL is how long the evidence that a parked entry exists is kept.
	//
	// It must comfortably exceed the window in which the source entry can still be
	// redelivered, which includes a process that crashed after the write and before the
	// acknowledgement and is restarted much later. Losing the evidence is not data loss -
	// it can only produce a duplicate dead letter - but a short TTL would make duplicates
	// routine, and parked events are rare enough that the memory cost is irrelevant.
	WrittenTTL time.Duration
}

// DefaultDeadLetterClaimConfig returns a write window of one minute and evidence kept for
// a week.
func DefaultDeadLetterClaimConfig() DeadLetterClaimConfig {
	return DeadLetterClaimConfig{WriteTTL: time.Minute, WrittenTTL: 7 * 24 * time.Hour}
}

func (c DeadLetterClaimConfig) validate() error {
	if c.WriteTTL <= 0 {
		return fmt.Errorf("dead-letter write ttl must be greater than zero")
	}
	if c.WrittenTTL <= 0 {
		return fmt.Errorf("dead-letter written ttl must be greater than zero")
	}
	if c.WrittenTTL < c.WriteTTL {
		// Evidence that expires before the claim protecting it would let a writer publish a
		// completed write and then have that evidence deleted while it is still the only
		// thing preventing a second write.
		return fmt.Errorf("dead-letter written ttl (%s) must not be shorter than the write ttl (%s)", c.WrittenTTL, c.WriteTTL)
	}
	return nil
}

// DeadLetterClaimState is what the store knows about an event's parked entry.
//
// The three states demand three different actions from a worker, which is why they are not
// collapsed into a boolean: "somebody is writing" must not be treated as "it is written".
type DeadLetterClaimState string

const (
	// DeadLetterClaimTaken means this caller owns the write and must perform it.
	DeadLetterClaimTaken DeadLetterClaimState = "taken"
	// DeadLetterClaimWritten means a completed write exists, so a retry only has to finish
	// the remaining steps instead of writing a second copy.
	DeadLetterClaimWritten DeadLetterClaimState = "written"
	// DeadLetterClaimWriting means another delivery holds the write claim right now. Its
	// outcome is unknown, so the only safe action is to leave the source entry pending.
	DeadLetterClaimWriting DeadLetterClaimState = "writing"
)

// DeadLetterClaims records who is writing an event's parked entry, and that a write is
// complete.
//
// It is deliberately NOT the correctness guarantee, in the same way the duplicate guard is
// not: the parked entry in the dead-letter stream is the fact, and this store only makes
// writing it idempotent. Losing the store therefore costs duplicates, never events, which is
// why every operation degrades in the direction of writing again.
type DeadLetterClaims struct {
	commands Commands
	observer observer
	config   DeadLetterClaimConfig
	tokens   TokenSource
}

// NewDeadLetterClaims validates its inputs. A nil token source uses crypto/rand, which is
// what makes each claim's owner unique.
func NewDeadLetterClaims(commands Commands, policy Policy, stats *Degradation, config DeadLetterClaimConfig, tokens TokenSource) (*DeadLetterClaims, error) {
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
	return &DeadLetterClaims{commands: commands, observer: newObserver(policy, stats), config: config, tokens: tokens}, nil
}

// Claim decides what this delivery may do about the event's parked entry.
//
// The order of the two checks is what makes the result safe:
//
//  1. A completed write is the authority. It is checked first, so a delivery that arrives
//     after the write never takes a claim it does not need.
//  2. Otherwise the write claim is taken. If it is already held, the completed-write check is
//     repeated: the marker is published BEFORE the claim is dropped, so a holder that has
//     finished is always visible as "written" to a delivery that just failed to take the
//     claim. What remains when that check also fails is a writer that is still working, and
//     its outcome is unknown - reporting that as "written" is exactly the mistake that loses
//     events.
//
// An availability failure degrades towards writing again: a duplicate dead letter is visible
// and deletable, while an entry acknowledged on an assumption is not.
func (c *DeadLetterClaims) Claim(ctx context.Context, eventID string) (string, DeadLetterClaimState, error) {
	if eventID == "" {
		// An undecodable entry has no event id, so there is nothing to key a claim on. The
		// caller writes it and accepts that a retry may repeat it.
		return "", DeadLetterClaimTaken, nil
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, eventID)
	if err != nil {
		return "", "", err
	}
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		return "", "", err
	}

	written, err := c.written(ctx, writtenKey)
	if err != nil {
		return "", "", err
	}
	if written {
		return "", DeadLetterClaimWritten, nil
	}

	owner, err := c.tokens()
	if err != nil {
		return "", "", fmt.Errorf("generate dead-letter claim owner for %s: %w", eventID, err)
	}
	taken, err := run(ctx, c.observer, CapabilityIdempotency, true, func() (bool, error) {
		return c.commands.SetNX(ctx, writeKey, owner, c.config.WriteTTL)
	})
	if err != nil {
		return "", "", err
	}
	if taken {
		return owner, DeadLetterClaimTaken, nil
	}

	// The claim is held. Only a completed write lets this delivery skip the write.
	written, err = c.written(ctx, writtenKey)
	if err != nil {
		return "", "", err
	}
	if written {
		return "", DeadLetterClaimWritten, nil
	}
	return "", DeadLetterClaimWriting, nil
}

// MarkWritten publishes that a parked entry exists and releases the write claim.
//
// The order matters: the evidence is published first, so a delivery that fails to take the
// claim always sees it. Releasing second means the claim is not left behind either.
//
// An error here is NOT a reason to treat the park as failed: the entry is already in the
// dead-letter stream, and the caller must go on to record and acknowledge. What an error
// costs is that a later retry cannot tell the write already happened and parks a duplicate,
// which is the direction this whole store is allowed to fail in.
func (c *DeadLetterClaims) MarkWritten(ctx context.Context, eventID string, token string) error {
	if eventID == "" {
		return nil
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, eventID)
	if err != nil {
		return err
	}
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		return err
	}

	// SetNX rather than Set: the first completed write is the one that is kept, and a
	// second writer publishing its own owner afterwards would change nothing.
	//
	// This one call does not go through the degradation helper: the caller has to hear about a
	// failure, because an unpublished write marker is what makes a later retry park a second
	// copy. Degrading silently here would hide exactly the case an operator would want to see.
	if _, err := c.commands.SetNX(ctx, writtenKey, token, c.config.WrittenTTL); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		c.observer.stats.Record(CapabilityIdempotency, c.observer.policy.modeFor(CapabilityIdempotency), err)
		return fmt.Errorf("publish dead-letter write for %s: %w", eventID, err)
	}

	if token != "" {
		// Best effort, and only ever the claim this caller took: a compare-and-delete that
		// fails means the claim had already expired or moved on, which is not an error.
		_, _ = c.commands.CompareAndDelete(ctx, writeKey, token)
	}
	return nil
}

// Release gives the write claim back after a failed write, so a retry writes the entry
// instead of waiting for the claim to expire.
//
// It deletes only a claim this caller still owns, so a claim another delivery has since
// acquired is left alone. Releasing after a write whose outcome is unknown can park a
// duplicate; that trade is deliberate, because a duplicate is visible and an operator can
// delete it, while a lost event is neither.
func (c *DeadLetterClaims) Release(ctx context.Context, eventID string, token string) error {
	if eventID == "" || token == "" {
		return nil
	}
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		return err
	}
	_, err = run(ctx, c.observer, CapabilityIdempotency, struct{}{}, func() (struct{}, error) {
		if _, err := c.commands.CompareAndDelete(ctx, writeKey, token); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// Written reports whether a completed write is on record, for diagnostics and tests.
func (c *DeadLetterClaims) Written(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	writtenKey, err := IdempotencyKey(deadLetterWrittenScope, eventID)
	if err != nil {
		return false, err
	}
	return c.written(ctx, writtenKey)
}

// InFlight reports whether a write claim is currently held, for diagnostics and tests.
func (c *DeadLetterClaims) InFlight(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	writeKey, err := IdempotencyKey(deadLetterWriteScope, eventID)
	if err != nil {
		return false, err
	}
	_, err = c.commands.Get(ctx, writeKey)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// written reads the completed-write marker. A missing key is the normal case, and an
// availability failure is reported as "no completed write" so a caller writes again rather
// than skipping a write it cannot prove happened.
func (c *DeadLetterClaims) written(ctx context.Context, writtenKey string) (bool, error) {
	value, err := run(ctx, c.observer, CapabilityIdempotency, "", func() (string, error) {
		return c.commands.Get(ctx, writtenKey)
	})
	switch {
	case err == nil:
		// The value is checked rather than the error alone: the degraded path returns an empty
		// value with a nil error, and reading that as "a write exists" would acknowledge an
		// entry nothing had parked.
		return value != "", nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}
