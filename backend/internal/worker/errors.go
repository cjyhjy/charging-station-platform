// Package worker provides the Redis Streams consumer loop shared by event
// workers, plus the reliability layer that decides whether a failed delivery is
// retried or dead-lettered.
package worker

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrDuplicate reports that an event was already consumed. The worker acknowledges such a
	// delivery without applying it again and without counting a failure: duplicates are
	// expected under at-least-once delivery, because a consumer can be redelivered an entry
	// it already handled.
	ErrDuplicate = errors.New("event already consumed")

	// ErrLeaseHeld reports that another delivery currently owns this event through a live
	// reservation lease.
	//
	// It is deliberately NOT ErrDuplicate, because the correct action is the opposite one:
	// the entry must be left unacknowledged. Acknowledging it would mean treating "somebody
	// is handling this" as "this is finished", and if that somebody has died, the event would
	// be lost with nothing having applied it.
	ErrLeaseHeld = errors.New("event is held by another delivery")

	// ErrUnknownEventType reports an event whose type no handler accepts. It is
	// permanent by definition: retrying cannot teach the worker a new event type.
	ErrUnknownEventType = errors.New("no handler for event type")
)

// permanentError marks a failure that retrying cannot fix.
//
// The distinction matters because a transient failure should consume the retry
// budget and then reach the dead-letter stream, while a permanent one should go
// straight there. Retrying a permanently invalid event wastes the budget that a
// genuinely transient failure needs, and delays the operator seeing the bad event.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as unfixable by retrying. A nil err returns nil so callers
// can wrap unconditionally.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Permanentf marks a formatted message as unfixable by retrying.
func Permanentf(format string, args ...any) error {
	return Permanent(fmt.Errorf(format, args...))
}

// IsPermanent reports whether err was marked permanent, at any depth.
func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}

// DuplicateGuard is the fast duplicate short circuit in front of the authoritative
// consumption record.
//
// The B line implements it over Redis (ncs:idempotency:{scope}:{key}), but it is
// declared here so the handlers depend on the behaviour rather than on Redis. A nil
// guard disables the short circuit, which is legitimate: the consumption record is
// what guarantees correctness.
type DuplicateGuard interface {
	// Claim reserves an event and returns the token that identifies this specific claim.
	//
	// A non-empty token means this caller owns the claim and must pass that same token to
	// Release. An empty token with a nil error means another consumer holds the claim, so
	// this delivery must be skipped.
	//
	// The token must be unique per claim rather than derived from the key: a consumer whose
	// processing outlived the claim's TTL would otherwise be able to release the claim a
	// different consumer has since acquired.
	Claim(ctx context.Context, scope, key string) (token string, err error)
	// Release returns a claim so a redelivery can be processed, and must do nothing unless
	// token still owns the claim.
	//
	// It is called when handling failed, because keeping the claim would make the
	// redelivery look like a duplicate and the event would be lost.
	Release(ctx context.Context, scope, key, token string) error
}
