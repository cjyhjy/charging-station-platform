package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SessionConfig tunes login-state storage.
//
// Two independent limits are needed, because they protect against different
// attacks and a single TTL cannot express both:
//
//   - IdleTTL is how long a session survives without use. It is the sliding
//     window that keeps an active user logged in.
//   - AbsoluteTTL is the hard ceiling from the moment the session was created.
//     Without it, a client that refreshes once per IdleTTL keeps a session alive
//     indefinitely, so a stolen token would never expire.
type SessionConfig struct {
	IdleTTL     time.Duration
	AbsoluteTTL time.Duration
}

// DefaultSessionConfig returns a 30 minute idle window inside a 12 hour absolute
// lifetime, which matches a working day without letting a session live forever.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{IdleTTL: 30 * time.Minute, AbsoluteTTL: 12 * time.Hour}
}

func (s SessionConfig) validate() error {
	if s.IdleTTL <= 0 {
		return fmt.Errorf("session idle ttl must be greater than zero")
	}
	if s.AbsoluteTTL <= 0 {
		return fmt.Errorf("session absolute ttl must be greater than zero")
	}
	if s.AbsoluteTTL < s.IdleTTL {
		return fmt.Errorf("session absolute ttl %s must not be shorter than the idle ttl %s", s.AbsoluteTTL, s.IdleTTL)
	}
	return nil
}

// sessionEnvelope is the stored value. It carries the absolute deadline
// alongside the opaque payload, because the deadline cannot be derived from the
// key's TTL once the TTL is refreshed.
//
// Only this package reads the envelope, so the payload stays opaque to the auth
// module: the stored bytes are an implementation detail of Sessions.
type sessionEnvelope struct {
	Payload string `json:"payload"`
	// ExpiresAt is the absolute deadline as a UTC Unix second, matching the
	// repository-wide time convention.
	ExpiresAt int64 `json:"expires_at"`
}

// sessionValue distinguishes a missing session from a session whose payload is
// legitimately empty.
type sessionValue struct {
	payload string
	found   bool
}

// Sessions stores login state under ncs:session:{session_id}.
//
// Redis holds only the session lookup. PostgreSQL remains the source of truth
// for accounts, roles and permissions, so losing Redis logs users out instead of
// corrupting business state. That is why the default Session policy is
// FailClosed: an unverifiable login must never be accepted as valid.
type Sessions struct {
	commands Commands
	observer observer
	config   SessionConfig
	clock    func() time.Time
}

// NewSessions validates its inputs. A nil clock uses the wall clock.
func NewSessions(commands Commands, policy Policy, stats *Degradation, config SessionConfig, clock func() time.Time) (*Sessions, error) {
	if commands == nil {
		return nil, fmt.Errorf("redis commands are required")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Sessions{commands: commands, observer: newObserver(policy, stats), config: config, clock: clock}, nil
}

// Save creates a session with the configured idle window and a fresh absolute
// deadline.
func (s *Sessions) Save(ctx context.Context, sessionID, payload string) error {
	return s.SaveWithIdleTTL(ctx, sessionID, payload, s.config.IdleTTL)
}

// SaveWithIdleTTL creates a session with an explicit idle window, used when a
// caller must shorten one, for example after a privileged action. The absolute
// deadline is still taken from the configuration and cannot be extended by this
// call.
//
// The effective idle window is capped by the absolute lifetime, so a request for
// a longer idle window can never outlive the session's hard ceiling.
func (s *Sessions) SaveWithIdleTTL(ctx context.Context, sessionID, payload string, idleTTL time.Duration) error {
	key, err := SessionKey(sessionID)
	if err != nil {
		return err
	}
	if idleTTL <= 0 {
		return fmt.Errorf("session idle ttl must be greater than zero")
	}

	now := s.clock()
	deadline := now.Add(s.config.AbsoluteTTL)
	if idleTTL > s.config.AbsoluteTTL {
		// The idle window may be shortened but never stretched past the ceiling.
		idleTTL = s.config.AbsoluteTTL
	}
	encoded, err := json.Marshal(sessionEnvelope{Payload: payload, ExpiresAt: deadline.Unix()})
	if err != nil {
		return fmt.Errorf("encode session %s: %w", sessionID, err)
	}

	_, err = run(ctx, s.observer, CapabilitySession, struct{}{}, func() (struct{}, error) {
		return struct{}{}, s.commands.Set(ctx, key, string(encoded), idleTTL)
	})
	return err
}

// Load reads a session.
//
// found is false when the session is absent, has idled out, or has passed its
// absolute deadline. A session past its deadline is deleted on the spot, so an
// expired session cannot be resurrected by a later refresh.
//
// Under the default FailClosed policy an unreachable Redis returns an error
// wrapping ErrUnavailable, so the caller answers 401 or 503 instead of treating
// the request as anonymous.
func (s *Sessions) Load(ctx context.Context, sessionID string) (string, bool, error) {
	envelope, found, err := s.read(ctx, sessionID)
	if err != nil || !found {
		return "", false, err
	}
	return envelope.Payload, true, nil
}

// Refresh extends the idle window without ever moving the absolute deadline. It
// reports whether the session is still valid.
//
// A refresh after the absolute deadline fails and removes the session, which is
// what makes the ceiling real: a client that calls Refresh in a loop keeps its
// idle window open but can never extend the session beyond AbsoluteTTL.
func (s *Sessions) Refresh(ctx context.Context, sessionID string) (bool, error) {
	key, err := SessionKey(sessionID)
	if err != nil {
		return false, err
	}
	now := s.clock()

	return run(ctx, s.observer, CapabilitySession, false, func() (bool, error) {
		raw, err := s.commands.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		envelope, err := decodeSession(raw)
		if err != nil {
			// A value this package cannot read is unusable. Removing it forces a
			// fresh login instead of serving a corrupt session.
			_, _ = s.commands.Del(ctx, key)
			return false, fmt.Errorf("decode session %s: %w", sessionID, err)
		}

		remaining := time.Unix(envelope.ExpiresAt, 0).Sub(now)
		if remaining <= 0 {
			// The absolute ceiling has passed: end the session for good.
			if _, err := s.commands.Del(ctx, key); err != nil {
				return false, fmt.Errorf("delete expired session %s: %w", sessionID, err)
			}
			return false, nil
		}

		effective := s.config.IdleTTL
		if remaining < effective {
			effective = remaining
		}
		changed, err := s.commands.Expire(ctx, key, effective)
		if err != nil {
			return false, fmt.Errorf("refresh session %s: %w", sessionID, err)
		}
		return changed, nil
	})
}

// Delete removes a session. Deleting a session that is already gone is not an
// error, so logout stays idempotent.
func (s *Sessions) Delete(ctx context.Context, sessionID string) error {
	key, err := SessionKey(sessionID)
	if err != nil {
		return err
	}
	_, err = run(ctx, s.observer, CapabilitySession, struct{}{}, func() (struct{}, error) {
		_, err := s.commands.Del(ctx, key)
		return struct{}{}, err
	})
	return err
}

// AbsoluteDeadline reports when a session's hard ceiling falls, for callers that
// need to surface or audit it. It reports found=false when the session is gone.
func (s *Sessions) AbsoluteDeadline(ctx context.Context, sessionID string) (time.Time, bool, error) {
	envelope, found, err := s.read(ctx, sessionID)
	if err != nil || !found {
		return time.Time{}, false, err
	}
	return time.Unix(envelope.ExpiresAt, 0).UTC(), true, nil
}

// read loads and validates a session envelope, treating both an idled-out key
// and a past absolute deadline as "not found".
func (s *Sessions) read(ctx context.Context, sessionID string) (sessionEnvelope, bool, error) {
	key, err := SessionKey(sessionID)
	if err != nil {
		return sessionEnvelope{}, false, err
	}

	result, err := run(ctx, s.observer, CapabilitySession, sessionValue{}, func() (sessionValue, error) {
		raw, err := s.commands.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return sessionValue{}, nil
		}
		if err != nil {
			return sessionValue{}, err
		}
		return sessionValue{payload: raw, found: true}, nil
	})
	if err != nil || !result.found {
		return sessionEnvelope{}, false, err
	}

	envelope, err := decodeSession(result.payload)
	if err != nil {
		// A value this package cannot read is unusable. Removing it forces a
		// fresh login instead of serving a corrupt session.
		_, _ = s.commands.Del(ctx, key)
		return sessionEnvelope{}, false, fmt.Errorf("decode session %s: %w", sessionID, err)
	}

	// The stored TTL normally enforces the idle window and the envelope enforces
	// the absolute ceiling. The ceiling is checked on every read as well, so a
	// session cannot outlive AbsoluteTTL even if its TTL was set too generously.
	if !s.clock().Before(time.Unix(envelope.ExpiresAt, 0)) {
		if _, err := s.commands.Del(ctx, key); err != nil {
			return sessionEnvelope{}, false, fmt.Errorf("delete expired session %s: %w", sessionID, err)
		}
		return sessionEnvelope{}, false, nil
	}
	return envelope, true, nil
}

func decodeSession(raw string) (sessionEnvelope, error) {
	var envelope sessionEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return sessionEnvelope{}, err
	}
	if envelope.ExpiresAt <= 0 {
		return sessionEnvelope{}, fmt.Errorf("session envelope has no absolute deadline")
	}
	return envelope, nil
}
