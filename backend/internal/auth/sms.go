package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
)

// SMS login parameters. Codes live in Redis (ncs:sms:*), so verification and
// the resend cooldown work across API instances and survive restarts; the
// in-memory alternatives would quietly break every second replica.
const (
	smsCodeLength = 6
	// The code window is fixed by requirements: ten minutes from issue.
	smsCodeTTL = 10 * time.Minute
	// Resend cooldown before a new code may be requested.
	smsResendCooldown = 60 * time.Second
	// After this many wrong submissions the code is voided and a new one
	// must be requested (shared registry: CODE_INVALID, 错误或次数超限).
	maxSMSCodeFailures = 5
)

var (
	phonePattern   = regexp.MustCompile(`^1[3-9]\d{9}$`)
	smsCodePattern = regexp.MustCompile(`^\d{6}$`)
)

// ErrSMSCodeInvalid and ErrSMSProviderNotConfigured are declared in
// service.go with the other sentinel errors.

// PhoneHash keeps personal phone numbers out of Redis keys (privacy review):
// keys carry a SHA-256 digest of the number instead of the number itself.
func PhoneHash(phone string) string {
	digest := sha256.Sum256([]byte(phone))
	return hex.EncodeToString(digest[:])
}

// SMSSender delivers login codes to end users. The interface keeps delivery
// pluggable: development uses simulated delivery, production wires a
// provider-backed implementation (HTTP gateway, cloud SMS service, ...).
type SMSSender interface {
	SendLoginCode(ctx context.Context, phone, code string) error
}

// SMSCodeStore stores one-time login codes. Implementations must be shared
// state (Redis) so any API instance can verify a code any instance issued,
// and verification must be atomic so a code can never be consumed twice.
type SMSCodeStore interface {
	// Issue stores a fresh code for the phone, replacing any previous code
	// and resetting its failure counter.
	Issue(ctx context.Context, phone, code string, ttl time.Duration) error
	// Verify atomically compares the stored code with the expected value:
	// on a match the code is consumed immediately (single use, no concurrent
	// double login) and on a mismatch the failure counter advances, voiding
	// the code once maxFailures is reached. It reports verified and
	// lockedOut, and found=false when nothing is stored.
	Verify(ctx context.Context, phone, expected string, maxFailures int) (verified, lockedOut, found bool, err error)
	// BeginCooldown reserves the resend window; false means a previous
	// request is still inside the cooldown and no new code may be sent.
	BeginCooldown(ctx context.Context, phone string, window time.Duration) (bool, error)
	// ClearCooldown releases the resend window; the issuer uses it when the
	// delivery attempt failed so the user may retry at once.
	ClearCooldown(ctx context.Context, phone string) error
}

// NewSMSCode mints a six-digit numeric code. Rejection sampling keeps the
// distribution uniform across 000000..999999 despite the power-of-two draw.
func NewSMSCode() (string, error) {
	const (
		max     = 1_000_000
		ceiling = 4_294_000_000 // largest multiple of max below 2^32
	)
	buffer := make([]byte, 4)
	for {
		if _, err := rand.Read(buffer); err != nil {
			return "", fmt.Errorf("auth: generate sms code: %w", err)
		}
		value := int(buffer[0])<<24 | int(buffer[1])<<16 | int(buffer[2])<<8 | int(buffer[3])
		if value >= ceiling {
			continue
		}
		return fmt.Sprintf("%06d", value%max), nil
	}
}

// IsValidPhone reports whether the value is a mainland China mobile number.
func IsValidPhone(phone string) bool {
	return phonePattern.MatchString(phone)
}

// IsValidSMSCode reports whether the value has the code shape.
func IsValidSMSCode(code string) bool {
	return smsCodePattern.MatchString(code)
}

// verifySMSScript atomically verifies and consumes a login code:
//
//	Keys: 1 = code key, 2 = failure counter key
//	Args: 1 = expected code, 2 = max failures, 3 = failure window (ms)
//
// Return codes: 1 verified (code consumed, counter cleared), -1 not found,
// -2 wrong code (counter advanced), -3 wrong code and the failure budget is
// exhausted (code voided). One script covers compare, consume, counting and
// voiding, so a correct code racing a wrong one can never interleave.
const verifySMSScript = `
local code = redis.call('GET', KEYS[1])
if not code then
  return {-1, 0}
end
if code ~= ARGV[1] then
  local fails = redis.call('INCR', KEYS[2])
  local ttl = redis.call('PTTL', KEYS[2])
  if fails == 1 or ttl < 0 then
    redis.call('PEXPIRE', KEYS[2], ARGV[3])
  end
  if fails >= tonumber(ARGV[2]) then
    redis.call('DEL', KEYS[1])
    redis.call('DEL', KEYS[2])
    return {-3, fails}
  end
  return {-2, fails}
end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return {1, 0}
`

// InMemorySMSCodeStore is the test double for the Redis store. Its Verify
// mirrors the script semantics without any concurrency guarantees.
type InMemorySMSCodeStore struct {
	mu       map[string]smsEntry
	cooldown map[string]time.Time
	failures map[string]int
	clock    func() time.Time
}

type smsEntry struct {
	code      string
	expiresAt time.Time
}

// NewInMemorySMSCodeStore returns a store for unit tests.
func NewInMemorySMSCodeStore(clock func() time.Time) *InMemorySMSCodeStore {
	if clock == nil {
		clock = time.Now
	}
	return &InMemorySMSCodeStore{
		mu:       make(map[string]smsEntry),
		cooldown: make(map[string]time.Time),
		failures: make(map[string]int),
		clock:    clock,
	}
}

func (s *InMemorySMSCodeStore) Issue(_ context.Context, phone, code string, ttl time.Duration) error {
	s.mu[phone] = smsEntry{code: code, expiresAt: s.clock().Add(ttl)}
	delete(s.failures, phone)
	return nil
}

func (s *InMemorySMSCodeStore) Peek(phone string) (code string, found bool) {
	entry, ok := s.mu[phone]
	if !ok || !s.clock().Before(entry.expiresAt) {
		return "", false
	}
	return entry.code, true
}

func (s *InMemorySMSCodeStore) Verify(_ context.Context, phone, expected string, maxFailures int) (verified, lockedOut, found bool, err error) {
	entry, ok := s.mu[phone]
	if !ok || !s.clock().Before(entry.expiresAt) {
		return false, false, false, nil
	}
	if subtle.ConstantTimeCompare([]byte(entry.code), []byte(expected)) != 1 {
		s.failures[phone]++
		if s.failures[phone] >= maxFailures {
			delete(s.mu, phone)
			delete(s.failures, phone)
			return false, true, true, nil
		}
		return false, false, true, nil
	}
	delete(s.mu, phone)
	delete(s.failures, phone)
	return true, false, true, nil
}

func (s *InMemorySMSCodeStore) BeginCooldown(_ context.Context, phone string, window time.Duration) (bool, error) {
	if until, ok := s.cooldown[phone]; ok && s.clock().Before(until) {
		return false, nil
	}
	s.cooldown[phone] = s.clock().Add(window)
	return true, nil
}

func (s *InMemorySMSCodeStore) ClearCooldown(_ context.Context, phone string) error {
	delete(s.cooldown, phone)
	return nil
}

var _ SMSCodeStore = (*InMemorySMSCodeStore)(nil)
