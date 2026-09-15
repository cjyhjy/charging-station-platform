package redis

import (
	"fmt"
	"strings"
	"unicode"
)

// Key prefixes. They are the frozen naming baseline from
// docs/migration/directory-and-contract-definition.md section 7. Changing a
// prefix is a contract change: it orphans every key written by a running
// process, so these constants are the single source of truth.
const (
	keyPrefixSession     = "ncs:session"
	keyPrefixAuthLimit   = "ncs:auth:rate-limit"
	keyPrefixStation     = "ncs:station"
	keyPrefixCharger     = "ncs:charger"
	keyPrefixOrderLock   = "ncs:lock:order"
	keyPrefixIdempotency = "ncs:idempotency"

	// keyPrefixLock marks every distributed lock. Locker refuses a key outside
	// this namespace so a bad argument cannot delete session or cache data.
	keyPrefixLock = "ncs:lock:"
)

// SessionKey builds ncs:session:{session_id}.
func SessionKey(sessionID string) (string, error) {
	if err := validateKeyPart("session id", sessionID); err != nil {
		return "", err
	}
	return keyPrefixSession + ":" + sessionID, nil
}

// AuthRateLimitKey builds ncs:auth:rate-limit:{identity}.
func AuthRateLimitKey(identity string) (string, error) {
	if err := validateKeyPart("rate limit identity", identity); err != nil {
		return "", err
	}
	return keyPrefixAuthLimit + ":" + identity, nil
}

// StationKey builds ncs:station:{station_id}.
func StationKey(stationID string) (string, error) {
	if err := validateKeyPart("station id", stationID); err != nil {
		return "", err
	}
	return keyPrefixStation + ":" + stationID, nil
}

// ChargerKey builds ncs:charger:{charger_id}.
func ChargerKey(chargerID string) (string, error) {
	if err := validateKeyPart("charger id", chargerID); err != nil {
		return "", err
	}
	return keyPrefixCharger + ":" + chargerID, nil
}

// OrderLockKey builds ncs:lock:order:{order_id}.
func OrderLockKey(orderID string) (string, error) {
	if err := validateKeyPart("order id", orderID); err != nil {
		return "", err
	}
	return keyPrefixOrderLock + ":" + orderID, nil
}

// IdempotencyKey builds ncs:idempotency:{scope}:{key}.
//
// The B line exposes the key only: the authoritative idempotency record lives in
// PostgreSQL (idempotency_records), and this key exists so a caller can take a
// cheap Redis short-circuit in front of that record. It never replaces it.
func IdempotencyKey(scope, key string) (string, error) {
	if err := validateKeyPart("idempotency scope", scope); err != nil {
		return "", err
	}
	if err := validateKeyPart("idempotency key", key); err != nil {
		return "", err
	}
	return keyPrefixIdempotency + ":" + scope + ":" + key, nil
}

// IsLockKey reports whether key is inside the distributed lock namespace.
func IsLockKey(key string) bool { return strings.HasPrefix(key, keyPrefixLock) }

// validateKeyPart rejects an identifier that would silently collapse two
// different callers onto one key. An empty part would turn
// ncs:session:{session_id} into the shared key ncs:session:, and an embedded
// colon would let an identity forge another segment of the key, so both are
// errors rather than something the caller can ignore.
func validateKeyPart(label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", label)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not have surrounding whitespace", label)
	}
	for _, r := range value {
		switch {
		case r == ':':
			return fmt.Errorf("%s must not contain %q", label, ':')
		case r < 0x20 || r == 0x7f:
			return fmt.Errorf("%s must not contain control characters", label)
		case unicode.IsSpace(r):
			return fmt.Errorf("%s must not contain whitespace", label)
		}
	}
	return nil
}
