package redis

import (
	"testing"
)

func TestKeyBuildersFollowTheFrozenNamingBaseline(t *testing.T) {
	tests := []struct {
		name string
		call func() (string, error)
		want string
	}{
		{"session", func() (string, error) { return SessionKey("sess_01") }, "ncs:session:sess_01"},
		{"rate limit", func() (string, error) { return AuthRateLimitKey("13800000000") }, "ncs:auth:rate-limit:13800000000"},
		{"station", func() (string, error) { return StationKey("st_01") }, "ncs:station:st_01"},
		{"charger", func() (string, error) { return ChargerKey("ch_01") }, "ncs:charger:ch_01"},
		{"order lock", func() (string, error) { return OrderLockKey("o_01") }, "ncs:lock:order:o_01"},
		{"idempotency", func() (string, error) { return IdempotencyKey("order-start", "key_01") }, "ncs:idempotency:order-start:key_01"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.call()
			if err != nil {
				t.Fatalf("build key: %v", err)
			}
			// These literals are a frozen contract: changing one orphans every
			// key a running process wrote, so the assertion is exact.
			if got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
	}
}

// An empty identifier is the dangerous case: ncs:session:{session_id} with an
// empty id would collapse every caller onto the single shared key ncs:session:,
// which would leak one user's session to another. It must be an error.
func TestKeyBuildersRejectEmptyIdentifiers(t *testing.T) {
	builders := []struct {
		name string
		call func() (string, error)
	}{
		{"session", func() (string, error) { return SessionKey("") }},
		{"session blank", func() (string, error) { return SessionKey("   ") }},
		{"session tab", func() (string, error) { return SessionKey("\t") }},
		{"rate limit", func() (string, error) { return AuthRateLimitKey("") }},
		{"station", func() (string, error) { return StationKey("") }},
		{"charger", func() (string, error) { return ChargerKey("") }},
		{"order lock", func() (string, error) { return OrderLockKey("") }},
		{"idempotency scope", func() (string, error) { return IdempotencyKey("", "key_01") }},
		{"idempotency key", func() (string, error) { return IdempotencyKey("order", "") }},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			if _, err := builder.call(); err == nil {
				t.Fatal("expected an error for an empty identifier")
			}
		})
	}
}

// A colon inside an identifier would let a caller forge another key segment: an
// idempotency scope of "order-start:key_01" with an empty key would otherwise
// collide with the pair ("order-start", "key_01").
func TestKeyBuildersRejectIdentifierInjection(t *testing.T) {
	builders := []struct {
		name string
		call func() (string, error)
	}{
		{"session colon", func() (string, error) { return SessionKey("a:b") }},
		{"rate limit colon", func() (string, error) { return AuthRateLimitKey("a:b") }},
		{"station colon", func() (string, error) { return StationKey("a:b") }},
		{"idempotency scope colon", func() (string, error) { return IdempotencyKey("a:b", "key") }},
		{"idempotency key colon", func() (string, error) { return IdempotencyKey("order", "a:b") }},
		{"station newline", func() (string, error) { return StationKey("st\n01") }},
		{"charger space", func() (string, error) { return ChargerKey("ch 01") }},
		{"charger control", func() (string, error) { return ChargerKey("ch\x0001") }},
		{"lock surrounding space", func() (string, error) { return OrderLockKey(" o_01 ") }},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			if _, err := builder.call(); err == nil {
				t.Fatal("expected an error for an injectable identifier")
			}
		})
	}
}

func TestIsLockKeyRecognisesOnlyTheLockNamespace(t *testing.T) {
	lockKey, err := OrderLockKey("o_01")
	if err != nil {
		t.Fatalf("order lock key: %v", err)
	}
	if !IsLockKey(lockKey) {
		t.Fatalf("expected %q to be a lock key", lockKey)
	}

	for _, key := range []string{"", "ncs:session:x", "ncs:station:x", "ncs:auth:rate-limit:x"} {
		if IsLockKey(key) {
			t.Errorf("expected %q not to be a lock key", key)
		}
	}
}

func TestKeyBuildersAcceptRealisticIdentifiers(t *testing.T) {
	// Sessions and idempotency keys commonly carry base64url padding and station
	// ids carry a prefix, so the validator must not be over-eager.
	identifiers := []string{
		"st_01HZX9",
		"ab12+/=",
		"a-b_c.d",
		"13800000000",
		"order-2026-09-14",
	}
	for _, identifier := range identifiers {
		if _, err := SessionKey(identifier); err != nil {
			t.Errorf("expected %q to be accepted: %v", identifier, err)
		}
	}
}
