package auth

import (
	"strings"
	"testing"
)

func TestHashAndVerifyPasswordRoundTrip(t *testing.T) {
	hash, err := HashPasswordWithIterations("Dev-Password-01", 1000)
	if err != nil {
		t.Fatalf("HashPasswordWithIterations() error = %v", err)
	}
	if !strings.HasPrefix(hash, "pbkdf2-sha256$1000$") {
		t.Fatalf("hash = %q, want pbkdf2-sha256 format with iteration count", hash)
	}
	if !VerifyPassword("Dev-Password-01", hash) {
		t.Fatal("VerifyPassword(correct password) = false")
	}
	if VerifyPassword("Dev-Password-02", hash) {
		t.Fatal("VerifyPassword(wrong password) = true")
	}
}

func TestHashPasswordUsesUniqueSalts(t *testing.T) {
	first, err := HashPasswordWithIterations("Dev-Password-01", 1000)
	if err != nil {
		t.Fatalf("first hash: %v", err)
	}
	second, err := HashPasswordWithIterations("Dev-Password-01", 1000)
	if err != nil {
		t.Fatalf("second hash: %v", err)
	}
	if first == second {
		t.Fatal("two hashes of the same password are identical; salt is not random")
	}
}

func TestVerifyPasswordRejectsMalformedHashes(t *testing.T) {
	cases := map[string]string{
		"empty":             "",
		"bcrypt legacy":     "$2b$10$52EE6YgzPQ.0yP.m/oynSOGNWQbzLpFwJAW1qShTcnemd7TwhKufy",
		"wrong algorithm":   "scrypt$1000$c2FsdA$aGVsbG8",
		"missing parts":     "pbkdf2-sha256$1000$c2FsdA",
		"bad iterations":    "pbkdf2-sha256$many$c2FsdA$aGVsbG8",
		"zero iterations":   "pbkdf2-sha256$0$c2FsdA$aGVsbG8",
		"huge iterations":   "pbkdf2-sha256$99999999$c2FsdA$aGVsbG8",
		"bad salt encoding": "pbkdf2-sha256$1000$!!not-base64!!$aGVsbG8",
		"short derived key": "pbkdf2-sha256$1000$c2FsdA$abcd",
		"bad derived key":   "pbkdf2-sha256$1000$c2FsdA$!!not-base64!!",
	}
	for name, stored := range cases {
		if VerifyPassword("Dev-Password-01", stored) {
			t.Errorf("%s: malformed hash verified", name)
		}
	}
}

func TestHashPasswordRejectsOutOfRangePasswords(t *testing.T) {
	if _, err := HashPasswordWithIterations("short", 1000); err == nil {
		t.Fatal("7-character password accepted")
	}
	if _, err := HashPasswordWithIterations(strings.Repeat("x", 129), 1000); err == nil {
		t.Fatal("129-character password accepted")
	}
	if VerifyPassword("short", DummyHash()) {
		// Short plaintexts must fail verification, not panic.
		t.Fatal("short password unexpectedly verified")
	}
}

func TestDummyHashNeverMatchesAndParses(t *testing.T) {
	if !strings.HasPrefix(DummyHash(), "pbkdf2-sha256$") {
		t.Fatalf("dummy hash = %q, want package format", DummyHash())
	}
	// It must cost the same as a real verification (same iteration count)
	// while never matching a caller-chosen password.
	if VerifyPassword("Dev-Password-01", DummyHash()) {
		t.Fatal("dummy hash unexpectedly verified")
	}
}
