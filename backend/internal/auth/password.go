package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// Password hashing uses PBKDF2-HMAC-SHA256, keeping go.mod free of heavyweight
// password dependencies. The encoded format carries its parameters so the
// iteration count can rise later without invalidating stored hashes:
//
//	pbkdf2-sha256$<iterations>$<salt base64url>$<derived key base64url>
const (
	passwordHashAlgorithm = "pbkdf2-sha256"
	defaultHashIterations = 210000
	saltLength            = 16
	derivedKeyLength      = 32
	minPasswordLength     = 8
	maxPasswordLength     = 128
)

var (
	// ErrInvalidPasswordFormat reports a stored hash that cannot be parsed.
	ErrInvalidPasswordFormat = errors.New("auth: stored password hash format is invalid")
	// ErrPasswordLength reports a plaintext outside the contract bounds.
	ErrPasswordLength = errors.New("auth: password length is outside 8..128")
)

// HashPassword derives a storable hash with the default iteration count.
func HashPassword(password string) (string, error) {
	return HashPasswordWithIterations(password, defaultHashIterations)
}

// HashPasswordWithIterations derives a storable hash with an explicit
// iteration count. Tests use a low count; production callers use the default.
func HashPasswordWithIterations(password string, iterations int) (string, error) {
	if err := validatePasswordLength(password); err != nil {
		return "", err
	}
	if iterations < 1 {
		return "", fmt.Errorf("auth: iteration count must be positive")
	}

	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: generate salt: %w", err)
	}
	derived := pbkdf2.Key([]byte(password), salt, iterations, derivedKeyLength, sha256.New)

	encode := base64.RawURLEncoding.EncodeToString
	return fmt.Sprintf("%s$%d$%s$%s", passwordHashAlgorithm, iterations, encode(salt), encode(derived)), nil
}

// VerifyPassword reports whether the plaintext matches the stored hash.
// Malformed stored hashes never match; the caller decides how to surface
// account problems so this function leaks nothing about the hash state.
func VerifyPassword(password, stored string) bool {
	if err := validatePasswordLength(password); err != nil {
		return false
	}

	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != passwordHashAlgorithm {
		return false
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 1 || iterations > 10_000_000 {
		return false
	}
	decode := base64.RawURLEncoding.DecodeString
	salt, err := decode(parts[2])
	if err != nil || len(salt) == 0 {
		return false
	}
	expected, err := decode(parts[3])
	if err != nil || len(expected) < derivedKeyLength {
		return false
	}

	derived := pbkdf2.Key([]byte(password), salt, iterations, len(expected), sha256.New)
	return hmac.Equal(derived, expected)
}

// dummyHash is a valid-format hash of an unknowable secret. Login flows
// verify against it when the account does not exist so response timing does
// not reveal account existence.
var dummyHash = func() string {
	salt := []byte("ncs-dummy-salt-0000000000016b")
	derived := pbkdf2.Key([]byte("ncs-dummy-password-not-a-real-secret"), salt, defaultHashIterations, derivedKeyLength, sha256.New)
	encode := base64.RawURLEncoding.EncodeToString
	return fmt.Sprintf("%s$%d$%s$%s", passwordHashAlgorithm, defaultHashIterations, encode(salt), encode(derived))
}()

// DummyHash returns the shared dummy hash used to equalize login timing.
func DummyHash() string {
	return dummyHash
}

func validatePasswordLength(password string) error {
	if len(password) < minPasswordLength || len(password) > maxPasswordLength {
		return ErrPasswordLength
	}
	return nil
}
