package auth

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/httpapi"
)

const bearerPrefix = "Bearer "

// Registry codes for SMS verification outcomes (docs/database-api.md §1.10).
const (
	codeSMSCodeInvalid      = 20 // CODE_INVALID
	codeExternalServiceDown = 12 // EXTERNAL_SERVICE_UNAVAILABLE
)

type contextKey string

const identityContextKey contextKey = "auth_identity"

// Handlers expose the auth endpoints and the authentication middleware.
// Method enforcement stays here so every error keeps the unified envelope.
type Handlers struct {
	service *Service
}

// NewHandlers binds the handlers to the auth service.
func NewHandlers(service *Service) (*Handlers, error) {
	if service == nil {
		return nil, errors.New("auth: service is required")
	}
	return &Handlers{service: service}, nil
}

// identityBody is the contract Identity payload. adminRole is present only
// for administrators.
type identityBody struct {
	ID          int64  `json:"id"`
	Role        string `json:"role"`
	AdminRole   string `json:"adminRole,omitempty"`
	DisplayName string `json:"displayName"`
	Status      string `json:"status"`
}

type loginRequest struct {
	Account  string `json:"account"`
	Password string `json:"password"`
}

type loginResponse struct {
	AccessToken string       `json:"accessToken"`
	ExpiresAt   string       `json:"expiresAt"`
	Identity    identityBody `json:"identity"`
}

type smsCodeRequest struct {
	Phone string `json:"phone"`
}

type smsLoginRequest struct {
	Phone string `json:"phone"`
	Code  string `json:"code"`
}

type smsCodeResponse struct {
	Phone        string `json:"phone"`
	ExpiresInSec int64  `json:"expiresInSec"`
	// Code is only returned when simulated SMS delivery is configured
	// (development environments, SRS UC-U-01).
	Code string `json:"code,omitempty"`
}

type smsLoginResponse struct {
	AccessToken string       `json:"accessToken"`
	ExpiresAt   string       `json:"expiresAt"`
	Identity    identityBody `json:"identity"`
}

// Register attaches every auth route to the server.
func (h *Handlers) Register(server interface {
	Register(pattern string, handler http.HandlerFunc)
}) {
	server.Register("/api/v1/auth/user/sms/code", h.RequestSMSCode)
	server.Register("/api/v1/auth/user/login/sms", h.SMSLogin)
	server.Register("/api/v1/auth/user/login", h.UserLogin)
	server.Register("/api/v1/auth/admin/login", h.AdminLogin)
	server.Register("/api/v1/auth/logout", h.Logout)
	server.Register("/api/v1/me", h.Me)
}

// RequestSMSCode handles POST /api/v1/auth/user/sms/code. Development
// environments return the generated code in the response (simulated SMS);
// without simulated delivery the request fails 503 because no provider is
// wired yet.
func (h *Handlers) RequestSMSCode(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	body, ok := decodeJSONBody(w, r, &smsCodeRequest{})
	if !ok {
		return
	}
	request := body.(*smsCodeRequest)

	code, err := h.service.IssueSMSCode(r.Context(), request.Phone)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}

	response := smsCodeResponse{
		Phone:        strings.TrimSpace(request.Phone),
		ExpiresInSec: int64(smsCodeTTL.Seconds()),
	}
	if h.service.SMSMock {
		response.Code = code
	}
	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data:    response,
	})
}

// SMSLogin handles POST /api/v1/auth/user/login/sms. Unknown phones register
// automatically with a wallet (UC-U-01).
func (h *Handlers) SMSLogin(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	body, ok := decodeJSONBody(w, r, &smsLoginRequest{})
	if !ok {
		return
	}
	request := body.(*smsLoginRequest)

	result, err := h.service.LoginBySms(r.Context(), request.Phone, request.Code)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data:    newLoginResponse(result),
	})
}

// UserLogin handles POST /api/v1/auth/user/login.
func (h *Handlers) UserLogin(w http.ResponseWriter, r *http.Request) {
	h.login(w, r, h.service.Login)
}

// AdminLogin handles POST /api/v1/auth/admin/login.
func (h *Handlers) AdminLogin(w http.ResponseWriter, r *http.Request) {
	h.login(w, r, h.service.LoginAdmin)
}

func (h *Handlers) login(w http.ResponseWriter, r *http.Request, action func(ctx context.Context, account, password string) (LoginResult, error)) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	body, ok := decodeJSONBody(w, r, &loginRequest{})
	if !ok {
		return
	}
	request := body.(*loginRequest)

	// Field-length violations are malformed requests (400), not failed
	// authentication; the service guard behind this stays as defense.
	if len(request.Account) < minAccountLength || len(request.Account) > maxAccountLength ||
		len(request.Password) < minPasswordLength || len(request.Password) > maxPasswordLength {
		httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "account or password length out of range", nil)
		return
	}

	result, err := action(r.Context(), request.Account, request.Password)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data:    newLoginResponse(result),
	})
}

// Logout handles POST /api/v1/auth/logout. Revoking an unknown token still
// returns 204 so logout is idempotent.
func (h *Handlers) Logout(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	token, ok := bearerToken(w, r)
	if !ok {
		return
	}
	if err := h.service.Logout(r.Context(), token); err != nil {
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "logout failed", nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Me handles GET /api/v1/me.
func (h *Handlers) Me(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	identity, ok := h.requireIdentity(w, r)
	if !ok {
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data:    newIdentityBody(identity),
	})
}

// RequireIdentity wraps a handler so it only runs with a valid bearer token.
// The resolved identity is attached to the request context.
func (h *Handlers) RequireIdentity(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity, ok := h.requireIdentity(w, r)
		if !ok {
			return
		}
		next(w, r.WithContext(WithIdentity(r.Context(), identity)))
	}
}

// RequireRole additionally enforces a role (for example RoleAdmin) and maps
// mismatches to 403 FORBIDDEN.
func (h *Handlers) RequireRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return h.RequireIdentity(func(w http.ResponseWriter, r *http.Request) {
		identity, _ := IdentityFromContext(r.Context())
		if !Authorize(identity, role) {
			httpapi.WriteError(w, r, http.StatusForbidden, httpapi.CodeForbidden, "insufficient permission", nil)
			return
		}
		next(w, r)
	})
}

// RequireAdminWrite additionally enforces the write-capable admin roles
// (SUPER_ADMIN, OPERATOR): read-only auditors receive 403 FORBIDDEN.
func (h *Handlers) RequireAdminWrite(next http.HandlerFunc) http.HandlerFunc {
	return h.RequireRole(RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
		identity, _ := IdentityFromContext(r.Context())
		if !AdminCanWrite(identity) {
			httpapi.WriteError(w, r, http.StatusForbidden, httpapi.CodeForbidden, "insufficient permission for administrative writes", nil)
			return
		}
		next(w, r)
	})
}

func (h *Handlers) requireIdentity(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	token, ok := bearerToken(w, r)
	if !ok {
		return Identity{}, false
	}

	identity, err := h.service.Identify(r.Context(), token)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "session is missing or expired", nil)
			return Identity{}, false
		}
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "session check failed", nil)
		return Identity{}, false
	}
	return identity, true
}

func bearerToken(w http.ResponseWriter, r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, bearerPrefix) {
		httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "bearer token is required", nil)
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, bearerPrefix))
	if token == "" {
		httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "bearer token is required", nil)
		return "", false
	}
	return token, true
}

func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrInvalidPhone), errors.Is(err, ErrInvalidSMSCode):
		httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid phone number or code", nil)
	case errors.Is(err, ErrSMSCooldownActive):
		httpapi.WriteError(w, r, http.StatusTooManyRequests, httpapi.CodeRateLimited, "sms code was requested recently", map[string]any{"retryAfterSec": int64(smsResendCooldown.Seconds())})
	case errors.Is(err, ErrSMSCodeInvalid):
		httpapi.WriteError(w, r, http.StatusUnprocessableEntity, codeSMSCodeInvalid, "sms code is wrong, expired, or exceeded its failure budget", nil)
	case errors.Is(err, ErrSMSProviderNotConfigured):
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, codeExternalServiceDown, "no sms provider is configured", nil)
	case errors.Is(err, ErrInvalidCredentials):
		httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "invalid credentials", nil)
	case errors.Is(err, ErrUserFrozen):
		httpapi.WriteError(w, r, http.StatusForbidden, httpapi.CodeUserFrozen, "account is disabled", nil)
	case errors.Is(err, ErrUnauthorized):
		httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "session is missing or expired", nil)
	case isRateLimited(err):
		var limited *RateLimitedError
		errors.As(err, &limited)
		retryAfterSec := int64(math.Ceil(limited.RetryAfter.Seconds()))
		if retryAfterSec < 1 {
			retryAfterSec = 1
		}
		httpapi.WriteError(w, r, http.StatusTooManyRequests, httpapi.CodeRateLimited, "too many requests", map[string]any{"retryAfterSec": retryAfterSec})
	default:
		// Credential lookup or session persistence failed; the request never
		// reached the caller's fault. Database details stay in server logs.
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "login is temporarily unavailable", nil)
	}
}

func isRateLimited(err error) bool {
	var limited *RateLimitedError
	return errors.As(err, &limited)
}

func newIdentityBody(identity Identity) identityBody {
	return identityBody{
		ID:          identity.ID,
		Role:        identity.Role,
		AdminRole:   identity.AdminRole,
		DisplayName: identity.DisplayName,
		Status:      identity.Status,
	}
}

func newLoginResponse(result LoginResult) loginResponse {
	return loginResponse{
		AccessToken: result.Token,
		ExpiresAt:   result.ExpiresAt.UTC().Format(time.RFC3339),
		Identity:    newIdentityBody(result.Identity),
	}
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	httpapi.WriteError(w, r, http.StatusMethodNotAllowed, httpapi.CodeMethodNotAllowed, "method not allowed", nil)
	return false
}

const maxJSONBodyBytes = 64 << 10

// decodeJSONBody parses a bounded JSON body. On failure the error envelope is
// already written and ok is false.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) (any, bool) {
	reader := http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(target); err != nil {
		httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid request body", nil)
		return nil, false
	}
	return target, true
}

// IdentityFromContext returns the identity attached by RequireIdentity.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityContextKey).(Identity)
	return identity, ok
}

// WithIdentity attaches an identity to a context. It is part of the
// middleware contract and lets composed middleware and tests supply the
// identity the same way RequireIdentity does.
func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityContextKey, identity)
}
