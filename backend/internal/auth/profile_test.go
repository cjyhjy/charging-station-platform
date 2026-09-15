package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestMaskPhone(t *testing.T) {
	if got := MaskPhone("13812345678"); got != "138****5678" {
		t.Fatalf("MaskPhone = %q, want 138****5678", got)
	}
	if got := MaskPhone("short"); got != "***" {
		t.Fatalf("MaskPhone(short) = %q, want ***", got)
	}
}

func TestIsValidNickname(t *testing.T) {
	if !IsValidNickname("开发用户") {
		t.Fatal("valid nickname rejected")
	}
	if IsValidNickname("") || IsValidNickname("   ") || IsValidNickname(strings.Repeat("x", 21)) {
		t.Fatal("invalid nickname accepted")
	}
}

func TestProfileAndDeletionFlow(t *testing.T) {
	f := newHandlerFixture(t)
	server := f.server()

	// login
	recorder, _ := doJSON(t, server.Handler(), http.MethodPost, "/api/v1/auth/user/login",
		`{"account":"13800000001","password":"`+testPassword+`"}`, nil)
	var login envelope
	if err := json.NewDecoder(recorder.Body).Decode(&login); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	token := login.Data["accessToken"].(string)
	headers := map[string]string{"Authorization": "Bearer " + token}

	// profile view
	recorder, payload := doJSON(t, server.Handler(), http.MethodGet, "/api/v1/me/profile", "", headers)
	if recorder.Code != http.StatusOK {
		t.Fatalf("profile status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	profile := payload.Data
	if profile["phoneMasked"] != "138****0001" {
		t.Fatalf("phoneMasked = %v", profile["phoneMasked"])
	}

	// nickname update
	recorder, payload = doJSON(t, server.Handler(), http.MethodPut, "/api/v1/me/profile",
		`{"displayName":"新昵称"}`, headers)
	if recorder.Code != http.StatusOK {
		t.Fatalf("update status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	profile = payload.Data
	if profile["displayName"] != "新昵称" {
		t.Fatalf("displayName = %v", profile["displayName"])
	}

	// invalid nickname
	recorder, payload = doJSON(t, server.Handler(), http.MethodPut, "/api/v1/me/profile",
		`{"displayName":"   "}`, headers)
	if recorder.Code != http.StatusBadRequest || payload.Code != 1 {
		t.Fatalf("blank nickname: status = %d code = %v", recorder.Code, payload.Code)
	}

	// deletion: 204 and the session is revoked
	recorder, _ = doJSON(t, server.Handler(), http.MethodDelete, "/api/v1/me", "", headers)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	recorder, _ = doJSON(t, server.Handler(), http.MethodGet, "/api/v1/me", "", headers)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("me after deletion status = %d, want 401", recorder.Code)
	}
}

func TestFreezeRevokesSessionsAndBlocksLogin(t *testing.T) {
	f := newHandlerFixture(t)
	server := f.server()

	recorder, _ := doJSON(t, server.Handler(), http.MethodPost, "/api/v1/auth/user/login",
		`{"account":"13800000001","password":"`+testPassword+`"}`, nil)
	var login envelope
	_ = json.NewDecoder(recorder.Body).Decode(&login)
	token := login.Data["accessToken"].(string)
	headers := map[string]string{"Authorization": "Bearer " + token}

	// The admin freezes the user: sessions die immediately.
	adminHeaders := map[string]string{"Authorization": "Bearer adminToken"}
	_ = adminHeaders
	// Freeze through the service directly (the admin identity middleware is
	// covered by the admin package tests).
	if err := f.handlers.service.FreezeUser(context.Background(), 1, 7); err != nil {
		t.Fatalf("FreezeUser() error = %v", err)
	}

	recorder, _ = doJSON(t, server.Handler(), http.MethodGet, "/api/v1/me", "", headers)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("me after freeze status = %d, want 401", recorder.Code)
	}

	// BR-07: the frozen user cannot log in again.
	recorder, payload := doJSON(t, server.Handler(), http.MethodPost, "/api/v1/auth/user/login",
		`{"account":"13800000001","password":"`+testPassword+`"}`, nil)
	if recorder.Code != http.StatusForbidden || payload.Code != 6 {
		t.Fatalf("frozen login: status = %d code = %v", recorder.Code, payload.Code)
	}
}
