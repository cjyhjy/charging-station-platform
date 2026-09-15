package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/config"
)

func testServer() *Server {
	return NewServer(config.Config{RequestIDHeader: "X-Request-ID"}, nil)
}

func TestHealthzReturnsUnifiedResponseAndRequestID(t *testing.T) {
	server := testServer()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("Content-Type = %q", recorder.Header().Get("Content-Type"))
	}
	if !validRequestID(recorder.Header().Get("X-Request-ID")) {
		t.Fatalf("generated request ID is invalid: %q", recorder.Header().Get("X-Request-ID"))
	}

	var response Response
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Success || response.Code != CodeOK || response.Message != "ok" {
		t.Fatalf("response = %#v", response)
	}
}

func TestReadyzTracksReadiness(t *testing.T) {
	server := testServer()

	initial := httptest.NewRecorder()
	server.Handler().ServeHTTP(initial, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if initial.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial status = %d, want %d", initial.Code, http.StatusServiceUnavailable)
	}

	server.SetReady(true)
	ready := httptest.NewRecorder()
	server.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", ready.Code, http.StatusOK)
	}
}

func TestRequestIDAcceptsSafeIncomingValue(t *testing.T) {
	server := testServer()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("X-Request-ID", "req-test-01")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if got := recorder.Header().Get("X-Request-ID"); got != "req-test-01" {
		t.Fatalf("request ID = %q, want req-test-01", got)
	}
}

func TestAccessLogIncludesRequestID(t *testing.T) {
	var logs bytes.Buffer
	server := NewServer(
		config.Config{RequestIDHeader: "X-Request-ID"},
		slog.New(slog.NewTextHandler(&logs, nil)),
	)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("X-Request-ID", "req-log-01")

	server.Handler().ServeHTTP(httptest.NewRecorder(), request)

	if !strings.Contains(logs.String(), "request_id=req-log-01") {
		t.Fatalf("access log = %q", logs.String())
	}
}

func TestUnsupportedMethodAndUnknownPathUseJSONErrors(t *testing.T) {
	server := testServer()

	methodRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(methodRecorder, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if methodRecorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d, want %d", methodRecorder.Code, http.StatusMethodNotAllowed)
	}
	if methodRecorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", methodRecorder.Header().Get("Allow"))
	}

	var methodResponse Response
	if err := json.NewDecoder(methodRecorder.Body).Decode(&methodResponse); err != nil {
		t.Fatalf("decode method response: %v", err)
	}
	if methodResponse.Success || methodResponse.Code != CodeMethodNotAllowed {
		t.Fatalf("method response = %#v", methodResponse)
	}

	missingRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingRecorder, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d", missingRecorder.Code, http.StatusNotFound)
	}
	var missingResponse Response
	if err := json.NewDecoder(missingRecorder.Body).Decode(&missingResponse); err != nil {
		t.Fatalf("decode missing response: %v", err)
	}
	if missingResponse.Success || missingResponse.Code != CodeNotFound {
		t.Fatalf("missing response = %#v", missingResponse)
	}
}
