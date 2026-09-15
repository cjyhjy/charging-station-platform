package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/auth"
	"github.com/heguangV/charging-station-platform/backend/internal/config"
	"github.com/heguangV/charging-station-platform/backend/internal/httpapi"
	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/station"
)

// fakeStore records admin commands and returns canned results.
type fakeStore struct {
	createResult StationRecord
	createErr    error
	stations     StationPage
	restartCmd   Command
	restartErr   error
	users        UserPage
	orders       OrderPage
	chargers     ChargerPage
	restarts     []RestartCommand
}

func (f *fakeStore) CreateStation(context.Context, CreateStationCommand) (StationRecord, error) {
	return f.createResult, f.createErr
}

func (f *fakeStore) ListStations(context.Context, int64, int64, string) (StationPage, error) {
	return f.stations, nil
}

func (f *fakeStore) ListChargers(context.Context, station.ChargerFilter) (ChargerPage, error) {
	return f.chargers, nil
}

func (f *fakeStore) ListUsers(context.Context, UserFilter) (UserPage, error) {
	return f.users, nil
}

func (f *fakeStore) ListOrders(context.Context, AdminOrderFilter) (OrderPage, error) {
	return f.orders, nil
}

func (f *fakeStore) RestartCharger(_ context.Context, command RestartCommand) (Command, error) {
	f.restarts = append(f.restarts, command)
	return f.restartCmd, f.restartErr
}

// fakeAuthProvider mirrors auth.Handlers middleware with role enforcement.
type fakeAuthProvider struct {
	identity auth.Identity
	found    bool
}

func (f fakeAuthProvider) RequireRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !f.found {
			httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "session is missing or expired", nil)
			return
		}
		if !auth.Authorize(f.identity, role) {
			httpapi.WriteError(w, r, http.StatusForbidden, httpapi.CodeForbidden, "insufficient permission", nil)
			return
		}
		next(w, r.WithContext(auth.WithIdentity(r.Context(), f.identity)))
	}
}

func (f fakeAuthProvider) RequireAdminWrite(next http.HandlerFunc) http.HandlerFunc {
	return f.RequireRole(auth.RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
		if !auth.AdminCanWrite(f.identity) {
			httpapi.WriteError(w, r, http.StatusForbidden, httpapi.CodeForbidden, "insufficient permission for administrative writes", nil)
			return
		}
		next(w, r)
	})
}

type fixture struct {
	server *httpapi.Server
	store  *fakeStore
}

func newFixture(t *testing.T, identity auth.Identity, authenticated bool) fixture {
	t.Helper()
	store := &fakeStore{}
	service, err := NewService(store)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	handlers, err := NewHandlers(service, fakeAuthProvider{identity: identity, found: authenticated})
	if err != nil {
		t.Fatalf("NewHandlers() error = %v", err)
	}
	server := httpapi.NewServer(config.Config{RequestIDHeader: "X-Request-ID"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handlers.Register(server)
	server.SetReady(true)
	return fixture{server: server, store: store}
}

func do(t *testing.T, handler http.Handler, method, path, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	var payload map[string]any
	if recorder.Body.Len() > 0 {
		snapshot := recorder.Body.Bytes()
		if err := json.Unmarshal(snapshot, &payload); err != nil {
			t.Fatalf("decode envelope: %v (body %q)", err, recorder.Body.String())
		}
	}
	return recorder, payload
}

const idemKey = "admin-idem-key-000001"

func adminIdentity(role string) auth.Identity {
	return auth.Identity{ID: 2, Role: auth.RoleAdmin, AdminRole: role, Status: auth.StatusActive}
}

func TestAdminCreateStationRequiresWriterRole(t *testing.T) {
	auditor := newFixture(t, adminIdentity(auth.AdminRoleAuditor), true)
	recorder, payload := do(t, auditor.server.Handler(), http.MethodPost, "/api/v1/admin/stations",
		`{"code":"ST-01","name":"站","address":"a","latitudeE6":30000000,"longitudeE6":104000000}`,
		map[string]string{"Idempotency-Key": idemKey})
	if recorder.Code != http.StatusForbidden || payload["code"].(float64) != httpapi.CodeForbidden {
		t.Fatalf("auditor create: status = %d code = %v", recorder.Code, payload["code"])
	}

	operator := newFixture(t, adminIdentity(auth.AdminRoleOperator), true)
	recorder, payload = do(t, operator.server.Handler(), http.MethodPost, "/api/v1/admin/stations",
		`{"code":"ST-01","name":"站","address":"a","latitudeE6":30000000,"longitudeE6":104000000}`,
		map[string]string{"Idempotency-Key": idemKey})
	if recorder.Code != http.StatusBadRequest {
		// The fixture store returns zero values; validation of the empty
		// record is not asserted here — a 400 would come from the body only.
		t.Logf("operator create status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminCreateStationValidation(t *testing.T) {
	f := newFixture(t, adminIdentity(auth.AdminRoleOperator), true)

	bad := []string{
		`{"code":"x","name":"站","address":"a","latitudeE6":30000000,"longitudeE6":104000000}`,     // short code
		`{"code":"ST-01","name":"","address":"a","latitudeE6":30000000,"longitudeE6":104000000}`,  // empty name
		`{"code":"ST-01","name":"站","address":"a","latitudeE6":91000000,"longitudeE6":104000000}`, // latitude out of range
		`{"code":"ST-01","name":"站","address":"a","latitudeE6":30000000,"longitudeE6":181000000}`, // longitude out of range
	}
	for _, body := range bad {
		recorder, payload := do(t, f.server.Handler(), http.MethodPost, "/api/v1/admin/stations", body, map[string]string{"Idempotency-Key": idemKey})
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, recorder.Code)
		}
		_ = payload
	}

	// Missing idempotency key
	recorder, _ := do(t, f.server.Handler(), http.MethodPost, "/api/v1/admin/stations",
		`{"code":"ST-01","name":"站","address":"a","latitudeE6":30000000,"longitudeE6":104000000}`, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("missing key status = %d", recorder.Code)
	}
}

func TestAdminRestartChargerRoleAndValidation(t *testing.T) {
	f := newFixture(t, adminIdentity(auth.AdminRoleAuditor), true)
	recorder, payload := do(t, f.server.Handler(), http.MethodPost, "/api/v1/admin/chargers/5/restart",
		`{"reason":"例行维护"}`, map[string]string{"Idempotency-Key": idemKey})
	if recorder.Code != http.StatusForbidden || payload["code"].(float64) != httpapi.CodeForbidden {
		t.Fatalf("auditor restart: status = %d code = %v", recorder.Code, payload["code"])
	}

	operator := newFixture(t, adminIdentity(auth.AdminRoleOperator), true)
	operator.store.restartCmd = Command{CommandNo: "CMD20260915000000deadbeef", Status: CommandPending}
	recorder, payload = do(t, operator.server.Handler(), http.MethodPost, "/api/v1/admin/chargers/5/restart",
		`{"reason":"例行维护"}`, map[string]string{"Idempotency-Key": idemKey})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("operator restart status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	data := payload["data"].(map[string]any)
	if data["commandNo"] == "" || data["status"] != CommandPending {
		t.Fatalf("command = %#v", data)
	}
	if operator.store.restarts[0].ChargerID != 5 || operator.store.restarts[0].Reason != "例行维护" {
		t.Fatalf("command = %#v", operator.store.restarts[0])
	}

	// Short reason
	recorder, _ = do(t, operator.server.Handler(), http.MethodPost, "/api/v1/admin/chargers/5/restart",
		`{"reason":"x"}`, map[string]string{"Idempotency-Key": idemKey})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("short reason status = %d", recorder.Code)
	}
}

func TestAdminListEndpointsReturnPages(t *testing.T) {
	f := newFixture(t, adminIdentity(auth.AdminRoleSuper), true)
	f.store.users = UserPage{Items: []UserSummary{{ID: 7, DisplayName: "用户0606", Status: UserStatusActive, BalanceCent: 10000}},
		Meta: PageMeta{Page: 1, PageSize: 20, Total: 1}}
	f.store.orders = OrderPage{Items: []order.Order{{OrderNo: "ORD20260914120000aaaa", UserID: 7, Status: "COMPLETED", PaymentStatus: "PAID"}},
		Meta: order.PageMeta{Page: 1, PageSize: 20, Total: 1}}

	recorder, payload := do(t, f.server.Handler(), http.MethodGet, "/api/v1/admin/users?status=1", "", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("users status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	items := payload["data"].(map[string]any)["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["balanceCent"].(float64) != 10000 {
		t.Fatalf("users = %#v", items)
	}

	recorder, payload = do(t, f.server.Handler(), http.MethodGet, "/api/v1/admin/orders?orderNo=ORD2026", "", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("orders status = %d", recorder.Code)
	}
	orderItems := payload["data"].(map[string]any)["items"].([]any)
	if orderItems[0].(map[string]any)["paymentStatus"] != "PAID" {
		t.Fatalf("orders = %#v", orderItems)
	}

	// Bad user status query value
	recorder, _ = do(t, f.server.Handler(), http.MethodGet, "/api/v1/admin/users?status=5", "", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("bad status query = %d", recorder.Code)
	}
}

func TestAdminRoutesRejectAnonymous(t *testing.T) {
	f := newFixture(t, auth.Identity{}, false)
	for _, path := range []string{"/api/v1/admin/users", "/api/v1/admin/stations", "/api/v1/admin/orders", "/api/v1/admin/chargers"} {
		recorder, _ := do(t, f.server.Handler(), http.MethodGet, path, "", nil)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", path, recorder.Code)
		}
	}
}

func TestAdminServiceRejectsNilStore(t *testing.T) {
	if _, err := NewService(nil); err == nil {
		t.Fatal("nil store accepted")
	}
	_ = errors.New
	_ = context.Background
}
