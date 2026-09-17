package station

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/heguangV/charging-station-platform/backend/internal/auth"
	"github.com/heguangV/charging-station-platform/backend/internal/config"
	"github.com/heguangV/charging-station-platform/backend/internal/httpapi"
)

// fakeAuthProvider stands in for auth.Handlers in middleware tests.
type fakeAuthProvider struct {
	identity auth.Identity
	found    bool
}

func (f fakeAuthProvider) RequireIdentity(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !f.found {
			httpapi.WriteError(w, r, http.StatusUnauthorized, httpapi.CodeUnauthorized, "session is missing or expired", nil)
			return
		}
		next(w, r)
	}
}

type fixture struct {
	server *httpapi.Server
	reader *fakeReader
}

func newFixture(t *testing.T, identity auth.Identity, authenticated bool) fixture {
	t.Helper()
	reader := &fakeReader{stations: StationPage{
		Items: []Station{{ID: 1, Code: "ST-DEV-01", Name: "开发充电站", Address: "成都市", Status: StatusOpen, LatitudeE6: 30545200, LongitudeE6: 104070800, ChargerCount: 2}},
		Meta:  PageMeta{Page: 1, PageSize: 20, Total: 1},
	}}
	service, err := NewService(reader)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	provider := fakeAuthProvider{identity: identity, found: authenticated}
	handlers, err := NewHandlers(service, provider)
	if err != nil {
		t.Fatalf("NewHandlers() error = %v", err)
	}

	server := httpapi.NewServer(config.Config{RequestIDHeader: "X-Request-ID"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handlers.Register(server)
	server.SetReady(true)
	return fixture{server: server, reader: reader}
}

func get(t *testing.T, handler http.Handler, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	var payload map[string]any
	if recorder.Body.Len() > 0 {
		if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
			t.Fatalf("decode envelope: %v (body %q)", err, recorder.Body.String())
		}
	}
	return recorder, payload
}

func TestStationsRequireAuthentication(t *testing.T) {
	fixture := newFixture(t, auth.Identity{}, false)

	recorder, payload := get(t, fixture.server.Handler(), "/api/v1/stations")
	if recorder.Code != http.StatusUnauthorized || payload["code"].(float64) != httpapi.CodeUnauthorized {
		t.Fatalf("anonymous stations: status = %d payload = %#v", recorder.Code, payload)
	}
}

func TestListStationsReturnsContractShape(t *testing.T) {
	fixture := newFixture(t, auth.Identity{ID: 7, Role: auth.RoleUser, Status: auth.StatusActive}, true)

	recorder, payload := get(t, fixture.server.Handler(), "/api/v1/stations?page=1&pageSize=20&keyword=%E5%85%85%E7%94%B5")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if payload["success"] != true || payload["code"].(float64) != httpapi.CodeOK {
		t.Fatalf("envelope = %#v", payload)
	}

	data := payload["data"].(map[string]any)
	items := data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %#v", items)
	}
	stationItem := items[0].(map[string]any)
	if stationItem["id"].(float64) != 1 || stationItem["code"] != "ST-DEV-01" ||
		stationItem["status"] != StatusOpen || stationItem["latitudeE6"].(float64) != 30545200 ||
		stationItem["chargerCount"].(float64) != 2 {
		t.Fatalf("station = %#v", stationItem)
	}
	meta := data["meta"].(map[string]any)
	if meta["page"].(float64) != 1 || meta["pageSize"].(float64) != 20 || meta["total"].(float64) != 1 {
		t.Fatalf("meta = %#v", meta)
	}

	filter := fixture.reader.requests[0].(StationFilter)
	if filter.Keyword != "充电" || filter.Page != 1 || filter.PageSize != 20 {
		t.Fatalf("filter = %#v", filter)
	}
}

func TestListStationsHTTPRejectsInvalidPagination(t *testing.T) {
	fixture := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)

	for _, path := range []string{"/api/v1/stations?page=0", "/api/v1/stations?pageSize=0", "/api/v1/stations?pageSize=101"} {
		recorder, payload := get(t, fixture.server.Handler(), path)
		if recorder.Code != http.StatusBadRequest || payload["code"].(float64) != httpapi.CodeInvalidArgument {
			t.Errorf("%s: status = %d payload = %#v", path, recorder.Code, payload)
		}
	}
}

func TestGetStationNotFoundAndInvalidID(t *testing.T) {
	notFound := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)
	notFound.reader.err = ErrStationNotFound

	recorder, payload := get(t, notFound.server.Handler(), "/api/v1/stations/99")
	if recorder.Code != http.StatusNotFound || payload["code"].(float64) != httpapi.CodeResourceNotFound {
		t.Fatalf("missing station: status = %d payload = %#v", recorder.Code, payload)
	}

	fixture := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)
	recorder, payload = get(t, fixture.server.Handler(), "/api/v1/stations/not-a-number")
	if recorder.Code != http.StatusBadRequest || payload["code"].(float64) != httpapi.CodeInvalidArgument {
		t.Fatalf("bad id: status = %d payload = %#v", recorder.Code, payload)
	}
}

func TestListChargersValidationAndFilters(t *testing.T) {
	fixture := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)

	recorder, payload := get(t, fixture.server.Handler(), "/api/v1/chargers?stationId=1&status=IDLE")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	data := payload["data"].(map[string]any)
	items := data["items"].([]any)
	first := items[0].(map[string]any)
	if first["type"] != ConnectorAC || first["powerWatt"].(float64) != 7000 || first["status"] != ChargerStatusIdle {
		t.Fatalf("charger = %#v", first)
	}
	filter := fixture.reader.requests[0].(ChargerFilter)
	if !filter.StationIDSet || filter.StationID != 1 || filter.Status != ChargerStatusIdle {
		t.Fatalf("filter = %#v", filter)
	}

	for _, path := range []string{
		"/api/v1/chargers?status=AVAILABLE",
		"/api/v1/chargers?stationId=zero",
		"/api/v1/chargers?pageSize=1000",
	} {
		recorder, payload := get(t, fixture.server.Handler(), path)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d payload = %#v", path, recorder.Code, payload)
		}
	}
}

func TestStationServiceErrorSurfacesAs503(t *testing.T) {
	fixture := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)
	fixture.reader.err = context.DeadlineExceeded

	recorder, payload := get(t, fixture.server.Handler(), "/api/v1/stations")
	if recorder.Code != http.StatusServiceUnavailable || payload["code"].(float64) != httpapi.CodeDatabaseError {
		t.Fatalf("status = %d payload = %#v, want 503/%d", recorder.Code, payload, httpapi.CodeDatabaseError)
	}
}

func TestWrongMethodOnStationRoutes(t *testing.T) {
	fixture := newFixture(t, auth.Identity{Status: auth.StatusActive}, true)

	request := httptest.NewRequest(http.MethodDelete, "/api/v1/stations", nil)
	recorder := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", recorder.Header().Get("Allow"))
	}
}

// auth.Handlers must keep satisfying the middleware contract.
var _ identityProvider = (*auth.Handlers)(nil)
