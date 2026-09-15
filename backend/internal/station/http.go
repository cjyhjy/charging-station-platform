package station

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/heguangV/charging-station-platform/backend/internal/httpapi"
)

// identityProvider supplies RequireIdentity. auth.Handlers implements it,
// which keeps the station package free of a direct auth dependency while the
// middleware behaviour stays identical for every module.
type identityProvider interface {
	RequireIdentity(next http.HandlerFunc) http.HandlerFunc
}

// Handlers expose the station and charger query endpoints. Every route
// requires an authenticated identity (user or admin).
type Handlers struct {
	service *Service
	auth    identityProvider
}

// NewHandlers binds the query handlers to the service and auth middleware.
func NewHandlers(service *Service, auth identityProvider) (*Handlers, error) {
	if service == nil {
		return nil, errors.New("station: service is required")
	}
	if auth == nil {
		return nil, errors.New("station: auth middleware is required")
	}
	return &Handlers{service: service, auth: auth}, nil
}

// Register attaches every station route to the server.
func (h *Handlers) Register(server interface {
	Register(pattern string, handler http.HandlerFunc)
}) {
	server.Register("/api/v1/stations", h.auth.RequireIdentity(h.ListStations))
	server.Register("/api/v1/stations/{stationId}", h.auth.RequireIdentity(h.GetStation))
	server.Register("/api/v1/chargers", h.auth.RequireIdentity(h.ListChargers))
}

// ListStations handles GET /api/v1/stations.
func (h *Handlers) ListStations(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	page, pageSize, ok := parsePagination(w, r)
	if !ok {
		return
	}
	keyword := r.URL.Query().Get("keyword")
	if len(keyword) > 100 {
		writeInvalidQuery(w, r)
		return
	}

	filter := StationFilter{Page: page, PageSize: pageSize, Keyword: keyword}

	// Coordinates drive UC-U-02 distance ordering; both values are required
	// as a pair.
	query := r.URL.Query()
	latitudeE6, longitudeE6, hasLocation, ok := parseOptionalE6(w, r)
	if !ok {
		return
	}
	if hasLocation {
		// E6 values must stay inside the physical coordinate ranges
		// (±90° latitude, ±180° longitude) — a 90e6 bound would exclude the
		// poles, so the check mirrors the database CHECK constraints.
		if latitudeE6 < -90_000_000 || latitudeE6 > 90_000_000 ||
			longitudeE6 < -180_000_000 || longitudeE6 > 180_000_000 {
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "coordinates out of range", nil)
			return
		}
		filter.HasLocation = true
		filter.Latitude = float64(latitudeE6) / 1e6
		filter.Longitude = float64(longitudeE6) / 1e6
	}

	if raw := query.Get("radiusMeters"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid radius", nil)
			return
		}
		filter.RadiusMeters = value
	}
	if raw := query.Get("connectorType"); raw != "" {
		filter.ConnectorType = raw
	}
	if raw := query.Get("maxPriceCentPerKwh"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid price cap", nil)
			return
		}
		filter.MaxPriceCentPerKwh = value
	}
	if raw := query.Get("minIdleChargers"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid idle charger minimum", nil)
			return
		}
		filter.MinIdleChargers = value
	}

	result, err := h.service.ListStations(r.Context(), filter)
	if err != nil {
		if errors.Is(err, ErrInvalidPagination) || errors.Is(err, ErrInvalidStationFilter) {
			writeInvalidQuery(w, r)
			return
		}
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "stations are temporarily unavailable", nil)
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data: map[string]any{
			"items": result.Items,
			"meta":  result.Meta,
		},
	})
}

// GetStation handles GET /api/v1/stations/{stationId}.
func (h *Handlers) GetStation(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	stationID, err := strconv.ParseInt(r.PathValue("stationId"), 10, 64)
	if err != nil || stationID < 1 {
		httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid station id", nil)
		return
	}

	result, err := h.service.GetStation(r.Context(), stationID)
	if err != nil {
		if errors.Is(err, ErrStationNotFound) {
			httpapi.WriteError(w, r, http.StatusNotFound, httpapi.CodeResourceNotFound, "station not found", nil)
			return
		}
		httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "station is temporarily unavailable", nil)
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data:    result,
	})
}

// ListChargers handles GET /api/v1/chargers.
func (h *Handlers) ListChargers(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	page, pageSize, ok := parsePagination(w, r)
	if !ok {
		return
	}

	filter := ChargerFilter{Page: page, PageSize: pageSize}
	query := r.URL.Query()
	if raw := query.Get("stationId"); raw != "" {
		stationID, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || stationID < 1 {
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid station id", nil)
			return
		}
		filter.StationID = stationID
		filter.StationIDSet = true
	}
	if raw := query.Get("status"); raw != "" {
		filter.Status = raw
	}

	result, err := h.service.ListChargers(r.Context(), filter)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidPagination), errors.Is(err, ErrInvalidStationID):
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid query parameter", nil)
		case errors.Is(err, ErrInvalidChargerStatus):
			httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid charger status", nil)
		default:
			httpapi.WriteError(w, r, http.StatusServiceUnavailable, httpapi.CodeDatabaseError, "chargers are temporarily unavailable", nil)
		}
		return
	}

	httpapi.WriteJSON(w, http.StatusOK, httpapi.Response{
		Success: true,
		Code:    httpapi.CodeOK,
		Message: "ok",
		Data: map[string]any{
			"items": result.Items,
			"meta":  result.Meta,
		},
	})
}

func parsePagination(w http.ResponseWriter, r *http.Request) (page, pageSize int64, ok bool) {
	page, pageSize, err := httpapi.ParsePagination(r)
	if err != nil {
		writeInvalidQuery(w, r)
		return 0, 0, false
	}
	return page, pageSize, true
}

// parseOptionalE6 validates the contract coordinate query parameters when
// present. Both must parse as int64; hasLocation reports whether the pair
// was supplied.
func parseOptionalE6(w http.ResponseWriter, r *http.Request) (latitudeE6, longitudeE6 int64, hasLocation, ok bool) {
	query := r.URL.Query()
	rawLat := query.Get("latitudeE6")
	rawLng := query.Get("longitudeE6")
	if rawLat == "" && rawLng == "" {
		return 0, 0, false, true
	}
	if rawLat == "" || rawLng == "" {
		writeInvalidQuery(w, r)
		return 0, 0, false, false
	}
	latitudeE6, latErr := strconv.ParseInt(rawLat, 10, 64)
	longitudeE6, lngErr := strconv.ParseInt(rawLng, 10, 64)
	if latErr != nil || lngErr != nil {
		writeInvalidQuery(w, r)
		return 0, 0, false, false
	}
	return latitudeE6, longitudeE6, true, true
}

func writeInvalidQuery(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteError(w, r, http.StatusBadRequest, httpapi.CodeInvalidArgument, "invalid query parameter", nil)
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	httpapi.WriteError(w, r, http.StatusMethodNotAllowed, httpapi.CodeMethodNotAllowed, "method not allowed", nil)
	return false
}
