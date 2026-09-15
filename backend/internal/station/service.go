package station

import (
	"context"
	"errors"
	"fmt"
)

// Status values from the frozen contract.
const (
	StatusOpen     = "OPEN"
	StatusClosed   = "CLOSED"
	StatusDisabled = "DISABLED"

	ChargerStatusIdle       = "IDLE"
	ChargerStatusOccupied   = "OCCUPIED"
	ChargerStatusFault      = "FAULT"
	ChargerStatusRestarting = "RESTARTING"
	ChargerStatusDisabled   = "DISABLED"

	ConnectorAC = "AC"
	ConnectorDC = "DC"
)

var chargerStatuses = map[string]bool{
	ChargerStatusIdle:       true,
	ChargerStatusOccupied:   true,
	ChargerStatusFault:      true,
	ChargerStatusRestarting: true,
	ChargerStatusDisabled:   true,
}

// Station is the contract Station payload. Coordinates use the contract E6
// integer form (degrees multiplied by 1,000,000). DistanceMeter is present
// (possibly zero) exactly when the query carried coordinates.
type Station struct {
	ID                 int64  `json:"id"`
	Code               string `json:"code"`
	Name               string `json:"name"`
	Address            string `json:"address"`
	Status             string `json:"status"`
	LatitudeE6         int64  `json:"latitudeE6"`
	LongitudeE6        int64  `json:"longitudeE6"`
	ChargerCount       int64  `json:"chargerCount"`
	IdleChargerCount   int64  `json:"idleChargerCount"`
	MinPriceCentPerKwh int64  `json:"minPriceCentPerKwh"`
	DistanceMeter      *int64 `json:"distanceMeter,omitempty"`
}

// Charger is the contract Charger payload.
type Charger struct {
	ID        int64  `json:"id"`
	StationID int64  `json:"stationId"`
	Code      string `json:"code"`
	Type      string `json:"type"`
	PowerWatt int64  `json:"powerWatt"`
	Status    string `json:"status"`
}

// PageMeta is the contract pagination metadata.
type PageMeta struct {
	Page     int64 `json:"page"`
	PageSize int64 `json:"pageSize"`
	Total    int64 `json:"total"`
}

// StationPage is one page of stations.
type StationPage struct {
	Items []Station `json:"items"`
	Meta  PageMeta  `json:"meta"`
}

// ChargerPage is one page of chargers.
type ChargerPage struct {
	Items []Charger `json:"items"`
	Meta  PageMeta  `json:"meta"`
}

// StationFilter carries validated list parameters. Latitude/Longitude drive
// distance ordering (UC-U-02) and are only applied when HasLocation is set.
// IncludeDisabled is for administrative views; the user-facing C-end list
// always hides disabled stations.
type StationFilter struct {
	Page               int64
	PageSize           int64
	Keyword            string
	Latitude           float64
	Longitude          float64
	HasLocation        bool
	RadiusMeters       int64 // 0 = unlimited
	ConnectorType      string
	MaxPriceCentPerKwh int64 // 0 = no cap
	MinIdleChargers    int64
	IncludeDisabled    bool
}

// ChargerFilter carries validated list parameters. StationID is only applied
// when StationIDSet is true.
type ChargerFilter struct {
	Page         int64
	PageSize     int64
	StationID    int64
	StationIDSet bool
	Status       string
}

var (
	// ErrInvalidPagination reports page or pageSize outside contract bounds.
	ErrInvalidPagination = errors.New("station: invalid pagination parameters")
	// ErrInvalidChargerStatus reports a status filter outside the contract enum.
	ErrInvalidChargerStatus = errors.New("station: invalid charger status")
	// ErrInvalidStationID reports a station identifier below 1.
	ErrInvalidStationID = errors.New("station: invalid station id")
	// ErrStationNotFound reports a missing station; handlers map it to 404.
	ErrStationNotFound = errors.New("station: station not found")
	// ErrInvalidStationFilter reports an out-of-range list filter.
	ErrInvalidStationFilter = errors.New("station: invalid station filter")
)

// Reader is the PostgreSQL boundary for station and charger queries.
type Reader interface {
	ListStations(ctx context.Context, filter StationFilter) (StationPage, error)
	GetStation(ctx context.Context, stationID int64) (Station, error)
	ListChargers(ctx context.Context, filter ChargerFilter) (ChargerPage, error)
}

// Service validates parameters and delegates persistence to a Reader.
type Service struct {
	reader Reader
}

// NewService wires the service to its reader.
func NewService(reader Reader) (*Service, error) {
	if reader == nil {
		return nil, errors.New("station: reader is required")
	}
	return &Service{reader: reader}, nil
}

// ListStations returns one page of stations. With coordinates the result is
// ordered by distance (nearest first); otherwise by stable id order.
func (s *Service) ListStations(ctx context.Context, filter StationFilter) (StationPage, error) {
	if err := validatePagination(filter.Page, filter.PageSize); err != nil {
		return StationPage{}, err
	}
	if err := validateListFilters(filter); err != nil {
		return StationPage{}, err
	}

	result, err := s.reader.ListStations(ctx, filter)
	if err != nil {
		return StationPage{}, err
	}
	if result.Items == nil {
		result.Items = []Station{}
	}
	return result, nil
}

func validateListFilters(filter StationFilter) error {
	if filter.RadiusMeters < 0 || filter.MaxPriceCentPerKwh < 0 || filter.MinIdleChargers < 0 {
		return ErrInvalidStationFilter
	}
	if filter.RadiusMeters > 0 && !filter.HasLocation {
		// A radius without a query point silently returns an empty page —
		// reject it as a malformed request instead.
		return ErrInvalidStationFilter
	}
	if filter.ConnectorType != "" && filter.ConnectorType != ConnectorAC && filter.ConnectorType != ConnectorDC {
		return ErrInvalidStationFilter
	}
	return nil
}

// GetStation returns one station or ErrStationNotFound.
func (s *Service) GetStation(ctx context.Context, stationID int64) (Station, error) {
	if stationID < 1 {
		return Station{}, ErrInvalidStationID
	}

	result, err := s.reader.GetStation(ctx, stationID)
	if err != nil {
		if errors.Is(err, ErrStationNotFound) {
			return Station{}, ErrStationNotFound
		}
		return Station{}, fmt.Errorf("station: get station: %w", err)
	}
	return result, nil
}

// ListChargers returns one page of chargers with optional filters.
func (s *Service) ListChargers(ctx context.Context, filter ChargerFilter) (ChargerPage, error) {
	if err := validatePagination(filter.Page, filter.PageSize); err != nil {
		return ChargerPage{}, err
	}
	if filter.StationIDSet && filter.StationID < 1 {
		return ChargerPage{}, ErrInvalidStationID
	}
	if filter.Status != "" && !chargerStatuses[filter.Status] {
		return ChargerPage{}, ErrInvalidChargerStatus
	}

	result, err := s.reader.ListChargers(ctx, filter)
	if err != nil {
		return ChargerPage{}, err
	}
	if result.Items == nil {
		result.Items = []Charger{}
	}
	return result, nil
}

func validatePagination(page, pageSize int64) error {
	if page < 1 || pageSize < 1 || pageSize > 100 {
		return ErrInvalidPagination
	}
	return nil
}
