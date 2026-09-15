package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"

	"github.com/heguangV/charging-station-platform/backend/internal/station"
)

// StationStore implements station.Reader against the stations and chargers
// tables. Queries are parameterized; list totals are computed with a CTE so
// pages beyond the last row still report the real total.
type StationStore struct {
	db *sql.DB
}

// NewStationStore binds the store to a connection pool.
func NewStationStore(db *sql.DB) (*StationStore, error) {
	if db == nil {
		return nil, errors.New("postgres: station store requires a database")
	}
	return &StationStore{db: db}, nil
}

// stationFilterWhere and stationDistance are shared by the page query and
// the count query so the two can never drift apart. Placeholder numbering:
// $1/$2 coordinates (NULL when absent), $3 keyword, $4 connector type,
// $5 price cap, $6 idle minimum, $7 radius, $8 include-disabled.
const stationDistance = `CASE WHEN $1::float8 IS NULL THEN NULL::float8 ELSE
    6371000 * acos(least(1.0, greatest(-1.0,
        sin(radians($1)) * sin(radians(s.latitude)) +
        cos(radians($1)) * cos(radians(s.latitude)) * cos(radians(s.longitude) - radians($2)))))
END`

const stationFilterWhere = `($8::boolean OR s.status <> 'DISABLED')
      AND ($3 = '' OR s.code ILIKE $3 OR s.name ILIKE $3 OR s.address ILIKE $3)
      AND ($4 = '' OR EXISTS (SELECT 1 FROM chargers c WHERE c.station_id = s.id AND c.connector_type = $4))
      AND ($5 <= 0 OR COALESCE((SELECT min(c.price_per_kwh_cents + c.service_price_per_kwh_cents) FROM chargers c WHERE c.station_id = s.id), 0) <= $5)
      AND ($6 <= 0 OR (SELECT count(*) FROM chargers c WHERE c.station_id = s.id AND c.status = 'IDLE') >= $6)`

// stationListParams expands the shared filter arguments shared by both
// queries (coordinates, keyword, connector, price cap, idle minimum, radius,
// include-disabled).
func stationListParams(filter station.StationFilter) []any {
	var latitude, longitude any
	if filter.HasLocation {
		latitude = filter.Latitude
		longitude = filter.Longitude
	}
	return []any{
		latitude, longitude,
		likePattern(filter.Keyword),
		filter.ConnectorType,
		filter.MaxPriceCentPerKwh,
		filter.MinIdleChargers,
		filter.RadiusMeters,
		filter.IncludeDisabled,
	}
}

// ListStations returns one page ordered by distance when located, otherwise
// by stable id order. The total is a separate count over the same filter, so
// pages beyond the last row still report the real total.
func (s *StationStore) ListStations(ctx context.Context, filter station.StationFilter) (station.StationPage, error) {
	pageArgs := append(stationListParams(filter), filter.PageSize, (filter.Page-1)*filter.PageSize)
	rows, err := s.db.QueryContext(ctx, `WITH base AS (
    SELECT s.id, s.code, s.name, s.address, s.status,
           COALESCE(s.latitude::float8, 0) AS latitude,
           COALESCE(s.longitude::float8, 0) AS longitude,
           (SELECT count(*) FROM chargers c WHERE c.station_id = s.id) AS charger_count,
           (SELECT count(*) FROM chargers c WHERE c.station_id = s.id AND c.status = 'IDLE') AS idle_count,
           COALESCE((SELECT min(c.price_per_kwh_cents + c.service_price_per_kwh_cents) FROM chargers c WHERE c.station_id = s.id), 0) AS min_price,
           `+stationDistance+` AS distance
    FROM stations s
    WHERE `+stationFilterWhere+`
),
matching AS (
    SELECT * FROM base
    WHERE ($7 <= 0 OR distance <= $7::float8)
)
SELECT m.id, m.code, m.name, m.address, m.status, m.latitude, m.longitude,
       m.charger_count, m.idle_count, m.min_price, m.distance
FROM matching m
ORDER BY m.distance ASC NULLS LAST, m.id ASC
LIMIT $9 OFFSET $10`, pageArgs...)
	if err != nil {
		return station.StationPage{}, err
	}
	defer rows.Close()

	page := station.StationPage{Meta: station.PageMeta{Page: filter.Page, PageSize: filter.PageSize}}
	for rows.Next() {
		var item station.Station
		var latitude, longitude, distance sql.NullFloat64
		if err := rows.Scan(&item.ID, &item.Code, &item.Name, &item.Address, &item.Status,
			&latitude, &longitude, &item.ChargerCount, &item.IdleChargerCount, &item.MinPriceCentPerKwh,
			&distance); err != nil {
			return station.StationPage{}, err
		}
		item.LatitudeE6 = toE6(latitude.Float64)
		item.LongitudeE6 = toE6(longitude.Float64)
		if distance.Valid {
			meters := int64(math.Round(distance.Float64))
			item.DistanceMeter = &meters
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return station.StationPage{}, err
	}

	if err := s.db.QueryRowContext(ctx, `WITH base AS (
    SELECT `+stationDistance+` AS distance
    FROM stations s
    WHERE `+stationFilterWhere+`
)
SELECT count(*) FROM base WHERE ($7 <= 0 OR distance <= $7::float8)`,
		stationListParams(filter)...).Scan(&page.Meta.Total); err != nil {
		return station.StationPage{}, err
	}
	return page, nil
}

// stationDetailQuery mirrors the list columns for one station. Disabled
// stations are excluded: the C-end must not retrieve them.
const stationDetailQuery = `SELECT s.id, s.code, s.name, s.address, s.status,
COALESCE(s.latitude::float8, 0), COALESCE(s.longitude::float8, 0),
(SELECT count(*) FROM chargers c WHERE c.station_id = s.id),
(SELECT count(*) FROM chargers c WHERE c.station_id = s.id AND c.status = 'IDLE'),
COALESCE((SELECT min(c.price_per_kwh_cents + c.service_price_per_kwh_cents) FROM chargers c WHERE c.station_id = s.id), 0)
FROM stations s
WHERE s.id = $1 AND s.status <> 'DISABLED'`

// GetStation returns one station or station.ErrStationNotFound.
func (s *StationStore) GetStation(ctx context.Context, stationID int64) (station.Station, error) {
	var item station.Station
	var latitude, longitude sql.NullFloat64
	err := s.db.QueryRowContext(ctx, stationDetailQuery, stationID).Scan(
		&item.ID, &item.Code, &item.Name, &item.Address, &item.Status,
		&latitude, &longitude, &item.ChargerCount, &item.IdleChargerCount, &item.MinPriceCentPerKwh,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return station.Station{}, station.ErrStationNotFound
	}
	if err != nil {
		return station.Station{}, err
	}
	item.LatitudeE6 = toE6(latitude.Float64)
	item.LongitudeE6 = toE6(longitude.Float64)
	return item, nil
}

// chargerListQuery computes the page; the total is a separate count over the
// same filter so empty pages still carry the real total.
const chargerListQuery = `SELECT c.id, c.station_id, c.code, c.connector_type, c.power_watt, c.status
FROM chargers c
WHERE ($1::bigint IS NULL OR c.station_id = $1)
  AND ($2 = '' OR c.status = $2)
ORDER BY c.station_id, c.code
LIMIT $3 OFFSET $4`

const chargerCountQuery = `SELECT count(*) FROM chargers c
WHERE ($1::bigint IS NULL OR c.station_id = $1)
  AND ($2 = '' OR c.status = $2)`

// ListChargers returns one page ordered by station then code, optionally
// filtered by station and status.
func (s *StationStore) ListChargers(ctx context.Context, filter station.ChargerFilter) (station.ChargerPage, error) {
	var stationFilter any
	if filter.StationIDSet {
		stationFilter = filter.StationID
	}
	offset := (filter.Page - 1) * filter.PageSize

	rows, err := s.db.QueryContext(ctx, chargerListQuery, stationFilter, filter.Status, filter.PageSize, offset)
	if err != nil {
		return station.ChargerPage{}, err
	}
	defer rows.Close()

	page := station.ChargerPage{Meta: station.PageMeta{Page: filter.Page, PageSize: filter.PageSize}}
	for rows.Next() {
		var item station.Charger
		if err := rows.Scan(&item.ID, &item.StationID, &item.Code, &item.Type, &item.PowerWatt, &item.Status); err != nil {
			return station.ChargerPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return station.ChargerPage{}, err
	}

	if err := s.db.QueryRowContext(ctx, chargerCountQuery, stationFilter, filter.Status).Scan(&page.Meta.Total); err != nil {
		return station.ChargerPage{}, err
	}
	return page, nil
}

// likePattern turns a keyword into an escaped ILIKE pattern so user input
// cannot inject wildcards.
func likePattern(keyword string) string {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return ""
	}
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(keyword) + "%"
}

// toE6 converts stored degrees into the contract E6 integer form.
func toE6(degrees float64) int64 {
	return int64(math.Round(degrees * 1e6))
}
