package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/heguangV/charging-station-platform/backend/internal/admin"
	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/station"
)

// AdminStore implements admin.Store: management queries and the mutating
// operations, each with its audit trail and idempotency record inside the
// business transaction.
type AdminStore struct {
	*StationStore
	*OrderStore
	db *sql.DB
}

// NewAdminStore binds the store to a connection pool. The embedded stores
// promote the shared helpers (idempotency claims, outbox appends) and the
// list queries; admin-specific behavior is declared on AdminStore itself.
func NewAdminStore(db *sql.DB) (*AdminStore, error) {
	if db == nil {
		return nil, errors.New("postgres: admin store requires a database")
	}
	stations, err := NewStationStore(db)
	if err != nil {
		return nil, err
	}
	orders, err := NewOrderStore(db)
	if err != nil {
		return nil, err
	}
	return &AdminStore{StationStore: stations, OrderStore: orders, db: db}, nil
}

// CreateStation inserts a station in status OPEN with its audit trail and
// idempotency record in one transaction.
func (s *AdminStore) CreateStation(ctx context.Context, command admin.CreateStationCommand) (admin.StationRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return admin.StationRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	scope := fmt.Sprintf("admin:%d:station:create", command.AdminID)
	replay, replayed, err := s.claimIdempotency(tx, ctx, scope, command.IdempotencyKey, command.RequestHash)
	if err != nil {
		return admin.StationRecord{}, err
	}
	if replayed {
		var record admin.StationRecord
		if err := json.Unmarshal(replay, &record); err != nil {
			return admin.StationRecord{}, fmt.Errorf("decode idempotency replay: %w", err)
		}
		return record, nil
	}

	var id int64
	err = tx.QueryRowContext(ctx, `INSERT INTO stations (code, name, address, latitude, longitude, status)
VALUES ($1, $2, $3, $4::float8, $5::float8, 'OPEN')
RETURNING id`, command.Code, command.Name, command.Address,
		float64(command.LatitudeE6)/1e6, float64(command.LongitudeE6)/1e6).Scan(&id)
	if err != nil {
		return admin.StationRecord{}, err
	}

	record := admin.StationRecord{
		ID:          id,
		Code:        command.Code,
		Name:        command.Name,
		Address:     command.Address,
		LatitudeE6:  command.LatitudeE6,
		LongitudeE6: command.LongitudeE6,
		Status:      "OPEN",
	}
	if err := s.appendAudit(tx, ctx, command.AdminID, "station.create", "station",
		strconvFormatInt64(id), command.TraceID, map[string]any{"code": command.Code, "name": command.Name}); err != nil {
		return admin.StationRecord{}, err
	}

	body, err := json.Marshal(record)
	if err != nil {
		return admin.StationRecord{}, err
	}
	if err := s.finalizeIdempotency(tx, ctx, scope, command.IdempotencyKey, body); err != nil {
		return admin.StationRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return admin.StationRecord{}, err
	}
	return record, nil
}

// ListStations returns one page of stations including disabled ones.
func (s *AdminStore) ListStations(ctx context.Context, page, pageSize int64, keyword string) (station.StationPage, error) {
	return s.StationStore.ListStations(ctx, station.StationFilter{
		Page: page, PageSize: pageSize, Keyword: keyword, IncludeDisabled: true,
	})
}

// ListChargers returns one page of chargers with optional filters.
func (s *AdminStore) ListChargers(ctx context.Context, filter station.ChargerFilter) (station.ChargerPage, error) {
	return s.StationStore.ListChargers(ctx, filter)
}

// ListUsers returns one page of users with their wallet balances.
func (s *AdminStore) ListUsers(ctx context.Context, filter admin.UserFilter) (admin.UserPage, error) {
	const pageQuery = `SELECT u.id, u.display_name, u.status, COALESCE(w.balance_cents, 0)
FROM user_accounts u
LEFT JOIN wallet_accounts w ON w.user_id = u.id
WHERE ($1 = '' OR u.phone ILIKE $1 OR u.display_name ILIKE $1)
  AND ($2 = '' OR u.status = $2)
ORDER BY u.id
LIMIT $3 OFFSET $4`
	const countQuery = `SELECT count(*) FROM user_accounts u
WHERE ($1 = '' OR u.phone ILIKE $1 OR u.display_name ILIKE $1)
  AND ($2 = '' OR u.status = $2)`

	keyword := likePattern(filter.Keyword)
	offset := (filter.Page - 1) * filter.PageSize

	rows, err := s.db.QueryContext(ctx, pageQuery, keyword, filter.Status, filter.PageSize, offset)
	if err != nil {
		return admin.UserPage{}, err
	}
	defer rows.Close()

	page := admin.UserPage{Meta: admin.PageMeta{Page: filter.Page, PageSize: filter.PageSize}}
	for rows.Next() {
		var item admin.UserSummary
		if err := rows.Scan(&item.ID, &item.DisplayName, &item.Status, &item.BalanceCent); err != nil {
			return admin.UserPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return admin.UserPage{}, err
	}
	if err := s.db.QueryRowContext(ctx, countQuery, keyword, filter.Status).Scan(&page.Meta.Total); err != nil {
		return admin.UserPage{}, err
	}
	return page, nil
}

// ListOrders returns one page of orders across all users with the payment
// fields, optionally filtered by business number prefix and status.
func (s *AdminStore) ListOrders(ctx context.Context, filter admin.AdminOrderFilter) (order.OrderPage, error) {
	const filterSQL = `($1 = '' OR status = $1)
  AND ($2 = '' OR order_no ILIKE $2)`
	const pageQuery = `SELECT ` + orderSelectColumns + ` FROM charging_orders
WHERE ` + filterSQL + `
ORDER BY created_at DESC
LIMIT $3 OFFSET $4`
	const countQuery = `SELECT count(*) FROM charging_orders WHERE ` + filterSQL

	offset := (filter.Page - 1) * filter.PageSize
	keyword := likePattern(filter.OrderNo)

	rows, err := s.db.QueryContext(ctx, pageQuery, filter.Status, keyword, filter.PageSize, offset)
	if err != nil {
		return order.OrderPage{}, err
	}
	defer rows.Close()

	page := order.OrderPage{Meta: order.PageMeta{Page: filter.Page, PageSize: filter.PageSize}}
	for rows.Next() {
		result, err := scanAdminOrderRow(rows)
		if err != nil {
			return order.OrderPage{}, err
		}
		page.Items = append(page.Items, result)
	}
	if err := rows.Err(); err != nil {
		return order.OrderPage{}, err
	}
	if err := s.db.QueryRowContext(ctx, countQuery, filter.Status, keyword).Scan(&page.Meta.Total); err != nil {
		return order.OrderPage{}, err
	}
	return page, nil
}

// RestartCharger queues a device restart command for an idle or faulted
// charger: the charger moves to RESTARTING, the command goes to the outbox
// for the B-line worker and the action lands in the audit trail — all in
// one transaction. Occupied or disabled chargers are rejected (BR-11).
func (s *AdminStore) RestartCharger(ctx context.Context, command admin.RestartCommand) (admin.Command, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return admin.Command{}, err
	}
	defer func() { _ = tx.Rollback() }()

	scope := fmt.Sprintf("admin:%d:charger:restart:%d", command.AdminID, command.ChargerID)
	replay, replayed, err := s.claimIdempotency(tx, ctx, scope, command.IdempotencyKey, command.RequestHash)
	if err != nil {
		return admin.Command{}, err
	}
	if replayed {
		var replayedCommand admin.Command
		if err := json.Unmarshal(replay, &replayedCommand); err != nil {
			return admin.Command{}, fmt.Errorf("decode idempotency replay: %w", err)
		}
		return replayedCommand, nil
	}

	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM chargers WHERE id = $1 FOR UPDATE`, command.ChargerID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return admin.Command{}, admin.ErrChargerUnavailable
	}
	if err != nil {
		return admin.Command{}, err
	}
	switch status {
	case admin.ChargerStatusOccupied, admin.ChargerStatusDisabled:
		return admin.Command{}, admin.ErrChargerUnavailable
	case admin.ChargerStatusRestarting:
		return admin.Command{}, admin.ErrInvalidStateTransition
	case admin.ChargerStatusIdle, admin.ChargerStatusFault:
		// restartable
	default:
		return admin.Command{}, admin.ErrChargerUnavailable
	}

	commandNo, err := admin.NewCommandNo(s.clock())
	if err != nil {
		return admin.Command{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE chargers SET status = 'RESTARTING', updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
		command.ChargerID); err != nil {
		return admin.Command{}, err
	}
	// The payload must match the B-04 consumer contract exactly: command_id,
	// charger_id and action (the only supported action is RESTART).
	if err := s.appendOutbox(tx, ctx, order.EventChargerCommandRequested, strconvFormatInt64(command.ChargerID),
		map[string]any{"command_id": commandNo, "charger_id": strconvFormatInt64(command.ChargerID), "action": "RESTART", "reason": command.Reason},
		command.TraceID); err != nil {
		return admin.Command{}, err
	}
	if err := s.appendAudit(tx, ctx, command.AdminID, "charger.restart", "charger",
		strconvFormatInt64(command.ChargerID), command.TraceID,
		map[string]any{"commandNo": commandNo, "reason": command.Reason}); err != nil {
		return admin.Command{}, err
	}

	result := admin.Command{CommandNo: commandNo, Status: admin.CommandPending}
	body, err := json.Marshal(result)
	if err != nil {
		return admin.Command{}, err
	}
	if err := s.finalizeIdempotency(tx, ctx, scope, command.IdempotencyKey, body); err != nil {
		return admin.Command{}, err
	}
	if err := tx.Commit(); err != nil {
		return admin.Command{}, err
	}
	return result, nil
}

// appendAudit writes one operation log row inside the caller's transaction.
func (s *AdminStore) appendAudit(tx *sql.Tx, ctx context.Context, adminID int64, action, resourceType, resourceID, requestID string, payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal audit payload: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operation_logs
    (actor_type, actor_id, action, resource_type, resource_id, request_id, payload)
VALUES ('ADMIN', $1, $2, $3, $4, $5, $6::jsonb)`,
		fmt.Sprintf("%d", adminID), action, resourceType, resourceID, requestID, string(encoded))
	return err
}

// claimIdempotency and finalizeIdempotency are shared with the order store;
// they are defined once in orders.go and reused here.

func scanAdminOrderRow(rows *sql.Rows) (order.Order, error) {
	var result order.Order
	var id int64
	var pricePerKwh, servicePrice, offPeakPrice sql.NullInt64
	var offPeakStartHour, offPeakEndHour sql.NullInt16
	var startedAt sql.NullTime
	if err := rows.Scan(&id, &result.OrderNo, &result.UserID, &result.StationID, &result.ChargerID,
		&result.Status, &result.AmountCent, &result.PaidCent, &result.PaymentStatus, &result.EnergyWh,
		&pricePerKwh, &servicePrice, &offPeakPrice, &offPeakStartHour, &offPeakEndHour, &startedAt,
		&result.CreatedAt, &result.UpdatedAt); err != nil {
		return order.Order{}, err
	}
	result.CreatedAt = result.CreatedAt.UTC()
	result.UpdatedAt = result.UpdatedAt.UTC()
	return result, nil
}

func strconvFormatInt64(value int64) string {
	return fmt.Sprintf("%d", value)
}
