// Package admin implements the P0 management API: station management,
// charger management, user and order queries, device commands and the
// operation audit trail.
//
// Authorization is layered in the middleware (auth.RequireAdminWrite):
// every admin role may read, only SUPER_ADMIN and OPERATOR may write.
package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/order"
	"github.com/heguangV/charging-station-platform/backend/internal/station"
	"github.com/heguangV/charging-station-platform/backend/internal/wallet"
)

// Charger and user status values from the frozen contract.
const (
	ChargerStatusIdle       = "IDLE"
	ChargerStatusFault      = "FAULT"
	ChargerStatusRestarting = "RESTARTING"
	ChargerStatusOccupied   = "OCCUPIED"
	ChargerStatusDisabled   = "DISABLED"

	UserStatusActive   = "ACTIVE"
	UserStatusDisabled = "DISABLED"

	CommandPending = "PENDING"
)

// Status filter values for the user list query parameter (integer 0/1 in
// the contract): 1 maps to ACTIVE, 0 to DISABLED.
const (
	UserStatusQueryActive   = 1
	UserStatusQueryDisabled = 0
)

// Sentinel errors mapped by the HTTP layer to the shared error-code registry.
var (
	// ErrInvalidStationFilter reports a malformed admin list request.
	ErrInvalidStationFilter = errors.New("admin: invalid station filter")
	// ErrInvalidAdminActor reports a missing or non-writer administrator.
	ErrInvalidAdminActor = errors.New("admin: administrator may not perform this operation")
	// ErrInvalidReason reports a restart reason outside the contract bounds.
	ErrInvalidReason = errors.New("admin: restart reason length is outside 2..200")
	// ErrInvalidTariff reports a malformed tariff or target-status value.
	ErrInvalidTariff = errors.New("admin: invalid tariff or target status")
	// ErrInvalidLedgerFilter reports a malformed ledger or audit query.
	ErrInvalidLedgerFilter = errors.New("admin: invalid ledger or audit filter")
	// ErrInvalidUserStatus reports a status query value outside 0/1.
	ErrInvalidUserStatus = errors.New("admin: invalid user status")
	// ErrChargerUnavailable maps to 409 CHARGER_UNAVAILABLE: the charger is
	// occupied, disabled or unknown.
	ErrChargerUnavailable = errors.New("admin: charger is unavailable")
	// ErrInvalidStateTransition maps to 409 INVALID_STATE_TRANSITION: the
	// charger is already restarting.
	ErrInvalidStateTransition = errors.New("admin: charger is already restarting")
)

var stationCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{2,32}$`)

// UserSummary is the contract UserSummary payload.
type UserSummary struct {
	ID          int64  `json:"id"`
	DisplayName string `json:"displayName"`
	Status      string `json:"status"`
	BalanceCent int64  `json:"balanceCent"`
}

// UserPage is one page of users.
type UserPage struct {
	Items []UserSummary `json:"items"`
	Meta  PageMeta      `json:"meta"`
}

// StationPage and ChargerPage reuse the user-side payloads; the admin views
// additionally see DISABLED stations.
type StationPage = station.StationPage
type ChargerPage = station.ChargerPage
type OrderPage = order.OrderPage

// PageMeta is the contract pagination metadata.
type PageMeta struct {
	Page     int64 `json:"page"`
	PageSize int64 `json:"pageSize"`
	Total    int64 `json:"total"`
}

// UserFilter carries validated user list parameters.
type UserFilter struct {
	Page     int64
	PageSize int64
	Keyword  string
	Status   string // "", ACTIVE or DISABLED
}

// AdminOrderFilter carries validated admin order list parameters.
type AdminOrderFilter struct {
	Page     int64
	PageSize int64
	OrderNo  string
	Status   string
}

// Command is the contract Command payload for a device command.
type Command struct {
	CommandNo string `json:"commandNo"`
	Status    string `json:"status"`
}

// CreateStationCommand carries a validated station creation request.
type CreateStationCommand struct {
	AdminID        int64
	Code           string
	Name           string
	Address        string
	LatitudeE6     int64
	LongitudeE6    int64
	IdempotencyKey string
	RequestHash    string
	TraceID        string
}

// RestartCommand carries a validated charger restart request.
type RestartCommand struct {
	AdminID        int64
	ChargerID      int64
	Reason         string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
}

// StationRecord is the created station as stored.
type StationRecord struct {
	ID          int64
	Code        string
	Name        string
	Address     string
	LatitudeE6  int64
	LongitudeE6 int64
	Status      string
}

// TariffView is the charger tariff snapshot the admin API exposes.
type TariffView struct {
	ChargerID            int64  `json:"chargerId"`
	ElectricityPriceCent int64  `json:"electricityPriceCentPerKwh"`
	ServicePriceCent     int64  `json:"servicePriceCentPerKwh"`
	OffPeakPriceCent     *int64 `json:"offPeakElectricityPriceCentPerKwh,omitempty"`
	OffPeakStartHour     *int16 `json:"offPeakStartHour,omitempty"`
	OffPeakEndHour       *int16 `json:"offPeakEndHour,omitempty"`
}

// TariffUpdate carries a validated tariff change. Nil off-peak fields clear
// the time-of-use window (flat tariff).
type TariffUpdate struct {
	AdminID              int64
	ChargerID            int64
	ElectricityPriceCent int64
	ServicePriceCent     int64
	OffPeakPriceCent     *int64
	OffPeakStartHour     *int16
	OffPeakEndHour       *int16
}

// ForceReleaseCommand carries a validated forced-release request (BR-11).
type ForceReleaseCommand struct {
	AdminID        int64
	ChargerID      int64
	Reason         string
	TargetStatus   string // IDLE or DISABLED
	IdempotencyKey string
	RequestHash    string
	TraceID        string
}

// UserDetail is the admin user view with the full registration data
// (SRS 管理端列表: ID、手机号、昵称、余额、注册时间、状态).
type UserDetail struct {
	ID           int64      `json:"id"`
	Phone        string     `json:"phone"`
	DisplayName  string     `json:"displayName"`
	AvatarURL    string     `json:"avatarUrl,omitempty"`
	Status       string     `json:"status"`
	BalanceCent  int64      `json:"balanceCent"`
	RegisteredAt time.Time  `json:"registeredAt"`
	DeletedAt    *time.Time `json:"deletedAt,omitempty"`
}

// LedgerEntry re-exports the wallet ledger shape for the admin user view.
type LedgerEntry = wallet.Entry
type LedgerPage = wallet.EntryPage

// AuditEntry is one operation_logs row for the audit query.
type AuditEntry struct {
	ID           int64     `json:"id"`
	ActorType    string    `json:"actorType"`
	ActorID      string    `json:"actorId"`
	Action       string    `json:"action"`
	ResourceType string    `json:"resourceType"`
	ResourceID   string    `json:"resourceId"`
	RequestID    string    `json:"requestId,omitempty"`
	Payload      string    `json:"payload,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
}

// AuditPage is one page of the audit trail.
type AuditPage struct {
	Items []AuditEntry `json:"items"`
	Meta  PageMeta     `json:"meta"`
}

// AuditFilter carries validated audit query parameters.
type AuditFilter struct {
	Page         int64
	PageSize     int64
	ActorID      string
	Action       string
	ResourceType string
	ResourceID   string
}

// UserLedgerFilter carries the admin view of one user's ledger.
type UserLedgerFilter struct {
	UserID   int64
	Page     int64
	PageSize int64
	Type     string
}

// Store persists admin operations. Mutating methods own their transactions
// so business rows, the audit trail and idempotency records commit together.
type Store interface {
	CreateStation(ctx context.Context, command CreateStationCommand) (StationRecord, error)
	ListStations(ctx context.Context, page, pageSize int64, keyword string) (StationPage, error)
	ListChargers(ctx context.Context, filter station.ChargerFilter) (ChargerPage, error)
	ListUsers(ctx context.Context, filter UserFilter) (UserPage, error)
	GetUserDetail(ctx context.Context, userID int64) (UserDetail, error)
	ListUserLedger(ctx context.Context, filter UserLedgerFilter) (LedgerPage, error)
	ListOrders(ctx context.Context, filter AdminOrderFilter) (OrderPage, error)
	RestartCharger(ctx context.Context, command RestartCommand) (Command, error)
	GetTariff(ctx context.Context, chargerID int64) (TariffView, error)
	UpdateTariff(ctx context.Context, update TariffUpdate) (TariffView, error)
	ForceRelease(ctx context.Context, command ForceReleaseCommand) (StationRecordCharger, error)
	ListAudit(ctx context.Context, filter AuditFilter) (AuditPage, error)
}

// StationRecordCharger reports the charger after a forced release.
type StationRecordCharger struct {
	ChargerID   int64  `json:"chargerId"`
	ChargerCode string `json:"chargerCode"`
	OrderNo     string `json:"orderNo,omitempty"`
	Status      string `json:"status"`
}

// Service validates commands and delegates persistence to a Store.
type Service struct {
	store Store
	clock func() time.Time
}

// NewService wires the service to its store.
func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("admin: store is required")
	}
	return &Service{store: store, clock: time.Now}, nil
}

// Create validates and creates a station in status OPEN.
func (s *Service) Create(ctx context.Context, command CreateStationCommand) (StationRecord, error) {
	if command.AdminID < 1 {
		return StationRecord{}, ErrInvalidAdminActor
	}
	if !stationCodePattern.MatchString(command.Code) {
		return StationRecord{}, fmt.Errorf("%w: station code", ErrInvalidStationFilter)
	}
	if len(command.Name) < 1 || len(command.Name) > 100 {
		return StationRecord{}, fmt.Errorf("%w: station name length", ErrInvalidStationFilter)
	}
	if len(command.Address) < 1 || len(command.Address) > 255 {
		return StationRecord{}, fmt.Errorf("%w: station address length", ErrInvalidStationFilter)
	}
	if command.LatitudeE6 < -90_000_000 || command.LatitudeE6 > 90_000_000 ||
		command.LongitudeE6 < -180_000_000 || command.LongitudeE6 > 180_000_000 {
		return StationRecord{}, fmt.Errorf("%w: coordinates out of range", ErrInvalidStationFilter)
	}
	return s.store.CreateStation(ctx, command)
}

// GetTariff returns the charger tariff view.
func (s *Service) GetTariff(ctx context.Context, chargerID int64) (TariffView, error) {
	if chargerID < 1 {
		return TariffView{}, ErrChargerUnavailable
	}
	return s.store.GetTariff(ctx, chargerID)
}

// UpdateTariff applies a tariff change under audit (价格调整). The order
// domain snapshots the tariff at charging start, so running orders are not
// affected — exactly what the snapshot semantics guarantee.
func (s *Service) UpdateTariff(ctx context.Context, update TariffUpdate) (TariffView, error) {
	if update.AdminID < 1 {
		return TariffView{}, ErrInvalidAdminActor
	}
	if update.ChargerID < 1 {
		return TariffView{}, ErrChargerUnavailable
	}
	if update.ElectricityPriceCent < 0 || update.ServicePriceCent < 0 {
		return TariffView{}, ErrInvalidTariff
	}
	if update.OffPeakPriceCent != nil {
		if *update.OffPeakPriceCent < 0 {
			return TariffView{}, ErrInvalidTariff
		}
		if update.OffPeakStartHour == nil || update.OffPeakEndHour == nil ||
			*update.OffPeakStartHour < 0 || *update.OffPeakStartHour > 23 ||
			*update.OffPeakEndHour < 0 || *update.OffPeakEndHour > 23 ||
			*update.OffPeakStartHour == *update.OffPeakEndHour {
			return TariffView{}, ErrInvalidTariff
		}
	}
	return s.store.UpdateTariff(ctx, update)
}

// ForceRelease forcibly releases a charger held by a CREATED or STARTING
// order (BR-11): the order is cancelled, the charger moves to the requested
// target status, and the action is audited. Charging devices are rejected —
// they must go through the controlled stop flow first.
func (s *Service) ForceRelease(ctx context.Context, command ForceReleaseCommand) (StationRecordCharger, error) {
	if command.AdminID < 1 {
		return StationRecordCharger{}, ErrInvalidAdminActor
	}
	if command.ChargerID < 1 {
		return StationRecordCharger{}, ErrChargerUnavailable
	}
	if len(command.Reason) < 2 || len(command.Reason) > 200 {
		return StationRecordCharger{}, ErrInvalidReason
	}
	if command.TargetStatus != "IDLE" && command.TargetStatus != "DISABLED" {
		return StationRecordCharger{}, ErrInvalidTariff
	}
	return s.store.ForceRelease(ctx, command)
}

// UserDetail returns the administrative user view.
func (s *Service) UserDetail(ctx context.Context, userID int64) (UserDetail, error) {
	if userID < 1 {
		return UserDetail{}, ErrInvalidAdminActor
	}
	return s.store.GetUserDetail(ctx, userID)
}

// UserLedger returns one page of a user's ledger for the admin view.
func (s *Service) UserLedger(ctx context.Context, filter UserLedgerFilter) (LedgerPage, error) {
	if filter.UserID < 1 {
		return LedgerPage{}, ErrInvalidAdminActor
	}
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > 100 {
		return LedgerPage{}, ErrInvalidLedgerFilter
	}
	switch filter.Type {
	case "", wallet.TypeTopUp, wallet.TypeCharge, wallet.TypeRefund, wallet.TypeAdjustment:
	default:
		return LedgerPage{}, ErrInvalidLedgerFilter
	}
	return s.store.ListUserLedger(ctx, filter)
}

// Audit returns one page of the operation audit trail.
func (s *Service) Audit(ctx context.Context, filter AuditFilter) (AuditPage, error) {
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > 100 {
		return AuditPage{}, ErrInvalidLedgerFilter
	}
	return s.store.ListAudit(ctx, filter)
}

// Restart validates and requests a device restart for an idle or faulted
// charger. Charging and disabled devices are rejected (BR-11).
func (s *Service) Restart(ctx context.Context, command RestartCommand) (Command, error) {
	if command.AdminID < 1 {
		return Command{}, ErrInvalidAdminActor
	}
	if command.ChargerID < 1 {
		return Command{}, ErrChargerUnavailable
	}
	if len(command.Reason) < 2 || len(command.Reason) > 200 {
		return Command{}, ErrInvalidReason
	}
	return s.store.RestartCharger(ctx, command)
}

// NewCommandNo mints a device command business number: CMD + UTC timestamp
// + 8 random hex chars. The audit trail and the outbox event carry it so
// the command can be traced end to end.
func NewCommandNo(now time.Time) (string, error) {
	buffer := make([]byte, 4)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("admin: generate command number: %w", err)
	}
	return "CMD" + now.UTC().Format("20060102150405") + hex.EncodeToString(buffer), nil
}
