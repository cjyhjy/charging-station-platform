package station

import (
	"context"
	"errors"
	"testing"
)

type fakeReader struct {
	stations StationPage
	station  Station
	err      error
	requests []any
}

func (f *fakeReader) ListStations(_ context.Context, filter StationFilter) (StationPage, error) {
	f.requests = append(f.requests, filter)
	if f.err != nil {
		return StationPage{}, f.err
	}
	return f.stations, nil
}

func (f *fakeReader) GetStation(_ context.Context, id int64) (Station, error) {
	f.requests = append(f.requests, id)
	if f.err != nil {
		return Station{}, f.err
	}
	return f.station, nil
}

func (f *fakeReader) ListChargers(_ context.Context, filter ChargerFilter) (ChargerPage, error) {
	f.requests = append(f.requests, filter)
	if f.err != nil {
		return ChargerPage{}, f.err
	}
	return ChargerPage{
		Items: []Charger{{ID: 1, StationID: 1, Code: "A01", Type: ConnectorAC, PowerWatt: 7000, Status: ChargerStatusIdle}},
		Meta:  PageMeta{Page: filter.Page, PageSize: filter.PageSize, Total: 1},
	}, nil
}

func newTestService(t *testing.T, reader Reader) *Service {
	t.Helper()
	service, err := NewService(reader)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func TestListStationsPassesValidFilter(t *testing.T) {
	reader := &fakeReader{stations: StationPage{
		Items: []Station{{ID: 1, Code: "ST-01", Name: "站", Status: StatusOpen, LatitudeE6: 30545200, ChargerCount: 2}},
		Meta:  PageMeta{Page: 2, PageSize: 20, Total: 21},
	}}
	service := newTestService(t, reader)

	result, err := service.ListStations(context.Background(), StationFilter{Page: 2, PageSize: 20, Keyword: "充电"})
	if err != nil {
		t.Fatalf("ListStations() error = %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Code != "ST-01" || result.Meta.Total != 21 {
		t.Fatalf("result = %#v", result)
	}
}

func TestListStationsRejectsInvalidPagination(t *testing.T) {
	service := newTestService(t, &fakeReader{})

	for _, filter := range []StationFilter{
		{Page: 0, PageSize: 20},
		{Page: 1, PageSize: 0},
		{Page: 1, PageSize: 101},
	} {
		if _, err := service.ListStations(context.Background(), filter); !errors.Is(err, ErrInvalidPagination) {
			t.Fatalf("filter %#v error = %v, want ErrInvalidPagination", filter, err)
		}
	}
}

func TestGetStationValidatesAndPropagatesNotFound(t *testing.T) {
	reader := &fakeReader{err: ErrStationNotFound}
	service := newTestService(t, reader)

	if _, err := service.GetStation(context.Background(), 0); !errors.Is(err, ErrInvalidStationID) {
		t.Fatalf("id 0 error = %v, want ErrInvalidStationID", err)
	}
	if _, err := service.GetStation(context.Background(), 99); !errors.Is(err, ErrStationNotFound) {
		t.Fatalf("missing station error = %v, want ErrStationNotFound", err)
	}

	dbErr := errors.New("connection reset")
	reader.err = dbErr
	if _, err := service.GetStation(context.Background(), 1); !errors.Is(err, dbErr) {
		t.Fatalf("unexpected error wrapping: %v", err)
	}
}

func TestListChargersValidatesFilters(t *testing.T) {
	reader := &fakeReader{}
	service := newTestService(t, reader)

	valid := ChargerFilter{Page: 1, PageSize: 20, StationID: 3, StationIDSet: true, Status: ChargerStatusIdle}
	if _, err := service.ListChargers(context.Background(), valid); err != nil {
		t.Fatalf("valid filter error = %v", err)
	}

	for _, invalid := range []ChargerFilter{
		{Page: 1, PageSize: 20, StationID: 0, StationIDSet: true},
		{Page: 1, PageSize: 20, Status: "AVAILABLE"},
		{Page: 1, PageSize: 20, Status: "available"},
	} {
		if _, err := service.ListChargers(context.Background(), invalid); err == nil {
			t.Fatalf("invalid filter %#v accepted", invalid)
		}
	}
}

func TestNewServiceRequiresReader(t *testing.T) {
	if _, err := NewService(nil); err == nil {
		t.Fatal("nil reader accepted")
	}
}

func TestRadiusFilterRequiresCoordinates(t *testing.T) {
	service := newTestService(t, &fakeReader{})

	_, err := service.ListStations(context.Background(), StationFilter{
		Page: 1, PageSize: 20, RadiusMeters: 1000,
	})
	if !errors.Is(err, ErrInvalidStationFilter) {
		t.Fatalf("radius without coordinates error = %v, want ErrInvalidStationFilter", err)
	}

	// The same radius with coordinates passes validation.
	if _, err := service.ListStations(context.Background(), StationFilter{
		Page: 1, PageSize: 20, RadiusMeters: 1000,
		Latitude: 30.0, Longitude: 104.0, HasLocation: true,
	}); err != nil {
		t.Fatalf("radius with coordinates error = %v", err)
	}
}
