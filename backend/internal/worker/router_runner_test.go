package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

func TestRouterDispatchesByEventType(t *testing.T) {
	router := NewRouter()
	var chargeCalls, commandCalls int32

	if err := router.Register(event.ChargeStarted, HandlerFunc(func(context.Context, event.Event) error {
		atomic.AddInt32(&chargeCalls, 1)
		return nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := router.Register(event.ChargerCommandRequested, HandlerFunc(func(context.Context, event.Event) error {
		atomic.AddInt32(&commandCalls, 1)
		return nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := router.Handle(context.Background(), newChargeStartedEvent(t, "evt_r1")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	command, err := event.New(event.ChargerCommandRequested, "charger", "ch_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	if err := router.Handle(context.Background(), command); err != nil {
		t.Fatalf("handle: %v", err)
	}

	if atomic.LoadInt32(&chargeCalls) != 1 || atomic.LoadInt32(&commandCalls) != 1 {
		t.Fatalf("expected one call each, got charge=%d command=%d", chargeCalls, commandCalls)
	}
}

// An unregistered type is permanent: dead-lettering it immediately is what surfaces a
// missing route instead of retrying an event no handler will ever accept.
func TestRouterTreatsAnUnregisteredTypeAsPermanent(t *testing.T) {
	router := NewRouter()
	if err := router.Register(event.ChargeStarted, HandlerFunc(func(context.Context, event.Event) error { return nil })); err != nil {
		t.Fatalf("register: %v", err)
	}

	// A registered type must succeed, so the permanence below is about the missing
	// route and not about the router rejecting everything.
	if err := router.Handle(context.Background(), newChargeStartedEvent(t, "evt_registered")); err != nil {
		t.Fatalf("expected a registered type to be handled, got %v", err)
	}

	unrouted, err := event.New(event.OrderCompleted, "order", "o_01", "trace", nil)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	err = router.Handle(context.Background(), unrouted)
	if !IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if !errors.Is(err, ErrUnknownEventType) {
		t.Fatalf("expected ErrUnknownEventType, got %v", err)
	}
}

// Two handlers claiming one type means behaviour would depend on startup order, so it
// must be rejected rather than silently overwritten.
func TestRouterRejectsDuplicateRegistration(t *testing.T) {
	router := NewRouter()
	handler := HandlerFunc(func(context.Context, event.Event) error { return nil })
	if err := router.Register(event.ChargeStarted, handler); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := router.Register(event.ChargeStarted, handler); err == nil {
		t.Fatal("expected a duplicate registration to be rejected")
	}
	if err := router.Register("", handler); err == nil {
		t.Fatal("expected an empty event type to be rejected")
	}
	if err := router.Register(event.ChargeStopped, nil); err == nil {
		t.Fatal("expected a nil handler to be rejected")
	}
}

func TestRouterRegisterAllIsAtomicOnFailure(t *testing.T) {
	router := NewRouter()
	handler := HandlerFunc(func(context.Context, event.Event) error { return nil })
	if err := router.Register(event.ChargeStarted, handler); err != nil {
		t.Fatalf("register: %v", err)
	}

	// ChargeStarted is already taken, so the batch must fail.
	err := router.RegisterAll([]event.Type{event.ChargeStopped, event.ChargeStarted}, handler)
	if err == nil {
		t.Fatal("expected the batch registration to fail")
	}
}

// The router must forward the transport metadata, or a handler would lose the attempt
// count and could not tell a retry from a first delivery.
func TestRouterForwardsDeliveryMetadata(t *testing.T) {
	router := NewRouter()
	seen := make(chan observed, 1)
	if err := router.Register(event.ChargeStarted, recordingHandler{seen: seen}); err != nil {
		t.Fatalf("register: %v", err)
	}

	delivery := DeliveryInfo{Stream: "s", StreamID: "9-9", Consumer: "c", Attempt: 3}
	if err := router.HandleDelivery(context.Background(), newChargeStartedEvent(t, "evt_meta_router"), delivery); err != nil {
		t.Fatalf("handle delivery: %v", err)
	}
	got := (<-seen).delivery
	if got != delivery {
		t.Fatalf("expected %+v, got %+v", delivery, got)
	}
}

func TestRouterTypesIsStableAndSorted(t *testing.T) {
	router := NewRouter()
	if err := router.RegisterAll([]event.Type{event.OrderCompleted, event.ChargeStarted, event.ChargeStopped}, HandlerFunc(func(context.Context, event.Event) error { return nil })); err != nil {
		t.Fatalf("register: %v", err)
	}
	types := router.Types()
	want := []string{"CHARGE_STARTED", "CHARGE_STOPPED", "ORDER_COMPLETED"}
	if len(types) != len(want) {
		t.Fatalf("expected %v, got %v", want, types)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, types)
		}
	}
	if router.Len() != 3 {
		t.Fatalf("expected 3 routes, got %d", router.Len())
	}
}

func TestStreamConfigDefaultsAreApplied(t *testing.T) {
	defaults := DefaultConfig()
	config := StreamConfig{Stream: "s", Group: "g", Consumer: "c"}.toConfig()
	if config.Count != defaults.Count || config.Block != defaults.Block ||
		config.PendingInterval != defaults.PendingInterval ||
		config.RetryAfter != defaults.RetryAfter || config.MaxAttempts != defaults.MaxAttempts {
		t.Fatalf("expected the defaults to be applied, got %+v", config)
	}

	explicit := StreamConfig{
		Stream: "s", Group: "g", Consumer: "c",
		Count: 5, Block: time.Second, PendingInterval: 2 * time.Second, RetryAfter: 3 * time.Second, MaxAttempts: 7,
	}.toConfig()
	if explicit.Count != 5 || explicit.MaxAttempts != 7 || explicit.RetryAfter != 3*time.Second {
		t.Fatalf("expected explicit values to win, got %+v", explicit)
	}
}

func TestNewRunnerValidatesInputs(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	handler := HandlerFunc(func(context.Context, event.Event) error { return nil })
	valid := StreamConfig{Stream: "s", Group: "g", Consumer: "c"}

	if _, err := NewRunner(nil, handler, []StreamConfig{valid}); err == nil {
		t.Fatal("expected a nil client to be rejected")
	}
	if _, err := NewRunner(stream, nil, []StreamConfig{valid}); err == nil {
		t.Fatal("expected a nil handler to be rejected")
	}
	if _, err := NewRunner(stream, handler, nil); err == nil {
		t.Fatal("expected an empty stream list to be rejected")
	}
	for _, bad := range []StreamConfig{
		{Group: "g", Consumer: "c"},
		{Stream: "s", Consumer: "c"},
		{Stream: "s", Group: "g"},
	} {
		if _, err := NewRunner(stream, handler, []StreamConfig{bad}); err == nil {
			t.Fatalf("expected %+v to be rejected", bad)
		}
	}
	// Two identical stream/group/consumer triples would make pending recovery ambiguous.
	if _, err := NewRunner(stream, handler, []StreamConfig{valid, valid}); err == nil {
		t.Fatal("expected a duplicate stream configuration to be rejected")
	}
}

// The runner must consume from every configured stream through the router.
func TestRunnerConsumesFromEveryStream(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()

	seen := make(chan string, 8)
	router := NewRouter()
	if err := router.Register(event.ChargeStarted, HandlerFunc(func(_ context.Context, e event.Event) error {
		seen <- "charge:" + e.EventID
		return nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := router.Register(event.ChargerCommandRequested, HandlerFunc(func(_ context.Context, e event.Event) error {
		seen <- "command:" + e.EventID
		return nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	streams := []StreamConfig{
		{Stream: event.StreamChargeEvent, Group: "g-charge", Consumer: "c-charge", Block: time.Millisecond, PendingInterval: time.Millisecond},
		{Stream: event.StreamChargerCommand, Group: "g-cmd", Consumer: "c-cmd", Block: time.Millisecond, PendingInterval: time.Millisecond},
	}
	runner, err := NewRunner(stream, router, streams)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Give the workers a moment to create their groups.
	time.Sleep(50 * time.Millisecond)

	chargeFields, err := newChargeStartedEvent(t, "evt_charge").Fields()
	if err != nil {
		t.Fatalf("fields: %v", err)
	}
	if _, err := stream.Add(context.Background(), event.StreamChargeEvent, chargeFields); err != nil {
		t.Fatalf("add: %v", err)
	}
	command, err := event.New(event.ChargerCommandRequested, "charger", "ch_01", "trace", map[string]string{
		"command_id": "cmd_01", "charger_id": "ch_01", "action": "RESTART",
	})
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	commandFields, err := command.Fields()
	if err != nil {
		t.Fatalf("fields: %v", err)
	}
	if _, err := stream.Add(context.Background(), event.StreamChargerCommand, commandFields); err != nil {
		t.Fatalf("add: %v", err)
	}

	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case value := <-seen:
			got[value] = true
		case <-deadline:
			t.Fatalf("expected both streams to be consumed, got %v", got)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("expected a clean shutdown, got %v", err)
	}
}

// A stream that cannot be read is a real failure, so the runner must surface it instead
// of limping along with a partially working chain.
func TestRunnerSurfacesAWorkerFailure(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()

	router := NewRouter()
	if err := router.Register(event.ChargeStarted, HandlerFunc(func(context.Context, event.Event) error { return nil })); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Closing the stream makes every read fail immediately.
	runner, err := NewRunner(stream, router, []StreamConfig{
		{Stream: event.StreamChargeEvent, Group: "g", Consumer: "c", Block: time.Millisecond, PendingInterval: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = runner.Run(ctx)
	if err == nil {
		t.Fatal("expected the runner to surface the worker failure")
	}
	if !errors.Is(err, redisrepo.ErrClosed) {
		t.Fatalf("expected ErrClosed to be reported, got %v", err)
	}
}

func TestRunnerStopsOnCallerCancellationWithoutAnError(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()

	router := NewRouter()
	if err := router.Register(event.ChargeStarted, HandlerFunc(func(context.Context, event.Event) error { return nil })); err != nil {
		t.Fatalf("register: %v", err)
	}
	runner, err := NewRunner(stream, router, []StreamConfig{
		{Stream: event.StreamChargeEvent, Group: "g", Consumer: "c", Block: time.Millisecond, PendingInterval: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	requireRunnerRecorder(t, runner, event.NewMemoryConsumptionStore())

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan StreamConfig, 1)
	runner.SetOnStart(func(stream StreamConfig) { started <- stream })

	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case got := <-started:
		if got.Stream != event.StreamChargeEvent {
			t.Fatalf("expected the configured stream, got %s", got.Stream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the worker never started")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected a clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the runner did not stop")
	}
}
