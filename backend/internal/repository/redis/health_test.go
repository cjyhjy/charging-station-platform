package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func newTestHealth(t *testing.T, commands Commands, stats *Degradation, timeout time.Duration, clock *testClock) *Health {
	t.Helper()
	health, err := NewHealth(commands, stats, timeout, clock.Now)
	if err != nil {
		t.Fatalf("new health: %v", err)
	}
	return health
}

func TestNewHealthValidatesInputsAndDefaultsTimeout(t *testing.T) {
	if _, err := NewHealth(nil, nil, time.Second, nil); err == nil {
		t.Fatal("expected nil commands to be rejected")
	}

	health, err := NewHealth(NewMemoryCommands(), nil, 0, nil)
	if err != nil {
		t.Fatalf("new health: %v", err)
	}
	if health.timeout != DefaultHealthTimeout {
		t.Fatalf("expected the default timeout %s, got %s", DefaultHealthTimeout, health.timeout)
	}
}

func TestHealthCheckSucceedsWhenRedisAnswers(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	health := newTestHealth(t, NewMemoryCommands(), stats, time.Second, newTestClock())

	if err := health.Check(ctx); err != nil {
		t.Fatalf("expected a healthy probe, got %v", err)
	}
	status := health.Status(ctx)
	if !status.OK {
		t.Fatal("expected OK")
	}
	if status.Error != "" {
		t.Fatalf("expected no error text, got %q", status.Error)
	}
	if got := stats.Failures(CapabilityHealth); got != 0 {
		t.Fatalf("expected no recorded failures, got %d", got)
	}
}

// A probe must never apply a degradation policy: it reports the truth so a
// readiness endpoint can shed traffic.
func TestHealthCheckSurfacesOutageAsUnavailable(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands := NewMemoryCommands()
	health := newTestHealth(t, commands, stats, time.Second, newTestClock())

	commands.SetDown(errors.New("connection refused"))
	err := health.Check(ctx)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable so the caller can answer 503, got %v", err)
	}

	status := health.Status(ctx)
	if status.OK {
		t.Fatal("expected a failing status")
	}
	if status.Error == "" {
		t.Fatal("expected error text for an operator")
	}
	if got := stats.Failures(CapabilityHealth); got != 2 {
		t.Fatalf("expected both probe failures counted, got %d", got)
	}
}

func TestHealthStatusRecordsLatency(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	health := newTestHealth(t, NewMemoryCommands(), nil, time.Second, clock)

	status := health.Status(ctx)
	if !status.CheckedAt.Equal(clock.Now()) {
		t.Fatalf("expected the check to be timestamped at %s, got %s", clock.Now(), status.CheckedAt)
	}
	if status.Latency < 0 {
		t.Fatalf("expected a non-negative latency, got %s", status.Latency)
	}
}

// A caller cancellation is our own request ending, so it must not be recorded as
// a Redis outage.
func TestHealthCheckDoesNotCountCallerCancellation(t *testing.T) {
	stats := NewDegradation()
	health := newTestHealth(t, NewMemoryCommands(), stats, time.Second, newTestClock())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := health.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := stats.Failures(CapabilityHealth); got != 0 {
		t.Fatalf("expected no degradation recorded, got %d", got)
	}
}

// A hung Redis must not stall a readiness endpoint, so the probe bounds itself
// and reports the timeout as an availability failure.
func TestHealthCheckBoundsAHangingRedis(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands := &hangingCommands{MemoryCommands: NewMemoryCommands()}
	health := newTestHealth(t, commands, stats, 20*time.Millisecond, newTestClock())

	startedAt := time.Now()
	err := health.Check(ctx)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable from a timeout, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("expected the probe to give up quickly, took %s", elapsed)
	}
	if got := stats.Failures(CapabilityHealth); got != 1 {
		t.Fatalf("expected the timeout counted, got %d", got)
	}
}

func TestHealthStatusErrorIsTruncated(t *testing.T) {
	ctx := context.Background()
	commands := NewMemoryCommands()
	// A driver error can be long and repetitive; the status must stay bounded so
	// an ops endpoint cannot be flooded with it.
	commands.SetDown(errors.New(strings.Repeat("x", maxLastErrorLength*2)))
	health := newTestHealth(t, commands, nil, time.Second, newTestClock())

	status := health.Status(ctx)
	if status.OK {
		t.Fatal("expected a failing status")
	}
	if len(status.Error) != maxLastErrorLength {
		t.Fatalf("expected the message truncated to %d bytes, got %d", maxLastErrorLength, len(status.Error))
	}
}

// hangingCommands blocks in Ping until the probe's context expires, modelling a
// Redis that accepts the connection but never answers.
type hangingCommands struct {
	*MemoryCommands
}

func (h *hangingCommands) Ping(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
