package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	"github.com/heguangV/charging-station-platform/backend/internal/observability"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
	"github.com/heguangV/charging-station-platform/backend/internal/worker"
)

// The placeholder that used to make this process refuse to start is gone (BE-I-01 item 5): the
// appliers and the dispatcher now exist. What replaces it is the rule that a dispatcher must be
// pointed at a real gateway, because the failure mode of the placeholder - acknowledging work
// nobody did - has a sibling in a dispatcher that silently talks to a development mock.
func TestBuildRouterRefusesAGatewayAddressItCannotUse(t *testing.T) {
	cases := map[string]string{
		"empty":            "",
		"not a url":        "not-a-url",
		"unsupported":      "ftp://gateway.example/",
		"missing scheme":   "gateway.example:8080",
		"unsupported file": "file:///tmp/gateway",
	}
	for name, address := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newCommandDispatcher(address, nil, testLogger()); err == nil {
				t.Fatalf("expected %q to be rejected", address)
			}
		})
	}
}

// A usable address produces a dispatcher whose completion recorder is the order domain, which is
// what makes the device outcome reach the database.
func TestBuildRouterAcceptsAnHTTPGateway(t *testing.T) {
	dispatcher, err := newCommandDispatcher("http://127.0.0.1:8091", nil, testLogger())
	if err != nil {
		t.Fatalf("newCommandDispatcher() error = %v", err)
	}
	if dispatcher == nil {
		t.Fatal("expected a dispatcher")
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// These tests pin two review findings about this process's reporting loop, both of which are about
// the loop's lifecycle rather than about a metric's value.
//
// The pipeline is exercised through servePipeline rather than through main, because main's remaining
// work is process wiring that ends in os.Exit.

// countingInspector reports canned stream state and counts how often it was asked.
//
// The sampling interval in these tests is an hour, so no periodic sample can happen: every call it
// sees was made by the startup or shutdown snapshot itself.
type countingInspector struct {
	mu    sync.Mutex
	calls int
}

func (i *countingInspector) StreamInfo(_ context.Context, stream string) (redisrepo.StreamInfo, error) {
	i.mu.Lock()
	i.calls++
	i.mu.Unlock()
	return redisrepo.StreamInfo{Stream: stream, Length: 0, EntriesAdded: 0}, nil
}

func (i *countingInspector) GroupInfo(_ context.Context, stream, group string) (redisrepo.GroupInfo, error) {
	return redisrepo.GroupInfo{Stream: stream, Group: group, Lag: 0, EntriesRead: 0}, nil
}

func (i *countingInspector) StreamLen(_ context.Context, _ string) (int64, error) { return 0, nil }

func (i *countingInspector) samples() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

// unreachableClient models a Redis that a worker cannot reach, which is how a worker failure reaches
// the runner without any test-only hook.
type unreachableClient struct {
	*redisrepo.MemoryStream
	err error
}

func (c *unreachableClient) Ping(context.Context) error { return c.err }

func newTestCollector(t *testing.T, inspector observability.StreamInspector) *observability.Collector {
	t.Helper()
	collector, err := observability.NewCollector(inspector, observability.NewRegistry(), []observability.StreamTarget{
		{Stream: "ncs:stream:charge-event", Group: "charge-event-workers"},
	}, event.StreamDeadLetter)
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	return collector
}

func chargeStreamConfig() worker.StreamConfig {
	return worker.StreamConfig{
		Stream:          "ncs:stream:charge-event",
		Group:           "charge-event-workers",
		Consumer:        "worker-test",
		Count:           1,
		Block:           10 * time.Millisecond,
		PendingInterval: time.Hour,
		RetryAfter:      time.Hour,
		MaxAttempts:     3,
	}
}

// Finding: the sampler used the process signal context and was awaited after the runner returned. A
// worker failure cancels the runner but not the signal, so the wait never ended: the process could
// neither serve nor exit, and a supervisor could not restart it. The reporting loop now owns a
// context that the pipeline cancels itself.
func TestPipelineReturnsWhenAWorkerFailsWithoutASignal(t *testing.T) {
	client := &unreachableClient{MemoryStream: redisrepo.NewMemoryStream(), err: errors.New("connection refused")}
	t.Cleanup(func() { _ = client.Close() })

	runner, err := worker.NewRunner(client, worker.HandlerFunc(func(context.Context, event.Event) error { return nil }),
		[]worker.StreamConfig{chargeStreamConfig()})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Deliberately a context that is never cancelled: only the pipeline itself can end the wait, so
	// this is the shape a worker failure produces in production.
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- servePipeline(ctx, runner, newTestCollector(t, &countingInspector{}), time.Hour, logger)
	}()

	select {
	case runErr := <-done:
		if runErr == nil {
			t.Fatal("expected the worker failure to be reported")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pipeline did not return after a worker failure, so the process could not exit")
	}
}

// The shutdown snapshot is a fresh sample, not the last periodic one repeated: an operator reads that
// line to decide whether the process drained its work before stopping, and a reading taken up to a
// whole interval earlier cannot answer it.
func TestPipelineResamplesStreamStateAtShutdown(t *testing.T) {
	client := redisrepo.NewMemoryStream()
	t.Cleanup(func() { _ = client.Close() })

	runner, err := worker.NewRunner(client, worker.HandlerFunc(func(context.Context, event.Event) error { return nil }),
		[]worker.StreamConfig{chargeStreamConfig()})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	// A worker refuses to consume without a dead-letter recorder, which the runner needs to relay
	// to every stream.
	recorder, err := worker.NewDeadLetterRecorder(event.NewMemoryConsumptionStore())
	if err != nil {
		t.Fatalf("new dead-letter recorder: %v", err)
	}
	runner.SetDeadLetterRecorder(recorder)

	// The runner is told when it is ready, which is after the consumer group exists, so this also
	// proves the sample is taken late enough to mean something.
	ready := make(chan struct{})
	runner.SetOnReady(func() { close(ready) })

	inspector := &countingInspector{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- servePipeline(ctx, runner, newTestCollector(t, inspector), time.Hour, logger) }()

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the runner never became ready")
	}
	if samples := inspector.samples(); samples != 0 {
		t.Fatalf("expected no sample before shutdown with an hourly interval, got %d", samples)
	}

	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("expected a clean stop, got %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pipeline did not stop")
	}

	if samples := inspector.samples(); samples != 1 {
		t.Fatalf("expected the shutdown snapshot to sample stream state once, got %d", samples)
	}
	if !strings.Contains(logs.String(), "stream state at shutdown") {
		t.Fatalf("expected a shutdown snapshot line, got %q", logs.String())
	}
}

// buildStreams must carry the start position through to the worker. Before this, an operator could
// only get the documented "$" behaviour by bypassing the runner, because the position was dropped
// when the runner built each worker's config.
func TestBuildStreamsCarriesTheStartPosition(t *testing.T) {
	streams, err := buildStreams("worker-1", "ncs:stream:charge-event=charge@$,ncs:stream:charger-command=command")
	if err != nil {
		t.Fatalf("build streams: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
	if streams[0].StartID != "$" {
		t.Fatalf("expected the first stream to start at %q, got %q", "$", streams[0].StartID)
	}
	// An entry without a position keeps the worker's default, which is the beginning of the stream.
	if streams[1].StartID != "" {
		t.Fatalf("expected the second stream to leave the position unset, got %q", streams[1].StartID)
	}
}

func TestBuildStreamsRejectsAMalformedStartPosition(t *testing.T) {
	for _, override := range []string{
		"ncs:stream:charge-event=charge@",
		"ncs:stream:charge-event@$",
		"ncs:stream:charge-event",
	} {
		if _, err := buildStreams("worker-1", override); err == nil {
			t.Fatalf("expected %q to be rejected", override)
		}
	}
}
