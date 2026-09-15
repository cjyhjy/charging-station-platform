package redis

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests pin the connection-reuse race found in the third review:
//
//   - roundTrip returned without waiting for its cancellation watcher, so the
//     watcher could force an immediate deadline onto a connection that had
//     already been released and picked up by the next request;
//   - a watcher that fired just before SetWriteDeadline or SetReadDeadline had
//     its immediate deadline overwritten, so the cancellation was lost until the
//     configured timeout elapsed.
//
// The race is normally a matter of luck. gatedDeadlineConn removes the luck: it
// parks the immediate deadline that only the cancellation watcher installs, so a
// test knows exactly when the watcher has committed to the cancellation branch
// and can observe whether roundTrip waits for it.

// gatedDeadlineConn wraps a net.Conn and parks an immediate deadline, which only
// the cancellation watcher installs. Other deadline calls pass straight through:
// Release clears deadlines with the zero time, and roundTrip uses
// SetWriteDeadline and SetReadDeadline, neither of which must be blocked.
type gatedDeadlineConn struct {
	net.Conn

	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once

	mu        sync.Mutex
	completed int
}

func newGatedDeadlineConn(conn net.Conn) *gatedDeadlineConn {
	return &gatedDeadlineConn{
		Conn:    conn,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *gatedDeadlineConn) SetDeadline(t time.Time) error {
	immediate := !t.IsZero() && !t.After(time.Now())
	if immediate {
		c.enteredOnce.Do(func() { close(c.entered) })
		<-c.release
	}
	err := c.Conn.SetDeadline(t)
	if immediate {
		c.mu.Lock()
		c.completed++
		c.mu.Unlock()
	}
	return err
}

// completedWatcherDeadlines reports how many gated deadlines have finished.
func (c *gatedDeadlineConn) completedWatcherDeadlines() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed
}

// serverSignals reports progress of a scripted server without ever calling
// t.Fatal from a goroutine, which would panic if the test had already finished.
type serverSignals struct {
	commandRead   chan struct{}
	bytesConsumed chan struct{}
	finished      chan struct{}
}

func newServerSignals() *serverSignals {
	return &serverSignals{
		commandRead:   make(chan struct{}),
		bytesConsumed: make(chan struct{}),
		finished:      make(chan struct{}),
	}
}

// awaitReplyDelivery waits until the client has taken the reply bytes into its
// buffer. With net.Pipe, Write returns only once the reader has consumed the
// bytes, so after this point a socket deadline can no longer interrupt the parse.
//
// Only bytesConsumed is awaited. finished is closed by serveScript's defer on success
// as well as on failure, so selecting on it here would be a coin flip whenever both
// channels are closed, and the test would report a server failure for a reply that was
// in fact delivered. finished is therefore consulted only after the wait has already
// timed out, where it distinguishes "the server stopped early" from "the client never
// read".
func (s *serverSignals) awaitReplyDelivery(t *testing.T) {
	t.Helper()
	select {
	case <-s.bytesConsumed:
		return
	case <-time.After(5 * time.Second):
	}
	select {
	case <-s.finished:
		t.Fatal("the scripted server stopped before delivering the reply")
	default:
		t.Fatal("the reply was never consumed by the client")
	}
}

// serveScript answers one command, waiting for the gate to be CLOSED before
// replying. Closing is the "proceed" signal, so the receive must not be treated
// as a stop condition.
func serveScript(conn net.Conn, reply string, signals *serverSignals, allowReply <-chan struct{}) {
	defer close(signals.finished)
	buffer := make([]byte, 4096)
	if _, err := conn.Read(buffer); err != nil {
		return
	}
	close(signals.commandRead)
	if allowReply != nil {
		<-allowReply
	}
	if _, err := conn.Write([]byte(reply)); err != nil {
		return
	}
	close(signals.bytesConsumed)
}

// newGatedConnPair returns a pooledConn speaking over an in-memory pipe whose
// watcher deadline is gated, plus the server end and the probe.
func newGatedConnPair(t *testing.T) (*pooledConn, net.Conn, *gatedDeadlineConn) {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})

	gated := newGatedDeadlineConn(clientSide)
	now := time.Now()
	conn := &pooledConn{
		netConn:   gated,
		reader:    newRespReader(gated),
		createdAt: now,
		lastUsed:  now,
	}
	return conn, serverSide, gated
}

type roundTripOutcome struct {
	value any
	err   error
}

func longTimeoutConfig() ConnConfig {
	config := DefaultConnConfig()
	config.ReadTimeout = 30 * time.Second
	config.WriteTimeout = 30 * time.Second
	return config
}

// The watcher must have fully exited before roundTrip returns, because the caller
// releases the connection immediately afterwards.
func TestRoundTripWaitsForTheCancellationWatcherBeforeReturning(t *testing.T) {
	config := longTimeoutConfig()
	conn, serverSide, gated := newGatedConnPair(t)

	signals := newServerSignals()
	allowReply := make(chan struct{})
	go serveScript(serverSide, "+PONG\r\n", signals, allowReply)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan roundTripOutcome, 1)
	go func() {
		value, err := roundTrip(ctx, conn, config, "PING")
		results <- roundTripOutcome{value: value, err: err}
	}()

	<-signals.commandRead
	// Let the main flow reach the pending read, so a reply delivered afterwards is
	// still read successfully.
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-gated.entered // the watcher is now parked before installing its deadline

	// Deliver the reply while the watcher is parked, then wait until the client has
	// actually buffered it. Only then release the watcher, so its deadline cannot
	// interrupt a parse that has already finished.
	close(allowReply)
	signals.awaitReplyDelivery(t)

	// roundTrip must not have returned: the watcher has not exited.
	select {
	case got := <-results:
		t.Fatalf("roundTrip returned while the cancellation watcher was still running: %+v", got)
	case <-time.After(300 * time.Millisecond):
	}
	if got := gated.completedWatcherDeadlines(); got != 0 {
		t.Fatalf("expected the watcher deadline to still be in flight, got %d completed", got)
	}

	close(gated.release)

	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("expected the buffered reply to win, got %v", got.err)
		}
		if got.value != "PONG" {
			t.Fatalf("expected PONG, got %#v", got.value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("roundTrip did not return after the watcher was released")
	}

	// The invariant that matters: the watcher had finished before roundTrip
	// returned, so nothing can touch the connection after it is released.
	if got := gated.completedWatcherDeadlines(); got != 1 {
		t.Fatalf("expected the watcher to have completed before returning, got %d", got)
	}
	<-signals.finished
}

// The consequence of a stale watcher deadline: the next request that reuses the
// connection must not inherit an immediate timeout.
func TestReusedConnectionIsNotPoisonedByAStaleWatcherDeadline(t *testing.T) {
	config := longTimeoutConfig()
	conn, serverSide, gated := newGatedConnPair(t)

	signals := newServerSignals()
	allowFirstReply := make(chan struct{})
	secondConsumed := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		// First command: delivered only when the test allows it.
		serveScript(serverSide, "+PONG\r\n", signals, allowFirstReply)
		// Second command: answered immediately.
		buffer := make([]byte, 4096)
		if _, err := serverSide.Read(buffer); err != nil {
			return
		}
		if _, err := serverSide.Write([]byte("+PONG\r\n")); err != nil {
			return
		}
		close(secondConsumed)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan roundTripOutcome, 1)
	go func() {
		value, err := roundTrip(ctx, conn, config, "PING")
		results <- roundTripOutcome{value: value, err: err}
	}()

	<-signals.commandRead
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-gated.entered

	close(allowFirstReply)
	signals.awaitReplyDelivery(t)
	close(gated.release)

	first := <-results
	if first.err != nil {
		t.Fatalf("expected the first request to succeed, got %v", first.err)
	}

	// The connection now carries the watcher's immediate deadline, exactly as a
	// pooled connection would after a cancelled-but-successful request. A new
	// request must reset it and complete, not time out.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer secondCancel()

	second := make(chan roundTripOutcome, 1)
	go func() {
		value, err := roundTrip(secondCtx, conn, config, "PING")
		second <- roundTripOutcome{value: value, err: err}
	}()

	select {
	case got := <-second:
		if got.err != nil {
			t.Fatalf("the reused connection must not fail: %v", got.err)
		}
		if got.value != "PONG" {
			t.Fatalf("expected PONG, got %#v", got.value)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("the reused connection timed out, so a stale deadline leaked into the next request")
	}

	select {
	case <-secondConsumed:
	case <-time.After(2 * time.Second):
		t.Fatal("the scripted server never saw the second command")
	}
	<-serverDone
}

// The end-to-end version through the real pool: a cancelled request must not make
// the next request fail.
func TestPoolReuseAfterAConcurrentCancellationDoesNotTimeout(t *testing.T) {
	// The first PING is answered only when the test allows it, so the client is
	// parked in the pending read while the context is cancelled.
	firstReplyGate := make(chan struct{})
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(firstReplyGate) }) }
	t.Cleanup(releaseGate)

	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "PING") {
			<-firstReplyGate
			return replyPong
		}
		return replyOK
	})
	defer server.Close()

	config := fakeConfig(server)
	config.ReadTimeout = 3 * time.Second
	config.WriteTimeout = 3 * time.Second
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	// Wrap every dialled connection so the watcher deadline can be gated. Only the
	// first connection is ever gated; later ones pass deadlines straight through.
	var (
		mu     sync.Mutex
		gateds []*gatedDeadlineConn
	)
	client.pool.dialer = func(ctx context.Context) (*pooledConn, error) {
		conn, err := dialRedis(ctx, config)
		if err != nil {
			return nil, err
		}
		gated := newGatedDeadlineConn(conn.netConn)
		conn.netConn = gated
		conn.reader = newRespReader(gated)
		mu.Lock()
		gateds = append(gateds, gated)
		mu.Unlock()
		return conn, nil
	}

	firstGated := func() *gatedDeadlineConn {
		mu.Lock()
		defer mu.Unlock()
		if len(gateds) == 0 {
			return nil
		}
		return gateds[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstDone := make(chan error, 1)
	go func() { firstDone <- client.Ping(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for firstGated() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the client never dialled a connection")
		}
		time.Sleep(2 * time.Millisecond)
	}
	// The client is now parked in the read: the server has the command and will not
	// answer until the gate opens.
	time.Sleep(100 * time.Millisecond)

	cancel()
	gated := firstGated()
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cancellation watcher never ran")
	}
	releaseGate()
	close(gated.release)

	select {
	case err := <-firstDone:
		// Either outcome is legitimate: the reply may have been buffered before the
		// watcher's deadline took effect, or the read may have been interrupted.
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected success or context.Canceled from the first request, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first request never returned")
	}

	// The requirement: the next request must complete promptly. A leaked watcher
	// deadline would make this fail with an immediate timeout.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer secondCancel()

	if err := client.Ping(secondCtx); err != nil {
		t.Fatalf("the second request must not fail after a cancellation: %v", err)
	}
	if stats := client.Stats(); stats.Open > 1 {
		t.Fatalf("expected at most one live connection, got %+v", stats)
	}
}

// The other half of the fix: a watcher that fires between the two deadline calls
// must not be silently overwritten, which would delay cancellation until the
// configured timeout elapses.
func TestRoundTripDoesNotLoseACancellationAcrossADeadlineCall(t *testing.T) {
	config := longTimeoutConfig()

	// A server that never answers, so only the cancellation can end the read.
	server := newFakeRedis(t, func([]string) string { return "" })
	defer server.Close()

	config.Address = server.Addr()
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- client.Ping(ctx) }()

	// Cancel repeatedly across the window in which the deadlines are set, then
	// confirm the call still ends promptly rather than after ReadTimeout.
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 20; i++ {
		cancel()
		time.Sleep(time.Millisecond)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancellation was lost and the call waited out the configured timeout")
	}
}
