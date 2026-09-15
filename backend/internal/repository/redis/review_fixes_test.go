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

// These tests cover the review findings on the pooled client: context
// cancellation must actually stop work, every transport failure must become an
// availability failure, and Close must release every waiter.

// A request whose context is already cancelled must not reach Redis at all.
func TestClientDoesNotSendACommandWithACancelledContext(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := client.Get(ctx, "ncs:station:st_01"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if err := client.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(server.Commands()) != 0 {
		t.Fatalf("expected no command to reach the server, got %#v", server.Commands())
	}
}

// A cancellation during the round trip must interrupt the in-flight read instead
// of leaving the caller to wait out ReadTimeout.
func TestClientCancellationInterruptsAnInFlightRead(t *testing.T) {
	// An empty reply means the handler never answers.
	server := newFakeRedis(t, func([]string) string { return "" })
	defer server.Close()

	config := fakeConfig(server)
	// A long read timeout: without interruption the call would block this long.
	config.ReadTimeout = 30 * time.Second
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Ping(ctx) }()

	// Let the command reach the server, then cancel.
	time.Sleep(80 * time.Millisecond)
	startedAt := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if elapsed := time.Since(startedAt); elapsed > 5*time.Second {
			t.Fatalf("expected cancellation to interrupt promptly, took %s", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not interrupt the in-flight read")
	}
}

// A context deadline must interrupt a read even when the configured read timeout
// is far longer, and the caller must deterministically see the context error
// rather than a raw socket timeout.
func TestClientContextDeadlineInterruptsALongReadTimeout(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return "" })
	defer server.Close()

	config := fakeConfig(server)
	config.ReadTimeout = 30 * time.Second
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	err = client.Ping(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 5*time.Second {
		t.Fatalf("expected the context deadline to apply, took %s", elapsed)
	}
}

// An interrupted command leaves the connection in an unknown state, so it must be
// discarded rather than reused.
func TestClientDiscardsAConnectionAfterCancellation(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return "" })
	defer server.Close()

	config := fakeConfig(server)
	config.ReadTimeout = 30 * time.Second
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.Ping(ctx)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	if stats := client.Stats(); stats.Idle != 0 {
		t.Fatalf("expected the interrupted connection to be discarded, got %+v", stats)
	}
}

// A write to a connection the server has already closed must surface as an
// availability failure. Otherwise the cache and rate-limit FailOpen policies would
// not apply and a raw socket error would reach the business layer.
func TestClientWrapWriteFailuresAsUnavailable(t *testing.T) {
	// A server that accepts one connection and immediately closes it.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	defer func() {
		_ = listener.Close()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Close without reading: the next write fails with a reset.
			_ = conn.(*net.TCPConn).SetLinger(0)
			_ = conn.Close()
		}
	}()

	config := DefaultConnConfig()
	config.Address = listener.Addr().String()
	config.DialTimeout = time.Second
	config.ReadTimeout = 500 * time.Millisecond
	config.WriteTimeout = 500 * time.Millisecond
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		// Retry while the failure looks like a reset, so the test reliably reaches
		// the write path rather than racing the accept loop.
		lastErr = client.Ping(ctx)
		if lastErr != nil {
			break
		}
	}
	if lastErr == nil {
		t.Fatal("expected a transport failure")
	}
	if !errors.Is(lastErr, ErrUnavailable) {
		t.Fatalf("expected a transport failure to be wrapped as ErrUnavailable, got %v", lastErr)
	}
}

// The consequence of the previous test: a FailOpen cache must degrade, not leak
// the raw transport error to the caller.
func TestCacheDegradesWhenAReusedConnectionWasClosedByTheServer(t *testing.T) {
	// The server answers the first command then closes, so the second command
	// reuses a dead connection.
	var mu sync.Mutex
	handled := 0
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// The accept loop only ends when the listener closes, so the listener must be
	// closed before waiting. Defers are LIFO, so a bare `defer listener.Close()`
	// followed by `defer wg.Wait()` would wait first and block forever.
	var wg sync.WaitGroup
	defer func() {
		_ = listener.Close()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 4096)
				// Read one command, reply OK, then close.
				if _, err := conn.Read(buf); err != nil {
					return
				}
				mu.Lock()
				handled++
				mu.Unlock()
				_, _ = conn.Write([]byte("+OK\r\n"))
			}()
		}
	}()

	config := DefaultConnConfig()
	config.Address = listener.Addr().String()
	config.DialTimeout = time.Second
	config.ReadTimeout = 300 * time.Millisecond
	config.WriteTimeout = 300 * time.Millisecond
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	stats := NewDegradation()
	cache, err := NewCache(client, DefaultPolicy(), stats, DefaultCacheConfig())
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}

	ctx := context.Background()
	key, _ := StationKey("st_01")

	// Keep issuing reads. Once the dead connection is reused, the default FailOpen
	// policy must turn the transport failure into a miss.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _, err := cache.Get(ctx, key)
		if err != nil {
			t.Fatalf("a FailOpen cache must not surface a transport failure: %v", err)
		}
		if stats.Failures(CapabilityCache) > 0 {
			// The outage was observed and degraded, which is the required behaviour.
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected the closed connection to produce a degradation record")
}

// Every caller blocked in a saturated pool must be released by Close. A single
// notification would wake one and leave the rest blocked forever.
func TestConnPoolCloseReleasesEveryWaiter(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.MaxOpen = 1
		config.Pool.MaxIdle = 1
	})
	ctx := context.Background()

	held, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	const waiters = 8
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	started := make(chan struct{}, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			started <- struct{}{}
			_, err := pool.Get(ctx)
			errs[index] = err
		}(i)
	}
	for i := 0; i < waiters; i++ {
		<-started
	}
	// Let every waiter reach the saturation wait.
	time.Sleep(60 * time.Millisecond)

	if err := pool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	pool.Release(held, true)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release every blocked waiter")
	}

	for i, err := range errs {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter %d expected ErrClosed, got %v", i, err)
		}
	}
}

// A connection being dialled while Close runs must never be handed to a caller.
func TestConnPoolCloseDuringDialReturnsErrClosed(t *testing.T) {
	server := echoServer(t)
	pool, err := newConnPool(DefaultConnConfig())
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	pool.dialer = func(ctx context.Context) (*pooledConn, error) {
		close(dialStarted)
		<-releaseDial
		return dialRedis(ctx, fakeConfig(server))
	}
	defer server.Close()

	type result struct {
		conn *pooledConn
		err  error
	}
	results := make(chan result, 1)
	go func() {
		conn, err := pool.Get(context.Background())
		results <- result{conn: conn, err: err}
	}()

	<-dialStarted
	if err := pool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	close(releaseDial)

	select {
	case got := <-results:
		if !errors.Is(got.err, ErrClosed) {
			t.Fatalf("expected ErrClosed for a connection dialled during Close, got %v", got.err)
		}
		if got.conn != nil {
			t.Fatal("a connection from a closed pool must not be handed out")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not return after Close")
	}
}

// A saturated pool must still respect the caller's context while waiting.
func TestConnPoolSaturatedWaitHonoursContextDeadline(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.MaxOpen = 1
		config.Pool.MaxIdle = 1
	})

	held, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer pool.Release(held, true)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	if _, err := pool.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 3*time.Second {
		t.Fatalf("expected the deadline to apply, took %s", elapsed)
	}
}

// A release must wake all waiters, so several queued callers can proceed in turn
// rather than only the first.
func TestConnPoolReleaseWakesEveryWaiter(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.MaxOpen = 1
		config.Pool.MaxIdle = 1
	})
	ctx := context.Background()

	held, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	const waiters = 6
	var wg sync.WaitGroup
	acquired := make([]bool, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			conn, err := pool.Get(ctx)
			if err != nil {
				return
			}
			acquired[index] = true
			pool.Release(conn, true)
		}(i)
	}
	time.Sleep(60 * time.Millisecond)

	// Releasing must unblock the queue one at a time; each acquirer releases in
	// turn, so all of them must eventually run.
	pool.Release(held, true)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not every waiter was woken by a release")
	}

	for i, ok := range acquired {
		if !ok {
			t.Fatalf("waiter %d never acquired a connection", i)
		}
	}
}

// wrapUnavailable must classify correctly: transport faults degrade, server
// errors do not.
func TestWrapUnavailableClassification(t *testing.T) {
	if err := wrapUnavailable("read", errors.New("connection reset by peer")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected a transport error to be an availability failure, got %v", err)
	}
	if !strings.Contains(wrapUnavailable("read", errors.New("boom")).Error(), "read") {
		t.Fatal("expected the operation to appear in the message")
	}

	serverErr := &serverError{message: "ERR bad command"}
	wrapped := wrapUnavailable("read", serverErr)
	if errors.Is(wrapped, ErrUnavailable) {
		t.Fatalf("a server error must not become an availability failure: %v", wrapped)
	}
	if !isServerSideError(wrapped) {
		t.Fatal("expected the server error to stay identifiable through the wrapper")
	}

	if err := wrapUnavailable("read", nil); err != nil {
		t.Fatalf("expected a nil error to stay nil, got %v", err)
	}
}

// TrimSpace on the password would silently change a secret that legitimately has
// leading or trailing spaces, breaking authentication.
func TestConnConfigFromEnvPreservesCredentialWhitespace(t *testing.T) {
	const password = "  s3cret with spaces  "
	const username = " user "
	config, err := ConnConfigFromEnv(envMap(map[string]string{
		envRedisPassword: password,
		envRedisUsername: username,
	}))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Password != password {
		t.Fatalf("expected the password preserved verbatim, got %q", config.Password)
	}
	if config.Username != username {
		t.Fatalf("expected the username preserved verbatim, got %q", config.Username)
	}
}

// A blank credential is unset, not a space, so the "no password" path is still
// taken for an empty value.
func TestConnConfigFromEnvTreatsAnEmptyCredentialAsUnset(t *testing.T) {
	config, err := ConnConfigFromEnv(envMap(map[string]string{envRedisPassword: ""}))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Password != "" {
		t.Fatalf("expected no password, got %q", config.Password)
	}
}

// The settings layer must not trim credentials either.
func TestSettingsFromEnvPreservesCredentialWhitespace(t *testing.T) {
	const password = " padded-secret "
	settings, err := SettingsFromEnv(envMap(map[string]string{envRedisPassword: password}))
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.Conn.Password != password {
		t.Fatalf("expected the password preserved verbatim, got %q", settings.Conn.Password)
	}
}
