package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// These tests drive the real pool against the fake server. They cover the
// recycling and saturation behaviour that a bounded connection pool exists for,
// which unit tests over the in-memory adapter cannot reach.

// waitForConnections blocks until the fake server has accepted at least want
// connections, then lets any straggler accept land.
//
// The server accepts asynchronously, so a count read immediately after a dial can
// legitimately be zero. Waiting makes an exact assertion meaningful instead of
// flaky.
func waitForConnections(t *testing.T, server *fakeRedis, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if server.ConnectionCount() >= want {
			time.Sleep(20 * time.Millisecond)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected at least %d accepted connections, got %d", want, server.ConnectionCount())
}

func newPoolFixture(t *testing.T, server *fakeRedis, mutate func(*ConnConfig)) *connPool {
	t.Helper()
	config := fakeConfig(server)
	config.Pool.MaxOpen = 2
	config.Pool.MaxIdle = 2
	if mutate != nil {
		mutate(&config)
	}
	pool, err := newConnPool(config)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func TestConnPoolRejectsInvalidConfig(t *testing.T) {
	config := DefaultConnConfig()
	config.Pool.MaxOpen = 0
	if _, err := newConnPool(config); err == nil {
		t.Fatal("expected an invalid configuration to be rejected")
	}
}

func TestConnPoolReusesAReleasedConnection(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, nil)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		pool.Release(conn, true)
	}

	waitForConnections(t, server, 1)
	if got := server.ConnectionCount(); got != 1 {
		t.Fatalf("expected one dialled connection, got %d", got)
	}
	if stats := pool.Stats(); stats.Open != 1 || stats.Idle != 1 {
		t.Fatalf("expected one idle connection, got %+v", stats)
	}
}

// An idle connection must not be handed out forever: a proxy or firewall may have
// dropped it silently, and reusing it would surface as a spurious command error.
func TestConnPoolRecyclesConnectionsPastTheIdleTimeout(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.IdleTimeout = 20 * time.Millisecond
	})
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, true)

	time.Sleep(40 * time.Millisecond)

	conn, err = pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, true)

	waitForConnections(t, server, 2)
	if got := server.ConnectionCount(); got != 2 {
		t.Fatalf("expected the idle connection to be recycled, got %d connections", got)
	}
}

// A connection must also be retired once it is older than MaxLifetime, so a
// long-lived pool does not accumulate connections a server-side timeout would
// eventually kill.
func TestConnPoolRecyclesConnectionsPastTheMaxLifetime(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.MaxLifetime = 20 * time.Millisecond
		config.Pool.IdleTimeout = time.Hour
	})
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, true)

	time.Sleep(40 * time.Millisecond)

	conn, err = pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, true)

	waitForConnections(t, server, 2)
	if got := server.ConnectionCount(); got != 2 {
		t.Fatalf("expected the aged connection to be recycled, got %d connections", got)
	}
}

// The pool must never exceed MaxOpen: a saturated pool waits for a release rather
// than dialling without bound.
func TestConnPoolSaturationWaitsForARelease(t *testing.T) {
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

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		waited  = make(chan struct{})
		gotConn *pooledConn
		gotErr  error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(waited)
		conn, err := pool.Get(ctx)
		mu.Lock()
		gotConn, gotErr = conn, err
		mu.Unlock()
	}()

	<-waited
	// Give the waiter a moment to reach the saturation branch, then release.
	time.Sleep(30 * time.Millisecond)
	pool.Release(held, true)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if gotErr != nil {
		t.Fatalf("expected the waiter to obtain a connection, got %v", gotErr)
	}
	if gotConn == nil {
		t.Fatal("expected a connection")
	}
	pool.Release(gotConn, true)

	waitForConnections(t, server, 1)
	if got := server.ConnectionCount(); got != 1 {
		t.Fatalf("expected the saturated pool to reuse one connection, got %d", got)
	}
	if stats := pool.Stats(); stats.Open > 1 {
		t.Fatalf("expected at most 1 open connection, got %+v", stats)
	}
}

func TestConnPoolGetHonoursContextCancellationWhileSaturated(t *testing.T) {
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
	defer pool.Release(held, true)

	blocked, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := pool.Get(blocked)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected the saturated Get to unblock on cancellation")
	}
}

// A connection that hit an I/O error has unknown socket state, so releasing it as
// non-reusable must close it rather than hand it to the next caller.
func TestConnPoolReleaseClosesANonReusableConnection(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, nil)
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, false)

	if stats := pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("expected the pool to forget the connection, got %+v", stats)
	}

	fresh, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(fresh, true)

	waitForConnections(t, server, 2)
	if got := server.ConnectionCount(); got != 2 {
		t.Fatalf("expected a replacement connection, got %d", got)
	}
}

func TestConnPoolClosesIdleConnectionsAndRefusesFurtherGets(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, nil)
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pool.Release(conn, true)
	if stats := pool.Stats(); stats.Open != 1 {
		t.Fatalf("expected one open connection, got %+v", stats)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if stats := pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("expected every idle connection to be released, got %+v", stats)
	}
	if _, err := pool.Get(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// A connection checked out when Close is called must be closed on release rather
// than returned to a closed pool.
func TestConnPoolCloseWhileAConnectionIsCheckedOut(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, nil)
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Releasing after Close must not resurrect the pool.
	pool.Release(conn, true)

	if stats := pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("expected an empty pool after close, got %+v", stats)
	}
	if _, err := pool.Get(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestConnPoolStatsTracksCheckouts(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, nil)
	ctx := context.Background()

	first, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	second, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stats := pool.Stats(); stats.Open != 2 || stats.Idle != 0 {
		t.Fatalf("expected two checked out connections, got %+v", stats)
	}

	pool.Release(first, true)
	if stats := pool.Stats(); stats.Open != 2 || stats.Idle != 1 {
		t.Fatalf("expected one idle connection, got %+v", stats)
	}
	pool.Release(second, true)
	if stats := pool.Stats(); stats.Open != 2 || stats.Idle != 2 {
		t.Fatalf("expected two idle connections, got %+v", stats)
	}
}

// Idle connections beyond MaxIdle are closed rather than kept, so a burst of
// concurrency does not leave a large idle set behind.
func TestConnPoolDoesNotKeepMoreIdleThanMaxIdle(t *testing.T) {
	server := echoServer(t)
	pool := newPoolFixture(t, server, func(config *ConnConfig) {
		config.Pool.MaxOpen = 4
		config.Pool.MaxIdle = 1
	})
	ctx := context.Background()

	conns := make([]*pooledConn, 0, 4)
	for i := 0; i < 4; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		pool.Release(conn, true)
	}

	stats := pool.Stats()
	if stats.Idle != 1 {
		t.Fatalf("expected MaxIdle to cap the idle set at 1, got %+v", stats)
	}
	if stats.Open != 1 {
		t.Fatalf("expected the surplus connections to be closed, got %+v", stats)
	}
}

// The lane below proves the whole foundation can be constructed over a real
// pooled client, which is the path a deployment takes. The fake server answers
// PING, so readiness and pool wiring are exercised for real.

func TestNewCapabilitiesBuildsAPooledClient(t *testing.T) {
	server := echoServer(t)
	settings := DefaultSettings()
	settings.Conn = fakeConfig(server)
	settings.Conn.Pool.MaxOpen = 2
	settings.Conn.Pool.MaxIdle = 2

	capabilities, err := NewCapabilities(settings)
	if err != nil {
		t.Fatalf("new capabilities: %v", err)
	}

	ctx := context.Background()
	if err := capabilities.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if err := capabilities.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	// Two probes must have reused one pooled connection.
	waitForConnections(t, server, 1)
	if got := server.ConnectionCount(); got != 1 {
		t.Fatalf("expected a pooled connection to be reused, got %d", got)
	}
	if err := capabilities.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := capabilities.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestNewCapabilitiesSurfacesAnUnreachableServerOnProbe(t *testing.T) {
	settings := DefaultSettings()
	settings.Conn.Address = "127.0.0.1:1"
	settings.Conn.DialTimeout = 300 * time.Millisecond

	// Construction is lazy, so this must succeed and the probe must fail.
	capabilities, err := NewCapabilities(settings)
	if err != nil {
		t.Fatalf("new capabilities: %v", err)
	}
	defer capabilities.Close()

	if err := capabilities.Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

// SettingsFromEnv with a nil getenv must read the real process environment, which
// is the deployment path.
func TestSettingsFromEnvWithNilGetenvReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv(envRedisAddress, "env-supplied:6399")
	t.Setenv(envPolicyLock, "fail-open")
	t.Setenv(envSessionIdleTTL, "15m")
	t.Setenv(envSessionAbsoluteTTL, "2h")

	settings, err := SettingsFromEnv(nil)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.Conn.Address != "env-supplied:6399" {
		t.Fatalf("expected the process environment to be read, got %q", settings.Conn.Address)
	}
	if settings.Policy.Lock != FailOpen {
		t.Fatal("expected the lock policy from the environment")
	}
	if settings.Session.IdleTTL != 15*time.Minute {
		t.Fatalf("expected a 15m idle ttl, got %s", settings.Session.IdleTTL)
	}
}
