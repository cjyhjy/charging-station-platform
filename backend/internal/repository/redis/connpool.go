package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// pooledConn is one TCP connection to Redis plus its buffered reader. A
// connection is owned by exactly one caller at a time, so it needs no lock: the
// pool guarantees exclusive ownership between Get and Release.
type pooledConn struct {
	netConn   net.Conn
	reader    *respReader
	createdAt time.Time
	lastUsed  time.Time
}

func (c *pooledConn) close() {
	_ = c.netConn.Close()
}

// expiry reports why a pooled connection can no longer be reused, so an operator
// reading a log knows whether the pool is recycling too aggressively.
func (c *pooledConn) expiry(now time.Time, config ConnConfig) string {
	if config.Pool.MaxLifetime > 0 && now.Sub(c.createdAt) >= config.Pool.MaxLifetime {
		return "max lifetime exceeded"
	}
	if config.Pool.IdleTimeout > 0 && now.Sub(c.lastUsed) >= config.Pool.IdleTimeout {
		return "idle timeout exceeded"
	}
	return ""
}

// wrapUnavailable marks an I/O or deadline failure as an availability failure.
//
// Every transport-level failure must go through this: the degradation policy in
// degradation.go keys off ErrUnavailable, so an unwrapped socket error would skip
// the policy entirely and surface to the business layer as an opaque failure. A
// FailOpen cache or rate limiter would then stop degrading exactly when it is
// needed.
func wrapUnavailable(operation string, err error) error {
	if err == nil {
		return nil
	}
	if isUnavailable(err) || isServerSideError(err) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s: %w: %w", operation, ErrUnavailable, err)
}

// connPool is a bounded, self-healing connection pool.
//
// Accounting rule that keeps the pool correct: open is incremented exactly once
// when a connection is dialled, and decremented exactly once when it is closed.
// Every checkout ends in exactly one Release call, which either returns the
// connection to the idle list or closes it. There is no other path that changes
// open, so the MaxOpen bound cannot drift.
//
// The pool deliberately does not retry a failed command. A retry would duplicate
// a non-idempotent command such as INCR, and the Commands contract already
// requires callers to treat ErrUnavailable as "the command may or may not have
// run".
type connPool struct {
	config ConnConfig
	dialer func(context.Context) (*pooledConn, error)

	mu     sync.Mutex
	idle   []*pooledConn
	open   int
	closed bool
	// released is closed - and replaced - whenever a connection becomes available
	// or the pool closes. Closing a channel is a broadcast, which is what wakes
	// every waiter. A single-slot notification channel would wake only one and
	// leave the rest blocked forever.
	released chan struct{}
}

func newConnPool(config ConnConfig) (*connPool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	pool := &connPool{
		config:   config,
		released: make(chan struct{}),
	}
	pool.dialer = func(ctx context.Context) (*pooledConn, error) {
		return dialRedis(ctx, config)
	}
	return pool, nil
}

// broadcastLocked wakes every waiter. The caller must hold the mutex.
func (p *connPool) broadcastLocked() {
	close(p.released)
	p.released = make(chan struct{})
}

// dialRedis opens and authenticates one connection.
func dialRedis(ctx context.Context, config ConnConfig) (*pooledConn, error) {
	dialer := &net.Dialer{Timeout: config.DialTimeout}
	netConn, err := dialer.DialContext(ctx, "tcp", config.Address)
	if err != nil {
		return nil, wrapUnavailable(fmt.Sprintf("dial redis at %s", config.Address), err)
	}

	now := time.Now()
	conn := &pooledConn{
		netConn:   netConn,
		reader:    newRespReader(bufio.NewReader(netConn)),
		createdAt: now,
		lastUsed:  now,
	}

	// Authentication is part of establishing the connection: a connection that
	// cannot authenticate is not usable, so it must not enter the pool.
	if config.Password != "" || config.Username != "" {
		if err := authConn(ctx, conn, config); err != nil {
			conn.close()
			return nil, err
		}
	}
	if config.Database != 0 {
		if err := selectDatabase(ctx, conn, config); err != nil {
			conn.close()
			return nil, err
		}
	}
	return conn, nil
}

// authConn sends AUTH. A blank username selects single-argument AUTH, which is
// what a password-only Redis expects.
func authConn(ctx context.Context, conn *pooledConn, config ConnConfig) error {
	args := []string{"AUTH"}
	if config.Username != "" {
		args = append(args, config.Username, config.Password)
	} else {
		args = append(args, config.Password)
	}
	if _, err := roundTrip(ctx, conn, config, args...); err != nil {
		return fmt.Errorf("authenticate redis connection: %w", err)
	}
	return nil
}

func selectDatabase(ctx context.Context, conn *pooledConn, config ConnConfig) error {
	if _, err := roundTrip(ctx, conn, config, "SELECT", fmt.Sprintf("%d", config.Database)); err != nil {
		return fmt.Errorf("select redis database %d: %w", config.Database, err)
	}
	return nil
}

// roundTrip writes one command and reads exactly one reply.
//
// The socket deadlines come from the configuration. The caller's context is
// enforced separately, by a watcher that forces an immediate deadline the moment
// the context is done, which unblocks a read or write already in progress.
//
// The context deadline is deliberately NOT copied onto the socket. Clamping the
// socket deadline to the context deadline would arm two timers for the same
// instant, and whichever fired first would decide whether the caller sees
// context.DeadlineExceeded or a raw timeout, making the result nondeterministic.
// The watcher fires only after the context is already done, so the context error
// is always observable when it interrupts.
//
// Ordering rules, both of which are required for correctness:
//
//   - The watcher must have fully exited before this function returns, because
//     the caller releases the connection to the pool immediately afterwards. A
//     watcher still running could install an immediate deadline on a connection
//     the next request now owns, making that request time out. The deferred wait
//     below is what enforces this.
//   - The context is re-checked after each deadline is set. A watcher that fires
//     just before SetWriteDeadline or SetReadDeadline would have its immediate
//     deadline overwritten, losing the cancellation until the configured timeout
//     elapses.
//
// Interrupting a command means its outcome is unknown: it may or may not have
// run. That is the documented meaning of ErrUnavailable, and the connection is
// discarded because a partially written command or partially read reply leaves
// the stream unusable.
func roundTrip(ctx context.Context, conn *pooledConn, config ConnConfig, args ...string) (any, error) {
	return roundTripWithReadTimeout(ctx, conn, config, config.ReadTimeout, args...)
}

// roundTripWithReadTimeout is roundTrip with an explicit read deadline. A blocking
// Streams read (XREADGROUP ... BLOCK) legitimately waits longer than ReadTimeout,
// so the caller must be able to raise the deadline for that one command instead of
// having it reported as an availability failure.
func roundTripWithReadTimeout(ctx context.Context, conn *pooledConn, config ConnConfig, readTimeout time.Duration, args ...string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if readTimeout <= 0 {
		readTimeout = config.ReadTimeout
	}

	stopWatcher := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			// Force the deadline into the past so an in-flight syscall returns now.
			_ = conn.netConn.SetDeadline(time.Now())
		case <-stopWatcher:
		}
	}()

	// Wait for the watcher, not merely signal it. SetDeadline is a local
	// operation that cannot block, so this wait is bounded.
	defer func() {
		close(stopWatcher)
		<-watcherDone
	}()

	if err := conn.netConn.SetWriteDeadline(time.Now().Add(config.WriteTimeout)); err != nil {
		return nil, wrapUnavailable("set redis write deadline", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := conn.netConn.Write(encodeCommand(args...)); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, wrapUnavailable("write redis command", err)
	}
	if err := conn.netConn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, wrapUnavailable("set redis read deadline", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, err := conn.reader.readValue()
	conn.lastUsed = time.Now()
	if err == nil {
		return value, nil
	}
	if isServerSideError(err) {
		// The server understood the command and answered. The connection is still
		// healthy and must be returned to the pool.
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return nil, wrapUnavailable("read redis reply", err)
}

// isServerSideError reports whether err came from the server as a well-formed
// error reply, which leaves the connection reusable.
//
// errors.As is used rather than a type switch so the answer survives wrapping. A
// type switch would report false for a wrapped server error, and the caller would
// then discard a perfectly healthy connection.
func isServerSideError(err error) bool {
	var serverErr *serverError
	if errors.As(err, &serverErr) {
		return true
	}
	var authErr *authFailure
	return errors.As(err, &authErr)
}

// Get checks out a connection, waiting for one if the pool is at MaxOpen.
func (p *connPool) Get(ctx context.Context) (*pooledConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("redis connection pool: %w", ErrClosed)
		}

		now := time.Now()
		var stale *pooledConn
		for len(p.idle) > 0 {
			conn := p.idle[len(p.idle)-1]
			p.idle = p.idle[:len(p.idle)-1]
			if conn.expiry(now, p.config) == "" {
				p.mu.Unlock()
				return conn, nil
			}
			// Recycle instead of handing out a stale or half-dead socket. The close
			// happens outside the lock, so the loop stops here and retries.
			p.open--
			stale = conn
			break
		}
		if stale != nil {
			p.mu.Unlock()
			stale.close()
			continue
		}

		if p.open < p.config.Pool.MaxOpen {
			p.open++
			p.mu.Unlock()

			conn, err := p.dialer(ctx)
			if err != nil {
				p.mu.Lock()
				p.open--
				p.broadcastLocked()
				p.mu.Unlock()
				return nil, err
			}

			// Close may have run while this connection was being dialled. A
			// connection belonging to a closed pool must never be handed out, so it
			// is discarded here rather than used for a command.
			p.mu.Lock()
			if p.closed {
				p.open--
				p.mu.Unlock()
				conn.close()
				return nil, fmt.Errorf("redis connection pool: %w", ErrClosed)
			}
			p.mu.Unlock()
			return conn, nil
		}

		// The pool is saturated. Wake on the next release, on Close, or when the
		// caller gives up.
		released := p.released
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-released:
		}
	}
}

// Release returns a connection. A connection that hit an I/O or protocol error
// must be reported as not reusable, so it is closed instead of handed to the
// next caller.
//
// A connection returning to the idle list has its deadlines cleared. roundTrip
// already sets fresh deadlines before every I/O, and the cancellation watcher is
// awaited before this point, so this is defence in depth: it makes the pool
// invariant "an idle connection carries no deadline" explicit, so no future code
// path can inherit an immediate deadline and time out.
func (p *connPool) Release(conn *pooledConn, reusable bool) {
	p.mu.Lock()
	if !reusable || p.closed || len(p.idle) >= p.config.Pool.MaxIdle {
		p.open--
		p.broadcastLocked()
		p.mu.Unlock()
		conn.close()
		return
	}
	_ = conn.netConn.SetDeadline(time.Time{})
	conn.lastUsed = time.Now()
	p.idle = append(p.idle, conn)
	p.broadcastLocked()
	p.mu.Unlock()
}

// Close closes every idle connection and marks the pool closed. A connection
// currently checked out, or being dialled, is closed when it is released or
// completes, and is never handed to a caller.
func (p *connPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.open -= len(idle)
	// Broadcast, not signal: every waiter must observe the closure.
	p.broadcastLocked()
	p.mu.Unlock()

	for _, conn := range idle {
		conn.close()
	}
	return nil
}

// PoolStats reports pool occupancy. It exists for tests and for an ops endpoint.
type PoolStats struct {
	Open int
	Idle int
}

func (p *connPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{Open: p.open, Idle: len(p.idle)}
}
