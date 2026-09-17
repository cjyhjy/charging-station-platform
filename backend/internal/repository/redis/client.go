package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// Client is the production Commands implementation: a pooled TCP client speaking
// RESP2, built entirely on the standard library.
//
// It exists instead of a third-party client because backend/go.mod and
// backend/go.sum are shared lock files that the integration owner manages, and
// because the command surface this package needs is small enough that the
// dependency would cost more than it saves. The pool is bounded by ConnConfig,
// every command carries a deadline, and transport faults are always reported as
// ErrUnavailable so the degradation policy can act on them.
//
// A Client is safe for concurrent use.
type Client struct {
	pool *connPool
}

var (
	_ Commands = (*Client)(nil)
	_ Opener   = (*Client)(nil)
)

// NewClient opens a pooled client. It does not dial eagerly: the first command,
// or an explicit Ping, establishes the connections. A caller that wants startup
// failure detection should call Ping or use Health.
func NewClient(config ConnConfig) (*Client, error) {
	pool, err := newConnPool(config)
	if err != nil {
		return nil, err
	}
	return &Client{pool: pool}, nil
}

// Open implements Opener, so a Client can be used as the single construction
// point for the B line.
func (c *Client) Open(_ context.Context, config ConnConfig) (Commands, error) {
	return NewClient(config)
}

// Stats reports current pool occupancy.
func (c *Client) Stats() PoolStats { return c.pool.Stats() }

// do runs one command, returning the raw reply. It owns the checkout/release
// pairing so no caller can leak a connection.
//
// A cancelled context is checked before a connection is used, so an already-dead
// request never reaches Redis. Cancellation during the round trip interrupts the
// in-flight operation; see roundTrip.
func (c *Client) do(ctx context.Context, args ...string) (any, error) {
	return c.doWithReadTimeout(ctx, c.pool.config.ReadTimeout, args...)
}

// doWithReadTimeout runs one command with an explicit read deadline, so a
// blocking Streams read is not cut short by the default ReadTimeout.
func (c *Client) doWithReadTimeout(ctx context.Context, readTimeout time.Duration, args ...string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	value, err := roundTripWithReadTimeout(ctx, conn, c.pool.config, readTimeout, args...)
	if err != nil {
		// A server-side error leaves the connection healthy; anything else means
		// the socket state is unknown and the connection must not be reused. An
		// interrupted command counts as unknown, because part of it may have been
		// written or read.
		c.pool.Release(conn, isServerSideError(err))
		return nil, err
	}
	c.pool.Release(conn, true)
	return value, nil
}

func (c *Client) Ping(ctx context.Context) error {
	reply, err := c.do(ctx, "PING")
	if err != nil {
		return err
	}
	// A server may answer +PONG or a bulk "PONG" depending on the deployment.
	text, err := replyString(reply, "PING")
	if err != nil {
		return err
	}
	if text != "PONG" {
		return fmt.Errorf("ping redis: unexpected reply %q", text)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("get: key is required")
	}
	reply, err := c.do(ctx, "GET", key)
	if err != nil {
		return "", err
	}
	if reply == nil {
		return "", fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	return replyString(reply, "GET")
}

func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("set: key is required")
	}
	if ttl < 0 {
		return fmt.Errorf("set %s: %w", key, ErrInvalidTTL)
	}
	args := []string{"SET", key, value}
	if ttl > 0 {
		// PX keeps sub-second windows exact, which a session or cache TTL needs.
		args = append(args, "PX", scriptArg(ttl))
	}
	reply, err := c.do(ctx, args...)
	if err != nil {
		return err
	}
	return expectOK(reply, "SET")
}

func (c *Client) Del(ctx context.Context, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	args := append([]string{"DEL"}, keys...)
	reply, err := c.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	removed, err := replyInteger(reply, "DEL")
	if err != nil {
		return 0, err
	}
	return int(removed), nil
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, fmt.Errorf("incr: key is required")
	}
	reply, err := c.do(ctx, "INCR", key)
	if err != nil {
		return 0, err
	}
	return replyInteger(reply, "INCR")
}

// IncrWithWindow increments a counter and guarantees a window in one atomic
// script. Use it for every windowed counter: the two-command form cannot be made
// crash safe.
func (c *Client) IncrWithWindow(ctx context.Context, key string, window time.Duration) (int64, time.Duration, error) {
	if key == "" {
		return 0, 0, fmt.Errorf("incr with window: key is required")
	}
	if window <= 0 {
		return 0, 0, fmt.Errorf("incr with window: window must be greater than zero")
	}
	reply, err := c.do(ctx, "EVAL", rateLimitScript, "1", key, scriptArg(window))
	if err != nil {
		return 0, 0, err
	}
	values, err := replyArray(reply, "EVAL")
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, &protocolError{message: fmt.Sprintf("EVAL rate limit expected 2 values, got %d", len(values))}
	}
	count, err := replyInteger(values[0], "EVAL")
	if err != nil {
		return 0, 0, err
	}
	ttlMillis, err := replyInteger(values[1], "EVAL")
	if err != nil {
		return 0, 0, err
	}
	if ttlMillis < 0 {
		return count, 0, nil
	}
	return count, time.Duration(ttlMillis) * time.Millisecond, nil
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl < 0 {
		return false, fmt.Errorf("expire %s: %w", key, ErrInvalidTTL)
	}
	// A zero TTL means "remove the expiration", matching the in-memory adapter.
	command := "PEXPIRE"
	argument := scriptArg(ttl)
	if ttl == 0 {
		command = "PERSIST"
		argument = ""
	}
	args := []string{command, key}
	if argument != "" {
		args = append(args, argument)
	}
	reply, err := c.do(ctx, args...)
	if err != nil {
		return false, err
	}
	changed, err := replyInteger(reply, command)
	if err != nil {
		return false, err
	}
	return changed == 1, nil
}

func (c *Client) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("setnx: key is required")
	}
	if ttl < 0 {
		return false, fmt.Errorf("setnx %s: %w", key, ErrInvalidTTL)
	}
	args := []string{"SET", key, value, "NX"}
	if ttl > 0 {
		args = append(args, "PX", scriptArg(ttl))
	}
	reply, err := c.do(ctx, args...)
	if err != nil {
		return false, err
	}
	// SET NX replies with a null bulk string when the key already exists.
	return reply != nil, nil
}

// CompareAndDelete deletes a key only if it still holds the expected value, in a
// single script so a lock a different holder acquired in between is never
// deleted.
// RunScript executes a Lua script server-side (EVAL) and returns the raw
// reply. See the Commands interface for the rationale.
func (c *Client) RunScript(ctx context.Context, script string, keys []string, args []string) (any, error) {
	if script == "" {
		return nil, fmt.Errorf("run script: script is required")
	}
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("run script: key is required")
		}
	}
	argv := make([]string, 0, len(keys)+len(args)+3)
	argv = append(argv, "EVAL", script, strconv.Itoa(len(keys)))
	argv = append(argv, keys...)
	argv = append(argv, args...)
	return c.do(ctx, argv...)
}

func (c *Client) CompareAndDelete(ctx context.Context, key, expected string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("compare and delete: key is required")
	}
	reply, err := c.do(ctx, "EVAL", lockReleaseScript, "1", key, expected)
	if err != nil {
		return false, err
	}
	deleted, err := replyInteger(reply, "EVAL")
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}

func (c *Client) TTL(ctx context.Context, key string) (time.Duration, bool, error) {
	if key == "" {
		return 0, false, fmt.Errorf("ttl: key is required")
	}
	// PTTL is used so a sub-second window is reported exactly.
	reply, err := c.do(ctx, "PTTL", key)
	if err != nil {
		return 0, false, err
	}
	millis, err := replyInteger(reply, "PTTL")
	if err != nil {
		return 0, false, err
	}
	switch {
	case millis == -2:
		return 0, false, fmt.Errorf("ttl %s: %w", key, ErrNotFound)
	case millis == -1:
		return 0, false, nil // exists without an expiration
	case millis < 0:
		return 0, false, &protocolError{message: fmt.Sprintf("PTTL returned an unexpected value %d", millis)}
	}
	return time.Duration(millis) * time.Millisecond, true, nil
}

func (c *Client) Close() error { return c.pool.Close() }

// expectOK asserts the "+OK" reply used by SET.
func expectOK(reply any, command string) error {
	text, err := replyString(reply, command)
	if err != nil {
		return err
	}
	if text != "OK" {
		return &protocolError{message: fmt.Sprintf("%s expected OK, got %q", command, text)}
	}
	return nil
}
