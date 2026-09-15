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

// echoServer answers PING and records everything, which is enough to assert how
// the client frames its commands.
func echoServer(t *testing.T) *fakeRedis {
	t.Helper()
	server := newFakeRedis(t, func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "PING":
			return replyPong
		case "AUTH", "SELECT":
			return replyOK
		default:
			return replyOK
		}
	})
	t.Cleanup(server.Close)
	return server
}

func newTestClient(t *testing.T, server *fakeRedis) *Client {
	t.Helper()
	client, err := NewClient(fakeConfig(server))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestNewClientRejectsInvalidConfig(t *testing.T) {
	config := DefaultConnConfig()
	config.Address = ""
	if _, err := NewClient(config); err == nil {
		t.Fatal("expected an invalid configuration to be rejected")
	}
}

func TestClientPing(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	last := server.LastCommand()
	if len(last) != 1 || last[0] != "PING" {
		t.Fatalf("expected a bare PING, got %#v", last)
	}
}

func TestClientPingRejectsAnUnexpectedReply(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return bulkReply("nonsense") })
	defer server.Close()
	client := newTestClient(t, server)

	if err := client.Ping(context.Background()); err == nil {
		t.Fatal("expected an unexpected PING reply to be rejected")
	}
}

func TestClientGetReturnsNotFoundOnNullReply(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return replyNil })
	defer server.Close()
	client := newTestClient(t, server)

	if _, err := client.Get(context.Background(), "ncs:station:st_01"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClientGetFramesTheCommand(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string { return bulkReply("cached") })
	defer server.Close()
	client := newTestClient(t, server)

	value, err := client.Get(context.Background(), "ncs:station:st_01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "cached" {
		t.Fatalf("expected cached, got %q", value)
	}
	last := server.LastCommand()
	if len(last) != 2 || last[0] != "GET" || last[1] != "ncs:station:st_01" {
		t.Fatalf("unexpected command %#v", last)
	}
}

func TestClientGetRequiresKey(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)
	if _, err := client.Get(context.Background(), ""); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
}

// A TTL must be sent as PX milliseconds in the same command as the value, so the
// key can never be written without its expiration.
func TestClientSetUsesPXInOneCommand(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	if err := client.Set(context.Background(), "ncs:station:st_01", "v", 90*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	last := server.LastCommand()
	want := []string{"SET", "ncs:station:st_01", "v", "PX", "90000"}
	if len(last) != len(want) {
		t.Fatalf("expected %#v, got %#v", want, last)
	}
	for i := range want {
		if last[i] != want[i] {
			t.Fatalf("expected %#v, got %#v", want, last)
		}
	}
}

func TestClientSetWithoutTTLOmitsPX(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	if err := client.Set(context.Background(), "ncs:lock:order:o_01", "token", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	last := server.LastCommand()
	if len(last) != 3 {
		t.Fatalf("expected a bare SET, got %#v", last)
	}
}

func TestClientSetRejectsNegativeTTL(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)
	if err := client.Set(context.Background(), "k", "v", -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestClientSetRejectsNonOKReply(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return simpleReply("QUEUED") })
	defer server.Close()
	client := newTestClient(t, server)

	if err := client.Set(context.Background(), "k", "v", 0); err == nil {
		t.Fatal("expected a non-OK SET reply to be rejected")
	}
}

func TestClientSetNXDetectsAnExistingKey(t *testing.T) {
	var mu sync.Mutex
	existing := false
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "SET") {
			mu.Lock()
			defer mu.Unlock()
			if existing {
				return replyNil
			}
			existing = true
			return replyOK
		}
		return replyOK
	})
	defer server.Close()
	client := newTestClient(t, server)

	acquired, err := client.SetNX(context.Background(), "ncs:lock:order:o_01", "token-a", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if !acquired {
		t.Fatal("expected the first SET NX to succeed")
	}
	last := server.LastCommand()
	// SET key value NX PX <ms>
	want := []string{"SET", "ncs:lock:order:o_01", "token-a", "NX", "PX", "60000"}
	if len(last) != len(want) {
		t.Fatalf("expected %#v, got %#v", want, last)
	}
	for i := range want {
		if last[i] != want[i] {
			t.Fatalf("expected %#v, got %#v", want, last)
		}
	}

	acquired, err = client.SetNX(context.Background(), "ncs:lock:order:o_01", "token-b", time.Minute)
	if err != nil {
		t.Fatalf("setnx: %v", err)
	}
	if acquired {
		t.Fatal("expected the second SET NX to be refused")
	}
}

// INCR on a key holding a non-integer is a server error, not an outage, so the
// connection must stay usable.
func TestClientSurfacesServerErrorsWithoutPoisoningTheConnection(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "INCR") {
			return errorReply("ERR value is not an integer or out of range")
		}
		return replyPong
	})
	defer server.Close()
	client := newTestClient(t, server)
	ctx := context.Background()

	_, err := client.Incr(ctx, "ncs:auth:rate-limit:id")
	if err == nil {
		t.Fatal("expected the server error to surface")
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("a server error must not be reported as an outage: %v", err)
	}

	// The connection was returned to the pool, so the next command works.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("expected the connection to remain usable: %v", err)
	}
	if server.ConnectionCount() != 1 {
		t.Fatalf("expected the pooled connection to be reused, got %d connections", server.ConnectionCount())
	}
}

// The windowed counter must be a single atomic EVAL, never INCR followed by
// EXPIRE, because only the single command closes the permanent-counter gap.
func TestClientIncrWithWindowUsesOneAtomicEval(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "EVAL") {
			return arrayReply(integerReply(3), integerReply(45000))
		}
		return replyOK
	})
	defer server.Close()
	client := newTestClient(t, server)

	count, remaining, err := client.IncrWithWindow(context.Background(), "ncs:auth:rate-limit:id", 60*time.Second)
	if err != nil {
		t.Fatalf("incr with window: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}
	if remaining != 45*time.Second {
		t.Fatalf("expected 45s remaining, got %s", remaining)
	}

	// Exactly one command reached the server: no separate INCR and no EXPIRE.
	commands := server.Commands()
	if len(commands) != 1 {
		t.Fatalf("expected a single command, got %#v", commands)
	}
	eval := commands[0]
	if !strings.EqualFold(eval[0], "EVAL") {
		t.Fatalf("expected EVAL, got %#v", eval)
	}
	if eval[2] != "1" || eval[3] != "ncs:auth:rate-limit:id" {
		t.Fatalf("expected one key as the script key, got %#v", eval)
	}
	if eval[4] != "60000" {
		t.Fatalf("expected the window as 60000ms, got %q", eval[4])
	}
	if !strings.Contains(eval[1], "INCR") || !strings.Contains(eval[1], "PEXPIRE") {
		t.Fatal("expected the script to increment and attach the window")
	}
}

func TestClientIncrWithWindowReportsNoWindow(t *testing.T) {
	server := newFakeRedis(t, func([]string) string {
		return arrayReply(integerReply(1), integerReply(-1))
	})
	defer server.Close()
	client := newTestClient(t, server)

	_, remaining, err := client.IncrWithWindow(context.Background(), "k", time.Minute)
	if err != nil {
		t.Fatalf("incr with window: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected no window reported, got %s", remaining)
	}
}

func TestClientIncrWithWindowRejectsBadArguments(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	if _, _, err := client.IncrWithWindow(context.Background(), "", time.Minute); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
	if _, _, err := client.IncrWithWindow(context.Background(), "k", 0); err == nil {
		t.Fatal("expected a zero window to be rejected")
	}
}

func TestClientIncrWithWindowRejectsAMalformedReply(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return arrayReply(integerReply(1)) })
	defer server.Close()
	client := newTestClient(t, server)

	if _, _, err := client.IncrWithWindow(context.Background(), "k", time.Minute); err == nil {
		t.Fatal("expected a short EVAL reply to be rejected")
	}
}

func TestClientCompareAndDeleteUsesOneAtomicEval(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "EVAL") {
			return integerReply(1)
		}
		return replyOK
	})
	defer server.Close()
	client := newTestClient(t, server)

	deleted, err := client.CompareAndDelete(context.Background(), "ncs:lock:order:o_01", "token-a")
	if err != nil {
		t.Fatalf("compare and delete: %v", err)
	}
	if !deleted {
		t.Fatal("expected the delete to be reported")
	}

	commands := server.Commands()
	if len(commands) != 1 {
		t.Fatalf("expected a single command, got %#v", commands)
	}
	if commands[0][3] != "ncs:lock:order:o_01" || commands[0][4] != "token-a" {
		t.Fatalf("expected the key and token as script arguments, got %#v", commands[0])
	}
}

func TestClientTTLMapsRedisSentinels(t *testing.T) {
	reply := replyMissing
	server := newFakeRedis(t, func([]string) string { return reply })
	defer server.Close()
	client := newTestClient(t, server)
	ctx := context.Background()

	// -2 means the key does not exist.
	if _, _, err := client.TTL(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for -2, got %v", err)
	}

	// -1 means the key exists without an expiration.
	reply = replyNoExpiry
	ttl, hasExpiry, err := client.TTL(ctx, "k")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if hasExpiry || ttl != 0 {
		t.Fatalf("expected no expiry reported, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}

	reply = integerReply(2500)
	ttl, hasExpiry, err = client.TTL(ctx, "k")
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl != 2500*time.Millisecond {
		t.Fatalf("expected 2.5s, got ttl=%s hasExpiry=%v", ttl, hasExpiry)
	}
}

func TestClientExpireUsesPERSISTForZeroTTL(t *testing.T) {
	// PERSIST replies with an integer, unlike the +OK that PEXPIRE-less commands
	// return, so the fake server must answer in kind.
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "PERSIST") {
			return replyOne
		}
		return replyOK
	})
	defer server.Close()
	client := newTestClient(t, server)

	changed, err := client.Expire(context.Background(), "k", 0)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if !changed {
		t.Fatal("expected PERSIST to report the expiration removed")
	}
	if last := server.LastCommand(); len(last) != 2 || last[0] != "PERSIST" {
		t.Fatalf("expected PERSIST, got %#v", last)
	}
}

func TestClientExpireUsesPEXPIREForAPositiveTTL(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return replyOne })
	defer server.Close()
	client := newTestClient(t, server)

	changed, err := client.Expire(context.Background(), "k", 30*time.Second)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if !changed {
		t.Fatal("expected the expiration to be reported as set")
	}
	last := server.LastCommand()
	if len(last) != 3 || last[0] != "PEXPIRE" || last[2] != "30000" {
		t.Fatalf("expected PEXPIRE k 30000, got %#v", last)
	}
}

func TestClientDelSendsEveryKeyInOneCommand(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return integerReply(2) })
	defer server.Close()
	client := newTestClient(t, server)

	removed, err := client.Del(context.Background(), "a", "b")
	if err != nil {
		t.Fatalf("del: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 removals, got %d", removed)
	}
	if last := server.LastCommand(); len(last) != 3 {
		t.Fatalf("expected DEL with two keys, got %#v", last)
	}
}

func TestClientDelWithoutKeysIsANoOp(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	removed, err := client.Del(context.Background())
	if err != nil || removed != 0 {
		t.Fatalf("expected a no-op, got %d and %v", removed, err)
	}
	if len(server.Commands()) != 0 {
		t.Fatal("expected no command to be sent")
	}
}

func TestClientAuthenticatesAndSelectsDatabase(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "PING") {
			return replyPong
		}
		return replyOK
	})
	defer server.Close()

	config := fakeConfig(server)
	config.Username = "ncs"
	config.Password = "secret"
	config.Database = 3
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}

	commands := server.Commands()
	if len(commands) != 3 {
		t.Fatalf("expected AUTH, SELECT and PING, got %#v", commands)
	}
	if commands[0][0] != "AUTH" || commands[0][1] != "ncs" || commands[0][2] != "secret" {
		t.Fatalf("expected AUTH with the username and password, got %#v", commands[0])
	}
	if commands[1][0] != "SELECT" || commands[1][1] != "3" {
		t.Fatalf("expected SELECT 3, got %#v", commands[1])
	}
}

func TestClientAuthenticationFailureIsUnavailable(t *testing.T) {
	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "AUTH") {
			return errorReply("WRONGPASS invalid username-password pair")
		}
		return replyOK
	})
	defer server.Close()

	config := fakeConfig(server)
	config.Password = "wrong"
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	// A bad credential is a deployment fault, so it must degrade rather than
	// surface as a business error.
	err = client.Ping(context.Background())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("expected the error to name authentication, got %v", err)
	}
}

func TestClientReportsAnUnreachableServerAsUnavailable(t *testing.T) {
	// Bind then close a port so nothing is listening on it.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	config := DefaultConnConfig()
	config.Address = address
	config.DialTimeout = 500 * time.Millisecond
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestClientTimesOutASilentServer(t *testing.T) {
	// An empty reply means the handler never answers.
	server := newFakeRedis(t, func([]string) string { return "" })
	defer server.Close()

	config := fakeConfig(server)
	config.ReadTimeout = 150 * time.Millisecond
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	startedAt := time.Now()
	err = client.Ping(context.Background())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable from a read timeout, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 3*time.Second {
		t.Fatalf("expected the read deadline to fire quickly, took %s", elapsed)
	}
}

// A connection that produced a protocol error has unknown socket state, so it
// must be discarded instead of handed to the next caller.
func TestClientDiscardsAConnectionAfterAProtocolError(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return "?garbage\r\n" })
	defer server.Close()
	client := newTestClient(t, server)
	ctx := context.Background()

	if err := client.Ping(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	if err := client.Ping(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected the second attempt to fail too, got %v", err)
	}
	if server.ConnectionCount() < 2 {
		t.Fatalf("expected the poisoned connection to be replaced, got %d connections", server.ConnectionCount())
	}
}

// The pool must reuse connections rather than dialling per command, and it must
// never exceed MaxOpen.
func TestClientPoolReusesConnections(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := client.Ping(ctx); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
	if got := server.ConnectionCount(); got != 1 {
		t.Fatalf("expected a single reused connection, got %d", got)
	}
	if stats := client.Stats(); stats.Open != 1 || stats.Idle != 1 {
		t.Fatalf("expected one idle connection, got %+v", stats)
	}
}

func TestClientPoolBoundsConcurrency(t *testing.T) {
	server := echoServer(t)
	config := fakeConfig(server)
	config.Pool.MaxOpen = 3
	config.Pool.MaxIdle = 3
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if err := client.Ping(context.Background()); err != nil {
				t.Errorf("ping: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := server.ConnectionCount(); got > 3 {
		t.Fatalf("expected at most 3 connections, got %d", got)
	}
	if stats := client.Stats(); stats.Open > 3 {
		t.Fatalf("expected at most 3 open connections, got %+v", stats)
	}
}

func TestClientCloseIsIdempotentAndRejectsFurtherCommands(t *testing.T) {
	server := echoServer(t)
	client := newTestClient(t, server)

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestClientOpenImplementsTheOpenerContract(t *testing.T) {
	server := echoServer(t)
	var opener Opener = mustClient(t)
	defer opener.(*Client).Close()

	commands, err := opener.Open(context.Background(), fakeConfig(server))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer commands.Close()
	if err := commands.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func mustClient(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(DefaultConnConfig())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}
