package redis

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRedis is a scriptable RESP2 server used to drive the real Client without a
// live Redis. It lets a test provoke replies a healthy server will not produce -
// null bulks, null arrays, protocol garbage, credential errors, silence - and
// assert exactly which commands the client sent.
//
// Behaviour that can be checked against a real server belongs in
// integration_test.go instead; this server exists for the edge cases.
type fakeRedis struct {
	listener net.Listener

	mu       sync.Mutex
	received [][]string
	handler  func(args []string) string
	conns    []net.Conn
	stopping bool

	wg     sync.WaitGroup
	closed chan struct{}
	once   sync.Once
}

func newFakeRedis(t *testing.T, handler func(args []string) string) *fakeRedis {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &fakeRedis{
		listener: listener,
		handler:  handler,
		closed:   make(chan struct{}),
	}
	server.wg.Add(1)
	go server.serve()
	return server
}

func (f *fakeRedis) serve() {
	defer f.wg.Done()
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		if f.stopping {
			f.mu.Unlock()
			_ = conn.Close()
			continue
		}
		f.conns = append(f.conns, conn)
		f.wg.Add(1)
		f.mu.Unlock()
		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer f.wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.received = append(f.received, args)
		handler := f.handler
		f.mu.Unlock()

		reply := handler(args)
		if reply == "" {
			// An empty reply means "never answer", which tests a read timeout.
			<-f.closed
			return
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

// Addr returns the address to point a ConnConfig at.
func (f *fakeRedis) Addr() string { return f.listener.Addr().String() }

// Commands returns everything the server received, low level AUTH and SELECT
// included.
func (f *fakeRedis) Commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.received))
	copy(out, f.received)
	return out
}

// LastCommand returns the most recent command, or nil.
func (f *fakeRedis) LastCommand() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.received) == 0 {
		return nil
	}
	return f.received[len(f.received)-1]
}

// ConnectionCount reports how many TCP connections were accepted, which is how a
// test observes pool reuse.
func (f *fakeRedis) ConnectionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// Close stops accepting, closes every tracked connection and waits for the
// handler goroutines. Closing the connections is what makes this deterministic:
// otherwise a handler would block reading from a client socket that the test
// still holds open, and Close would never return.
func (f *fakeRedis) Close() {
	f.once.Do(func() {
		f.mu.Lock()
		f.stopping = true
		conns := f.conns
		f.mu.Unlock()

		close(f.closed)
		_ = f.listener.Close()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	f.wg.Wait()
}

// readCommand decodes one RESP array of bulk strings, as a client sends.
func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 || line[0] != '*' {
		return nil, io.ErrUnexpectedEOF
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSuffix(strings.TrimSuffix(header, "\n"), "\r")
		if len(header) == 0 || header[0] != '$' {
			return nil, io.ErrUnexpectedEOF
		}
		length, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		args = append(args, string(payload[:length]))
	}
	return args, nil
}

// fakeConfig returns a ConnConfig aimed at the fake server with short timeouts so
// a misbehaving server fails a test quickly.
func fakeConfig(server *fakeRedis) ConnConfig {
	config := DefaultConnConfig()
	config.Address = server.Addr()
	config.DialTimeout = time.Second
	config.ReadTimeout = time.Second
	config.WriteTimeout = time.Second
	return config
}

// replyOK and the rest are the canned replies a handler returns.
const (
	replyOK       = "+OK\r\n"
	replyPong     = "+PONG\r\n"
	replyNil      = "$-1\r\n"
	replyZero     = ":0\r\n"
	replyOne      = ":1\r\n"
	replyMissing  = ":-2\r\n"
	replyNoExpiry = ":-1\r\n"
)

func simpleReply(value string) string { return "+" + value + "\r\n" }

func bulkReply(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}

func integerReply(value int64) string {
	return ":" + strconv.FormatInt(value, 10) + "\r\n"
}

func errorReply(message string) string { return "-" + message + "\r\n" }

func arrayReply(values ...string) string {
	return "*" + strconv.Itoa(len(values)) + "\r\n" + strings.Join(values, "")
}
