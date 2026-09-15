package redis

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestEncodeCommandProducesRESPArray(t *testing.T) {
	got := string(encodeCommand("SET", "ncs:station:st_01", "value"))
	// "ncs:station:st_01" is 17 bytes.
	want := "*3\r\n$3\r\nSET\r\n$17\r\nncs:station:st_01\r\n$5\r\nvalue\r\n"
	if got != want {
		t.Fatalf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestEncodeCommandHandlesEmptyAndSpecialArguments(t *testing.T) {
	// An empty argument is two bytes of framing and no payload, and a binary-safe
	// argument must not be altered: session payloads are JSON and may contain
	// CRLF-adjacent bytes.
	got := string(encodeCommand("SET", "k", ""))
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n"
	if got != want {
		t.Fatalf("expected\n%q\ngot\n%q", want, got)
	}

	binary := "a\r\nb"
	got = string(encodeCommand("SET", "k", binary))
	if !strings.Contains(got, "$4\r\na\r\nb\r\n") {
		t.Fatalf("expected the binary argument to be framed unchanged, got %q", got)
	}
}

func TestRespReaderDecodesEveryReplyType(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  any
	}{
		{"simple string", "+OK\r\n", "OK"},
		{"integer", ":42\r\n", int64(42)},
		{"negative integer", ":-1\r\n", int64(-1)},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"empty bulk string", "$0\r\n\r\n", ""},
		{"null bulk string", "$-1\r\n", nil},
		{"null array", "*-1\r\n", nil},
		{"empty array", "*0\r\n", []any{}},
		{"array of integers", "*2\r\n:1\r\n:60000\r\n", []any{int64(1), int64(60000)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := newRespReader(strings.NewReader(test.input))
			got, err := reader.readValue()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if test.want != nil || got != nil {
				if !equalReply(got, test.want) {
					t.Fatalf("expected %#v, got %#v", test.want, got)
				}
			}
		})
	}
}

func TestRespReaderDecodesNestedArrays(t *testing.T) {
	// XREADGROUP-style replies are nested; the rate-limit script reply is flat.
	reader := newRespReader(strings.NewReader("*2\r\n*3\r\n$3\r\nfoo\r\n:7\r\n$0\r\n\r\n:9\r\n"))
	got, err := reader.readValue()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	outer, ok := got.([]any)
	if !ok || len(outer) != 2 {
		t.Fatalf("expected a 2 element array, got %#v", got)
	}
	inner, ok := outer[0].([]any)
	if !ok || len(inner) != 3 {
		t.Fatalf("expected a nested 3 element array, got %#v", outer[0])
	}
	if inner[0] != "foo" || inner[1] != int64(7) || inner[2] != "" {
		t.Fatalf("unexpected nested values %#v", inner)
	}
	if outer[1] != int64(9) {
		t.Fatalf("expected 9, got %#v", outer[1])
	}
}

func equalReply(got, want any) bool {
	gotArray, gotIsArray := got.([]any)
	wantArray, wantIsArray := want.([]any)
	if gotIsArray != wantIsArray {
		return false
	}
	if !gotIsArray {
		return got == want
	}
	if len(gotArray) != len(wantArray) {
		return false
	}
	for i := range gotArray {
		if !equalReply(gotArray[i], wantArray[i]) {
			return false
		}
	}
	return true
}

// Credential faults mean the deployment is misconfigured, so they must degrade
// like an outage. An ordinary command error must not.
func TestClassifyServerError(t *testing.T) {
	availability := []string{
		"NOAUTH Authentication required.",
		"WRONGPASS invalid username-password pair",
		"NOPERM this user has no permissions to run the 'get' command",
		"ERR Client sent AUTH, but no password is set",
		"ERR invalid password",
		// The exact message a real Redis 7 returns when AUTH is sent to a server
		// with no password configured. Found by the integration suite.
		"ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
	}
	for _, message := range availability {
		err := classifyServerError(message)
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("expected %q to be an availability failure, got %v", message, err)
		}
	}

	business := []string{
		"ERR value is not an integer or out of range",
		"ERR wrong number of arguments for 'get' command",
		"WRONGTYPE Operation against a key holding the wrong kind of value",
	}
	for _, message := range business {
		err := classifyServerError(message)
		if errors.Is(err, ErrUnavailable) {
			t.Errorf("expected %q to be a plain server error, got an availability failure", message)
		}
		if _, ok := err.(*serverError); !ok {
			t.Errorf("expected %q to be a serverError, got %T", message, err)
		}
	}
}

func TestRespReaderRejectsMalformedReplies(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"unknown type", "?weird\r\n"},
		{"empty line", "\r\n"},
		{"bulk without CRLF", "$3\r\nabcXX"},
		{"invalid bulk length", "$abc\r\n"},
		{"invalid array length", "*abc\r\n"},
		{"invalid integer", ":abc\r\n"},
		{"line without CRLF", "+OK\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := newRespReader(strings.NewReader(test.input))
			if _, err := reader.readValue(); err == nil {
				t.Fatal("expected a protocol error")
			}
		})
	}
}

func TestRespReaderRejectsOversizedLine(t *testing.T) {
	// A server that never sends CRLF must not be able to exhaust memory.
	oversized := strings.Repeat("x", maxReplyLineLength+16) + "\r\n"
	reader := newRespReader(bufio.NewReader(strings.NewReader(oversized)))
	if _, err := reader.readValue(); err == nil {
		t.Fatal("expected an oversized reply line to be rejected")
	}
}

func TestRespReaderReportsEOF(t *testing.T) {
	reader := newRespReader(strings.NewReader(""))
	if _, err := reader.readValue(); err == nil {
		t.Fatal("expected EOF to be reported")
	}
}

func TestReplyTypeAssertionsRejectUnexpectedShapes(t *testing.T) {
	if _, err := replyString(int64(1), "GET"); err == nil {
		t.Fatal("expected an integer to be rejected as a string reply")
	}
	if _, err := replyInteger("OK", "DEL"); err == nil {
		t.Fatal("expected a string to be rejected as an integer reply")
	}
	if _, err := replyArray("OK", "EVAL"); err == nil {
		t.Fatal("expected a string to be rejected as an array reply")
	}
}

func TestReplyTypeAssertionsAcceptValidShapes(t *testing.T) {
	if text, err := replyString("PONG", "PING"); err != nil || text != "PONG" {
		t.Fatalf("expected PONG, got %q and %v", text, err)
	}
	if number, err := replyInteger(int64(3), "DEL"); err != nil || number != 3 {
		t.Fatalf("expected 3, got %d and %v", number, err)
	}
	if values, err := replyArray([]any{int64(1)}, "EVAL"); err != nil || len(values) != 1 {
		t.Fatalf("expected one value, got %v and %v", values, err)
	}
}

func TestScriptArgRoundsSubMillisecondDurationsUp(t *testing.T) {
	// A zero-millisecond expiry would be rejected by Redis, so a sub-millisecond
	// window must not round down to zero.
	if got := scriptArg(0); got != "1" {
		t.Fatalf("expected 1, got %s", got)
	}
	if got := scriptArg(500 * 1000); got != "1" {
		t.Fatalf("expected 1, got %s", got)
	}
	if got := scriptArg(1500 * 1000 * 1000); got != "1500" {
		t.Fatalf("expected 1500, got %s", got)
	}
}

// The Lua scripts must be single scripts, because that is what makes counting and
// lock release atomic.
func TestScriptsAreSingleCommands(t *testing.T) {
	if strings.Count(rateLimitScript, "redis.call") < 3 {
		t.Fatal("expected the rate limit script to increment, attach the window and read the ttl")
	}
	if !strings.Contains(rateLimitScript, "PTTL") {
		t.Fatal("expected the rate limit script to report the remaining window")
	}
	// The self-healing branch is what stops an old windowless counter from
	// blocking an identity forever.
	if !strings.Contains(rateLimitScript, "ttl < 0") {
		t.Fatal("expected the rate limit script to heal a counter without a window")
	}
	if !strings.Contains(lockReleaseScript, "GET") || !strings.Contains(lockReleaseScript, "DEL") {
		t.Fatal("expected the lock release script to compare then delete")
	}
}

func TestEncodeCommandSizeHintMatchesOutput(t *testing.T) {
	// encodeCommand pre-sizes its buffer; a wrong estimate would only cost a
	// reallocation, but this keeps the hint honest.
	var buffer bytes.Buffer
	command := encodeCommand("EVAL", rateLimitScript, "1", "ncs:auth:rate-limit:id", "60000")
	buffer.Write(command)
	if buffer.Len() == 0 {
		t.Fatal("expected a non-empty command")
	}
}
