package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// This file implements the small slice of the RESP2 protocol this adapter needs.
// It is deliberately internal and unexported: the package's contract is the
// Commands interface, and callers must never depend on the wire format.
//
// Only RESP2 is used. HELLO/RESP3 is not negotiated, so the reply shapes below
// are the complete set a Redis 6+ server returns for these commands.

// serverError is an error reply produced by Redis itself ("-ERR ..."). The
// command reached the server and was understood, so it is an application-level
// failure, not an availability failure, and must not be wrapped as
// ErrUnavailable.
type serverError struct{ message string }

func (e *serverError) Error() string { return "redis: " + e.message }

// authFailure reports a rejected credential or a missing authentication. That is
// a deployment/configuration fault, so it is treated as an availability failure
// and will trigger the capability's degradation policy rather than being passed
// to a caller as a business error.
type authFailure struct{ message string }

func (e *authFailure) Error() string { return "redis authentication failed: " + e.message }

func (e *authFailure) Unwrap() error { return ErrUnavailable }

// protocolError reports a reply this adapter could not parse. The connection is
// unusable afterwards, so it is also an availability failure.
type protocolError struct{ message string }

func (e *protocolError) Error() string { return "redis protocol error: " + e.message }

func (e *protocolError) Unwrap() error { return ErrUnavailable }

const maxReplyLineLength = 64 * 1024

// respReader decodes RESP2 replies from a buffered reader.
type respReader struct {
	reader *bufio.Reader
}

func newRespReader(reader io.Reader) *respReader {
	if buffered, ok := reader.(*bufio.Reader); ok {
		return &respReader{reader: buffered}
	}
	return &respReader{reader: bufio.NewReader(reader)}
}

// readValue decodes one reply. Bulk strings and arrays may be null, which is
// reported as a nil value: GET uses it for a missing key and EVAL uses it for a
// Lua nil.
func (r *respReader) readValue() (any, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, &protocolError{message: "empty reply line"}
	}

	switch line[0] {
	case '+': // simple string
		return string(line[1:]), nil

	case '-': // error
		return nil, classifyServerError(string(line[1:]))

	case ':': // integer
		value, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, &protocolError{message: "invalid integer reply " + strconv.Quote(string(line))}
		}
		return value, nil

	case '$': // bulk string
		length, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, &protocolError{message: "invalid bulk length " + strconv.Quote(string(line))}
		}
		if length < 0 {
			return nil, nil // null bulk string
		}
		payload := make([]byte, length+2) // include the trailing CRLF
		if _, err := io.ReadFull(r.reader, payload); err != nil {
			return nil, err
		}
		if payload[length] != '\r' || payload[length+1] != '\n' {
			return nil, &protocolError{message: "bulk string is not CRLF terminated"}
		}
		return string(payload[:length]), nil

	case '*': // array
		count, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, &protocolError{message: "invalid array length " + strconv.Quote(string(line))}
		}
		if count < 0 {
			return nil, nil // null array
		}
		values := make([]any, count)
		for i := 0; i < count; i++ {
			values[i], err = r.readValue()
			if err != nil {
				return nil, err
			}
		}
		return values, nil

	default:
		return nil, &protocolError{message: "unknown reply type " + strconv.Quote(string(line[0]))}
	}
}

// readLine reads one CRLF terminated line.
func (r *respReader) readLine() ([]byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) > maxReplyLineLength {
		return nil, &protocolError{message: "reply line exceeds the 64KiB limit"}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, &protocolError{message: "reply line is not CRLF terminated"}
	}
	return line[:len(line)-2], nil
}

// classifyServerError separates credential faults, which degrade, from ordinary
// command errors, which do not.
func classifyServerError(message string) error {
	// Redis error codes are case sensitive in practice but operators and proxies
	// vary, so the comparison is normalised to upper case on both sides.
	upper := strings.ToUpper(message)
	for _, prefix := range []string{
		"NOAUTH",
		"WRONGPASS",
		"NOPERM",
		// Redis 6+ answers AUTH against a server with no password configured
		// with "ERR AUTH <password> called without any password configured...".
		"ERR AUTH",
		"ERR CLIENT SENT AUTH",
		"ERR INVALID PASSWORD",
	} {
		if strings.HasPrefix(upper, prefix) {
			return &authFailure{message: message}
		}
	}
	return &serverError{message: message}
}

// encodeCommand renders a command as a RESP array of bulk strings. Building the
// whole request in memory keeps it to a single Write, so a partial command can
// never be left on the wire by an interrupt.
func encodeCommand(args ...string) []byte {
	size := 16
	for _, arg := range args {
		size += len(arg) + 16
	}
	var buffer bytes.Buffer
	buffer.Grow(size)

	buffer.WriteByte('*')
	buffer.WriteString(strconv.Itoa(len(args)))
	buffer.WriteString("\r\n")
	for _, arg := range args {
		buffer.WriteByte('$')
		buffer.WriteString(strconv.Itoa(len(arg)))
		buffer.WriteString("\r\n")
		buffer.WriteString(arg)
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes()
}

// replyString asserts that a reply is a non-null string.
func replyString(value any, command string) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", &protocolError{message: fmt.Sprintf("%s expected a string reply, got %T", command, value)}
	}
	return text, nil
}

// replyInteger asserts that a reply is an integer.
func replyInteger(value any, command string) (int64, error) {
	number, ok := value.(int64)
	if !ok {
		return 0, &protocolError{message: fmt.Sprintf("%s expected an integer reply, got %T", command, value)}
	}
	return number, nil
}

// replyArray asserts that a reply is a non-null array.
func replyArray(value any, command string) ([]any, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, &protocolError{message: fmt.Sprintf("%s expected an array reply, got %T", command, value)}
	}
	return values, nil
}
