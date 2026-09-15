package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// StreamInfo is the state of one stream, as reported by XINFO STREAM.
type StreamInfo struct {
	Stream string
	// Length is the number of entries currently in the stream.
	Length int64
	// EntriesAdded is the total ever added, including entries that were trimmed or
	// deleted. It is what makes a lag calculation stable across trimming.
	EntriesAdded int64
	// LastGeneratedID is the newest entry id the stream has produced.
	LastGeneratedID string
	// GroupCount is the number of consumer groups attached.
	GroupCount int64
}

// GroupInfo is the state of one consumer group, as reported by XINFO GROUPS.
type GroupInfo struct {
	Stream    string
	Group     string
	Name      string
	Consumers int64
	// Pending counts entries delivered but never acknowledged. A pending count that
	// keeps growing is the signal that consumers are failing or too slow.
	Pending int64
	// LastDeliveredID is how far the group has been served.
	LastDeliveredID string
	// EntriesRead is the number of entries the group has delivered. It is -1 when
	// Redis cannot report it, which happens after the group's position is changed.
	EntriesRead int64
	// Lag is the number of entries the group has not yet been served. It is -1 when
	// Redis cannot compute it.
	Lag int64
}

// LagKnown reports whether Redis could compute the lag itself.
func (g GroupInfo) LagKnown() bool { return g.Lag >= 0 }

// EntriesReadKnown reports whether Redis could report the delivered count.
func (g GroupInfo) EntriesReadKnown() bool { return g.EntriesRead >= 0 }

// EffectiveLag reports the group's backlog, using Redis' own lag when it is available
// and deriving it from the stream totals when it is not.
//
// Redis reports lag as nil when the group's last-delivered-id does not correspond to a
// stream entry, which happens after XSETID or after the entries it referred to were
// trimmed. Deriving the value keeps the metric meaningful in that case; reporting zero
// would look like a healthy stream, which is the one answer an operator must not get.
func EffectiveLag(stream StreamInfo, group GroupInfo) (lag int64, known bool) {
	if group.LagKnown() {
		return group.Lag, true
	}
	if !group.EntriesReadKnown() {
		return 0, false
	}
	derived := stream.EntriesAdded - group.EntriesRead
	if derived < 0 {
		// The group has delivered more than the stream currently counts, which trimming
		// can produce: a negative backlog is meaningless, so it is clamped.
		derived = 0
	}
	return derived, true
}

// StreamInspector reports the runtime state of consumed streams.
//
// It exists so the observability layer can sample lag and pending depth without
// depending on the StreamClient command surface, which is about consuming rather than
// inspecting.
type StreamInspector interface {
	StreamInfo(ctx context.Context, stream string) (StreamInfo, error)
	GroupInfo(ctx context.Context, stream, group string) (GroupInfo, error)
	StreamLen(ctx context.Context, stream string) (int64, error)
}

var _ StreamInspector = (*StreamsClient)(nil)

// StreamInfo reads XINFO STREAM.
//
// A stream that has never received an entry does not exist as a key, so Redis answers
// "no such key". That is a normal state for a subscribed stream and is reported as
// ErrNotFound with a zero StreamInfo, so a caller can treat it as empty rather than as
// a failure.
func (s *StreamsClient) StreamInfo(ctx context.Context, stream string) (StreamInfo, error) {
	if strings.TrimSpace(stream) == "" {
		return StreamInfo{}, fmt.Errorf("stream info: stream is required")
	}
	reply, err := s.client.do(ctx, "XINFO", "STREAM", stream)
	if err != nil {
		if isNoSuchKey(err) {
			return StreamInfo{Stream: stream}, fmt.Errorf("stream info %s: %w", stream, ErrNotFound)
		}
		return StreamInfo{}, fmt.Errorf("stream info %s: %w", stream, err)
	}
	fields, err := decodeFlatPairs(reply, "XINFO STREAM")
	if err != nil {
		return StreamInfo{}, err
	}
	info := StreamInfo{Stream: stream}
	info.Length = flatInt(fields, "length")
	info.EntriesAdded = flatInt(fields, "entries-added")
	info.LastGeneratedID = flatString(fields, "last-generated-id")
	info.GroupCount = flatInt(fields, "groups")
	return info, nil
}

// GroupInfo reads XINFO GROUPS and returns the entry for one group.
//
// An absent group is reported as ErrGroupMissing, which is different from an absent
// stream: the first means the worker has not created its group yet, the second means
// nothing was ever published.
func (s *StreamsClient) GroupInfo(ctx context.Context, stream, group string) (GroupInfo, error) {
	if strings.TrimSpace(stream) == "" || strings.TrimSpace(group) == "" {
		return GroupInfo{}, fmt.Errorf("group info: stream and group are required")
	}
	reply, err := s.client.do(ctx, "XINFO", "GROUPS", stream)
	if err != nil {
		if isNoSuchKey(err) {
			return GroupInfo{Stream: stream, Group: group}, fmt.Errorf("group info %s/%s: %w", stream, group, ErrNotFound)
		}
		return GroupInfo{}, fmt.Errorf("group info %s/%s: %w", stream, group, err)
	}
	if reply == nil {
		return GroupInfo{}, fmt.Errorf("group info %s/%s: %w", stream, group, ErrGroupMissing)
	}
	groups, err := replyArray(reply, "XINFO GROUPS")
	if err != nil {
		return GroupInfo{}, err
	}
	for _, entry := range groups {
		fields, err := decodeFlatPairs(entry, "XINFO GROUPS entry")
		if err != nil {
			return GroupInfo{}, err
		}
		if flatString(fields, "name") != group {
			continue
		}
		info := GroupInfo{
			Stream:          stream,
			Group:           group,
			Name:            group,
			Consumers:       flatInt(fields, "consumers"),
			Pending:         flatInt(fields, "pending"),
			LastDeliveredID: flatString(fields, "last-delivered-id"),
			// Redis reports these as nil when it cannot compute them; -1 carries that.
			EntriesRead: flatIntDefault(fields, "entries-read", -1),
			Lag:         flatIntDefault(fields, "lag", -1),
		}
		return info, nil
	}
	return GroupInfo{}, fmt.Errorf("group info %s/%s: %w", stream, group, ErrGroupMissing)
}

// StreamLen reads XLEN. A missing key is zero entries, not an error, which matches the
// Redis reply and keeps the dead-letter gauge meaningful before the first dead letter.
func (s *StreamsClient) StreamLen(ctx context.Context, stream string) (int64, error) {
	if strings.TrimSpace(stream) == "" {
		return 0, fmt.Errorf("stream length: stream is required")
	}
	reply, err := s.client.do(ctx, "XLEN", stream)
	if err != nil {
		return 0, fmt.Errorf("stream length %s: %w", stream, err)
	}
	length, err := replyInteger(reply, "XLEN")
	if err != nil {
		return 0, err
	}
	return length, nil
}

// isNoSuchKey reports Redis' "no such key" reply, which for XINFO means the stream has
// never received an entry rather than a configuration fault.
func isNoSuchKey(err error) bool {
	var serverErr *serverError
	if !errors.As(err, &serverErr) {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(serverErr.message), "ERR NO SUCH KEY")
}

// decodeFlatPairs decodes the alternating field/value array XINFO replies use.
//
// Some values are themselves arrays (first-entry, last-entry), so the walk is by index
// rather than by type, and an odd length is a protocol violation: silently ignoring the
// trailing field would report a stream as healthier than it is.
func decodeFlatPairs(value any, command string) (map[string]any, error) {
	items, err := replyArray(value, command)
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, &protocolError{message: fmt.Sprintf("%s has an odd field count %d", command, len(items))}
	}
	fields := make(map[string]any, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		name, ok := items[i].(string)
		if !ok {
			return nil, &protocolError{message: fmt.Sprintf("%s field name is not a string", command)}
		}
		fields[name] = items[i+1]
	}
	return fields, nil
}

func flatInt(fields map[string]any, name string) int64 {
	return flatIntDefault(fields, name, 0)
}

// flatIntDefault reads an integer field, returning fallback when the field is absent or
// Redis reported it as nil.
func flatIntDefault(fields map[string]any, name string, fallback int64) int64 {
	value, ok := fields[name]
	if !ok || value == nil {
		return fallback
	}
	switch typed := value.(type) {
	case int64:
		return typed
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return fallback
		}
		return parsed
	default:
		return fallback
	}
}

func flatString(fields map[string]any, name string) string {
	value, ok := fields[name]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}
