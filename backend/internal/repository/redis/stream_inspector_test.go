package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// nilBulk is how RESP2 encodes a nil value, which is what XINFO returns for lag and
// entries-read when Redis cannot compute them.
const nilBulk = "$-1\r\n"

// xinfoStreamReply builds an XINFO STREAM reply.
//
// The reply is a flat field/value array that also embeds nested arrays for first-entry and
// last-entry, which is the shape a parser is most likely to get wrong, so the fixture
// includes them deliberately.
func xinfoStreamReply(length, entriesAdded, groups int64, lastID string) string {
	return arrayReply(
		bulkReply("length"), integerReply(length),
		bulkReply("radix-tree-keys"), integerReply(1),
		bulkReply("radix-tree-nodes"), integerReply(2),
		bulkReply("last-generated-id"), bulkReply(lastID),
		bulkReply("max-deleted-entry-id"), bulkReply("0-0"),
		bulkReply("entries-added"), integerReply(entriesAdded),
		bulkReply("recorded-first-entry-id"), bulkReply("1-1"),
		bulkReply("groups"), integerReply(groups),
		bulkReply("first-entry"), arrayReply(bulkReply("1-1"), arrayReply(bulkReply("k"), bulkReply("v"))),
		bulkReply("last-entry"), arrayReply(bulkReply("9-9"), arrayReply(bulkReply("k"), bulkReply("v"))),
	)
}

// xinfoGroupEntry builds the flat field/value array for one group, which is one element of
// an XINFO GROUPS reply. entriesRead and lag may be nil to model the case where Redis
// cannot report them.
func xinfoGroupEntry(name string, consumers, pending int64, lastDelivered string, entriesRead, lag any) string {
	return arrayReply(
		bulkReply("name"), bulkReply(name),
		bulkReply("consumers"), integerReply(consumers),
		bulkReply("pending"), integerReply(pending),
		bulkReply("last-delivered-id"), bulkReply(lastDelivered),
		bulkReply("entries-read"), renderOptional(entriesRead),
		bulkReply("lag"), renderOptional(lag),
	)
}

// xinfoGroupsReply wraps group entries into a complete XINFO GROUPS reply.
func xinfoGroupsReply(entries ...string) string {
	return arrayReply(entries...)
}

func renderOptional(value any) string {
	switch typed := value.(type) {
	case nil:
		return nilBulk
	case int64:
		return integerReply(typed)
	case int:
		return integerReply(int64(typed))
	default:
		return bulkReply(fmt.Sprint(value))
	}
}

func newTestInspector(t *testing.T, handler func(args []string) string) *StreamsClient {
	t.Helper()
	server := newFakeRedis(t, handler)
	t.Cleanup(server.Close)
	return NewStreamsClientOver(newTestClient(t, server))
}

func TestStreamInfoParsesTheFlatPairReply(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string {
		return xinfoStreamReply(12, 40, 2, "1700000000000-0")
	})

	info, err := inspector.StreamInfo(context.Background(), "ncs:stream:charge-event")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Length != 12 {
		t.Fatalf("expected length 12, got %d", info.Length)
	}
	if info.EntriesAdded != 40 {
		t.Fatalf("expected 40 entries added, got %d", info.EntriesAdded)
	}
	if info.LastGeneratedID != "1700000000000-0" {
		t.Fatalf("expected the last generated id, got %q", info.LastGeneratedID)
	}
	if info.GroupCount != 2 {
		t.Fatalf("expected 2 groups, got %d", info.GroupCount)
	}
}

// A stream that has never received an entry does not exist as a key. That is normal for a
// freshly configured stream and must be distinguishable from a failure.
func TestStreamInfoReportsAMissingStreamAsNotFound(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string {
		return errorReply("ERR no such key")
	})

	info, err := inspector.StreamInfo(context.Background(), "ncs:stream:charge-event")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if info.Stream != "ncs:stream:charge-event" {
		t.Fatalf("expected the stream name on the zero value, got %q", info.Stream)
	}
}

func TestStreamInfoRequiresAStream(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string { return replyOK })
	if _, err := inspector.StreamInfo(context.Background(), ""); err == nil {
		t.Fatal("expected an empty stream to be rejected")
	}
}

func TestGroupInfoFindsTheRequestedGroupAmongMany(t *testing.T) {
	// A real stream usually has several groups, so the parser must select rather than take
	// the first.
	inspector := newTestInspector(t, func([]string) string {
		return xinfoGroupsReply(
			xinfoGroupEntry("other-group", 1, 7, "1-1", int64(5), int64(2)),
			xinfoGroupEntry("charge-event-workers", 3, 4, "9-9", int64(30), int64(10)),
		)
	})

	info, err := inspector.GroupInfo(context.Background(), "s", "charge-event-workers")
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.Name != "charge-event-workers" {
		t.Fatalf("expected the requested group, got %q", info.Name)
	}
	if info.Pending != 4 || info.Consumers != 3 {
		t.Fatalf("unexpected group state %+v", info)
	}
	if info.LastDeliveredID != "9-9" {
		t.Fatalf("expected the last delivered id, got %q", info.LastDeliveredID)
	}
	if info.Lag != 10 || !info.LagKnown() {
		t.Fatalf("expected lag 10 to be known, got %d", info.Lag)
	}
}

// Redis reports nil for lag and entries-read when it cannot compute them, which happens
// after XSETID or trimming. That must be carried as "unknown" rather than as zero.
func TestGroupInfoCarriesUnknownLagAsUnknown(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string {
		return xinfoGroupsReply(xinfoGroupEntry("g", 0, 0, "1789400749109-0", nil, nil))
	})

	info, err := inspector.GroupInfo(context.Background(), "s", "g")
	if err != nil {
		t.Fatalf("group info: %v", err)
	}
	if info.LagKnown() {
		t.Fatalf("expected lag to be reported unknown, got %d", info.Lag)
	}
	if info.EntriesReadKnown() {
		t.Fatalf("expected entries-read to be reported unknown, got %d", info.EntriesRead)
	}
}

func TestGroupInfoReportsAMissingStreamAndGroup(t *testing.T) {
	missingStream := newTestInspector(t, func([]string) string { return errorReply("ERR no such key") })
	if _, err := missingStream.GroupInfo(context.Background(), "s", "g"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a missing stream, got %v", err)
	}

	// The group exists in a different name: the worker has not created its own yet.
	otherGroup := newTestInspector(t, func([]string) string {
		return xinfoGroupsReply(xinfoGroupEntry("someone-else", 1, 0, "1-1", int64(1), int64(0)))
	})
	if _, err := otherGroup.GroupInfo(context.Background(), "s", "g"); !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing, got %v", err)
	}

	empty := newTestInspector(t, func([]string) string { return arrayReply() })
	if _, err := empty.GroupInfo(context.Background(), "s", "g"); !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing for no groups, got %v", err)
	}
}

func TestGroupInfoRequiresBothNames(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string { return arrayReply() })
	if _, err := inspector.GroupInfo(context.Background(), "", "g"); err == nil {
		t.Fatal("expected an empty stream to be rejected")
	}
	if _, err := inspector.GroupInfo(context.Background(), "s", ""); err == nil {
		t.Fatal("expected an empty group to be rejected")
	}
}

func TestStreamLenParsesAndValidates(t *testing.T) {
	inspector := newTestInspector(t, func([]string) string { return integerReply(7) })
	length, err := inspector.StreamLen(context.Background(), "ncs:stream:dead-letter")
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if length != 7 {
		t.Fatalf("expected 7, got %d", length)
	}

	if _, err := inspector.StreamLen(context.Background(), ""); err == nil {
		t.Fatal("expected an empty stream to be rejected")
	}
}

func TestStreamInspectorRejectsMalformedReplies(t *testing.T) {
	// An odd field count would mean a silently dropped field, which would understate a
	// backlog.
	odd := newTestInspector(t, func([]string) string {
		return arrayReply(bulkReply("length"), integerReply(3), bulkReply("dangling"))
	})
	if _, err := odd.StreamInfo(context.Background(), "s"); err == nil {
		t.Fatal("expected an odd field count to be rejected")
	}

	notAnArray := newTestInspector(t, func([]string) string { return bulkReply("nonsense") })
	if _, err := notAnArray.StreamInfo(context.Background(), "s"); err == nil {
		t.Fatal("expected a non-array reply to be rejected")
	}
}

func TestEffectiveLagPrefersTheServerValue(t *testing.T) {
	stream := StreamInfo{EntriesAdded: 40}
	group := GroupInfo{Lag: 10, EntriesRead: 30}
	lag, known := EffectiveLag(stream, group)
	if !known || lag != 10 {
		t.Fatalf("expected the server lag 10, got %d known=%v", lag, known)
	}
}

// When Redis cannot report lag, deriving it keeps the metric meaningful. Reporting zero
// would look like a healthy stream, which is the one answer an operator must not get.
func TestEffectiveLagDerivesWhenUnknown(t *testing.T) {
	stream := StreamInfo{EntriesAdded: 40}
	group := GroupInfo{Lag: -1, EntriesRead: 25}
	lag, known := EffectiveLag(stream, group)
	if !known {
		t.Fatal("expected a derived lag")
	}
	if lag != 15 {
		t.Fatalf("expected a derived lag of 15, got %d", lag)
	}
}

func TestEffectiveLagIsUnknownWithoutInputs(t *testing.T) {
	if _, known := EffectiveLag(StreamInfo{EntriesAdded: 40}, GroupInfo{Lag: -1, EntriesRead: -1}); known {
		t.Fatal("expected lag to be unknown when nothing can compute it")
	}
}

// Trimming can make a group appear to have delivered more than the stream holds; a
// negative backlog is meaningless, so it is clamped rather than reported.
func TestEffectiveLagNeverGoesNegative(t *testing.T) {
	lag, known := EffectiveLag(StreamInfo{EntriesAdded: 10}, GroupInfo{Lag: -1, EntriesRead: 25})
	if !known {
		t.Fatal("expected a derived lag")
	}
	if lag != 0 {
		t.Fatalf("expected the negative backlog clamped to 0, got %d", lag)
	}
}
