package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// These tests drive the Streams adapter against the scriptable fake server. They cover
// the reply shapes and error classifications a healthy Redis will not produce, which is
// exactly where a hand-written protocol adapter goes wrong.

func newTestStreamsClient(t *testing.T, server *fakeRedis) *StreamsClient {
	t.Helper()
	return NewStreamsClientOver(newTestClient(t, server))
}

func TestStreamsEnsureGroupTreatsBusyGroupAsSuccess(t *testing.T) {
	// BUSYGROUP is the normal reply on restart, so treating it as an error would stop a
	// worker from starting a second time.
	server := newFakeRedis(t, func([]string) string {
		return errorReply("BUSYGROUP Consumer Group name already exists")
	})
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if err := streams.EnsureGroup(context.Background(), "s", "g", "0-0"); err != nil {
		t.Fatalf("expected BUSYGROUP to be treated as success, got %v", err)
	}
}

func TestStreamsEnsureGroupSurfacesOtherErrors(t *testing.T) {
	server := newFakeRedis(t, func([]string) string {
		return errorReply("ERR unknown command 'XGROUP'")
	})
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	err := streams.EnsureGroup(context.Background(), "s", "g", "0-0")
	if err == nil {
		t.Fatal("expected the server error to surface")
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("a server error must not be reported as an outage: %v", err)
	}
}

func TestStreamsEnsureGroupFramesTheCommand(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return replyOK })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if err := streams.EnsureGroup(context.Background(), "ncs:stream:charge-event", "g1", "$"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	last := server.LastCommand()
	// MKSTREAM lets a worker start before the first event is ever published.
	want := []string{"XGROUP", "CREATE", "ncs:stream:charge-event", "g1", "$", "MKSTREAM"}
	if len(last) != len(want) {
		t.Fatalf("expected %v, got %#v", want, last)
	}
	for i := range want {
		if last[i] != want[i] {
			t.Fatalf("expected %v, got %#v", want, last)
		}
	}
}

func TestStreamsEnsureGroupDefaultsTheStartID(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return replyOK })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if err := streams.EnsureGroup(context.Background(), "s", "g", ""); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	last := server.LastCommand()
	// An empty start id would be rejected by Redis, so it must be defaulted.
	if last[4] != "0-0" {
		t.Fatalf("expected the start id defaulted to 0-0, got %q", last[4])
	}
}

func TestStreamsAddFramesFieldsInSortedOrder(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return bulkReply("1700000000000-0") })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	// A deterministic field order means a duplicate event produces an identical payload.
	id, err := streams.Add(context.Background(), "s", map[string]string{"zeta": "1", "alpha": "2", "mu": "3"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if id != "1700000000000-0" {
		t.Fatalf("expected the returned id, got %q", id)
	}
	last := server.LastCommand()
	want := []string{"XADD", "s", "*", "alpha", "2", "mu", "3", "zeta", "1"}
	if strings.Join(last, "|") != strings.Join(want, "|") {
		t.Fatalf("expected %v, got %#v", want, last)
	}
}

func TestStreamsAddRejectsAMissingId(t *testing.T) {
	// A null reply means the entry was not stored, which must not be reported as success.
	server := newFakeRedis(t, func([]string) string { return replyNil })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if _, err := streams.Add(context.Background(), "s", map[string]string{"k": "v"}); err == nil {
		t.Fatal("expected a missing id to be reported")
	}
}

func TestStreamsReadGroupTreatsANullArrayAsAnEmptyBatch(t *testing.T) {
	// This is what Redis answers when a BLOCK expires: the normal case for an idle
	// stream, not an error.
	server := newFakeRedis(t, func([]string) string { return "*-1\r\n" })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	deliveries, err := streams.ReadGroup(context.Background(), ReadGroupOptions{
		Stream: "s", Group: "g", Consumer: "c", Count: 5, Block: time.Second,
	})
	if err != nil {
		t.Fatalf("expected no error on a block timeout, got %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected an empty batch, got %d", len(deliveries))
	}
}

func TestStreamsReadGroupFramesBlockAndNoAck(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return "*-1\r\n" })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if _, err := streams.ReadGroup(context.Background(), ReadGroupOptions{
		Stream: "s", Group: "g", Consumer: "c", Count: 7, Block: 1500 * time.Millisecond, NoAck: true,
	}); err != nil {
		t.Fatalf("read group: %v", err)
	}
	last := server.LastCommand()
	want := []string{"XREADGROUP", "GROUP", "g", "c", "COUNT", "7", "BLOCK", "1500", "NOACK", "STREAMS", "s", ">"}
	if strings.Join(last, "|") != strings.Join(want, "|") {
		t.Fatalf("expected %v, got %#v", want, last)
	}
}

func TestStreamsReadGroupRejectsMalformedReplies(t *testing.T) {
	tests := []struct {
		name  string
		reply string
	}{
		{"entry is not a pair", arrayReply(arrayReply(bulkReply("s"), bulkReply("only-one")))},
		{"fields have an odd length", arrayReply(arrayReply(bulkReply("s"), arrayReply(bulkReply("1-1"), arrayReply(bulkReply("dangling")))))},
		{"field value is not a string", arrayReply(arrayReply(bulkReply("s"), arrayReply(bulkReply("1-1"), arrayReply(bulkReply("f"), integerReply(1)))))},
		{"stream entry is not an array", arrayReply(arrayReply(bulkReply("s"), integerReply(1)))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newFakeRedis(t, func([]string) string { return test.reply })
			defer server.Close()
			streams := newTestStreamsClient(t, server)

			// A malformed reply must never be silently decoded into a partial event.
			if _, err := streams.ReadGroup(context.Background(), ReadGroupOptions{
				Stream: "s", Group: "g", Consumer: "c", Count: 1,
			}); err == nil {
				t.Fatal("expected a protocol error")
			}
		})
	}
}

func TestStreamsReadGroupDecodesARecord(t *testing.T) {
	entry := arrayReply(bulkReply("1-1"), arrayReply(bulkReply("event_id"), bulkReply("evt_01"), bulkReply("event_type"), bulkReply("CHARGE_STARTED")))
	reply := arrayReply(arrayReply(bulkReply("ncs:stream:charge-event"), arrayReply(entry)))

	server := newFakeRedis(t, func([]string) string { return reply })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	deliveries, err := streams.ReadGroup(context.Background(), ReadGroupOptions{
		Stream: "ncs:stream:charge-event", Group: "g", Consumer: "c1", Count: 1,
	})
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	got := deliveries[0]
	if got.ID != "1-1" || got.Stream != "ncs:stream:charge-event" || got.Consumer != "c1" {
		t.Fatalf("unexpected delivery coordinates %+v", got)
	}
	if got.Values["event_id"] != "evt_01" || got.Values["event_type"] != "CHARGE_STARTED" {
		t.Fatalf("unexpected fields %#v", got.Values)
	}
	if got.DeliveryCount != 1 {
		t.Fatalf("a freshly read entry has been delivered once, got %d", got.DeliveryCount)
	}
}

func TestStreamsReadGroupMapsNoGroup(t *testing.T) {
	server := newFakeRedis(t, func([]string) string {
		return errorReply("NOGROUP No such consumer group 'g' for key name 's'")
	})
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	_, err := streams.ReadGroup(context.Background(), ReadGroupOptions{Stream: "s", Group: "g", Consumer: "c", Count: 1})
	if !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing, got %v", err)
	}
}

func TestStreamsPendingDecodesTheExtendedForm(t *testing.T) {
	// The extended XPENDING form is what supplies idle time and delivery count, which the
	// retry budget depends on.
	reply := arrayReply(arrayReply(
		bulkReply("1-1"), bulkReply("c1"), integerReply(1500), integerReply(3),
	))
	server := newFakeRedis(t, func([]string) string { return reply })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	pending, err := streams.Pending(context.Background(), "s", "g")
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending entry, got %d", len(pending))
	}
	got := pending[0]
	if got.ID != "1-1" || got.Consumer != "c1" {
		t.Fatalf("unexpected pending entry %+v", got)
	}
	if got.Idle != 1500*time.Millisecond {
		t.Fatalf("expected a 1.5s idle, got %s", got.Idle)
	}
	if got.DeliveryCount != 3 {
		t.Fatalf("expected delivery count 3, got %d", got.DeliveryCount)
	}

	last := server.LastCommand()
	if last[3] != "-" || last[4] != "+" {
		t.Fatalf("expected the extended range form, got %#v", last)
	}
}

func TestStreamsPendingRejectsTheSummaryForm(t *testing.T) {
	// The summary form lacks the per-entry idle time and delivery count, so decoding it
	// as if it were the extended form would silently invent retry state.
	reply := arrayReply(integerReply(2), bulkReply("1-1"), bulkReply("1-2"), arrayReply())
	server := newFakeRedis(t, func([]string) string { return reply })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	if _, err := streams.Pending(context.Background(), "s", "g"); err == nil {
		t.Fatal("expected the summary form to be rejected")
	}
}

func TestStreamsClaimSkipsAnEmptyIDList(t *testing.T) {
	server := echoServer(t)
	streams := newTestStreamsClient(t, server)

	deliveries, err := streams.Claim(context.Background(), "s", "g", "c", nil, time.Minute)
	if err != nil || deliveries != nil {
		t.Fatalf("expected a no-op, got %v and %v", deliveries, err)
	}
	if len(server.Commands()) != 0 {
		t.Fatal("expected no command to be sent for an empty id list")
	}
}

func TestStreamsClaimFramesTheCommandAndFillsDeliveryCounts(t *testing.T) {
	claimReply := arrayReply(arrayReply(bulkReply("1-1"), arrayReply(bulkReply("event_id"), bulkReply("evt_01"))))
	pendingReply := arrayReply(arrayReply(
		bulkReply("1-1"), bulkReply("recovery"), integerReply(80), integerReply(4),
	))

	server := newFakeRedis(t, func(args []string) string {
		if strings.EqualFold(args[0], "XCLAIM") {
			return claimReply
		}
		return pendingReply
	})
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	deliveries, err := streams.Claim(context.Background(), "s", "g", "recovery", []string{"1-1"}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	// XCLAIM's reply omits the delivery count, so it must come from the pending scan or a
	// poison message would retry forever.
	if deliveries[0].DeliveryCount != 4 {
		t.Fatalf("expected the delivery count from the pending scan, got %d", deliveries[0].DeliveryCount)
	}
	if deliveries[0].Consumer != "recovery" {
		t.Fatalf("expected consumer recovery, got %s", deliveries[0].Consumer)
	}

	commands := server.Commands()
	if len(commands) != 2 {
		t.Fatalf("expected XCLAIM then XPENDING, got %#v", commands)
	}
	claim := commands[0]
	want := []string{"XCLAIM", "s", "g", "recovery", "50", "1-1"}
	if strings.Join(claim, "|") != strings.Join(want, "|") {
		t.Fatalf("expected %v, got %#v", want, claim)
	}
}

func TestStreamsClaimMapsNoGroup(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return errorReply("NOGROUP no such key") })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	_, err := streams.Claim(context.Background(), "s", "g", "c", []string{"1-1"}, 0)
	if !errors.Is(err, ErrGroupMissing) {
		t.Fatalf("expected ErrGroupMissing, got %v", err)
	}
}

func TestStreamsClaimRejectsAMissingConsumer(t *testing.T) {
	server := echoServer(t)
	streams := newTestStreamsClient(t, server)

	if _, err := streams.Claim(context.Background(), "s", "g", "", []string{"1-1"}, time.Minute); err == nil {
		t.Fatal("expected a missing consumer to be rejected")
	}
}

func TestStreamsAckSkipsAnEmptyIDList(t *testing.T) {
	server := echoServer(t)
	streams := newTestStreamsClient(t, server)

	acked, err := streams.Ack(context.Background(), "s", "g")
	if err != nil || acked != 0 {
		t.Fatalf("expected a no-op, got %d and %v", acked, err)
	}
	if len(server.Commands()) != 0 {
		t.Fatal("expected no command to be sent")
	}
}

func TestStreamsAckFramesEveryID(t *testing.T) {
	server := newFakeRedis(t, func([]string) string { return integerReply(2) })
	defer server.Close()
	streams := newTestStreamsClient(t, server)

	acked, err := streams.Ack(context.Background(), "s", "g", "1-1", "1-2")
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if acked != 2 {
		t.Fatalf("expected 2 acks, got %d", acked)
	}
	last := server.LastCommand()
	want := []string{"XACK", "s", "g", "1-1", "1-2"}
	if strings.Join(last, "|") != strings.Join(want, "|") {
		t.Fatalf("expected %v, got %#v", want, last)
	}
}

func TestStreamsCloseClosesTheSharedPool(t *testing.T) {
	server := echoServer(t)
	streams := newTestStreamsClient(t, server)

	if err := streams.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := streams.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := streams.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestNewStreamsClientValidatesConfig(t *testing.T) {
	config := DefaultConnConfig()
	config.Address = ""
	if _, err := NewStreamsClient(config); err == nil {
		t.Fatal("expected an invalid configuration to be rejected")
	}
}

func TestSortedKeysIsDeterministic(t *testing.T) {
	values := map[string]string{"b": "2", "a": "1", "d": "4", "c": "3"}
	want := []string{"a", "b", "c", "d"}
	for i := 0; i < 10; i++ {
		got := sortedKeys(values)
		if strings.Join(got, "") != strings.Join(want, "") {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if len(sortedKeys(nil)) != 0 {
		t.Fatal("expected an empty result for no fields")
	}
}
