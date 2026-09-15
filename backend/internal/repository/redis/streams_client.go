package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StreamsClient is the production StreamClient: a real Redis Streams
// implementation over the pooled RESP2 client in client.go.
//
// It shares the pool with the capability layer, so one process holds one bounded
// set of connections regardless of how many streams it consumes. Field values are
// transported as the flat field/value array Redis uses; decodeStreamFields is the
// only place that shape is interpreted.
type StreamsClient struct {
	client *Client
}

var _ StreamClient = (*StreamsClient)(nil)

// NewStreamsClient opens a pooled Streams client. Like Client it dials lazily, so
// a successful call does not prove Redis is reachable.
func NewStreamsClient(config ConnConfig) (*StreamsClient, error) {
	client, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	return &StreamsClient{client: client}, nil
}

// NewStreamsClientOver shares an existing pooled client. NewCapabilities uses it so
// the capability layer and the Streams layer cannot drift into separate pools.
func NewStreamsClientOver(client *Client) *StreamsClient {
	return &StreamsClient{client: client}
}

// Stats reports pool occupancy, for ops and tests.
func (s *StreamsClient) Stats() PoolStats { return s.client.Stats() }

func (s *StreamsClient) Ping(ctx context.Context) error { return s.client.Ping(ctx) }

// EnsureGroup creates the consumer group and the stream when needed.
//
// The command is idempotent by design: Redis answers BUSYGROUP when the group
// already exists, which is the normal case on restart, so it is treated as
// success rather than an error. MKSTREAM allows a worker to start before the first
// event is ever published.
func (s *StreamsClient) EnsureGroup(ctx context.Context, stream, group, startID string) error {
	if strings.TrimSpace(stream) == "" || strings.TrimSpace(group) == "" {
		return fmt.Errorf("stream and group are required")
	}
	if strings.TrimSpace(startID) == "" {
		startID = "0-0"
	}
	reply, err := s.client.do(ctx, "XGROUP", "CREATE", stream, group, startID, "MKSTREAM")
	if err != nil {
		if isBusyGroup(err) {
			return nil
		}
		return fmt.Errorf("create consumer group %s on %s: %w", group, stream, err)
	}
	// A server normally answers +OK. A bulk "OK" is accepted for proxies that
	// re-encode the reply.
	if _, err := replyString(reply, "XGROUP CREATE"); err != nil {
		return err
	}
	return nil
}

// isBusyGroup reports the "group already exists" reply, which is not a failure.
func isBusyGroup(err error) bool {
	var serverErr *serverError
	if !errors.As(err, &serverErr) {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(serverErr.message), "BUSYGROUP")
}

// Add appends a record and returns its generated stream ID.
func (s *StreamsClient) Add(ctx context.Context, stream string, values map[string]string) (string, error) {
	if strings.TrimSpace(stream) == "" {
		return "", fmt.Errorf("stream is required")
	}
	if len(values) == 0 {
		return "", fmt.Errorf("add to %s: at least one field is required", stream)
	}
	args := make([]string, 0, 4+len(values)*2)
	args = append(args, "XADD", stream, "*")
	// Field order must be deterministic so a duplicate event produces an identical
	// payload, which matters when a stream is inspected or replayed.
	for _, field := range sortedKeys(values) {
		args = append(args, field, values[field])
	}
	reply, err := s.client.do(ctx, args...)
	if err != nil {
		return "", err
	}
	if reply == nil {
		return "", fmt.Errorf("add to %s: redis returned no id", stream)
	}
	id, err := replyString(reply, "XADD")
	if err != nil {
		return "", err
	}
	return id, nil
}

// ReadGroup reads new messages for a consumer.
//
// A blocking read must be allowed to outlive ReadTimeout, so the socket deadline is
// raised to cover the requested block plus the configured timeout. Redis answers a
// null array when the block expires, which is reported as an empty batch rather
// than an error.
func (s *StreamsClient) ReadGroup(ctx context.Context, options ReadGroupOptions) ([]Delivery, error) {
	if strings.TrimSpace(options.Stream) == "" || strings.TrimSpace(options.Group) == "" || strings.TrimSpace(options.Consumer) == "" {
		return nil, fmt.Errorf("stream, group and consumer are required")
	}
	count := options.Count
	if count <= 0 {
		count = 1
	}

	args := []string{"XREADGROUP", "GROUP", options.Group, options.Consumer, "COUNT", strconv.Itoa(count)}
	readTimeout := s.client.pool.config.ReadTimeout
	if options.Block > 0 {
		// BLOCK is expressed in milliseconds; 0 means "block forever", which this
		// adapter never requests because it would outlive any deadline.
		blockMillis := options.Block.Milliseconds()
		if blockMillis <= 0 {
			blockMillis = 1
		}
		args = append(args, "BLOCK", strconv.FormatInt(blockMillis, 10))
		readTimeout = options.Block + s.client.pool.config.ReadTimeout
	}
	if options.NoAck {
		args = append(args, "NOACK")
	}
	args = append(args, "STREAMS", options.Stream, ">")

	reply, err := s.client.doWithReadTimeout(ctx, readTimeout, args...)
	if err != nil {
		if isNoGroup(err) {
			return nil, ErrGroupMissing
		}
		return nil, fmt.Errorf("read group %s on %s: %w", options.Group, options.Stream, err)
	}
	// A null array means the block expired with no new entries.
	if reply == nil {
		return nil, nil
	}

	streams, err := replyArray(reply, "XREADGROUP")
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery, 0, count)
	for _, entry := range streams {
		pair, ok := entry.([]any)
		if !ok || len(pair) != 2 {
			return nil, &protocolError{message: "XREADGROUP expected a [stream, entries] reply"}
		}
		streamName, err := replyString(pair[0], "XREADGROUP")
		if err != nil {
			return nil, err
		}
		records, err := decodeRecordEntries(pair[1])
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			record.Stream = streamName
			deliveries = append(deliveries, Delivery{
				Record: record,
				// A newly delivered entry has been delivered once.
				Consumer:      options.Consumer,
				DeliveryCount: 1,
				DeliveredAt:   time.Now().UTC(),
			})
		}
	}
	return deliveries, nil
}

func isNoGroup(err error) bool {
	var serverErr *serverError
	if !errors.As(err, &serverErr) {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(serverErr.message), "NOGROUP")
}

// Ack acknowledges entries so they leave the pending list.
func (s *StreamsClient) Ack(ctx context.Context, stream, group string, ids ...string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := append([]string{"XACK", stream, group}, ids...)
	reply, err := s.client.do(ctx, args...)
	if err != nil {
		return 0, err
	}
	acked, err := replyInteger(reply, "XACK")
	if err != nil {
		return 0, err
	}
	return int(acked), nil
}

// pendingScanLimit caps the extended XPENDING scan. A worker only needs the
// entries it is about to claim or recover, and an unbounded scan would let a large
// backlog exhaust memory in one call.
const pendingScanLimit = 1024

// Pending lists entries delivered but not yet acknowledged.
//
// The extended form of XPENDING is used because only it reports the per-entry idle
// time and delivery count the worker's retry budget depends on.
func (s *StreamsClient) Pending(ctx context.Context, stream, group string) ([]PendingMessage, error) {
	reply, err := s.client.do(ctx, "XPENDING", stream, group, "-", "+", strconv.Itoa(pendingScanLimit))
	if err != nil {
		if isNoGroup(err) {
			return nil, ErrGroupMissing
		}
		return nil, fmt.Errorf("read pending for %s/%s: %w", stream, group, err)
	}
	if reply == nil {
		return nil, nil
	}
	entries, err := replyArray(reply, "XPENDING")
	if err != nil {
		return nil, err
	}

	messages := make([]PendingMessage, 0, len(entries))
	for _, entry := range entries {
		fields, ok := entry.([]any)
		if !ok || len(fields) != 4 {
			return nil, &protocolError{message: "XPENDING expected [id, consumer, idle, delivery-count]"}
		}
		id, err := replyString(fields[0], "XPENDING")
		if err != nil {
			return nil, err
		}
		consumer, err := replyString(fields[1], "XPENDING")
		if err != nil {
			return nil, err
		}
		idleMillis, err := replyInteger(fields[2], "XPENDING")
		if err != nil {
			return nil, err
		}
		deliveryCount, err := replyInteger(fields[3], "XPENDING")
		if err != nil {
			return nil, err
		}
		messages = append(messages, PendingMessage{
			ID:            id,
			Consumer:      consumer,
			Idle:          time.Duration(idleMillis) * time.Millisecond,
			DeliveryCount: int(deliveryCount),
		})
	}
	return messages, nil
}

// Claim takes over entries that another consumer left pending for longer than
// minIdle.
//
// XCLAIM's reply carries the entry bodies but not the delivery count, while the
// worker's retry budget needs the count that XCLAIM just incremented. One
// XPENDING scan after the claim supplies it, so a claimed entry reports the same
// number of attempts a fresh read would.
func (s *StreamsClient) Claim(ctx context.Context, stream, group, consumer string, ids []string, minIdle time.Duration) ([]Delivery, error) {
	if strings.TrimSpace(consumer) == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	minIdleMillis := minIdle.Milliseconds()
	if minIdleMillis < 0 {
		minIdleMillis = 0
	}
	args := append([]string{
		"XCLAIM", stream, group, consumer, strconv.FormatInt(minIdleMillis, 10),
	}, ids...)

	reply, err := s.client.do(ctx, args...)
	if err != nil {
		if isNoGroup(err) {
			return nil, ErrGroupMissing
		}
		return nil, fmt.Errorf("claim entries on %s/%s: %w", stream, group, err)
	}
	if reply == nil {
		return nil, nil
	}
	records, err := decodeRecordEntries(reply)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	counts, err := s.claimDeliveryCounts(ctx, stream, group)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	deliveries := make([]Delivery, 0, len(records))
	for _, record := range records {
		record.Stream = stream
		deliveries = append(deliveries, Delivery{
			Record:        record,
			Consumer:      consumer,
			DeliveryCount: counts[record.ID],
			DeliveredAt:   now,
		})
	}
	return deliveries, nil
}

// claimDeliveryCounts maps an entry ID to its current delivery count.
//
// A missing entry defaults to one, because XCLAIM only returns entries that are
// pending and therefore have been delivered at least once.
func (s *StreamsClient) claimDeliveryCounts(ctx context.Context, stream, group string) (map[string]int, error) {
	messages, err := s.Pending(ctx, stream, group)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(messages))
	for _, message := range messages {
		count := message.DeliveryCount
		if count < 1 {
			count = 1
		}
		counts[message.ID] = count
	}
	return counts, nil
}

// Close releases the shared pool. It is safe to call more than once, and the
// capability layer may close the same pool independently.
func (s *StreamsClient) Close() error { return s.client.Close() }

// decodeRecordEntries decodes the [[id, [f, v, ...]], ...] shape shared by
// XREADGROUP and XCLAIM replies.
func decodeRecordEntries(value any) ([]Record, error) {
	if value == nil {
		return nil, nil
	}
	entries, err := replyArray(value, "stream entries")
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		pair, ok := entry.([]any)
		if !ok || len(pair) != 2 {
			return nil, &protocolError{message: "stream entry expected an [id, fields] pair"}
		}
		id, err := replyString(pair[0], "stream entry")
		if err != nil {
			return nil, err
		}
		fields, err := decodeStreamFields(pair[1])
		if err != nil {
			return nil, err
		}
		records = append(records, Record{ID: id, Values: fields})
	}
	return records, nil
}

// decodeStreamFields decodes the flat field/value array Redis uses. An odd length
// is a protocol violation: silently dropping the last field would lose event data.
func decodeStreamFields(value any) (map[string]string, error) {
	items, err := replyArray(value, "stream fields")
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, &protocolError{message: fmt.Sprintf("stream fields have an odd length %d", len(items))}
	}
	fields := make(map[string]string, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		name, ok := items[i].(string)
		if !ok {
			return nil, &protocolError{message: "stream field name is not a string"}
		}
		raw, ok := items[i+1].(string)
		if !ok {
			return nil, &protocolError{message: fmt.Sprintf("stream field %q is not a string", name)}
		}
		fields[name] = raw
	}
	return fields, nil
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	// Insertion sort keeps this allocation-free for the small maps used by events
	// while still producing a deterministic order.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
