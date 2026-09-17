// Package redis contains the Redis/Redis Streams boundary. It deliberately
// uses application-owned types so the domain and worker packages do not depend
// on a particular Redis client library.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	ErrClosed       = errors.New("redis stream client is closed")
	ErrGroupExists  = errors.New("consumer group already exists")
	ErrGroupMissing = errors.New("consumer group does not exist")
	ErrInvalidID    = errors.New("invalid stream id")
)

type Record struct {
	Stream string
	ID     string
	Values map[string]string
}

type Delivery struct {
	Record
	Consumer      string
	DeliveryCount int
	DeliveredAt   time.Time
}

type PendingMessage struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int
}

type ReadGroupOptions struct {
	Stream   string
	Group    string
	Consumer string
	Count    int
	Block    time.Duration
	NoAck    bool
}

// StreamClient is the smallest boundary needed by publishers and workers.
// Production code can implement it with go-redis without changing callers.
type StreamClient interface {
	Ping(context.Context) error
	EnsureGroup(context.Context, string, string, string) error
	Add(context.Context, string, map[string]string) (string, error)
	ReadGroup(context.Context, ReadGroupOptions) ([]Delivery, error)
	Ack(context.Context, string, string, ...string) (int, error)
	Pending(context.Context, string, string) ([]PendingMessage, error)
	Claim(context.Context, string, string, string, []string, time.Duration) ([]Delivery, error)
	Close() error
}

type pendingState struct {
	record        Record
	consumer      string
	deliveredAt   time.Time
	deliveryCount int
}

type groupState struct {
	cursor  int
	pending map[string]*pendingState
}

// MemoryStream is a deterministic, concurrency-safe Streams implementation
// for unit tests and local worker smoke tests. It models the delivery semantics
// needed by the worker: groups, ACK, Pending and Claim.
type MemoryStream struct {
	mu      sync.Mutex
	streams map[string][]Record
	groups  map[string]*groupState
	nextSeq uint64
	changed chan struct{}
	closed  bool
}

func NewMemoryStream() *MemoryStream {
	return &MemoryStream{
		streams: make(map[string][]Record),
		groups:  make(map[string]*groupState),
		changed: make(chan struct{}),
	}
}

func (m *MemoryStream) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *MemoryStream) EnsureGroup(ctx context.Context, stream, group, startID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if stream == "" || group == "" {
		return fmt.Errorf("stream and group are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := groupKey(stream, group)
	if _, ok := m.groups[key]; ok {
		return nil
	}
	cursor, err := startCursor(m.streams[stream], startID)
	if err != nil {
		return err
	}
	m.groups[key] = &groupState{cursor: cursor, pending: make(map[string]*pendingState)}
	return nil
}

func (m *MemoryStream) Add(ctx context.Context, stream string, values map[string]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if stream == "" {
		return "", fmt.Errorf("stream is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return "", ErrClosed
	}
	m.nextSeq++
	id := fmt.Sprintf("%d-%d", time.Now().UnixMilli(), m.nextSeq)
	record := Record{Stream: stream, ID: id, Values: copyValues(values)}
	m.streams[stream] = append(m.streams[stream], record)
	m.signalLocked()
	return id, nil
}

func (m *MemoryStream) ReadGroup(ctx context.Context, options ReadGroupOptions) ([]Delivery, error) {
	if options.Stream == "" || options.Group == "" || options.Consumer == "" {
		return nil, fmt.Errorf("stream, group and consumer are required")
	}
	count := options.Count
	if count <= 0 {
		count = 1
	}
	var deadline time.Time
	if options.Block > 0 {
		deadline = time.Now().Add(options.Block)
	}
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrClosed
		}
		group := m.groups[groupKey(options.Stream, options.Group)]
		if group == nil {
			m.mu.Unlock()
			return nil, ErrGroupMissing
		}
		records := m.streams[options.Stream]
		if group.cursor < len(records) {
			limit := group.cursor + count
			if limit > len(records) {
				limit = len(records)
			}
			now := time.Now().UTC()
			result := make([]Delivery, 0, limit-group.cursor)
			for _, record := range records[group.cursor:limit] {
				if !options.NoAck {
					group.pending[record.ID] = &pendingState{record: cloneRecord(record), consumer: options.Consumer, deliveredAt: now, deliveryCount: 1}
				}
				result = append(result, Delivery{Record: cloneRecord(record), Consumer: options.Consumer, DeliveryCount: 1, DeliveredAt: now})
			}
			group.cursor = limit
			m.mu.Unlock()
			return result, nil
		}
		changed := m.changed
		m.mu.Unlock()

		if options.Block == 0 {
			return nil, nil
		}
		wait := options.Block
		if !deadline.IsZero() {
			wait = time.Until(deadline)
			if wait <= 0 {
				return nil, nil
			}
		}
		changedSignaled, err := waitForChange(ctx, changed, wait)
		if err != nil {
			return nil, err
		}
		if !changedSignaled {
			return nil, nil
		}
	}
}

func (m *MemoryStream) Ack(ctx context.Context, stream, group string, ids ...string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	state := m.groups[groupKey(stream, group)]
	if state == nil {
		return 0, ErrGroupMissing
	}
	acked := 0
	for _, id := range ids {
		if _, ok := state.pending[id]; ok {
			delete(state.pending, id)
			acked++
		}
	}
	return acked, nil
}

func (m *MemoryStream) Pending(ctx context.Context, stream, group string) ([]PendingMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	state := m.groups[groupKey(stream, group)]
	if state == nil {
		return nil, ErrGroupMissing
	}
	now := time.Now().UTC()
	result := make([]PendingMessage, 0, len(state.pending))
	for _, pending := range state.pending {
		result = append(result, PendingMessage{ID: pending.record.ID, Consumer: pending.consumer, Idle: now.Sub(pending.deliveredAt), DeliveryCount: pending.deliveryCount})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (m *MemoryStream) Claim(ctx context.Context, stream, group, consumer string, ids []string, minIdle time.Duration) ([]Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	state := m.groups[groupKey(stream, group)]
	if state == nil {
		return nil, ErrGroupMissing
	}
	now := time.Now().UTC()
	result := make([]Delivery, 0, len(ids))
	for _, id := range ids {
		pending := state.pending[id]
		if pending == nil || now.Sub(pending.deliveredAt) < minIdle {
			continue
		}
		pending.consumer = consumer
		pending.deliveredAt = now
		pending.deliveryCount++
		result = append(result, Delivery{Record: cloneRecord(pending.record), Consumer: consumer, DeliveryCount: pending.deliveryCount, DeliveredAt: now})
	}
	return result, nil
}

func (m *MemoryStream) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.signalLocked()
	return nil
}

// Records is intentionally concrete-type-only; it is useful for asserting
// dead-letter output in tests without expanding the production interface.
func (m *MemoryStream) Records(ctx context.Context, stream string) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	result := make([]Record, len(m.streams[stream]))
	for i, record := range m.streams[stream] {
		result[i] = cloneRecord(record)
	}
	return result, nil
}

func groupKey(stream, group string) string { return stream + "\x00" + group }

func startCursor(records []Record, startID string) (int, error) {
	if startID == "" || startID == "$" || startID == "0-0" {
		if startID == "$" {
			return len(records), nil
		}
		return 0, nil
	}
	for index, record := range records {
		if record.ID == startID {
			return index + 1, nil
		}
	}
	return 0, fmt.Errorf("%w: %s", ErrInvalidID, startID)
}

func waitForChange(ctx context.Context, changed <-chan struct{}, block time.Duration) (bool, error) {
	if block < 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-changed:
			return true, nil
		}
	}
	timer := time.NewTimer(block)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-changed:
		return true, nil
	case <-timer.C:
		return false, nil
	}
}

func (m *MemoryStream) signalLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func cloneRecord(record Record) Record {
	return Record{Stream: record.Stream, ID: record.ID, Values: copyValues(record.Values)}
}

func copyValues(values map[string]string) map[string]string {
	copyOf := make(map[string]string, len(values))
	for key, value := range values {
		copyOf[key] = value
	}
	return copyOf
}

// ParseDeliveryCount is kept for adapters that receive Redis metadata as a
// string. It defaults to one because a newly delivered message has count one.
func ParseDeliveryCount(value string) int {
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 {
		return 1
	}
	return count
}
