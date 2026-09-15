package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

type memoryOutboxSource struct {
	records   []event.OutboxRecord
	marked    map[string]time.Time
	markErr   error
	listErr   error
	markCalls int
}

func (s *memoryOutboxSource) ListUnpublished(_ context.Context, limit int) ([]event.OutboxRecord, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	result := make([]event.OutboxRecord, 0, len(s.records))
	for _, record := range s.records {
		if _, ok := s.marked[record.ID]; ok {
			continue
		}
		result = append(result, record)
		if limit > 0 && len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *memoryOutboxSource) MarkPublished(_ context.Context, id string, publishedAt time.Time) error {
	s.markCalls++
	if s.markErr != nil {
		return s.markErr
	}
	if s.marked == nil {
		s.marked = make(map[string]time.Time)
	}
	s.marked[id] = publishedAt
	return nil
}

func memoryOutboxRecord(id string) event.OutboxRecord {
	return event.OutboxRecord{
		ID:            id,
		EventID:       "evt_" + id,
		EventType:     event.OrderCreated,
		AggregateType: "order",
		AggregateID:   "order_" + id,
		OccurredAt:    time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC),
		TraceID:       "trace_" + id,
		Payload:       []byte(`{"amount_cent":100}`),
	}
}

func TestPublisherPublishesAndMarksOnlyAfterSuccess(t *testing.T) {
	ctx := context.Background()
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	source := &memoryOutboxSource{records: []event.OutboxRecord{memoryOutboxRecord("01")}, marked: make(map[string]time.Time)}
	publisher, err := NewPublisher(source, stream, PublisherConfig{BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOnce(ctx)
	if err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}
	if want := (PublishResult{Read: 1, Published: 1}); !reflect.DeepEqual(result, want) {
		t.Fatalf("PublishOnce() = %#v, want %#v", result, want)
	}
	records, err := stream.Records(ctx, event.StreamOrderEvent)
	if err != nil || len(records) != 1 {
		t.Fatalf("stream records = %#v, error = %v", records, err)
	}
	got, err := event.FromFields(records[0].Values)
	if err != nil {
		t.Fatalf("FromFields() error = %v", err)
	}
	if got.EventID != "evt_01" || got.AggregateID != "order_01" {
		t.Fatalf("published event = %#v", got)
	}
	if len(source.marked) != 1 || source.markCalls != 1 {
		t.Fatalf("marked records = %#v, calls = %d", source.marked, source.markCalls)
	}
}

// failingWriter and MemoryStream are test doubles. Tests never connect to
// real PostgreSQL or Redis services.
type failingWriter struct {
	err   error
	calls int
}

func (w *failingWriter) Add(context.Context, string, map[string]string) (string, error) {
	w.calls++
	return "", w.err
}

func TestPublisherLeavesRecordUnpublishedWhenStreamAppendFails(t *testing.T) {
	appendErr := errors.New("redis unavailable")
	source := &memoryOutboxSource{records: []event.OutboxRecord{memoryOutboxRecord("02")}, marked: make(map[string]time.Time)}
	writer := &failingWriter{err: appendErr}
	publisher, err := NewPublisher(source, writer, DefaultPublisherConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOnce(context.Background())
	if !errors.Is(err, appendErr) {
		t.Fatalf("PublishOnce() error = %v, want %v", err, appendErr)
	}
	if result.Published != 0 || len(source.marked) != 0 || source.markCalls != 0 {
		t.Fatalf("failed publish changed source/result: %#v, marked=%#v, calls=%d", result, source.marked, source.markCalls)
	}
	if writer.calls != 1 {
		t.Fatalf("writer calls = %d, want 1", writer.calls)
	}
}

func TestPublisherLeavesRecordUnpublishedWhenMarkFails(t *testing.T) {
	markErr := errors.New("database unavailable")
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	source := &memoryOutboxSource{records: []event.OutboxRecord{memoryOutboxRecord("03")}, marked: make(map[string]time.Time), markErr: markErr}
	publisher, err := NewPublisher(source, stream, DefaultPublisherConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOnce(context.Background())
	if !errors.Is(err, markErr) {
		t.Fatalf("PublishOnce() error = %v, want %v", err, markErr)
	}
	if result.Published != 0 || len(source.marked) != 0 {
		t.Fatalf("mark failure result/source = %#v/%#v", result, source.marked)
	}
	records, err := stream.Records(context.Background(), event.StreamOrderEvent)
	if err != nil || len(records) != 1 {
		t.Fatalf("stream records = %#v, error = %v", records, err)
	}
}

func TestPublisherRepeatedCallIsIdempotentAfterMark(t *testing.T) {
	stream := redisrepo.NewMemoryStream()
	defer stream.Close()
	source := &memoryOutboxSource{records: []event.OutboxRecord{memoryOutboxRecord("04")}, marked: make(map[string]time.Time)}
	publisher, err := NewPublisher(source, stream, DefaultPublisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	first, err := publisher.PublishOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := publisher.PublishOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Published != 1 || second.Read != 0 || second.Published != 0 {
		t.Fatalf("repeated publish results = %#v then %#v", first, second)
	}
	records, err := stream.Records(context.Background(), event.StreamOrderEvent)
	if err != nil || len(records) != 1 {
		t.Fatalf("stream records after repeated call = %#v, error = %v", records, err)
	}
}
