package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/heguangV/charging-station-platform/backend/internal/event"
	redisrepo "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// OutboxSource is intentionally re-exported from the worker package so the
// publisher's dependency is obvious to callers. The event package owns the
// storage-neutral contract.
type OutboxSource = event.OutboxSource

// StreamWriter is the smallest Redis boundary required by Publisher. It is
// narrower than redisrepo.StreamClient so the publisher cannot accidentally
// depend on consumer-group behavior.
type StreamWriter interface {
	Add(context.Context, string, map[string]string) (string, error)
}

// OutboxPublisher is the application-facing publisher contract. This module
// only defines the contract and orchestration; it does not connect to a real
// PostgreSQL database or Redis server.
type OutboxPublisher interface {
	PublishOnce(context.Context) (PublishResult, error)
}

type PublisherConfig struct {
	BatchSize int
}

func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{BatchSize: 100}
}

type PublishResult struct {
	Read      int
	Published int
}

// Publisher implements the outbox half of an at-least-once delivery flow:
// read unpublished rows, append canonical fields to a Stream, then mark the
// row published. If append or marking fails, the row is left unpublished for
// a later retry. A process crash between append and marking can create a
// duplicate Stream entry; exactly-once requires an idempotent storage adapter
// or consumer and is deliberately outside this interface.
type Publisher struct {
	source OutboxSource
	writer StreamWriter
	config PublisherConfig
	mu     sync.Mutex
}

func NewPublisher(source OutboxSource, writer StreamWriter, config PublisherConfig) (*Publisher, error) {
	if source == nil {
		return nil, fmt.Errorf("outbox source is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("stream writer is required")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultPublisherConfig().BatchSize
	}
	return &Publisher{source: source, writer: writer, config: config}, nil
}

// PublishOnce publishes at most one source batch. Calls on the same Publisher
// are serialized to prevent two in-process loops from publishing the same
// unpublished row concurrently. Cross-process safety remains the source
// adapter's responsibility.
func (p *Publisher) PublishOnce(ctx context.Context) (PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return PublishResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	records, err := p.source.ListUnpublished(ctx, p.config.BatchSize)
	if err != nil {
		return PublishResult{}, fmt.Errorf("list unpublished outbox records: %w", err)
	}
	result := PublishResult{Read: len(records)}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if record.ID == "" {
			return result, fmt.Errorf("publish outbox record: record id is required")
		}
		canonical, err := record.ToEvent()
		if err != nil {
			return result, err
		}
		stream, err := record.StreamName()
		if err != nil {
			return result, err
		}
		fields, err := canonical.Fields()
		if err != nil {
			return result, fmt.Errorf("build stream fields for outbox record %s: %w", record.ID, err)
		}
		if _, err := p.writer.Add(ctx, stream, fields); err != nil {
			return result, fmt.Errorf("publish outbox record %s to %s: %w", record.ID, stream, err)
		}
		if err := p.source.MarkPublished(ctx, record.ID, time.Now().UTC()); err != nil {
			return result, fmt.Errorf("mark outbox record %s published: %w", record.ID, err)
		}
		result.Published++
	}
	return result, nil
}

// Ensure the existing Redis Streams abstraction remains a valid writer when a
// real adapter is added later.
var _ StreamWriter = (redisrepo.StreamClient)(nil)
var _ OutboxPublisher = (*Publisher)(nil)
