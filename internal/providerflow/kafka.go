package providerflow

import (
	"context"
	"fmt"

	"github.com/Kale105/streamforge/internal/telemetry"
	transportkafka "github.com/Kale105/streamforge/internal/transport/kafka"
)

// KafkaPublisher adapts the normalized wire contract without exposing Kafka
// types to processors.
type KafkaPublisher struct {
	Producer *transportkafka.Producer
	Topic    string
	MaxBytes int
}

func (p KafkaPublisher) Publish(ctx context.Context, m Message) error {
	if p.Producer == nil {
		return fmt.Errorf("normalized Kafka producer is required")
	}
	b, err := Marshal(m)
	if err != nil {
		return err
	}
	if err := m.Validate(p.MaxBytes); err != nil {
		return err
	}
	return p.Producer.PublishBytesWithHeaders(ctx, p.Topic, m.IdempotencyKey(), b, m.Record.UpdatedAt, telemetry.Headers(telemetry.EnsureTraceparent(ctx)))
}

// RunKafkaSink converts the Kafka byte seam back into validated messages. The
// consumer commits only after Sink.Handle confirms durable completion.
func RunKafkaSink(ctx context.Context, c *transportkafka.Consumer, sink Sink, maxBytes int) error {
	if c == nil {
		return fmt.Errorf("normalized Kafka consumer is required")
	}
	return c.RunBytes(ctx, func(ctx context.Context, key string, b []byte) error {
		return handleKafkaMessage(ctx, key, b, sink, maxBytes)
	})
}

func handleKafkaMessage(ctx context.Context, key string, body []byte, sink Sink, maxBytes int) error {
	m, err := Unmarshal(body, maxBytes)
	if err != nil {
		return err
	}
	if key != m.IdempotencyKey() {
		return fmt.Errorf("normalized Kafka key does not match message identity")
	}
	return sink.Handle(ctx, m)
}
