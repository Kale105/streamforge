// Package transport defines broker-neutral event delivery contracts.
//
// Delivery is at-least-once: a handler can run again after a crash or failed
// acknowledgement. Ordering is preserved only for records with the same key
// in one partition; handlers must make durable work idempotent.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

var (
	ErrInvalidTopic        = errors.New("invalid transport topic")
	ErrMissingPartitionKey = errors.New("transport partition key is required")
	ErrMessageTooLarge     = errors.New("transport message exceeds maximum size")
)
var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

type Envelope struct {
	Topic        string
	PartitionKey string
	Event        event.Event
}

func ValidateTopic(topic string) error {
	if !topicName.MatchString(topic) || topic == "." || topic == ".." || len(topic) >= 2 && topic[:2] == "__" {
		return fmt.Errorf("%w: %q", ErrInvalidTopic, topic)
	}
	return nil
}
func (e Envelope) Validate(maxBytes int) error {
	if err := ValidateTopic(e.Topic); err != nil {
		return err
	}
	if e.PartitionKey == "" {
		return ErrMissingPartitionKey
	}
	if len(e.PartitionKey) > 1024 {
		return fmt.Errorf("%w: partition key", ErrMessageTooLarge)
	}
	if err := e.Event.Validate(); err != nil {
		return err
	}
	b, err := MarshalCanonicalEvent(e.Event)
	if err != nil {
		return err
	}
	if maxBytes > 0 && len(b) > maxBytes {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(b), maxBytes)
	}
	return nil
}

// MarshalCanonicalEvent normalizes JSON object-key ordering before encoding the fixed event envelope.
func MarshalCanonicalEvent(e event.Event) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	var data any
	decoder := json.NewDecoder(bytes.NewReader(e.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("decode event data: %w", err)
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("canonicalize event data: %w", err)
	}
	e.Data = b
	return json.Marshal(e)
}

type Publisher interface {
	Publish(context.Context, Envelope) error
	Close() error
}
type Handler func(context.Context, Envelope) error
type Consumer interface {
	Run(context.Context, Handler) error
	Close() error
}
type Limits struct {
	MaxMessageBytes int
	SendTimeout     time.Duration
	FetchMaxBytes   int
	FetchMaxWait    time.Duration
	MaxInFlight     int
}

func (l Limits) Validate() error {
	if l.MaxMessageBytes <= 0 || l.SendTimeout <= 0 || l.FetchMaxBytes <= 0 || l.FetchMaxWait <= 0 || l.MaxInFlight <= 0 {
		return errors.New("transport limits must be positive")
	}
	if l.FetchMaxBytes < l.MaxMessageBytes {
		return errors.New("fetch maximum must cover one message")
	}
	return nil
}
