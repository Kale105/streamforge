// Package kafka adapts the broker-neutral transport contracts to Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/transport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrRetryable       = errors.New("retryable kafka failure")
	ErrProducerFenced  = errors.New("Kafka producer is fenced")
	ErrInvalidHeader   = errors.New("invalid Kafka header")
	ErrHeadersTooLarge = errors.New("Kafka headers exceed transport limit")
)

const (
	// MaxHeaderCount and MaxHeaderBytes deliberately bound metadata independently
	// of the message body. They prevent an otherwise small record from consuming
	// unbounded broker/client memory through headers.
	MaxHeaderCount = 64
	MaxHeaderBytes = 8 << 10
)

// Header is transport metadata attached to a raw Kafka record. Values are
// opaque bytes because the transport layer must not interpret them.
type Header struct {
	Key   string
	Value []byte
}

// RawRecord is the lossless handoff from Kafka to a caller. It deliberately
// contains bytes rather than a decoded event, so a caller can route malformed
// or unknown contracts to a DLQ without the consumer committing the record.
// All byte slices are owned by this value and safe for the handler to retain.
type RawRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

// RecordHandler handles one raw Kafka record. Returning nil is the sole
// acknowledgement signal: Consumer.RunRecords commits only then.
type RecordHandler func(context.Context, RawRecord) error

// ProducerFencedError is fatal for this producer. A successor using the same
// transactional ID has a newer epoch, so retrying this client is unsafe.
type ProducerFencedError struct{ Err error }

func (e *ProducerFencedError) Error() string        { return fmt.Sprintf("%v: %v", ErrProducerFenced, e.Err) }
func (e *ProducerFencedError) Unwrap() error        { return e.Err }
func (e *ProducerFencedError) Is(target error) bool { return target == ErrProducerFenced }

// IsRetryable classifies transient network failures. Invalid data and cancelled
// work are fatal to this call and must not be silently retried.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ne net.Error
	return kerr.IsRetriable(err) || errors.As(err, &ne) && (ne.Timeout() || ne.Temporary())
}

type ProducerConfig struct {
	Brokers         []string
	Limits          transport.Limits
	TransactionalID string
	Security        SecurityConfig
}

func (c ProducerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return errors.New("Kafka broker is required")
		}
	}
	if c.TransactionalID != "" && strings.TrimSpace(c.TransactionalID) == "" {
		return errors.New("Kafka transactional ID must not be blank")
	}
	if _, err := c.Security.clientOptions(); err != nil {
		return err
	}
	return nil
}

type Producer struct {
	client        *kgo.Client
	limits        transport.Limits
	transactional bool
	begin         func() error
	produce       func(context.Context, *kgo.Record) error
	end           func(context.Context, kgo.TransactionEndTry) error
}

// NewProducer uses Kafka's idempotent writer and requires all in-sync replicas
// to acknowledge a record. If TransactionalID is set, every raw publish is one
// bounded transaction. The ID must be stable for an assignment: a successor
// using that same ID fences the old producer at the broker.
func NewProducer(c ProducerConfig) (*Producer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	security, err := c.Security.clientOptions()
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchMaxBytes(int32(c.Limits.MaxMessageBytes))}
	opts = append(opts, security.kgoOptions()...)
	if c.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(c.TransactionalID))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	p := &Producer{client: cl, limits: c.Limits, transactional: c.TransactionalID != ""}
	p.begin = cl.BeginTransaction
	p.produce = func(ctx context.Context, r *kgo.Record) error { return cl.ProduceSync(ctx, r).FirstErr() }
	p.end = cl.EndTransaction
	return p, nil
}
func (p *Producer) Publish(ctx context.Context, e transport.Envelope) error {
	if err := e.Validate(p.limits.MaxMessageBytes); err != nil {
		return err
	}
	b, err := transport.MarshalCanonicalEvent(e.Event)
	if err != nil {
		return err
	}
	return p.PublishBytes(ctx, e.Topic, e.PartitionKey, b, e.Event.Time)
}

// PublishBytes is the codec seam for non-event contracts. Callers remain
// responsible for supplying deterministic bytes; Kafka details stay here.
func (p *Producer) PublishBytes(ctx context.Context, topic, key string, value []byte, timestamp time.Time) error {
	return p.PublishBytesWithHeaders(ctx, topic, key, value, timestamp, nil)
}

// PublishOpaque preserves a broker record's key verbatim, including an empty
// key. It is reserved for DLQ replay: ordinary StreamForge contracts must use
// PublishBytes and retain their mandatory partition key.
func (p *Producer) PublishOpaque(ctx context.Context, topic string, key, value []byte, timestamp time.Time) error {
	return p.PublishOpaqueWithHeaders(ctx, topic, key, value, timestamp, nil)
}

// PublishOpaqueWithHeaders is the replay counterpart of
// PublishBytesWithHeaders. It preserves an empty original key while retaining
// bounded correlation headers on the replayed raw record.
func (p *Producer) PublishOpaqueWithHeaders(ctx context.Context, topic string, key, value []byte, timestamp time.Time, headers []Header) error {
	if err := transport.ValidateTopic(topic); err != nil {
		return err
	}
	if len(key) > 1024 || len(value) > p.limits.MaxMessageBytes {
		return transport.ErrMessageTooLarge
	}
	if err := validateHeaders(headers); err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...), Timestamp: timestamp, Headers: kafkaHeaders(headers)}
	if p.transactional {
		return p.publishTransaction(ctx, record)
	}
	parent := ctx
	child, cancel := context.WithTimeout(parent, p.limits.SendTimeout)
	defer cancel()
	err := p.produce(child, record)
	return classifyPublish(parent, child.Err(), err)
}

// PublishBytesWithHeaders publishes opaque bytes with bounded, opaque Kafka
// headers. PublishBytes remains the convenient no-header API for existing
// callers. Header validation happens before a transaction begins.
func (p *Producer) PublishBytesWithHeaders(ctx context.Context, topic, key string, value []byte, timestamp time.Time, headers []Header) error {
	if err := transport.ValidateTopic(topic); err != nil {
		return err
	}
	if key == "" {
		return transport.ErrMissingPartitionKey
	}
	if len(key) > 1024 || len(value) > p.limits.MaxMessageBytes {
		return transport.ErrMessageTooLarge
	}
	if err := validateHeaders(headers); err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: append([]byte(nil), value...), Timestamp: timestamp, Headers: kafkaHeaders(headers)}
	if p.transactional {
		return p.publishTransaction(ctx, record)
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(parent, p.limits.SendTimeout)
	defer cancel()
	err := p.produce(ctx, record)
	return classifyPublish(parent, ctx.Err(), err)
}

func validateHeaders(headers []Header) error {
	if len(headers) > MaxHeaderCount {
		return ErrHeadersTooLarge
	}
	total := 0
	for _, header := range headers {
		if strings.TrimSpace(header.Key) == "" || len(header.Key) > 249 {
			return fmt.Errorf("%w: key", ErrInvalidHeader)
		}
		total += len(header.Key) + len(header.Value)
		if total > MaxHeaderBytes {
			return ErrHeadersTooLarge
		}
	}
	return nil
}

func kafkaHeaders(headers []Header) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	result := make([]kgo.RecordHeader, len(headers))
	for i, header := range headers {
		result[i] = kgo.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return result
}

func (p *Producer) publishTransaction(parent context.Context, record *kgo.Record) error {
	if err := p.begin(); err != nil {
		return classifyPublish(parent, nil, err)
	}
	ctx, cancel := context.WithTimeout(parent, p.limits.SendTimeout)
	err := p.produce(ctx, record)
	childErr := ctx.Err()
	cancel()
	if err != nil {
		p.abort()
		return classifyPublish(parent, childErr, err)
	}
	ctx, cancel = context.WithTimeout(parent, p.limits.SendTimeout)
	err = p.end(ctx, kgo.TryCommit)
	childErr = ctx.Err()
	cancel()
	if err != nil {
		p.abort()
	}
	return classifyPublish(parent, childErr, err)
}

func (p *Producer) abort() {
	ctx, cancel := context.WithTimeout(context.Background(), p.limits.SendTimeout)
	defer cancel()
	_ = p.end(ctx, kgo.TryAbort)
}

func classifyPublish(parent context.Context, childErr, err error) error {
	if err == nil {
		return nil
	}
	// A caller cancellation is a clean stop. A deadline created for the send or
	// transaction commit is transient and is allowed to be retried.
	if parent.Err() != nil {
		return parent.Err()
	}
	if errors.Is(err, kerr.ProducerFenced) {
		return &ProducerFencedError{Err: err}
	}
	if errors.Is(childErr, context.DeadlineExceeded) || IsRetryable(err) {
		return fmt.Errorf("%w: %w", ErrRetryable, err)
	}
	return err
}
func (p *Producer) Close() error { p.client.Close(); return nil }

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	// Topics optionally supplies more than one input topic. Topic remains for
	// backwards compatibility; when Topics is supplied it is the complete,
	// explicit subscription (used by a processor's raw + revision replay flow).
	Topics            []string
	Group             string
	Limits            transport.Limits
	StartFromEarliest bool
	Security          SecurityConfig
}

func (c ConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 || strings.TrimSpace(c.Group) == "" {
		return errors.New("Kafka brokers and consumer group are required")
	}
	topics := c.topics()
	if len(topics) == 0 {
		return errors.New("at least one Kafka topic is required")
	}
	for _, topic := range topics {
		if err := transport.ValidateTopic(topic); err != nil {
			return err
		}
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	_, err := c.Security.clientOptions()
	return err
}

type Consumer struct {
	client *kgo.Client
	topic  string
	limits transport.Limits
}

func NewConsumer(c ConsumerConfig) (*Consumer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	topics := c.topics()
	security, err := c.Security.clientOptions()
	if err != nil {
		return nil, err
	}
	options := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ConsumeTopics(topics...), kgo.ConsumerGroup(c.Group), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(int32(c.Limits.FetchMaxBytes)), kgo.FetchMaxPartitionBytes(int32(c.Limits.MaxMessageBytes)), kgo.FetchMaxWait(c.Limits.FetchMaxWait), kgo.MaxConcurrentFetches(c.Limits.MaxInFlight)}
	options = append(options, security.kgoOptions()...)
	if c.StartFromEarliest {
		options = append(options, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}
	cl, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &Consumer{cl, topics[0], c.Limits}, nil
}

func (c ConsumerConfig) topics() []string {
	if len(c.Topics) > 0 {
		return append([]string(nil), c.Topics...)
	}
	if c.Topic == "" {
		return nil
	}
	return []string{c.Topic}
}

// Run decodes StreamForge event envelopes on top of RunRecords. New broker
// integrations should prefer RunRecords when they need control over malformed
// input or a contract other than an event envelope.
func (c *Consumer) Run(ctx context.Context, h transport.Handler) error {
	if h == nil {
		return errors.New("transport handler is required")
	}
	return c.RunRecords(ctx, func(ctx context.Context, record RawRecord) error {
		e, err := decode(record.Topic, record.Key, record.Value)
		if err != nil {
			return err
		}
		return h(ctx, e)
	})
}

// RunRecords polls one raw record at a time. Blocking rebalances prevents a
// partition hand-off while the handler is active. A nil handler result is the
// only path to a synchronous offset commit; decode is intentionally absent so
// callers can DLQ malformed bytes themselves.
func (c *Consumer) RunRecords(ctx context.Context, h RecordHandler) error {
	if h == nil {
		return errors.New("Kafka record handler is required")
	}
	for {
		fs := c.client.PollRecords(ctx, 1)
		if err := fs.Err0(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if IsRetryable(err) {
				continue
			}
			return err
		}
		var runErr error
		fs.EachRecord(func(r *kgo.Record) {
			if runErr != nil {
				return
			}
			err := handleRecordAndCommit(ctx, h, rawRecord(r), func(commitCtx context.Context) error {
				return c.client.CommitRecords(commitCtx, r)
			}, c.limits.SendTimeout)
			if err != nil {
				runErr = err
			}
		})
		// BlockRebalanceOnPoll transfers responsibility for releasing the
		// assignment back to us. Release only after handler/commit work has
		// finished, including the failure path where the offset stays uncommitted.
		c.client.AllowRebalance()
		if runErr != nil {
			// Never continue to a later record in the same partition after a
			// handler or commit failure: committing that later offset would skip
			// the failed event. Let the supervisor restart with backoff instead.
			return runErr
		}
	}
}

// RunBytes is the consumer codec seam for versioned contracts other than raw
// events. A successful handler is synchronously committed; failures are not.
func (c *Consumer) RunBytes(ctx context.Context, h func(context.Context, string, []byte) error) error {
	if h == nil {
		return errors.New("Kafka byte handler is required")
	}
	return c.RunRecords(ctx, func(ctx context.Context, record RawRecord) error {
		return h(ctx, string(record.Key), record.Value)
	})
}

// handleRecordAndCommit is deliberately Kafka-free so acknowledgement
// semantics are tested without a broker: a failed handler never commits input.
func handleRecordAndCommit(ctx context.Context, h RecordHandler, record RawRecord, commit func(context.Context) error, timeout time.Duration) error {
	if err := h(ctx, record); err != nil {
		return err
	}
	commitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return commit(commitCtx)
}
func rawRecord(r *kgo.Record) RawRecord {
	headers := make([]Header, len(r.Headers))
	for i, header := range r.Headers {
		headers[i] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return RawRecord{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       append([]byte(nil), r.Key...),
		Value:     append([]byte(nil), r.Value...),
		Headers:   headers,
	}
}

func decode(topic string, key, value []byte) (transport.Envelope, error) {
	var e event.Event
	if err := json.Unmarshal(value, &e); err != nil {
		return transport.Envelope{}, fmt.Errorf("decode Kafka record: %w", err)
	}
	return transport.Envelope{Topic: topic, PartitionKey: string(key), Event: e}, nil
}
func (c *Consumer) Close() error { c.client.Close(); return nil }
