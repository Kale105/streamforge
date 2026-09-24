// Package providerflow implements the broker-facing normalized-record path.
// It deliberately depends only on broker-neutral contracts: Kafka is an edge
// adapter, and raw provider events are never handed to a database writer.
package providerflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/transport"
)

const Version = "normalized-record/v1"

var (
	ErrInvalidMessage  = errors.New("invalid normalized record message")
	ErrMessageTooLarge = errors.New("normalized record message exceeds maximum size")
	ErrRejected        = errors.New("raw event rejected by provider normalizer")
)

// SourceEvent is the stable identity of the raw event from which a record was
// made.  Sinks can use it as their idempotency key.
type SourceEvent struct {
	ID     string    `json:"id"`
	Source string    `json:"source"`
	Type   string    `json:"type"`
	Time   time.Time `json:"time"`
}

// Message is the versioned wire contract between provider processors and
// sinks. Its JSON is intentionally canonicalized before it reaches a broker.
type Message struct {
	Version     string             `json:"version"`
	SourceEvent SourceEvent        `json:"source_event"`
	Record      recordstore.Record `json:"record"`
	// Contract pins the raw schema, normalized schema, and mapping used for
	// this output. Provider workers use the strict constructor below; an empty
	// value is retained only for personal-mode compatibility.
	Contract event.Contract `json:"contract,omitempty"`
}

func New(e event.Event, r recordstore.Record) Message {
	return Message{Version: Version, SourceEvent: SourceEvent{ID: e.ID, Source: e.Source, Type: e.Type, Time: e.Time}, Record: r, Contract: e.Contract}
}

func (m Message) IdempotencyKey() string { return m.SourceEvent.Source + "\x00" + m.SourceEvent.ID }

func (m Message) Validate(maxBytes int) error {
	if m.Version != Version || strings.TrimSpace(m.SourceEvent.ID) == "" || strings.TrimSpace(m.SourceEvent.Source) == "" || strings.TrimSpace(m.SourceEvent.Type) == "" || m.SourceEvent.Time.IsZero() {
		return ErrInvalidMessage
	}
	r := m.Record
	if strings.TrimSpace(r.Dataset) == "" || strings.TrimSpace(r.DatasetVersion) == "" || strings.TrimSpace(r.ID) == "" || r.UpdatedAt.IsZero() || !json.Valid(r.Data) {
		return ErrInvalidMessage
	}
	if err := m.Contract.Validate(); err != nil {
		return fmt.Errorf("%w: contract: %v", ErrInvalidMessage, err)
	}
	if len(m.SourceEvent.ID) > event.MaxIDBytes || len(m.SourceEvent.Source) > event.MaxSourceBytes || len(m.SourceEvent.Type) > event.MaxTypeBytes ||
		len(r.Dataset) > recordstore.MaxDatasetBytes || len(r.DatasetVersion) > recordstore.MaxDatasetVersionBytes ||
		len(r.ID) > recordstore.MaxRecordIDBytes || len(r.Data) > recordstore.MaxRecordDataBytes {
		return fmt.Errorf("%w: field bound", ErrMessageTooLarge)
	}
	b, err := marshalCanonical(m)
	if err != nil {
		return err
	}
	if maxBytes > 0 && len(b) > maxBytes {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(b), maxBytes)
	}
	return nil
}

// Marshal is deterministic for semantically identical JSON record payloads.
func Marshal(m Message) ([]byte, error) {
	if err := m.Validate(0); err != nil {
		return nil, err
	}
	return marshalCanonical(m)
}

// SemanticHash is the provider sink receipt hash. It deliberately excludes
// observation timestamps: redelivery of the same source event and normalized
// record must remain idempotent even when those local observations differ.
// It includes the source event type and complete record identity/content so a
// reused source identity cannot silently target another record or payload.
func SemanticHash(m Message) ([sha256.Size]byte, error) {
	if err := m.Validate(0); err != nil {
		return [sha256.Size]byte{}, err
	}
	var data any
	decoder := json.NewDecoder(bytes.NewReader(m.Record.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("canonicalize record data: %w", err)
	}
	canonicalData, err := json.Marshal(data)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("canonicalize record data: %w", err)
	}
	semantic := struct {
		Version  string         `json:"version"`
		Contract event.Contract `json:"contract,omitempty"`
		Source   struct {
			ID     string `json:"id"`
			Source string `json:"source"`
			Type   string `json:"type"`
		} `json:"source_event"`
		Record struct {
			Dataset        string          `json:"dataset"`
			DatasetVersion string          `json:"dataset_version"`
			ID             string          `json:"id"`
			Data           json.RawMessage `json:"data"`
		} `json:"record"`
	}{Version: m.Version, Contract: m.Contract}
	semantic.Source.ID, semantic.Source.Source, semantic.Source.Type = m.SourceEvent.ID, m.SourceEvent.Source, m.SourceEvent.Type
	semantic.Record.Dataset, semantic.Record.DatasetVersion, semantic.Record.ID, semantic.Record.Data = m.Record.Dataset, m.Record.DatasetVersion, m.Record.ID, canonicalData
	encoded, err := json.Marshal(semantic)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal provider receipt semantic content: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func marshalCanonical(m Message) ([]byte, error) {
	if m.Version != Version {
		return nil, ErrInvalidMessage
	}
	var data any
	decoder := json.NewDecoder(bytes.NewReader(m.Record.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("canonicalize record data: %w", err)
	}
	m.Record.Data, _ = json.Marshal(data)
	m.SourceEvent.Time = m.SourceEvent.Time.UTC()
	m.Record.UpdatedAt = m.Record.UpdatedAt.UTC()
	return json.Marshal(m)
}

func Unmarshal(b []byte, maxBytes int) (Message, error) {
	if maxBytes > 0 && len(b) > maxBytes {
		return Message{}, ErrMessageTooLarge
	}
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return Message{}, fmt.Errorf("decode normalized record: %w", err)
	}
	if err := m.Validate(maxBytes); err != nil {
		return Message{}, err
	}
	return m, nil
}

// Publisher receives durable normalized messages. Returning nil means the
// processor may acknowledge its raw input.
type Publisher interface {
	Publish(context.Context, Message) error
}

// Writer durably applies a normalized message. It must be idempotent by
// IdempotencyKey: a duplicate delivery must have the same durable outcome.
type Writer interface {
	Write(context.Context, Message) error
}

type Normalizer interface {
	Normalize(event.Event) (recordstore.Record, error)
}

// ContractNormalizer is implemented by the built-in normalizer. Keeping it
// optional avoids breaking small personal-mode normalizers while allowing
// provider startup code to demand an immutable identity.
type ContractNormalizer interface {
	Normalizer
	Contract() event.Contract
}

type Processor struct {
	Normalizer     Normalizer
	Publisher      Publisher
	DatasetVersion string
	Contract       event.Contract
}

func NewProcessor(n Normalizer, p Publisher, datasetVersion string) (*Processor, error) {
	if n == nil || p == nil || strings.TrimSpace(datasetVersion) == "" {
		return nil, errors.New("normalizer, publisher, and dataset version are required")
	}
	return &Processor{Normalizer: n, Publisher: p, DatasetVersion: datasetVersion}, nil
}

// NewStrictProcessor is the provider-mode constructor. A provider contract is
// complete only when both schema byte digests and an explicit mapping revision
// are present. This rejects unpinned configuration before any raw record is
// materialized.
func NewStrictProcessor(n Normalizer, publisher Publisher, datasetVersion string, contract event.Contract) (*Processor, error) {
	processor, err := NewProcessor(n, publisher, datasetVersion)
	if err != nil {
		return nil, err
	}
	if !contract.Complete() {
		return nil, errors.New("provider processor requires complete raw schema, normalized schema, and transform contract")
	}
	processor.Contract = contract
	return processor, nil
}

// NewProcessorFromNormalizer uses the identity computed by the built-in
// normalizer. It is the normal provider-worker wiring seam and intentionally
// fails for plug-ins that do not expose an immutable contract.
func NewProcessorFromNormalizer(n ContractNormalizer, p Publisher, datasetVersion string) (*Processor, error) {
	if n == nil {
		return nil, errors.New("contract normalizer is required")
	}
	return NewStrictProcessor(n, p, datasetVersion, n.Contract())
}

// HandleRaw is directly usable as transport.Handler. The Kafka transport only
// commits a raw offset after this method returns nil.
func (p *Processor) HandleRaw(ctx context.Context, raw transport.Envelope) error {
	if p == nil || p.Normalizer == nil || p.Publisher == nil {
		return errors.New("provider processor is not configured")
	}
	if !p.Contract.Empty() && raw.Event.Contract != p.Contract {
		return fmt.Errorf("%w: raw event contract does not match active provider contract", ErrRejected)
	}
	r, err := p.Normalizer.Normalize(raw.Event)
	if err != nil {
		return fmt.Errorf("%w: normalize raw event: %w", ErrRejected, err)
	}
	r.DatasetVersion = p.DatasetVersion
	m := New(raw.Event, r)
	if !p.Contract.Empty() {
		m.Contract = p.Contract
	}
	if err := m.Validate(0); err != nil {
		return err
	}
	if err := p.Publisher.Publish(ctx, m); err != nil {
		return fmt.Errorf("publish normalized record: %w", err)
	}
	return nil
}

type Sink struct {
	Writer   Writer
	Contract event.Contract
}

// NewStrictSink creates a provider sink that will reject any normalized
// message created under a different schema or transform revision.
func NewStrictSink(writer Writer, contract event.Contract) (Sink, error) {
	if writer == nil {
		return Sink{}, errors.New("normalized record writer is required")
	}
	if !contract.Complete() {
		return Sink{}, errors.New("provider sink requires complete raw schema, normalized schema, and transform contract")
	}
	return Sink{Writer: writer, Contract: contract}, nil
}

func (s Sink) Handle(ctx context.Context, m Message) error {
	if s.Writer == nil {
		return errors.New("normalized record writer is required")
	}
	if err := m.Validate(0); err != nil {
		return err
	}
	if !s.Contract.Empty() && m.Contract != s.Contract {
		return fmt.Errorf("%w: normalized message contract does not match active provider contract", ErrInvalidMessage)
	}
	// A consumer adapter commits only after this durable Write returns nil.
	return s.Writer.Write(ctx, m)
}
