package event

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// Contract identifies the immutable data contract used to admit and
// transform an event. It is an event extension so raw Kafka records retain
// the same identity needed by processor, sink, and DLQ paths without coupling
// the base event package to a particular broker.
type Contract struct {
	RawSchema        SchemaIdentity    `json:"raw_schema,omitempty"`
	NormalizedSchema SchemaIdentity    `json:"normalized_schema,omitempty"`
	Transform        TransformIdentity `json:"transform,omitempty"`
}

type SchemaIdentity struct {
	Revision string `json:"revision,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

type TransformIdentity struct {
	Revision string `json:"revision,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

func (c Contract) Empty() bool {
	return c.RawSchema == (SchemaIdentity{}) && c.NormalizedSchema == (SchemaIdentity{}) && c.Transform == (TransformIdentity{})
}

// Validate accepts an absent contract for backward-compatible personal mode.
// A present contract must be complete so a partially configured provider
// record can never be mistaken for an immutable revision.
func (c Contract) Validate() error {
	if c.Empty() {
		return nil
	}
	for _, item := range []struct{ name, revision, digest string }{
		{"raw schema", c.RawSchema.Revision, c.RawSchema.Digest},
		{"normalized schema", c.NormalizedSchema.Revision, c.NormalizedSchema.Digest},
		{"transform", c.Transform.Revision, c.Transform.Digest},
	} {
		if item.revision == "" || item.digest == "" {
			return errors.New("event contract " + item.name + " revision and digest are required")
		}
		if len(item.digest) != 64 {
			return errors.New("event contract " + item.name + " digest must be a SHA-256 hex value")
		}
		decoded, err := hex.DecodeString(item.digest)
		if err != nil || len(decoded) != 32 {
			return errors.New("event contract " + item.name + " digest must be a SHA-256 hex value")
		}
	}
	return nil
}

func (c Contract) Complete() bool { return c.Validate() == nil && !c.Empty() }

const SpecVersion = "1.0"

const (
	MaxIDBytes     = 1024
	MaxSourceBytes = 512
	MaxTypeBytes   = 256
	MaxDataBytes   = 16 << 20
)

var (
	ErrMissingID     = errors.New("event id is required")
	ErrMissingSource = errors.New("event source is required")
	ErrMissingType   = errors.New("event type is required")
	ErrMissingTime   = errors.New("event time is required")
	ErrInvalidData   = errors.New("event data must be valid JSON")
)

type Event struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Source      string          `json:"source"`
	Type        string          `json:"type"`
	Time        time.Time       `json:"time"`
	Data        json.RawMessage `json:"data"`
	Contract    Contract        `json:"contract,omitempty"`
}

// Validate checks the transport-level invariants shared by every collector.
// Payload-specific validation belongs to the dataset schema layer.
func (e Event) Validate() error {
	var errs []error

	if e.SpecVersion != SpecVersion {
		errs = append(errs, errors.New("event specversion must be 1.0"))
	}
	if e.ID == "" {
		errs = append(errs, ErrMissingID)
	} else if len(e.ID) > MaxIDBytes {
		errs = append(errs, errors.New("event id exceeds maximum size"))
	}
	if e.Source == "" {
		errs = append(errs, ErrMissingSource)
	} else if len(e.Source) > MaxSourceBytes {
		errs = append(errs, errors.New("event source exceeds maximum size"))
	}
	if e.Type == "" {
		errs = append(errs, ErrMissingType)
	} else if len(e.Type) > MaxTypeBytes {
		errs = append(errs, errors.New("event type exceeds maximum size"))
	}
	if e.Time.IsZero() {
		errs = append(errs, ErrMissingTime)
	}
	if !json.Valid(e.Data) {
		errs = append(errs, ErrInvalidData)
	} else if len(e.Data) > MaxDataBytes {
		errs = append(errs, errors.New("event data exceeds maximum size"))
	}
	if err := e.Contract.Validate(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
