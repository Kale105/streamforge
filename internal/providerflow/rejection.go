package providerflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
)

const (
	RejectionVersion       = "provider-rejection/v1"
	MaxDiagnosticBytes     = 1024
	MaxDatasetVersionBytes = 128
)

var ErrInvalidRejection = errors.New("invalid provider rejection")

// Rejection is the durable wire contract for an event that could not be
// normalized. It retains the original event plus the exact dataset revision
// that rejected it, so a replay tool can make an explicit revision choice.
type Rejection struct {
	Version        string         `json:"version"`
	Dataset        string         `json:"dataset"`
	DatasetVersion string         `json:"dataset_version"`
	FailedAt       time.Time      `json:"failed_at"`
	Diagnostic     string         `json:"diagnostic"`
	Event          event.Event    `json:"event"`
	Contract       event.Contract `json:"contract,omitempty"`
}

// NewRejectionWithContract is the provider-mode form. The DLQ record carries
// exactly the same identity as the raw and normalized contracts, so replay
// tooling can reject a target with different schema or mapping bytes.
func NewRejectionWithContract(dataset, datasetVersion string, failedAt time.Time, diagnostic string, raw event.Event, contract event.Contract) (Rejection, error) {
	if !contract.Complete() {
		return Rejection{}, errors.New("provider rejection requires complete raw schema, normalized schema, and transform contract")
	}
	if raw.Contract != contract {
		return Rejection{}, errors.New("provider rejection raw event contract does not match active contract")
	}
	r := NewRejection(dataset, datasetVersion, failedAt, diagnostic, raw)
	r.Contract = contract
	return r, nil
}

func NewRejection(dataset, datasetVersion string, failedAt time.Time, diagnostic string, raw event.Event) Rejection {
	return Rejection{
		Version: RejectionVersion, Dataset: dataset, DatasetVersion: datasetVersion,
		FailedAt: failedAt.UTC(), Diagnostic: diagnostic, Event: raw,
	}
}

func (r Rejection) IdempotencyKey() string { return r.Event.Source + "\x00" + r.Event.ID }

func (r Rejection) Validate(maxBytes int) error {
	if r.Version != RejectionVersion || strings.TrimSpace(r.Dataset) == "" ||
		strings.TrimSpace(r.DatasetVersion) == "" || r.FailedAt.IsZero() ||
		strings.TrimSpace(r.Diagnostic) == "" {
		return ErrInvalidRejection
	}
	if len(r.Dataset) > recordstore.MaxDatasetBytes || len(r.DatasetVersion) > MaxDatasetVersionBytes ||
		len(r.Diagnostic) > MaxDiagnosticBytes {
		return fmt.Errorf("%w: field bound", ErrInvalidRejection)
	}
	if err := r.Event.Validate(); err != nil {
		return fmt.Errorf("%w: event: %v", ErrInvalidRejection, err)
	}
	if err := r.Contract.Validate(); err != nil {
		return fmt.Errorf("%w: contract: %v", ErrInvalidRejection, err)
	}
	if maxBytes > 0 {
		body, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("%w: encode: %v", ErrInvalidRejection, err)
		}
		if len(body) > maxBytes {
			return fmt.Errorf("%w: %w", ErrInvalidRejection, ErrMessageTooLarge)
		}
	}
	return nil
}

func MarshalRejection(r Rejection, maxBytes int) ([]byte, error) {
	if err := r.Validate(maxBytes); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

func UnmarshalRejection(body []byte, maxBytes int) (Rejection, error) {
	if maxBytes > 0 && len(body) > maxBytes {
		return Rejection{}, fmt.Errorf("%w: %w", ErrInvalidRejection, ErrMessageTooLarge)
	}
	var r Rejection
	if err := json.Unmarshal(body, &r); err != nil {
		return Rejection{}, fmt.Errorf("%w: decode: %v", ErrInvalidRejection, err)
	}
	if err := r.Validate(maxBytes); err != nil {
		return Rejection{}, err
	}
	return r, nil
}
