// Package materializer connects deterministic normalization to durable storage.
package materializer

import (
	"context"
	"errors"
	"fmt"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/sink"
)

// ErrRejected marks a deterministic normalization failure. The raw event and
// failure ledger entry remain durable, so recovery may continue with other work.
var ErrRejected = errors.New("event rejected by normalizer")

type Normalizer interface {
	Normalize(event.Event) (recordstore.Record, error)
}

type Ingester interface {
	Admit(context.Context, event.Event) (bool, error)
	PrepareMaterialization(context.Context, event.Event, string, string) (bool, error)
	FailMaterialization(context.Context, event.Event, string, string, error) error
	CompleteMaterialization(context.Context, event.Event, recordstore.Record, string, string) error
}

type Sink struct {
	normalizer Normalizer
	ingester   Ingester
	dataset    string
	version    string
}

// New creates a materializer for one immutable dataset revision. The revision
// is part of the ledger identity, so a changed normalizer can retry old raw
// events without confusing the prior result with its own.
func New(normalizer Normalizer, ingester Ingester, dataset, version string) (*Sink, error) {
	if normalizer == nil {
		return nil, fmt.Errorf("materializer normalizer is required")
	}
	if ingester == nil {
		return nil, fmt.Errorf("materializer ingester is required")
	}
	if dataset == "" || version == "" {
		return nil, fmt.Errorf("materializer dataset and version are required")
	}
	return &Sink{normalizer: normalizer, ingester: ingester, dataset: dataset, version: version}, nil
}

func (s *Sink) Handle(ctx context.Context, e event.Event) error {
	// Admit commits raw input before any fallible transformation. Prepare is
	// deliberately separate: duplicate delivery retries a missing or failed
	// revision, while an already-succeeded revision is a no-op.
	if _, err := s.ingester.Admit(ctx, e); err != nil {
		return fmt.Errorf("admit raw event: %w", err)
	}
	// Prepare returns false for a completed revision. Do not re-normalize it.
	ready, err := s.ingester.PrepareMaterialization(ctx, e, s.dataset, s.version)
	if err != nil {
		return fmt.Errorf("prepare materialization: %w", err)
	}
	if !ready {
		return nil
	}
	record, err := s.normalizer.Normalize(e)
	if err != nil {
		if ledgerErr := s.ingester.FailMaterialization(ctx, e, s.dataset, s.version, err); ledgerErr != nil {
			return fmt.Errorf("normalize event: %w (record materialization failure: %v)", err, ledgerErr)
		}
		return fmt.Errorf("%w: normalize event: %w", ErrRejected, err)
	}
	record.DatasetVersion = s.version
	if err := s.ingester.CompleteMaterialization(ctx, e, record, s.dataset, s.version); err != nil {
		return fmt.Errorf("complete materialization: %w", err)
	}
	return nil
}

var _ sink.Sink = (*Sink)(nil)
