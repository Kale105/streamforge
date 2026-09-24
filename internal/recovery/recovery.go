// Package recovery drains durable materialization work at process startup.
package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/materializer"
)

// ErrRecoveryLimit reports that Drain reached its configured batch limit before
// observing an empty batch. It prevents a stale source from monopolizing startup.
var ErrRecoveryLimit = errors.New("recovery batch limit reached")

// Source obtains recoverable events for one immutable dataset revision.
type Source interface {
	RecoverableEvents(ctx context.Context, dataset, version, source string, limit int) ([]event.Event, error)
}

// Handler materializes one recovered event.
type Handler interface {
	Handle(context.Context, event.Event) error
}

// Config identifies work to recover and bounds the work done in one invocation.
type Config struct {
	Dataset    string
	Version    string
	Source     string
	BatchSize  int
	MaxBatches int
}

// Drain fetches and handles bounded batches sequentially. Deterministically
// rejected events have already been recorded durably, so they do not stop
// recovery of later events.
func Drain(ctx context.Context, config Config, source Source, handler Handler) error {
	if err := validate(config, source, handler); err != nil {
		return err
	}

	for batchNumber := 0; batchNumber < config.MaxBatches; batchNumber++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := source.RecoverableEvents(ctx, config.Dataset, config.Version, config.Source, config.BatchSize)
		if err != nil {
			return fmt.Errorf("recoverable events: %w", err)
		}
		if len(events) == 0 {
			return nil
		}
		for _, e := range events {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := handler.Handle(ctx, e); err != nil && !errors.Is(err, materializer.ErrRejected) {
				return fmt.Errorf("recover event %q: %w", e.ID, err)
			}
		}
	}

	return ErrRecoveryLimit
}

func validate(config Config, source Source, handler Handler) error {
	if config.Dataset == "" {
		return errors.New("recovery dataset is required")
	}
	if config.Version == "" {
		return errors.New("recovery version is required")
	}
	if config.Source == "" {
		return errors.New("recovery source is required")
	}
	if config.BatchSize <= 0 {
		return errors.New("recovery batch size must be positive")
	}
	if config.MaxBatches <= 0 {
		return errors.New("recovery max batches must be positive")
	}
	if source == nil {
		return errors.New("recovery source is required")
	}
	if handler == nil {
		return errors.New("recovery handler is required")
	}
	return nil
}
