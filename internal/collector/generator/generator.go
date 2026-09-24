// Package generator implements a deterministic synthetic collector.
package generator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

var ErrInvalidInterval = errors.New("generator interval must be positive")

type Generator struct {
	interval time.Duration
	limit    int
}

// New constructs a generator. A limit of zero means run until cancellation.
func New(interval time.Duration, limit int) (*Generator, error) {
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}
	if limit < 0 {
		return nil, errors.New("generator limit cannot be negative")
	}
	return &Generator{interval: interval, limit: limit}, nil
}

func (g *Generator) Collect(ctx context.Context, emit collector.EmitFunc) error {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for sequence := 1; g.limit == 0 || sequence <= g.limit; sequence++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case observedAt := <-ticker.C:
			payload, err := json.Marshal(struct {
				Sequence int `json:"sequence"`
			}{Sequence: sequence})
			if err != nil {
				return fmt.Errorf("encode generated payload: %w", err)
			}

			e := event.Event{
				SpecVersion: event.SpecVersion,
				ID:          fmt.Sprintf("generator-%d", sequence),
				Source:      "urn:streamforge:generator",
				Type:        "dev.streamforge.generator.observed.v1",
				Time:        observedAt.UTC(),
				Data:        payload,
			}
			if err := emit(ctx, e); err != nil {
				return fmt.Errorf("emit generated event: %w", err)
			}
		}
	}

	return nil
}

var _ collector.Collector = (*Generator)(nil)
