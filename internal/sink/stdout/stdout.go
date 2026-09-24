// Package stdout implements a newline-delimited JSON sink.
package stdout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/sink"
)

type Sink struct {
	encoder *json.Encoder
}

func New(w io.Writer) (*Sink, error) {
	if w == nil {
		return nil, errors.New("stdout sink writer is required")
	}
	return &Sink{encoder: json.NewEncoder(w)}, nil
}

func (s *Sink) Handle(ctx context.Context, e event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	if err := s.encoder.Encode(e); err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	return nil
}

var _ sink.Sink = (*Sink)(nil)
