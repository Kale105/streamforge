package observability

import (
	"context"
	"errors"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/sink"
)

// Collector wraps a collector without changing the collector package or its
// runtime. The enqueued-event counter advances only after the runtime accepts
// the event into its bounded queue; it deliberately does not claim durability.
type Collector struct {
	Next    collector.Collector
	Metrics *Registry
	Name    string
}

func (c Collector) Collect(ctx context.Context, emit collector.EmitFunc) error {
	if c.Next == nil {
		return errors.New("observability collector next is required")
	}
	if c.Metrics == nil {
		return c.Next.Collect(ctx, emit)
	}
	done := c.Metrics.CollectorAttempt(c.Name)
	err := c.Next.Collect(ctx, func(ctx context.Context, e event.Event) error {
		err := emit(ctx, e)
		if err == nil {
			c.Metrics.EventEnqueued()
		}
		return err
	})
	done(err)
	return err
}

// Sink wraps a materialization sink. Rejected classifies known deterministic
// rejection errors; callers can supply errors.Is-based logic without coupling
// this package to a materializer implementation.
type Sink struct {
	Next     sink.Sink
	Metrics  *Registry
	Rejected func(error) bool
}

func (s Sink) Handle(ctx context.Context, e event.Event) error {
	if s.Next == nil {
		return errors.New("observability sink next is required")
	}
	err := s.Next.Handle(ctx, e)
	if s.Metrics == nil {
		return err
	}
	if err == nil {
		s.Metrics.EventMaterialized()
	} else if s.Rejected != nil && s.Rejected(err) {
		s.Metrics.EventRejected()
	}
	return err
}
