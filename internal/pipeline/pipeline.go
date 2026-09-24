// Package pipeline owns concurrent collector-to-sink execution.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/sink"
)

var ErrDrainTimeout = errors.New("pipeline drain timed out")

type Config struct {
	BufferSize   int
	DrainTimeout time.Duration
}

// Run owns both component goroutines and does not return until both are joined.
// Accepted events are drained after intake stops, up to DrainTimeout.
func Run(ctx context.Context, cfg Config, source collector.Collector, destination sink.Sink) error {
	if cfg.BufferSize < 0 {
		return errors.New("pipeline buffer size cannot be negative")
	}
	if cfg.DrainTimeout <= 0 {
		return errors.New("pipeline drain timeout must be positive")
	}
	if source == nil {
		return errors.New("pipeline collector is required")
	}
	if destination == nil {
		return errors.New("pipeline sink is required")
	}

	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	sinkCtx, abortSink := context.WithCancel(context.WithoutCancel(ctx))
	defer abortSink()

	events := make(chan event.Event, cfg.BufferSize)
	results := make(chan componentResult, 2)
	var workers sync.WaitGroup

	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(events)

		err := source.Collect(intakeCtx, func(ctx context.Context, e event.Event) error {
			if err := e.Validate(); err != nil {
				return fmt.Errorf("validate collected event: %w", err)
			}
			select {
			case events <- e:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		results <- componentResult{name: "collector", err: err}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		for e := range events {
			if err := destination.Handle(sinkCtx, e); err != nil {
				results <- componentResult{name: "sink", err: err}
				return
			}
		}
		results <- componentResult{name: "sink"}
	}()

	var firstErr error
	remaining := 2
	var drainTimer *time.Timer
	var drainDeadline <-chan time.Time
	ctxDone := ctx.Done()

	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.name == "collector" && drainTimer == nil {
				drainTimer = time.NewTimer(cfg.DrainTimeout)
				drainDeadline = drainTimer.C
			}
			if result.err != nil && !isExpectedCancellation(result.err, intakeCtx, sinkCtx) && firstErr == nil {
				firstErr = fmt.Errorf("%s failed: %w", result.name, result.err)
				stopIntake()
				if drainTimer == nil {
					drainTimer = time.NewTimer(cfg.DrainTimeout)
					drainDeadline = drainTimer.C
				}
			}
		case <-ctxDone:
			stopIntake()
			ctxDone = nil
			if drainTimer == nil {
				drainTimer = time.NewTimer(cfg.DrainTimeout)
				drainDeadline = drainTimer.C
			}
		case <-drainDeadline:
			abortSink()
			if firstErr == nil {
				firstErr = ErrDrainTimeout
			}
			drainDeadline = nil
		}
	}

	if drainTimer != nil {
		drainTimer.Stop()
	}
	workers.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

type componentResult struct {
	name string
	err  error
}

func isExpectedCancellation(err error, intakeCtx, sinkCtx context.Context) bool {
	return (intakeCtx.Err() != nil && errors.Is(err, intakeCtx.Err())) ||
		(sinkCtx.Err() != nil && errors.Is(err, sinkCtx.Err()))
}
