// Package retry provides bounded recovery for collectors with transient failures.
package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

// ErrRetryBudgetExhausted is returned when a retryable failure exceeds the
// configured consecutive-failure budget. The returned error also wraps the
// final collector error.
var ErrRetryBudgetExhausted = errors.New("collector retry budget exhausted")

// Config controls retry recovery. All duration and failure-limit fields must
// be positive. Wait and Jitter are optional seams for deterministic tests.
//
// Wait must block until the delay expires or ctx is cancelled. Jitter receives
// the capped exponential delay and may adjust it; its result is capped again at
// MaxBackoff before being passed to Wait.
type Config struct {
	InitialBackoff         time.Duration
	MaxBackoff             time.Duration
	MaxConsecutiveFailures int
	Wait                   func(context.Context, time.Duration) error
	Jitter                 func(time.Duration) time.Duration
}

// Collector retries an inner collector only when Classify identifies its error
// as transient. It never creates goroutines and forwards events synchronously.
type Collector struct {
	inner    collector.Collector
	classify func(error) bool
	config   Config
}

// New constructs a bounded retrying collector.
func New(inner collector.Collector, classify func(error) bool, config Config) (*Collector, error) {
	if inner == nil {
		return nil, errors.New("retry collector requires an inner collector")
	}
	if classify == nil {
		return nil, errors.New("retry collector requires an error classifier")
	}
	if config.InitialBackoff <= 0 {
		return nil, errors.New("retry initial backoff must be positive")
	}
	if config.MaxBackoff <= 0 {
		return nil, errors.New("retry max backoff must be positive")
	}
	if config.MaxConsecutiveFailures <= 0 {
		return nil, errors.New("retry max consecutive failures must be positive")
	}
	if config.MaxBackoff < config.InitialBackoff {
		return nil, errors.New("retry max backoff must be at least initial backoff")
	}
	if config.Wait == nil {
		config.Wait = wait
	}
	if config.Jitter == nil {
		config.Jitter = func(delay time.Duration) time.Duration { return delay }
	}

	return &Collector{inner: inner, classify: classify, config: config}, nil
}

// Collect runs the inner collector, retrying transient failures with capped
// exponential backoff. A successful emitted event resets the failure budget.
func (c *Collector) Collect(ctx context.Context, emit collector.EmitFunc) error {
	failures := 0
	backoff := c.config.InitialBackoff

	for {
		emitted := false
		err := c.inner.Collect(ctx, func(emitCtx context.Context, e event.Event) error {
			if err := emit(emitCtx, e); err != nil {
				return err
			}
			emitted = true
			return nil
		})
		if err == nil {
			return nil
		}
		if !c.classify(err) {
			return err
		}
		if emitted {
			failures = 0
			backoff = c.config.InitialBackoff
		}

		failures++
		if failures >= c.config.MaxConsecutiveFailures {
			return fmt.Errorf("%w after %d consecutive failures: %w", ErrRetryBudgetExhausted, failures, err)
		}

		delay := c.config.Jitter(backoff)
		if delay < 0 {
			delay = 0
		}
		if delay > c.config.MaxBackoff {
			delay = c.config.MaxBackoff
		}
		if err := c.config.Wait(ctx, delay); err != nil {
			return err
		}
		backoff = nextBackoff(backoff, c.config.MaxBackoff)
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ collector.Collector = (*Collector)(nil)
