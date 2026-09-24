// Package workerretry runs self-contained worker attempts with bounded recovery.
//
// An attempt is deliberately a callback rather than a long-lived worker object:
// each call is a fresh ownership boundary.  Callers should open, use, and close
// all attempt-specific resources inside that callback.
package workerretry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrAttemptsExhausted identifies a retryable failure that used every allowed
// attempt.
var ErrAttemptsExhausted = errors.New("worker retry attempts exhausted")

// ExhaustionError reports the final error and the number of attempts made.
// It matches both ErrAttemptsExhausted and its final error through errors.Is.
type ExhaustionError struct {
	Attempts int
	Last     error
}

func (e *ExhaustionError) Error() string {
	return fmt.Sprintf("%v after %d attempts: %v", ErrAttemptsExhausted, e.Attempts, e.Last)
}

func (e *ExhaustionError) Unwrap() []error { return []error{ErrAttemptsExhausted, e.Last} }

// Clock supplies timers for the default wait implementation.  It is a small
// seam for tests that need to avoid wall-clock sleeps.
type Clock interface {
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config controls bounded retry behavior.  MaxAttempts includes the initial
// attempt.  Wait and Jitter are optional test seams.  Jitter receives the
// capped exponential backoff; its result is capped again before waiting.
type Config struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxAttempts    int
	Clock          Clock
	Wait           func(context.Context, time.Duration) error
	Jitter         func(time.Duration) time.Duration
}

// Run invokes attempt until it succeeds, returns a non-retryable error, the
// parent context ends, or the attempt limit is exhausted. Each invocation of
// attempt is a new resource-ownership boundary.
func Run(ctx context.Context, cfg Config, attempt func(context.Context) error, retryable func(error) bool) error {
	if err := validate(cfg, attempt, retryable); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("worker retry requires a context")
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Wait == nil {
		cfg.Wait = waitWith(cfg.Clock)
	}
	if cfg.Jitter == nil {
		cfg.Jitter = func(d time.Duration) time.Duration { return d }
	}

	backoff := cfg.InitialBackoff
	for n := 1; n <= cfg.MaxAttempts; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := attempt(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err == nil {
			return nil
		}
		if !retryable(err) {
			return err
		}
		if n == cfg.MaxAttempts {
			return &ExhaustionError{Attempts: n, Last: err}
		}

		delay := cfg.Jitter(backoff)
		if delay <= 0 {
			// A retry must yield; otherwise a faulty jitter function can create a
			// hot failure loop.
			delay = time.Nanosecond
		}
		if delay > cfg.MaxBackoff {
			delay = cfg.MaxBackoff
		}
		if err := cfg.Wait(ctx, delay); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		backoff = nextBackoff(backoff, cfg.MaxBackoff)
	}

	panic("unreachable")
}

func validate(cfg Config, attempt func(context.Context) error, retryable func(error) bool) error {
	if attempt == nil {
		return errors.New("worker retry requires an attempt")
	}
	if retryable == nil {
		return errors.New("worker retry requires a retry classifier")
	}
	if cfg.InitialBackoff <= 0 {
		return errors.New("worker retry initial backoff must be positive")
	}
	if cfg.MaxBackoff <= 0 {
		return errors.New("worker retry max backoff must be positive")
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		return errors.New("worker retry max backoff must be at least initial backoff")
	}
	if cfg.MaxAttempts <= 0 {
		return errors.New("worker retry max attempts must be positive")
	}
	return nil
}

func waitWith(clock Clock) func(context.Context, time.Duration) error {
	return func(ctx context.Context, delay time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clock.After(delay):
			return nil
		}
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}
