package workerretry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var retryErr = errors.New("temporary")

func testConfig(wait func(context.Context, time.Duration) error) Config {
	return Config{InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, MaxAttempts: 3, Wait: wait}
}

func TestRunRetryableFailureRebuildsAttempt(t *testing.T) {
	var built, ran int
	var delays []time.Duration
	err := Run(context.Background(), testConfig(func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	}), func(context.Context) error {
		built++ // Resource construction belongs to this fresh invocation.
		ran++
		if ran < 3 {
			return retryErr
		}
		return nil
	}, func(err error) bool { return errors.Is(err, retryErr) })
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if built != 3 || ran != 3 {
		t.Fatalf("attempts built/ran = %d/%d, want 3/3", built, ran)
	}
	if got, want := delays, []time.Duration{time.Millisecond, 2 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("delays = %v, want %v", got, want)
	}
}

func TestRunFatalFailureStopsImmediately(t *testing.T) {
	fatal := errors.New("fatal")
	attempts := 0
	err := Run(context.Background(), testConfig(func(context.Context, time.Duration) error {
		t.Fatal("wait must not run after fatal error")
		return nil
	}), func(context.Context) error { attempts++; return fatal }, func(error) bool { return false })
	if !errors.Is(err, fatal) || attempts != 1 {
		t.Fatalf("error/attempts = %v/%d, want fatal/1", err, attempts)
	}
}

func TestRunReturnsTypedExhaustion(t *testing.T) {
	err := Run(context.Background(), testConfig(func(context.Context, time.Duration) error { return nil }), func(context.Context) error { return retryErr }, func(error) bool { return true })
	var exhausted *ExhaustionError
	if !errors.As(err, &exhausted) || !errors.Is(err, ErrAttemptsExhausted) || !errors.Is(err, retryErr) {
		t.Fatalf("error = %v, want typed exhaustion wrapping sentinel and cause", err)
	}
	if exhausted.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", exhausted.Attempts)
	}
}

func TestRunParentCancellationDuringAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, testConfig(func(context.Context, time.Duration) error { return nil }), func(ctx context.Context) error {
			<-ctx.Done()
			return retryErr
		}, func(error) bool { return true })
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit when the parent context ended during an attempt")
	}
}

func TestRunParentCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, testConfig(func(ctx context.Context, _ time.Duration) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}), func(context.Context) error { return retryErr }, func(error) bool { return true })
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit when the parent context ended during backoff")
	}
}

func TestRunNeverBusyLoopsOnNonPositiveJitter(t *testing.T) {
	waits := 0
	cfg := testConfig(func(_ context.Context, d time.Duration) error {
		waits++
		if d <= 0 {
			t.Fatalf("wait delay = %s, want positive", d)
		}
		return nil
	})
	cfg.Jitter = func(time.Duration) time.Duration { return 0 }
	err := Run(context.Background(), cfg, func(context.Context) error { return retryErr }, func(error) bool { return true })
	if waits != 2 {
		t.Fatalf("wait calls = %d, want 2", waits)
	}
	var exhausted *ExhaustionError
	if !errors.As(err, &exhausted) {
		t.Fatalf("error = %v, want exhaustion", err)
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	err := Run(context.Background(), Config{}, func(context.Context) error { return nil }, func(error) bool { return true })
	if err == nil {
		t.Fatal("Run accepted invalid config")
	}
}
