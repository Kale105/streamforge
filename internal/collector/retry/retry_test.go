package retry

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

var errTransient = errors.New("transient")
var errPermanent = errors.New("permanent")

type collectorFunc func(context.Context, collector.EmitFunc) error

func (f collectorFunc) Collect(ctx context.Context, emit collector.EmitFunc) error {
	return f(ctx, emit)
}

func testConfig(wait func(context.Context, time.Duration) error) Config {
	return Config{InitialBackoff: time.Second, MaxBackoff: 4 * time.Second, MaxConsecutiveFailures: 4, Wait: wait}
}

func transient(err error) bool { return errors.Is(err, errTransient) }

func TestCollectRetriesWithCappedExponentialBackoff(t *testing.T) {
	var delays []time.Duration
	calls := 0
	c, err := New(collectorFunc(func(context.Context, collector.EmitFunc) error {
		calls++
		if calls <= 3 {
			return errTransient
		}
		return nil
	}), transient, testConfig(func(_ context.Context, delay time.Duration) error { delays = append(delays, delay); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Collect(context.Background(), func(context.Context, event.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got, want := delays, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
}

func TestCollectResetsFailureBudgetAfterEmission(t *testing.T) {
	var delays []time.Duration
	step := 0
	c, err := New(collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		step++
		switch step {
		case 1, 2:
			return errTransient
		case 3:
			if err := emit(ctx, event.Event{ID: "made-progress"}); err != nil {
				return err
			}
			return errTransient
		default:
			return nil
		}
	}), transient, Config{InitialBackoff: time.Second, MaxBackoff: 8 * time.Second, MaxConsecutiveFailures: 3, Wait: func(_ context.Context, d time.Duration) error { delays = append(delays, d); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Collect(context.Background(), func(context.Context, event.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got, want := delays, []time.Duration{time.Second, 2 * time.Second, time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
}

func TestCollectReturnsPermanentErrorWithoutWaiting(t *testing.T) {
	waited := false
	c, err := New(collectorFunc(func(context.Context, collector.EmitFunc) error { return errPermanent }), transient, testConfig(func(context.Context, time.Duration) error { waited = true; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, errPermanent) {
		t.Fatalf("error = %v", err)
	}
	if waited {
		t.Fatal("wait called for permanent error")
	}
}

func TestCollectHonorsCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := New(collectorFunc(func(context.Context, collector.EmitFunc) error { return errTransient }), transient, testConfig(func(ctx context.Context, _ time.Duration) error { cancel(); <-ctx.Done(); return ctx.Err() }))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Collect(ctx, func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}

func TestCollectReturnsBudgetErrorWrappingFinalCause(t *testing.T) {
	c, err := New(collectorFunc(func(context.Context, collector.EmitFunc) error { return errTransient }), transient, Config{InitialBackoff: time.Second, MaxBackoff: time.Second, MaxConsecutiveFailures: 2, Wait: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, ErrRetryBudgetExhausted) || !errors.Is(err, errTransient) {
		t.Fatalf("error = %v, want budget error wrapping final cause", err)
	}
}

func TestCollectorImplementsInterface(t *testing.T) {
	var _ collector.Collector = (*Collector)(nil)
}
