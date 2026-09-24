package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

func TestRunProcessesFiniteSequenceInOrder(t *testing.T) {
	t.Parallel()

	source := collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		for i := 1; i <= 5; i++ {
			if err := emit(ctx, testEvent(i)); err != nil {
				return err
			}
		}
		return nil
	})
	destination := &recordingSink{}

	err := Run(context.Background(), Config{BufferSize: 2, DrainTimeout: time.Second}, source, destination)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := destination.IDs()
	want := []string{"event-1", "event-2", "event-3", "event-4", "event-5"}
	if len(got) != len(want) {
		t.Fatalf("handled %d events; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("handled ID[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}

func TestRunPropagatesSinkFailureAndCancelsCollector(t *testing.T) {
	t.Parallel()

	collectorStopped := make(chan struct{})
	source := collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		defer close(collectorStopped)
		for i := 1; ; i++ {
			if err := emit(ctx, testEvent(i)); err != nil {
				return err
			}
		}
	})
	wantErr := errors.New("destination unavailable")
	destination := sinkFunc(func(context.Context, event.Event) error { return wantErr })

	err := Run(context.Background(), Config{BufferSize: 1, DrainTimeout: time.Second}, source, destination)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v; want destination error", err)
	}
	select {
	case <-collectorStopped:
	default:
		t.Fatal("Run returned before collector stopped")
	}
}

func TestRunRejectsInvalidCollectedEvent(t *testing.T) {
	t.Parallel()

	source := collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		invalid := testEvent(1)
		invalid.ID = ""
		return emit(ctx, invalid)
	})
	destination := &recordingSink{}

	err := Run(context.Background(), Config{BufferSize: 1, DrainTimeout: time.Second}, source, destination)
	if !errors.Is(err, event.ErrMissingID) {
		t.Fatalf("Run() error = %v; want ErrMissingID", err)
	}
	if got := destination.IDs(); len(got) != 0 {
		t.Fatalf("handled IDs = %v; want none", got)
	}
}

func TestRunBoundsDrainAfterCollectorCompletes(t *testing.T) {
	t.Parallel()

	source := collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		return emit(ctx, testEvent(1))
	})
	destination := sinkFunc(func(ctx context.Context, _ event.Event) error {
		<-ctx.Done()
		return ctx.Err()
	})

	err := Run(context.Background(), Config{BufferSize: 1, DrainTimeout: 10 * time.Millisecond}, source, destination)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Run() error = %v; want ErrDrainTimeout", err)
	}
}

type collectorFunc func(context.Context, collector.EmitFunc) error

func (f collectorFunc) Collect(ctx context.Context, emit collector.EmitFunc) error {
	return f(ctx, emit)
}

type sinkFunc func(context.Context, event.Event) error

func (f sinkFunc) Handle(ctx context.Context, e event.Event) error {
	return f(ctx, e)
}

type recordingSink struct {
	mu  sync.Mutex
	ids []string
}

func (s *recordingSink) Handle(_ context.Context, e event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, e.ID)
	return nil
}

func (s *recordingSink) IDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

func testEvent(sequence int) event.Event {
	return event.Event{
		SpecVersion: event.SpecVersion,
		ID:          fmt.Sprintf("event-%d", sequence),
		Source:      "urn:streamforge:test",
		Type:        "dev.streamforge.test.v1",
		Time:        time.Date(2026, time.September, 15, 12, 0, sequence, 0, time.UTC),
		Data:        json.RawMessage(fmt.Sprintf(`{"sequence":%d}`, sequence)),
	}
}

func TestRunValidatesConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "negative buffer", cfg: Config{BufferSize: -1, DrainTimeout: time.Second}, want: "buffer"},
		{name: "zero drain timeout", cfg: Config{}, want: "drain timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Run(context.Background(), test.cfg, nil, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run() error = %v; want error containing %q", err, test.want)
			}
		})
	}
}
