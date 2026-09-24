package generator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func TestCollectEmitsFiniteValidSequence(t *testing.T) {
	t.Parallel()

	generator, err := New(time.Millisecond, 3)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var sequences []int
	err = generator.Collect(context.Background(), func(_ context.Context, e event.Event) error {
		if err := e.Validate(); err != nil {
			t.Fatalf("emitted event is invalid: %v", err)
		}
		var payload struct {
			Sequence int `json:"sequence"`
		}
		if err := json.Unmarshal(e.Data, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		sequences = append(sequences, payload.Sequence)
		return nil
	})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	want := []int{1, 2, 3}
	if len(sequences) != len(want) {
		t.Fatalf("emitted %d events; want %d", len(sequences), len(want))
	}
	for i := range want {
		if sequences[i] != want[i] {
			t.Errorf("sequence[%d] = %d; want %d", i, sequences[i], want[i])
		}
	}
}

func TestCollectHonorsCancellation(t *testing.T) {
	t.Parallel()

	generator, err := New(time.Hour, 0)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = generator.Collect(ctx, func(context.Context, event.Event) error {
		t.Fatal("emit called after cancellation")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Collect() error = %v; want context.Canceled", err)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := New(0, 1); !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("New(0, 1) error = %v; want ErrInvalidInterval", err)
	}
	if _, err := New(time.Second, -1); err == nil {
		t.Error("New(time.Second, -1) error = nil; want error")
	}
}
