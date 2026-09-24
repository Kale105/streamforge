package recovery

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/materializer"
)

type sourceFunc func(context.Context, string, string, string, int) ([]event.Event, error)

func (f sourceFunc) RecoverableEvents(ctx context.Context, dataset, version, source string, limit int) ([]event.Event, error) {
	return f(ctx, dataset, version, source, limit)
}

type handlerFunc func(context.Context, event.Event) error

func (f handlerFunc) Handle(ctx context.Context, e event.Event) error { return f(ctx, e) }

func config() Config {
	return Config{Dataset: "games", Version: "v1", Source: "urn:test:games", BatchSize: 2, MaxBatches: 3}
}
func recovered(id string) event.Event { return event.Event{ID: id, Time: time.Unix(0, 0)} }

func TestDrainHandlesMultipleBatches(t *testing.T) {
	batches := [][]event.Event{{recovered("one"), recovered("two")}, {recovered("three")}, {}}
	var got []string
	calls := 0
	err := Drain(context.Background(), config(), sourceFunc(func(_ context.Context, dataset, version, source string, limit int) ([]event.Event, error) {
		if dataset != "games" || version != "v1" || source != "urn:test:games" || limit != 2 {
			t.Fatalf("unexpected source arguments: %q %q %q %d", dataset, version, source, limit)
		}
		result := batches[calls]
		calls++
		return result, nil
	}), handlerFunc(func(_ context.Context, e event.Event) error { got = append(got, e.ID); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"one", "two", "three"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("handled = %v, want %v", got, want)
	}
}

func TestDrainContinuesAfterRejectedEvent(t *testing.T) {
	calls := 0
	var got []string
	err := Drain(context.Background(), config(), sourceFunc(func(context.Context, string, string, string, int) ([]event.Event, error) {
		calls++
		if calls == 1 {
			return []event.Event{recovered("bad"), recovered("good")}, nil
		}
		return nil, nil
	}), handlerFunc(func(_ context.Context, e event.Event) error {
		got = append(got, e.ID)
		if e.ID == "bad" {
			return fmtRejected()
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bad", "good"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("handled = %v, want %v", got, want)
	}
}

func TestDrainStopsOnInfrastructureFailure(t *testing.T) {
	want := errors.New("database unavailable")
	err := Drain(context.Background(), config(), sourceFunc(func(context.Context, string, string, string, int) ([]event.Event, error) { return nil, want }), handlerFunc(func(context.Context, event.Event) error { t.Fatal("handler called"); return nil }))
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want source failure", err)
	}
}

func TestDrainStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceCalls := 0
	err := Drain(ctx, config(), sourceFunc(func(context.Context, string, string, string, int) ([]event.Event, error) {
		sourceCalls++
		return []event.Event{recovered("one")}, nil
	}), handlerFunc(func(context.Context, event.Event) error { cancel(); return nil }))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if sourceCalls != 1 {
		t.Fatalf("source calls = %d, want 1", sourceCalls)
	}
}

func TestDrainReturnsLimitWhenSourceMakesNoProgress(t *testing.T) {
	cfg := config()
	cfg.MaxBatches = 2
	calls := 0
	err := Drain(context.Background(), cfg, sourceFunc(func(context.Context, string, string, string, int) ([]event.Event, error) {
		calls++
		return []event.Event{recovered("stuck")}, nil
	}), handlerFunc(func(context.Context, event.Event) error { return materializer.ErrRejected }))
	if !errors.Is(err, ErrRecoveryLimit) {
		t.Fatalf("error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("source calls = %d, want 2", calls)
	}
}

func TestDrainValidatesConfiguration(t *testing.T) {
	validSource := sourceFunc(func(context.Context, string, string, string, int) ([]event.Event, error) { return nil, nil })
	validHandler := handlerFunc(func(context.Context, event.Event) error { return nil })
	cases := []Config{{}, {Dataset: "d"}, {Dataset: "d", Version: "v"}, {Dataset: "d", Version: "v", Source: "s"}, {Dataset: "d", Version: "v", Source: "s", BatchSize: 1}}
	for _, cfg := range cases {
		if err := Drain(context.Background(), cfg, validSource, validHandler); err == nil {
			t.Fatalf("Drain(%+v) succeeded", cfg)
		}
	}
	if err := Drain(context.Background(), config(), nil, validHandler); err == nil {
		t.Fatal("nil source accepted")
	}
	if err := Drain(context.Background(), config(), validSource, nil); err == nil {
		t.Fatal("nil handler accepted")
	}
}

func TestInterfacesCompile(t *testing.T) {
	var _ Source = sourceFunc(nil)
	var _ Handler = handlerFunc(nil)
}

func fmtRejected() error { return errors.Join(materializer.ErrRejected, errors.New("bad input")) }
