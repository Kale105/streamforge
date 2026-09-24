package materializer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
)

func TestHandleAdmitsThenCompletes(t *testing.T) {
	t.Parallel()
	e := validEvent()
	want := recordstore.Record{Dataset: "games", ID: "record-1", Data: json.RawMessage(`{"id":"record-1"}`), UpdatedAt: e.Time}
	store := &ingesterStub{ready: true}
	sink, err := New(normalizerStub{record: want}, store, "games", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if got, wantCalls := store.calls, []string{"admit", "prepare", "complete"}; !equal(got, wantCalls) {
		t.Fatalf("calls=%v want %v", got, wantCalls)
	}
	if store.completed.DatasetVersion != "v1" {
		t.Fatalf("completed dataset version = %q, want v1", store.completed.DatasetVersion)
	}
}

func TestHandleNormalizationFailurePreservesAdmittedInputAndRecordsFailure(t *testing.T) {
	t.Parallel()
	bad := errors.New("bad payload")
	store := &ingesterStub{ready: true}
	sink, err := New(normalizerStub{err: bad}, store, "games", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Handle(context.Background(), validEvent()); !errors.Is(err, bad) {
		t.Fatalf("Handle()=%v", err)
	}
	if got, want := store.calls, []string{"admit", "prepare", "failed"}; !equal(got, want) {
		t.Fatalf("calls=%v want %v", got, want)
	}
}

func TestHandleSkipsSucceededRevision(t *testing.T) {
	t.Parallel()
	store := &ingesterStub{}
	sink, err := New(normalizerStub{}, store, "games", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Handle(context.Background(), validEvent()); err != nil {
		t.Fatal(err)
	}
	if got, want := store.calls, []string{"admit", "prepare"}; !equal(got, want) {
		t.Fatalf("calls=%v want %v", got, want)
	}
}

func validEvent() event.Event {
	return event.Event{SpecVersion: event.SpecVersion, ID: "event-1", Source: "test", Type: "test.v1", Time: time.Now().UTC(), Data: json.RawMessage(`{"id":"record-1"}`)}
}
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type normalizerStub struct {
	record recordstore.Record
	err    error
}

func (s normalizerStub) Normalize(event.Event) (recordstore.Record, error) { return s.record, s.err }

type ingesterStub struct {
	calls     []string
	ready     bool
	completed recordstore.Record
}

func (s *ingesterStub) Admit(context.Context, event.Event) (bool, error) {
	s.calls = append(s.calls, "admit")
	return true, nil
}
func (s *ingesterStub) PrepareMaterialization(context.Context, event.Event, string, string) (bool, error) {
	s.calls = append(s.calls, "prepare")
	return s.ready, nil
}
func (s *ingesterStub) FailMaterialization(context.Context, event.Event, string, string, error) error {
	s.calls = append(s.calls, "failed")
	return nil
}
func (s *ingesterStub) CompleteMaterialization(_ context.Context, _ event.Event, record recordstore.Record, _, _ string) error {
	s.calls = append(s.calls, "complete")
	s.completed = record
	return nil
}
