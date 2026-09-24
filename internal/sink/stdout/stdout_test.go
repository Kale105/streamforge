package stdout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func TestHandleWritesOneJSONLine(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	sink, err := New(&output)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	want := validEvent()
	if err := sink.Handle(context.Background(), want); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if output.Len() == 0 || output.Bytes()[output.Len()-1] != '\n' {
		t.Fatalf("output = %q; want newline-terminated JSON", output.String())
	}

	var got event.Event
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &got); err != nil {
		t.Fatalf("output is not valid event JSON: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("decoded ID = %q; want %q", got.ID, want.ID)
	}
}

func TestHandleRejectsInvalidEvent(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	sink, err := New(&output)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	invalid := validEvent()
	invalid.ID = ""
	if err := sink.Handle(context.Background(), invalid); !errors.Is(err, event.ErrMissingID) {
		t.Fatalf("Handle() error = %v; want ErrMissingID", err)
	}
	if output.Len() != 0 {
		t.Fatalf("output = %q; want empty output", output.String())
	}
}

func validEvent() event.Event {
	return event.Event{
		SpecVersion: event.SpecVersion,
		ID:          "event-1",
		Source:      "urn:streamforge:test",
		Type:        "dev.streamforge.test.v1",
		Time:        time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC),
		Data:        json.RawMessage(`{"value":1}`),
	}
}
