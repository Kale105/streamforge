package event

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEventJSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := Event{
		SpecVersion: SpecVersion,
		ID:          "event-42",
		Source:      "urn:streamforge:test",
		Type:        "dev.streamforge.test.observed.v1",
		Time:        time.Date(2026, time.August, 7, 20, 30, 0, 0, time.UTC),
		Data:        json.RawMessage(`{"game_id":"game-42","score":98}`),
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var decoded Event
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	if decoded.SpecVersion != original.SpecVersion {
		t.Errorf("SpecVersion = %q; want %q", decoded.SpecVersion, original.SpecVersion)
	}
	if decoded.ID != original.ID {
		t.Errorf("ID = %q; want %q", decoded.ID, original.ID)
	}
	if decoded.Source != original.Source {
		t.Errorf("Source = %q; want %q", decoded.Source, original.Source)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q; want %q", decoded.Type, original.Type)
	}
	if !decoded.Time.Equal(original.Time) {
		t.Errorf("Time = %v; want %v", decoded.Time, original.Time)
	}

	var originalData, decodedData any
	if err := json.Unmarshal(original.Data, &originalData); err != nil {
		t.Fatalf("unmarshal original data: %v", err)
	}
	if err := json.Unmarshal(decoded.Data, &decodedData); err != nil {
		t.Fatalf("unmarshal decoded data: %v", err)
	}
	if !reflect.DeepEqual(decodedData, originalData) {
		t.Errorf("Data = %s; want %s", decoded.Data, original.Data)
	}
}

func TestEventValidateRejectsOversizedFields(t *testing.T) {
	valid := Event{SpecVersion: SpecVersion, ID: "id", Source: "source", Type: "type", Time: time.Now().UTC(), Data: json.RawMessage(`{}`)}
	for name, mutate := range map[string]func(*Event){
		"id":     func(e *Event) { e.ID = strings.Repeat("x", MaxIDBytes+1) },
		"source": func(e *Event) { e.Source = strings.Repeat("x", MaxSourceBytes+1) },
		"type":   func(e *Event) { e.Type = strings.Repeat("x", MaxTypeBytes+1) },
		"data":   func(e *Event) { e.Data = json.RawMessage(`"` + strings.Repeat("x", MaxDataBytes) + `"`) },
	} {
		t.Run(name, func(t *testing.T) {
			e := valid
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("oversized event was accepted")
			}
		})
	}
}

func TestEventValidate(t *testing.T) {
	t.Parallel()

	valid := Event{
		SpecVersion: SpecVersion,
		ID:          "event-1",
		Source:      "urn:streamforge:test",
		Type:        "dev.streamforge.test.v1",
		Time:        time.Now().UTC(),
		Data:        json.RawMessage(`{"ok":true}`),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v; want nil", err)
	}

	invalid := valid
	invalid.ID = ""
	invalid.Data = json.RawMessage(`{"broken":`)
	err := invalid.Validate()
	if !errors.Is(err, ErrMissingID) {
		t.Errorf("Validate() error = %v; want ErrMissingID", err)
	}
	if !errors.Is(err, ErrInvalidData) {
		t.Errorf("Validate() error = %v; want ErrInvalidData", err)
	}
}

func TestEventRejectsPartialContract(t *testing.T) {
	e := Event{SpecVersion: SpecVersion, ID: "id", Source: "source", Type: "type", Time: time.Now(), Data: json.RawMessage(`{}`), Contract: Contract{RawSchema: SchemaIdentity{Revision: "raw-v1"}}}
	if err := e.Validate(); err == nil || !strings.Contains(err.Error(), "contract") {
		t.Fatalf("partial contract accepted: %v", err)
	}
}

func TestEventRejectsNonSHA256ContractDigest(t *testing.T) {
	contract := Contract{
		RawSchema:        SchemaIdentity{Revision: "raw-v1", Digest: "not-a-digest"},
		NormalizedSchema: SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("a", 64)},
		Transform:        TransformIdentity{Revision: "mapping-v1", Digest: strings.Repeat("b", 64)},
	}
	e := Event{SpecVersion: SpecVersion, ID: "id", Source: "source", Type: "type", Time: time.Now(), Data: json.RawMessage(`{}`), Contract: contract}
	if err := e.Validate(); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("non-SHA-256 digest accepted: %v", err)
	}
}
