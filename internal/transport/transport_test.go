package transport

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func validEvent(data string) event.Event {
	return event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "source", Type: "type", Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Data: []byte(data)}
}
func TestMarshalCanonicalEvent(t *testing.T) {
	a, err := MarshalCanonicalEvent(validEvent(`{"z":1,"a":{"y":2,"x":3}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := MarshalCanonicalEvent(validEvent(` { "a" : {"x":3,"y":2}, "z": 1 } `))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("not canonical: %s != %s", a, b)
	}
}

func TestMarshalCanonicalEventPreservesLargeIntegers(t *testing.T) {
	encoded, err := MarshalCanonicalEvent(validEvent(`{"id":900719925474099312345}`))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`900719925474099312345`)) {
		t.Fatalf("large integer was changed: %s", encoded)
	}
}
func TestEnvelopeValidation(t *testing.T) {
	e := Envelope{Topic: "raw.events", PartitionKey: "dataset-1", Event: validEvent(`{}`)}
	if err := e.Validate(1024); err != nil {
		t.Fatal(err)
	}
	e.Topic = "__consumer_offsets"
	if !errors.Is(e.Validate(1024), ErrInvalidTopic) {
		t.Fatal("expected topic error")
	}
	e.Topic = "raw"
	e.PartitionKey = ""
	if !errors.Is(e.Validate(1024), ErrMissingPartitionKey) {
		t.Fatal("expected key error")
	}
}
func TestLimitsValidation(t *testing.T) {
	if (Limits{MaxMessageBytes: 10, SendTimeout: time.Second, FetchMaxBytes: 9, FetchMaxWait: time.Second, MaxInFlight: 1}).Validate() == nil {
		t.Fatal("expected limits error")
	}
}
