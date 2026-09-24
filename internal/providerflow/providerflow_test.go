package providerflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/transport"
)

func message() Message {
	return Message{Version: Version, SourceEvent: SourceEvent{ID: "e1", Source: "provider", Type: "upsert", Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, Record: recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "r1", Data: json.RawMessage(`{"z":1,"a":2}`), UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}}
}
func TestMessageCanonicalAndBounded(t *testing.T) {
	a := message()
	b := message()
	b.Record.Data = json.RawMessage(` { "a":2, "z":1 } `)
	ab, err := Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(ab) != string(bb) {
		t.Fatalf("not deterministic: %s != %s", ab, bb)
	}
	if _, err := Unmarshal(ab, len(ab)-1); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want size error, got %v", err)
	}
}

func TestMessageCanonicalizationPreservesLargeIntegers(t *testing.T) {
	m := message()
	m.Record.Data = json.RawMessage(`{"id":900719925474099312345}`)
	body, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`900719925474099312345`)) {
		t.Fatalf("large integer was changed: %s", body)
	}
}
func TestPublishFailurePreventsRawAcknowledgement(t *testing.T) {
	p := &Processor{Normalizer: norm{record: message().Record}, Publisher: pub{err: errors.New("broker down")}, DatasetVersion: "v1"}
	committed := false
	err := handleRaw(context.Background(), p.HandleRaw, raw(), func(context.Context) error { committed = true; return nil })
	if err == nil || committed {
		t.Fatalf("failure was acknowledged: %v, %v", err, committed)
	}
}

func TestNormalizationFailureIsClassifiedAsRejected(t *testing.T) {
	p := &Processor{Normalizer: norm{err: errors.New("bad payload")}, Publisher: pub{}, DatasetVersion: "v1"}
	if err := p.HandleRaw(context.Background(), raw()); !errors.Is(err, ErrRejected) {
		t.Fatalf("error = %v, want ErrRejected", err)
	}
}
func TestWriterFailurePreventsNormalizedAcknowledgementAndDuplicatesAreSafe(t *testing.T) {
	w := &writer{seen: map[string]bool{}}
	s := Sink{Writer: w}
	m := message()
	committed := false
	w.err = errors.New("database down")
	if err := handleNormalized(context.Background(), s.Handle, m, func(context.Context) error { committed = true; return nil }); err == nil || committed {
		t.Fatal("failed durability was acknowledged")
	}
	w.err = nil
	for range 2 {
		if err := handleNormalized(context.Background(), s.Handle, m, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if w.applied != 1 {
		t.Fatalf("duplicate was not idempotent: %d writes", w.applied)
	}
}

func TestKafkaMessageKeyMustMatchIdentity(t *testing.T) {
	body, err := Marshal(message())
	if err != nil {
		t.Fatal(err)
	}
	w := &writer{seen: map[string]bool{}}
	if err := handleKafkaMessage(context.Background(), "wrong", body, Sink{Writer: w}, len(body)); err == nil {
		t.Fatal("mismatched Kafka key was accepted")
	}
	if w.applied != 0 {
		t.Fatal("mismatched message reached writer")
	}
}

func completeContract() event.Contract {
	return event.Contract{
		RawSchema:        event.SchemaIdentity{Revision: "raw-v1", Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		Transform:        event.TransformIdentity{Revision: "mapping-v1", Digest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
	}
}

func TestStrictProviderContractMismatchCannotReachSink(t *testing.T) {
	contract := completeContract()
	w := &writer{seen: map[string]bool{}}
	sink, err := NewStrictSink(w, contract)
	if err != nil {
		t.Fatal(err)
	}
	m := message()
	m.Contract = contract
	m.Contract.Transform.Digest = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	if err := sink.Handle(context.Background(), m); err == nil {
		t.Fatal("mismatched contract reached strict sink")
	}
	if w.applied != 0 {
		t.Fatalf("mismatched contract was written: %d", w.applied)
	}
}

func TestStrictProcessorRejectsRawContractMismatch(t *testing.T) {
	contract := completeContract()
	p, err := NewStrictProcessor(norm{record: message().Record}, pub{}, "v1", contract)
	if err != nil {
		t.Fatal(err)
	}
	e := raw()
	e.Event.Contract = contract
	e.Event.Contract.RawSchema.Revision = "raw-v2"
	if err := p.HandleRaw(context.Background(), e); !errors.Is(err, ErrRejected) {
		t.Fatalf("raw contract mismatch = %v, want rejected", err)
	}
}
func handleRaw(ctx context.Context, h transport.Handler, e transport.Envelope, ack func(context.Context) error) error {
	if err := h(ctx, e); err != nil {
		return err
	}
	return ack(ctx)
}
func handleNormalized(ctx context.Context, h func(context.Context, Message) error, m Message, ack func(context.Context) error) error {
	if err := h(ctx, m); err != nil {
		return err
	}
	return ack(ctx)
}
func raw() transport.Envelope {
	return transport.Envelope{Topic: "raw", PartitionKey: "k", Event: event.Event{SpecVersion: event.SpecVersion, ID: "e1", Source: "provider", Type: "upsert", Time: time.Now(), Data: json.RawMessage(`{"id":"r1"}`)}}
}

type norm struct {
	record recordstore.Record
	err    error
}

func (n norm) Normalize(event.Event) (recordstore.Record, error) { return n.record, n.err }

type pub struct{ err error }

func (p pub) Publish(context.Context, Message) error { return p.err }

type writer struct {
	seen    map[string]bool
	applied int
	err     error
}

func (w *writer) Write(_ context.Context, m Message) error {
	if w.err != nil {
		return w.err
	}
	if !w.seen[m.IdempotencyKey()] {
		w.seen[m.IdempotencyKey()] = true
		w.applied++
	}
	return nil
}
