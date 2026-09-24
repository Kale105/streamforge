package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Kale105/streamforge/internal/coordination"
	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/observability"
	"github.com/Kale105/streamforge/internal/providerdlq"
	"github.com/Kale105/streamforge/internal/providerflow"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/telemetry"
	transportkafka "github.com/Kale105/streamforge/internal/transport/kafka"
)

func TestSourcePartitionKeyUsesLogicalRecordKey(t *testing.T) {
	e := event.Event{ID: "event-fallback", Data: json.RawMessage(`{"game_id":"game-42"}`)}
	if got := sourcePartitionKey(e, "game_id"); got != "game-42" {
		t.Fatalf("partition key = %q", got)
	}
	for _, data := range []string{`{}`, `{"game_id":1}`, `{`} {
		e.Data = json.RawMessage(data)
		if got := sourcePartitionKey(e, "game_id"); got != e.ID {
			t.Fatalf("fallback key for %s = %q", data, got)
		}
	}
}

func TestWorkerAdminReadinessAndBoundedShutdown(t *testing.T) {
	admin, err := startWorkerAdmin("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + admin.listener.Addr().String()
	response, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("liveness status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	response, err = http.Get(url + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("uninitialized readiness status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	admin.SetReady()
	response, err = http.Get(url + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ready status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	if err := admin.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerMetricsAvoidRecordLabels(t *testing.T) {
	metrics := observability.NewRegistry(observability.Config{})
	metrics.WorkerProcessed("processor")
	metrics.WorkerFailure("processor", "decode")
	metrics.WorkerDLQ("processor")
	metrics.WorkerLatency("processor", time.Millisecond)
	text := metrics.Text()
	for _, forbidden := range []string{"event", "payload", "principal", "url", "diagnostic"} {
		if strings.Contains(text, forbidden+"=") {
			t.Fatalf("unbounded metric label %q in %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "role=\"processor\"") || !strings.Contains(text, "class=\"decode\"") {
		t.Fatalf("worker metrics missing fixed labels: %s", text)
	}
}

type captureDLQ struct {
	failures []providerdlq.Failure
	err      error
}

func (d *captureDLQ) PublishFailure(_ context.Context, f providerdlq.Failure) error {
	d.failures = append(d.failures, f)
	return d.err
}

type workerNorm struct{ err error }

func (n workerNorm) Normalize(event.Event) (recordstore.Record, error) {
	if n.err != nil {
		return recordstore.Record{}, n.err
	}
	return recordstore.Record{Dataset: "games", ID: "g1", Data: json.RawMessage(`{"id":"g1"}`), UpdatedAt: time.Now()}, nil
}

func TestProcessorPoisonIsDLQedBeforeAcknowledgement(t *testing.T) {
	dlq := &captureDLQ{}
	p, err := providerflow.NewProcessor(workerNorm{err: errors.New("bad schema")}, providerPublisher{}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	record := transportkafka.RawRecord{Topic: "streamforge.games.raw", Partition: 1, Offset: 2, Timestamp: time.Now(), Key: []byte("g1"), Value: validRawEvent(t, "g1")}
	if err := handleProcessorRecord(context.Background(), record, "local", "games", "v1", "id", "streamforge.games.raw.replay.x", p, dlq); err != nil {
		t.Fatal(err)
	}
	if len(dlq.failures) != 1 || dlq.failures[0].Class != providerdlq.ClassNormalize {
		t.Fatalf("DLQ=%#v", dlq.failures)
	}
	dlq.err = errors.New("DLQ unavailable")
	if err := handleProcessorRecord(context.Background(), record, "local", "games", "v1", "id", "streamforge.games.raw.replay.x", p, dlq); err == nil {
		t.Fatal("DLQ failure was acknowledged")
	}
}

func TestProcessorMalformedAndKeyPoisonAreDLQed(t *testing.T) {
	dlq := &captureDLQ{}
	p, _ := providerflow.NewProcessor(workerNorm{}, providerPublisher{}, "v1")
	for _, record := range []transportkafka.RawRecord{{Topic: "raw", Offset: 1, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("{")}, {Topic: "raw", Offset: 2, Timestamp: time.Now(), Value: validRawEvent(t, "g1")}} {
		if err := handleProcessorRecord(context.Background(), record, "local", "games", "v1", "id", "raw.replay", p, dlq); err != nil {
			t.Fatal(err)
		}
	}
	if len(dlq.failures) != 2 || dlq.failures[0].Class != providerdlq.ClassDecode || dlq.failures[1].Class != providerdlq.ClassKey {
		t.Fatalf("failures=%#v", dlq.failures)
	}
}

func TestProviderDLQRetainsExpectedAndOriginalContracts(t *testing.T) {
	expected := testProviderContract()
	p, err := providerflow.NewProcessor(workerNorm{}, providerPublisher{}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	p.Contract = expected
	dlq := &captureDLQ{}
	malformed := transportkafka.RawRecord{Topic: "raw", Offset: 1, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("{")}
	if err := handleProcessorRecord(context.Background(), malformed, "local", "games", "v1", "id", "raw.replay", p, dlq); err != nil {
		t.Fatal(err)
	}
	if got := dlq.failures[0]; got.ExpectedContract != expected || !got.OriginalContract.Empty() {
		t.Fatalf("malformed DLQ contract = %#v", got)
	}
	original := expected
	original.Transform.Revision = "mapping-v2"
	body, err := json.Marshal(event.Event{SpecVersion: event.SpecVersion, ID: "e2", Source: "test", Type: "upsert", Time: time.Now(), Data: json.RawMessage(`{"id":"g2"}`), Contract: original})
	if err != nil {
		t.Fatal(err)
	}
	record := transportkafka.RawRecord{Topic: "raw", Offset: 2, Timestamp: time.Now(), Key: []byte("g2"), Value: body}
	if err := handleProcessorRecord(context.Background(), record, "local", "games", "v1", "id", "raw.replay", p, dlq); err != nil {
		t.Fatal(err)
	}
	if got := dlq.failures[1]; got.ExpectedContract != expected || got.OriginalContract != original {
		t.Fatalf("mismatch DLQ contract = %#v", got)
	}
}

type providerPublisher struct{}

func (providerPublisher) Publish(context.Context, providerflow.Message) error { return nil }

type traceCapturePublisher struct{ trace string }

func (p *traceCapturePublisher) Publish(ctx context.Context, _ providerflow.Message) error {
	p.trace = telemetry.Traceparent(ctx)
	return nil
}

func TestProcessorCarriesTraceparentAcrossRawToNormalizedContext(t *testing.T) {
	publisher := &traceCapturePublisher{}
	p, err := providerflow.NewProcessor(workerNorm{}, publisher, "v1")
	if err != nil {
		t.Fatal(err)
	}
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	record := transportkafka.RawRecord{Topic: "raw", Offset: 1, Timestamp: time.Now(), Key: []byte("g1"), Value: validRawEvent(t, "g1"), Headers: []transportkafka.Header{{Key: telemetry.TraceparentHeader, Value: []byte(parent)}}}
	metrics := observability.NewRegistry(observability.Config{})
	if err := instrumentProcessorRecord(context.Background(), record, metrics, "local", "games", "v1", "id", "raw.replay", p, &captureDLQ{}); err != nil {
		t.Fatal(err)
	}
	if publisher.trace != parent {
		t.Fatalf("normalized trace=%q", publisher.trace)
	}
	malformed := record
	malformed.Value = []byte("{")
	dlq := &captureDLQ{}
	if err := instrumentProcessorRecord(context.Background(), malformed, metrics, "local", "games", "v1", "id", "raw.replay", p, dlq); err != nil {
		t.Fatal(err)
	}
	if len(dlq.failures) != 1 {
		t.Fatal("malformed raw was not dead-lettered")
	}
	if dlq.failures[0].Traceparent != parent {
		t.Fatalf("DLQ trace=%q", dlq.failures[0].Traceparent)
	}
}
func validRawEvent(t *testing.T, id string) []byte {
	t.Helper()
	b, err := json.Marshal(event.Event{SpecVersion: event.SpecVersion, ID: "e1", Source: "test", Type: "upsert", Time: time.Now(), Data: json.RawMessage(`{"id":"` + id + `"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTransactionalIDsAreStableAndFencingSafe(t *testing.T) {
	a := coordination.Assignment{Dataset: "games", Source: "source", Partition: "default"}
	if collectorTransactionalID("games", "v1", a) != collectorTransactionalID("games", "v1", a) {
		t.Fatal("collector transactional ID is unstable")
	}
	if collectorTransactionalID("games", "v1", a) == collectorTransactionalID("games", "v2", a) {
		t.Fatal("revision should isolate collector transactional ID")
	}
	if replayTransactionalID("request-1") != replayTransactionalID("request-1") {
		t.Fatal("replay transactional ID is unstable")
	}
}

type workerWriter struct{ err error }

func (w workerWriter) Write(context.Context, providerflow.Message) error { return w.err }

func TestSinkPoisonAndTransientFailuresAreSeparated(t *testing.T) {
	dlq := &captureDLQ{}
	bad := transportkafka.RawRecord{Topic: "normalized", Offset: 3, Timestamp: time.Now(), Key: []byte("key"), Value: []byte(`{`)}
	if err := handleSinkRecord(context.Background(), bad, "local", "games", "v1", "normalized", providerflow.Sink{Writer: workerWriter{}}, dlq, 1024); err != nil {
		t.Fatal(err)
	}
	if len(dlq.failures) != 1 || dlq.failures[0].Class != providerdlq.ClassDecode {
		t.Fatalf("DLQ=%#v", dlq.failures)
	}
	message := providerflow.Message{Version: providerflow.Version, SourceEvent: providerflow.SourceEvent{ID: "e", Source: "s", Type: "t", Time: time.Now()}, Record: recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "id", Data: json.RawMessage(`{"id":"id"}`), UpdatedAt: time.Now()}}
	body, err := providerflow.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	record := transportkafka.RawRecord{Topic: "normalized", Offset: 4, Timestamp: time.Now(), Key: []byte(message.IdempotencyKey()), Value: body}
	if err := handleSinkRecord(context.Background(), record, "local", "games", "v1", "normalized", providerflow.Sink{Writer: workerWriter{err: errors.New("idempotency conflict")}}, dlq, 1024); err != nil {
		t.Fatal(err)
	}
	if len(dlq.failures) != 2 || dlq.failures[1].Class != providerdlq.ClassSink {
		t.Fatalf("DLQ=%#v", dlq.failures)
	}
	if err := handleSinkRecord(context.Background(), record, "local", "games", "v1", "normalized", providerflow.Sink{Writer: workerWriter{err: fmt.Errorf("%w: broker", transportkafka.ErrRetryable)}}, dlq, 1024); err == nil {
		t.Fatal("transient failure was acknowledged")
	}
	if len(dlq.failures) != 2 {
		t.Fatal("transient failure was dead-lettered")
	}
}

func TestBoundedDiagnosticPreservesUTF8AndLimit(t *testing.T) {
	got := boundedDiagnostic(strings.Repeat("é", providerflow.MaxDiagnosticBytes))
	if len(got) > providerflow.MaxDiagnosticBytes || !utf8.ValidString(got) {
		t.Fatalf("invalid bounded diagnostic: bytes=%d valid=%t", len(got), utf8.ValidString(got))
	}
}

func TestGeneratedHolderAndContentionDelayAreBounded(t *testing.T) {
	holder := generatedHolder(strings.Repeat("h", coordination.MaxIdentifierBytes), "identifier")
	if len(holder) > coordination.MaxIdentifierBytes || !strings.HasSuffix(holder, "-identifier") {
		t.Fatalf("holder = %q (%d bytes)", holder, len(holder))
	}
	for range 100 {
		delay := contentionDelay()
		if delay < 4*time.Second || delay > 6*time.Second {
			t.Fatalf("contention delay = %s", delay)
		}
	}
}

func TestKafkaDefaultsIsolateDatasetRevisions(t *testing.T) {
	a := kafkaFlags{}
	b := kafkaFlags{}
	a.normalize("games", "v1")
	b.normalize("games", "v2")
	if a.rawTopic != b.rawTopic {
		t.Fatal("raw history must be shared across revisions")
	}
	if a.normalized == b.normalized || a.dlqTopic == b.dlqTopic {
		t.Fatal("normalized and DLQ topics must be revision-isolated")
	}
	if revisionSuffix("v1") != revisionSuffix("v1") || len(revisionSuffix("v1")) != 12 {
		t.Fatal("revision suffix is not stable")
	}
}

func TestKafkaLimitsAreBounded(t *testing.T) {
	limits := (kafkaFlags{maxMessage: defaultKafkaMessageBytes, fetchMaxBytes: defaultKafkaFetchBytes}).limits()
	if err := limits.Validate(); err != nil {
		t.Fatal(err)
	}
	if limits.MaxInFlight != 1 || limits.SendTimeout <= 0 || limits.FetchMaxWait > time.Second {
		t.Fatalf("limits = %#v", limits)
	}
}

func TestKafkaSecurityFlagsValidateWithoutLeakingSecrets(t *testing.T) {
	values := kafkaFlags{
		maxMessage: defaultKafkaMessageBytes, fetchMaxBytes: defaultKafkaFetchBytes,
		tlsEnabled: "true", tlsServerName: "kafka.internal",
		saslMechanism: transportkafka.SASLSCRAMSHA512, saslUsername: "worker", saslPassword: "do-not-log-me",
	}
	security, err := values.security()
	if err != nil {
		t.Fatal(err)
	}
	if !security.TLS.Enabled || security.TLS.ServerName != "kafka.internal" || security.SASL.Mechanism != transportkafka.SASLSCRAMSHA512 {
		t.Fatal("security flags were not propagated")
	}
	values.saslMechanism = "unsupported"
	_, err = values.security()
	if err == nil || strings.Contains(err.Error(), "do-not-log-me") || strings.Contains(err.Error(), "worker") {
		t.Fatalf("unsafe security error: %v", err)
	}
}

func TestKafkaHelpDoesNotExposeEnvironmentCredentials(t *testing.T) {
	t.Setenv("STREAMFORGE_KAFKA_SASL_PASSWORD", "do-not-print-this")
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	var values kafkaFlags
	addKafkaFlags(flags, &values)
	var help bytes.Buffer
	flags.SetOutput(&help)
	flags.PrintDefaults()
	if strings.Contains(help.String(), "do-not-print-this") {
		t.Fatal("Kafka help exposed an environment credential")
	}
	values.saslMechanism = transportkafka.SASLPlain
	values.saslUsername = "worker"
	if _, err := values.security(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderContractRequiresPinnedSchemasAndMappingRevision(t *testing.T) {
	_, _, err := providerContract(dataset.Dataset{})
	if err == nil || !strings.Contains(err.Error(), "pinned raw schema") {
		t.Fatalf("incomplete provider contract error = %v", err)
	}
}

func TestStampCollectedEventCarriesImmutableContract(t *testing.T) {
	contract := testProviderContract()
	original := event.Event{ID: "event-1"}
	stamped := stampCollectedEvent(original, contract)
	if stamped.Contract != contract || !original.Contract.Empty() {
		t.Fatalf("raw contract was not stamped safely: stamped=%#v original=%#v", stamped.Contract, original.Contract)
	}
}

type countingWriter struct{ writes int }

func (w *countingWriter) Write(context.Context, providerflow.Message) error {
	w.writes++
	return nil
}

func TestMismatchedNormalizedContractIsDLQedWithoutMaterialization(t *testing.T) {
	active := testProviderContract()
	writer := &countingWriter{}
	sink, err := providerflow.NewStrictSink(writer, active)
	if err != nil {
		t.Fatal(err)
	}
	mismatch := active
	mismatch.Transform.Revision = "mapping-v2"
	message := providerflow.Message{
		Version:     providerflow.Version,
		SourceEvent: providerflow.SourceEvent{ID: "event-1", Source: "source", Type: "upsert", Time: time.Now()},
		Record:      recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "g1", Data: json.RawMessage(`{"id":"g1"}`), UpdatedAt: time.Now()},
		Contract:    mismatch,
	}
	body, err := providerflow.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	dlq := &captureDLQ{}
	record := transportkafka.RawRecord{Topic: "normalized", Offset: 8, Timestamp: time.Now(), Key: []byte(message.IdempotencyKey()), Value: body}
	if err := handleSinkRecord(context.Background(), record, "local", "games", "v1", "raw.replay", sink, dlq, 1024*1024); err != nil {
		t.Fatal(err)
	}
	if writer.writes != 0 || len(dlq.failures) != 1 || dlq.failures[0].Class != providerdlq.ClassSink {
		t.Fatalf("writes=%d failures=%#v", writer.writes, dlq.failures)
	}
	if dlq.failures[0].ExpectedContract != active || dlq.failures[0].OriginalContract != mismatch {
		t.Fatalf("sink DLQ contracts = %#v", dlq.failures[0])
	}
}

func testProviderContract() event.Contract {
	return event.Contract{
		RawSchema:        event.SchemaIdentity{Revision: "raw-v1", Digest: strings.Repeat("a", 64)},
		NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("b", 64)},
		Transform:        event.TransformIdentity{Revision: "mapping-v1", Digest: strings.Repeat("c", 64)},
	}
}
