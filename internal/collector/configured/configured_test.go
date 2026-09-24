package configured

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/collector/sse"
	"github.com/Kale105/streamforge/internal/collector/subprocess"
	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
)

func testSpec(serverURL string) dataset.Dataset {
	return dataset.Dataset{APIVersion: dataset.APIVersionV1, Kind: dataset.KindDataset, Metadata: dataset.Metadata{Name: "games", Version: "v1"}, Collector: dataset.HTTPCollector{Type: dataset.CollectorHTTPJSON, HTTPJSON: &dataset.HTTPJSONCollector{URL: serverURL, PollInterval: time.Hour, RequestTimeout: time.Second, MaxBodyBytes: 1024, Headers: map[string]string{"Authorization": "env:TEST_TOKEN"}}}, Normalization: dataset.Normalization{Key: "id", Fields: []dataset.FieldMapping{{Source: "id", Target: "id"}}}, Storage: dataset.Storage{Engine: dataset.StoragePostgres}, API: dataset.API{Enabled: true, Path: "/v1/games", Fields: []string{"id"}, Pagination: dataset.Pagination{Type: dataset.PaginationCursor, Default: 1, Maximum: 1}}}
}

func TestBuildResolvesHeadersWithoutLeakingSecretsAndStampsAdmission(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test" {
			t.Errorf("header=%q", got)
		}
		_, _ = w.Write([]byte(`{"id":"one"}`))
	}))
	defer server.Close()
	spec := testSpec(server.URL)
	result, err := Build(spec, Options{Mode: ModePersonal, LookupEnv: func(name string) (string, bool) { return "Bearer test", name == "TEST_TOKEN" }})
	if err != nil {
		t.Fatal(err)
	}
	var got event.Event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = result.Collector.Collect(ctx, func(_ context.Context, e event.Event) error { got = e; return errors.New("stop") })
	if err == nil || got.Source != Source(spec) || got.Type != Type(spec) || !got.Contract.Empty() {
		t.Fatalf("admission event=%#v err=%v", got, err)
	}
	if strings.Contains(err.Error(), "Bearer test") {
		t.Fatalf("secret leaked: %v", err)
	}
}

func TestBuildRejectsModeAndMissingSecret(t *testing.T) {
	spec := testSpec("https://example.test/data")
	if _, err := Build(spec, Options{Mode: ModePersonal}); err == nil || strings.Contains(err.Error(), "env:") {
		t.Fatalf("missing secret error=%v", err)
	}
	spec.Collector.Type = dataset.CollectorSSE
	spec.Collector.HTTPJSON = nil
	spec.Collector.SSE = &dataset.SSECollector{URL: "https://example.test/events", RequestTimeout: time.Second, MaxMessageBytes: 64, MaxLineBytes: 64}
	if _, err := Build(spec, Options{Mode: ModePersonal}); err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("personal SSE error=%v", err)
	}
}

func TestAdmissionRejectsForeignSourceAndOverridesConnectorAuthority(t *testing.T) {
	a := admission{inner: collectorFunc(func(ctx context.Context, emit collector.EmitFunc) error {
		return emit(ctx, event.Event{SpecVersion: event.SpecVersion, ID: "one", Source: "foreign", Type: "foreign", Time: time.Now(), Data: json.RawMessage(`{}`)})
	}), source: "assigned", typ: "host.type", contract: event.Contract{}}
	err := a.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("error=%v", err)
	}
}

func TestBuildSSEInjectsCursorAndCommitHook(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Last-Event-ID"); got != "prior" {
			t.Errorf("Last-Event-ID = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: next\ndata: {\"id\":\"one\"}\n\n"))
	}))
	defer server.Close()
	spec := testSpec(server.URL)
	spec.Collector.Type, spec.Collector.HTTPJSON = dataset.CollectorSSE, nil
	spec.Collector.SSE = &dataset.SSECollector{URL: server.URL, RequestTimeout: time.Second, MaxMessageBytes: 128, MaxLineBytes: 128}
	var committed struct {
		sequence uint64
		id       string
	}
	result, err := Build(spec, Options{Mode: ModeProvider, Contract: completeContract(), SSEInitialCursor: sse.Cursor{Sequence: 5, LastEventID: "prior"}, SSECommit: func(_ context.Context, sequence uint64, id, _ string) error {
		committed.sequence, committed.id = sequence, id
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	err = result.Collector.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if err == nil || !errors.Is(err, sse.ErrRetryable) {
		t.Fatalf("Collect error = %v", err)
	}
	if committed.sequence != 6 || committed.id != "next" {
		t.Fatalf("commit = %#v", committed)
	}
}

func TestRetryableUsesSubprocessClassification(t *testing.T) {
	if retryable(subprocess.RemoteError{Retryable: false}) {
		t.Fatal("permanent connector error classified retryable")
	}
	if !retryable(fmt.Errorf("wrapped: %w", subprocess.ErrRetryable)) {
		t.Fatal("retryable subprocess error not classified")
	}
}

func completeContract() event.Contract {
	return event.Contract{RawSchema: event.SchemaIdentity{Revision: "raw-v1", Digest: strings.Repeat("a", 64)}, NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("b", 64)}, Transform: event.TransformIdentity{Revision: "map-v1", Digest: strings.Repeat("c", 64)}}
}

type collectorFunc func(context.Context, collector.EmitFunc) error

func (f collectorFunc) Collect(ctx context.Context, emit collector.EmitFunc) error {
	return f(ctx, emit)
}
