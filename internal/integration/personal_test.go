package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	streamapi "github.com/Kale105/streamforge/internal/api"
	"github.com/Kale105/streamforge/internal/collector/httpjson"
	"github.com/Kale105/streamforge/internal/config"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/materializer"
	"github.com/Kale105/streamforge/internal/normalizer"
	"github.com/Kale105/streamforge/internal/pipeline"
	"github.com/Kale105/streamforge/internal/recordstore"
)

func TestPersonalPipelineFromHTTPSourceToPublishedAPI(t *testing.T) {
	t.Parallel()

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"game-42","status":"final","internal":"not-published"}`))
	}))
	defer sourceServer.Close()

	yaml := strings.ReplaceAll(datasetYAML, "SOURCE_URL", sourceServer.URL)
	spec, err := config.DecodeDataset(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("decode dataset: %v", err)
	}
	normalize, err := normalizer.New(spec)
	if err != nil {
		t.Fatalf("construct normalizer: %v", err)
	}
	store := newMemoryStore()
	destination, err := materializer.New(normalize, store, spec.Metadata.Name, spec.Metadata.Version)
	if err != nil {
		t.Fatalf("construct materializer: %v", err)
	}
	source, err := httpjson.New(httpjson.Config{
		URL: sourceServer.URL, Interval: time.Hour, RequestTimeout: time.Second,
		MaxResponseBytes: 1024, Source: "urn:test:games", Type: "test.game.v1",
	}, sourceServer.Client())
	if err != nil {
		t.Fatalf("construct collector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pipelineResult := make(chan error, 1)
	go func() {
		pipelineResult <- pipeline.Run(ctx, pipeline.Config{BufferSize: 1, DrainTimeout: time.Second}, source, destination)
	}()
	select {
	case <-store.ingested:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for ingested record")
	}
	if err := <-pipelineResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline error = %v; want context.Canceled", err)
	}

	handler, err := streamapi.New(store, store, streamapi.Config{
		Datasets: map[string]streamapi.DatasetPolicy{
			"games": {Path: "/v1/games", Version: spec.Metadata.Version, Fields: []string{"game_id", "status"}},
		},
		DefaultPageSize: 20,
		MaximumPageSize: 100,
	})
	if err != nil {
		t.Fatalf("construct API: %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/games", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("API status = %d; body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Records []recordstore.Record `json:"records"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode API response: %v", err)
	}
	if len(body.Records) != 1 || body.Records[0].ID != "game-42" {
		t.Fatalf("API records = %#v", body.Records)
	}
	if got := string(body.Records[0].Data); got != `{"game_id":"game-42","status":"final"}` {
		t.Fatalf("published data = %s", got)
	}
}

const datasetYAML = `
apiVersion: streamforge.dev/v1alpha1
kind: Dataset
metadata: {name: games, version: 1.0.0}
collector:
  type: http-json
  httpJson:
    url: SOURCE_URL
    pollInterval: 1h
    requestTimeout: 1s
    maxBodyBytes: 1024
normalization:
  key: id
  fields:
    - {source: id, target: game_id}
    - {source: status, target: status}
storage: {engine: postgres}
api:
  enabled: true
  path: /v1/games
  fields: [game_id, status]
  filters: []
  sorts: []
  pagination: {type: cursor, default: 20, maximum: 100}
`

type memoryStore struct {
	mu       sync.Mutex
	records  map[string]recordstore.Record
	raw      map[string]struct{}
	states   map[string]string
	ingested chan struct{}
	once     sync.Once
}

func newMemoryStore() *memoryStore {
	return &memoryStore{records: make(map[string]recordstore.Record), raw: make(map[string]struct{}), states: make(map[string]string), ingested: make(chan struct{})}
}

func (s *memoryStore) Admit(_ context.Context, e event.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := e.Source + "\x00" + e.ID
	if _, exists := s.raw[key]; exists {
		return false, nil
	}
	s.raw[key] = struct{}{}
	return true, nil
}

func (s *memoryStore) PrepareMaterialization(_ context.Context, e event.Event, dataset, version string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := e.Source + "\x00" + e.ID + "\x00" + dataset + "\x00" + version
	if s.states[key] == "succeeded" {
		return false, nil
	}
	s.states[key] = "pending"
	return true, nil
}

func (s *memoryStore) FailMaterialization(_ context.Context, e event.Event, dataset, version string, _ error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := e.Source + "\x00" + e.ID + "\x00" + dataset + "\x00" + version
	s.states[key] = "failed"
	return nil
}

func (s *memoryStore) CompleteMaterialization(_ context.Context, e event.Event, record recordstore.Record, dataset, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.Dataset+"\x00"+record.ID] = record
	key := e.Source + "\x00" + e.ID + "\x00" + dataset + "\x00" + version
	s.states[key] = "succeeded"
	s.once.Do(func() { close(s.ingested) })
	return nil
}

func (s *memoryStore) List(_ context.Context, dataset, version string, _ *recordstore.Cursor, limit int) (recordstore.Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	page := recordstore.Page{}
	for _, record := range s.records {
		if record.Dataset == dataset && record.DatasetVersion == version && len(page.Records) < limit {
			page.Records = append(page.Records, record)
		}
	}
	return page, nil
}

func (*memoryStore) Ready(context.Context) error { return nil }
