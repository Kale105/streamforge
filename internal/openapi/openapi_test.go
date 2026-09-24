package openapi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/dataset"
)

func TestGenerateGoldenAndDeterministic(t *testing.T) {
	t.Parallel()
	schemaPath := filepath.Join(t.TempDir(), "normalized.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object","properties":{"game_id":{"type":"integer","minimum":1},"status":{"type":"string","enum":["scheduled","final"]}},"required":["game_id","status"],"additionalProperties":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := Generate([]dataset.Dataset{testDataset("nba-games", "/v1/nba-games", schemaPath), testDataset("mlb-games", "/v1/mlb-games", schemaPath)}, Options{Title: "Sports API", Version: "2026.09", APIKeyEnabled: true})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	again, err := Generate([]dataset.Dataset{testDataset("mlb-games", "/v1/mlb-games", schemaPath), testDataset("nba-games", "/v1/nba-games", schemaPath)}, Options{Title: "Sports API", Version: "2026.09", APIKeyEnabled: true})
	if err != nil {
		t.Fatalf("second Generate() error = %v", err)
	}
	if string(doc) != string(again) {
		t.Fatalf("generation is not deterministic\nfirst: %s\nsecond: %s", doc, again)
	}

	var golden map[string]any
	if err := json.Unmarshal(doc, &golden); err != nil {
		t.Fatalf("generated JSON is invalid: %v", err)
	}
	if golden["openapi"] != "3.1.0" {
		t.Errorf("openapi = %v", golden["openapi"])
	}
	components := golden["components"].(map[string]any)
	scheme := components["securitySchemes"].(map[string]any)["ApiKeyAuth"].(map[string]any)
	if scheme["type"] != "apiKey" || scheme["in"] != "header" || scheme["name"] != "X-API-Key" {
		t.Errorf("unexpected API key scheme: %#v", scheme)
	}
	paths := golden["paths"].(map[string]any)
	operation := paths["/v1/nba-games"].(map[string]any)["get"].(map[string]any)
	if operation["operationId"] != "listNbaGames" {
		t.Errorf("operation ID = %v", operation["operationId"])
	}
	params := operation["parameters"].([]any)
	if params[0].(map[string]any)["name"] != "limit" || params[1].(map[string]any)["name"] != "cursor" {
		t.Errorf("parameters = %#v", params)
	}
	response := operation["responses"].(map[string]any)["200"].(map[string]any)
	record := response["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)["properties"].(map[string]any)["records"].(map[string]any)["items"].(map[string]any)
	data := record["properties"].(map[string]any)["data"].(map[string]any)
	if data["properties"].(map[string]any)["game_id"].(map[string]any)["type"] != "integer" {
		t.Errorf("normalized field schema = %#v", data)
	}
	if _, ok := operation["security"]; !ok {
		t.Error("protected operation has no security requirement")
	}
}

func TestGenerateFallbackAndRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	spec := testDataset("nba-games", "/v1/nba-games", "")
	doc, err := Generate([]dataset.Dataset{spec}, Options{})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if !strings.Contains(string(doc), "No normalized schema is configured") {
		t.Fatalf("fallback limitation missing: %s", doc)
	}
	if _, err := Generate([]dataset.Dataset{spec, testDataset("mlb-games", "/v1/nba-games", "")}, Options{}); err == nil || !strings.Contains(err.Error(), "duplicate API path") {
		t.Fatalf("duplicate path error = %v", err)
	}
	broken := spec
	broken.API.Path = "/invalid/"
	if _, err := Generate([]dataset.Dataset{broken}, Options{}); err == nil || !strings.Contains(err.Error(), "api.path") {
		t.Fatalf("validation error = %v", err)
	}
	if _, err := Generate([]dataset.Dataset{spec}, Options{APIKeyEnabled: true, APIKeyHeader: "bad header"}); err == nil {
		t.Fatal("invalid header was accepted")
	}
}

func TestQueryParametersMatchConfiguredSortContract(t *testing.T) {
	t.Parallel()
	noSort := testDataset("nba-games", "/v1/nba-games", "")
	doc, err := Generate([]dataset.Dataset{noSort}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(doc, &decoded); err != nil {
		t.Fatal(err)
	}
	operation := decoded["paths"].(map[string]any)["/v1/nba-games"].(map[string]any)["get"].(map[string]any)
	if operation["x-streamforge-max-response-bytes"] == nil {
		t.Fatal("response byte budget not advertised")
	}
	for _, parameter := range operation["parameters"].([]any) {
		name := parameter.(map[string]any)["name"]
		if name == "sort" || name == "direction" {
			t.Fatalf("unconfigured sort parameter advertised: %s", name)
		}
	}

	withSort := noSort
	withSort.API.Sorts = []string{"id"}
	doc, err = Generate([]dataset.Dataset{withSort}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(doc, &decoded); err != nil {
		t.Fatal(err)
	}
	parameters := decoded["paths"].(map[string]any)["/v1/nba-games"].(map[string]any)["get"].(map[string]any)["parameters"].([]any)
	for _, parameter := range parameters {
		entry := parameter.(map[string]any)
		if entry["name"] == "sort" {
			enum := entry["schema"].(map[string]any)["enum"].([]any)
			if len(enum) != 1 || enum[0] != "id" {
				t.Fatalf("sort enum = %#v; want exactly id", enum)
			}
			return
		}
	}
	t.Fatal("configured sort parameter missing")
}

func testDataset(name, path, normalizedSchema string) dataset.Dataset {
	return dataset.Dataset{APIVersion: dataset.APIVersionV1, Kind: dataset.KindDataset,
		Metadata:      dataset.Metadata{Name: name, Version: "1.2.3"},
		Collector:     dataset.HTTPCollector{Type: dataset.CollectorHTTPJSON, URL: "https://example.test/records", PollInterval: time.Minute, RequestTimeout: time.Second, MaxBodyBytes: 1},
		Normalization: dataset.Normalization{Key: "id", Fields: []dataset.FieldMapping{{Source: "id", Target: "game_id", Transform: "integer"}, {Source: "status", Target: "status"}}},
		Schemas:       dataset.Schemas{Normalized: dataset.SchemaReference{Path: normalizedSchema, Revision: map[bool]string{true: "1"}[normalizedSchema != ""]}},
		Storage:       dataset.Storage{Engine: dataset.StoragePostgres},
		API:           dataset.API{Enabled: true, Path: path, Fields: []string{"game_id", "status"}, Pagination: dataset.Pagination{Type: dataset.PaginationCursor, Default: 20, Maximum: 100}},
	}
}
