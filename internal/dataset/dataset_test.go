package dataset

import (
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/recordstore"
)

func validDataset() Dataset {
	return Dataset{
		APIVersion: APIVersionV1, Kind: KindDataset,
		Metadata:      Metadata{Name: "nba-games", Version: "1.0.0"},
		Collector:     HTTPCollector{Type: CollectorHTTPJSON, URL: "https://example.test/games", PollInterval: time.Minute, RequestTimeout: 10 * time.Second, MaxBodyBytes: 1 << 20},
		Normalization: Normalization{Key: "id", Fields: []FieldMapping{{Source: "id", Target: "game_id"}, {Source: "status", Target: "status"}}},
		Storage:       Storage{Engine: StoragePostgres},
		API:           API{Enabled: true, Path: "/v1/nba-games", Fields: []string{"game_id", "status"}, Pagination: Pagination{Type: PaginationCursor, Default: 20, Maximum: 100}},
	}
}

func TestValidateValidDataset(t *testing.T) {
	if err := validDataset().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAggregatesActionableErrors(t *testing.T) {
	d := validDataset()
	d.APIVersion = "v9"
	d.Collector.Type = "websocket"
	d.Collector.URL = "ftp://example.test"
	d.Normalization.Fields = []FieldMapping{{Source: "id", Target: "x", Transform: "jsonpath"}, {Source: "status", Target: "x"}}
	d.Normalization.Key = "missing"
	d.API.Filters = []string{"unknown"}
	d.API.Pagination.Maximum = 1
	d.API.Pagination.Default = 2
	err := d.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil")
	}
	for _, want := range []string{"apiVersion:", "collector.type:", "normalization.fields[0].transform:", "normalization.fields[1].target:", "normalization.key:", "api.filters[0]:", "api.pagination.default:"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestValidateAPIPath(t *testing.T) {
	t.Parallel()

	invalid := []string{
		"", "/", "/v1/games/", "/v1//games", "/v1/./games", "/v1/../games",
		"/v1/%67ames", "/v1/{game}", "/v1/*", "/v1/game name", "/v1/games?limit=1",
		"/v1/games#section", "/healthz", "/readyz", "/openapi.json", "GET /v1/games", "/v1/{$}",
	}
	for _, path := range invalid {
		t.Run(path, func(t *testing.T) {
			if err := ValidateAPIPath(path); err == nil {
				t.Fatalf("ValidateAPIPath(%q) error = nil", path)
			}
		})
	}
	if err := ValidateAPIPath("/v1/nba-games"); err != nil {
		t.Fatalf("ValidateAPIPath(valid) error = %v", err)
	}
}

func TestValidateRejectsPublishingFieldThatMayBeOmitted(t *testing.T) {
	d := validDataset()
	optional := false
	d.Normalization.Fields = append(d.Normalization.Fields, FieldMapping{Source: "venue", Target: "venue", Required: &optional})
	d.API.Fields = append(d.API.Fields, "venue")
	err := d.Validate()
	if err == nil || !strings.Contains(err.Error(), "cannot publish an optional mapping without a default") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateQueryAllowLists(t *testing.T) {
	t.Parallel()

	valid := validDataset()
	valid.API.Filters = []string{"status"}
	valid.API.Sorts = []string{"updated_at", "id"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() valid query allowlists: %v", err)
	}

	for name, mutate := range map[string]func(*Dataset){
		"unknown filter":   func(d *Dataset) { d.API.Filters = []string{"not_a_field"} },
		"duplicate filter": func(d *Dataset) { d.API.Filters = []string{"status", "status"} },
		"too many filters": func(d *Dataset) {
			d.API.Filters = []string{"status", "status", "status", "status", "status", "status", "status", "status", "status"}
		},
		"unsupported sort": func(d *Dataset) { d.API.Sorts = []string{"status"} },
		"duplicate sort":   func(d *Dataset) { d.API.Sorts = []string{"id", "id"} },
	} {
		t.Run(name, func(t *testing.T) {
			d := validDataset()
			mutate(&d)
			if err := d.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestValidateRejectsPageSizeAboveAbsoluteHardCap(t *testing.T) {
	d := validDataset()
	d.API.Pagination.Maximum = recordstore.MaxPageItems + 1
	if err := d.Validate(); err == nil {
		t.Fatal("Validate accepted maximum above hard cap")
	}
	d = validDataset()
	d.API.Pagination.Default = recordstore.MaxPageItems + 1
	d.API.Pagination.Maximum = recordstore.MaxPageItems + 1
	if err := d.Validate(); err == nil {
		t.Fatal("Validate accepted default above hard cap")
	}
}

func TestValidateCollectorDiscriminatedAndSecretSafe(t *testing.T) {
	d := validDataset()
	d.Collector.HTTPJSON = &HTTPJSONCollector{URL: "https://example.test/data", PollInterval: time.Second, RequestTimeout: time.Second, MaxBodyBytes: 100, Headers: map[string]string{"Authorization": "Bearer secret"}}
	if err := d.Validate(); err == nil || !strings.Contains(err.Error(), "env:NAME") {
		t.Fatalf("literal secret accepted: %v", err)
	}
	d.Collector.HTTPJSON.Headers = map[string]string{"Host": "env:TEST_TOKEN"}
	if err := d.Validate(); err == nil || !strings.Contains(err.Error(), "must not set") {
		t.Fatalf("host header accepted: %v", err)
	}
	d.Collector.HTTPJSON.Headers = nil
	d.Collector.SSE = &SSECollector{}
	if err := d.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("multiple collector blocks accepted: %v", err)
	}
}

func TestValidateSubprocessSafety(t *testing.T) {
	d := validDataset()
	d.Collector = HTTPCollector{Type: CollectorSubprocess, Subprocess: &SubprocessCollector{Path: "relative", Args: []string{"--key=${TOKEN}"}, Environment: map[string]string{"PATH": "env:PATH"}}}
	err := d.Validate()
	for _, want := range []string{"absolute path", "must not interpolate", "reserved"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}
