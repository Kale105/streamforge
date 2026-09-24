package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validYAML = `
apiVersion: streamforge.dev/v1alpha1
kind: Dataset
metadata:
  name: games
  version: 1.0.0
collector:
  type: http-json
  httpJson:
    url: https://example.test/games
    pollInterval: 30s
    requestTimeout: 5s
    maxBodyBytes: 1048576
normalization:
  key: id
  fields:
    - source: id
      target: id
    - source: status
      target: status
storage:
  engine: postgres
api:
  enabled: true
  path: /v1/games
  fields: [id, status]
  filters: []
  sorts: []
  pagination:
    type: cursor
    default: 20
    maximum: 100
`

func TestDecodeDatasetStrictValidYAML(t *testing.T) {
	t.Parallel()

	spec, err := DecodeDataset(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("DecodeDataset() error = %v", err)
	}
	if spec.Metadata.Name != "games" || spec.Collector.HTTPJSON.PollInterval.String() != "30s" {
		t.Fatalf("decoded spec = %#v", spec)
	}
}

func TestDecodeDatasetValidatesQueryAllowlists(t *testing.T) {
	t.Parallel()

	configured := strings.Replace(validYAML, "filters: []\n  sorts: []", "filters: [status]\n  sorts: [updated_at, id]", 1)
	spec, err := DecodeDataset(strings.NewReader(configured))
	if err != nil {
		t.Fatalf("DecodeDataset() configured query controls: %v", err)
	}
	if got, want := strings.Join(spec.API.Filters, ","), "status"; got != want {
		t.Fatalf("filters = %q, want %q", got, want)
	}
	if got, want := strings.Join(spec.API.Sorts, ","), "updated_at,id"; got != want {
		t.Fatalf("sorts = %q, want %q", got, want)
	}

	for name, replacement := range map[string]string{
		"unknown filter":   "filters: [unknown]\n  sorts: []",
		"duplicate filter": "filters: [status, status]\n  sorts: []",
		"unsupported sort": "filters: []\n  sorts: [status]",
		"duplicate sort":   "filters: []\n  sorts: [id, id]",
	} {
		t.Run(name, func(t *testing.T) {
			contents := strings.Replace(validYAML, "filters: []\n  sorts: []", replacement, 1)
			if _, err := DecodeDataset(strings.NewReader(contents)); err == nil {
				t.Fatal("DecodeDataset() error = nil")
			}
		})
	}
}

func TestLoadDatasetRejectsSchemaBytesChangedUnderSameRevision(t *testing.T) {
	// Keep this under the repository rather than the Windows temp directory:
	// LoadDataset deliberately resolves links to defend against path escapes.
	dir, err := os.MkdirTemp(".", "dataset-pin-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	schemaPath := filepath.Join(dir, "raw.json")
	initial := []byte(`{"type":"object"}`)
	if err := os.WriteFile(schemaPath, initial, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(initial)
	configuration := strings.Replace(validYAML, "storage:\n", fmt.Sprintf("schemas:\n  raw:\n    path: raw.json\n    revision: raw-v1\n    digest: %x\nstorage:\n", digest), 1)
	configPath := filepath.Join(dir, "dataset.yaml")
	if err := os.WriteFile(configPath, []byte(configuration), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDataset(configPath); err != nil {
		t.Fatalf("matching schema pin rejected: %v", err)
	}
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object","additionalProperties":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDataset(configPath); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("changed schema bytes accepted: %v", err)
	}
}

func TestDecodeDatasetRejectsUnknownField(t *testing.T) {
	t.Parallel()

	_, err := DecodeDataset(strings.NewReader(validYAML + "unknown: true\n"))
	if err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("DecodeDataset() error = %v; want unknown-field error", err)
	}
}

func TestDecodeDatasetRejectsRemovedInertFields(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"raw:\n  schema: schemas/game-v1.json\n", "storage:\n  engine: postgres\n  indexes: [[status]]\n"} {
		_, err := DecodeDataset(strings.NewReader(strings.Replace(validYAML, "storage:\n  engine: postgres\n", field, 1)))
		if err == nil || !strings.Contains(err.Error(), "field") {
			t.Fatalf("DecodeDataset() error = %v; want unknown-field error", err)
		}
	}
}

func TestDecodeDatasetRejectsMultipleDocuments(t *testing.T) {
	t.Parallel()

	_, err := DecodeDataset(strings.NewReader(validYAML + "---\n{}\n"))
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("DecodeDataset() error = %v; want single-document error", err)
	}
}

func TestDecodeDatasetRequiresDiscriminatedCollectorBlock(t *testing.T) {
	for name, contents := range map[string]string{
		"flat legacy YAML": strings.Replace(validYAML, "  httpJson:\n    url: https://example.test/games\n    pollInterval: 30s\n    requestTimeout: 5s\n    maxBodyBytes: 1048576\n", "  url: https://example.test/games\n", 1),
		"wrong block":      strings.Replace(validYAML, "httpJson:", "sse:", 1),
		"two blocks":       strings.Replace(validYAML, "normalization:\n", "  sse:\n    url: https://example.test/events\n    requestTimeout: 1s\n    maxMessageBytes: 20\n    maxLineBytes: 20\nnormalization:\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeDataset(strings.NewReader(contents)); err == nil {
				t.Fatal("invalid collector accepted")
			}
		})
	}
}
