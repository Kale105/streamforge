package normalizer

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
)

func spec() dataset.Dataset {
	return dataset.Dataset{APIVersion: dataset.APIVersionV1, Kind: dataset.KindDataset,
		Metadata:      dataset.Metadata{Name: "games", Version: "1"},
		Collector:     dataset.HTTPCollector{Type: dataset.CollectorHTTPJSON, URL: "https://example.test", PollInterval: time.Minute, RequestTimeout: 5 * time.Second, MaxBodyBytes: 100},
		Normalization: dataset.Normalization{Key: "id", Fields: []dataset.FieldMapping{{Source: "name", Target: "game_name"}, {Source: "id", Target: "game_id"}}},
		Storage:       dataset.Storage{Engine: dataset.StoragePostgres},
		API:           dataset.API{Enabled: true, Path: "/v1/games", Fields: []string{"game_name", "game_id"}, Pagination: dataset.Pagination{Type: dataset.PaginationCursor, Default: 1, Maximum: 10}},
	}
}

func TestNormalizeMappingAndDeterminism(t *testing.T) {
	n, err := New(spec())
	if err != nil {
		t.Fatal(err)
	}
	e := event.Event{Time: time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC), Data: []byte(`{ "name" : "Final", "extra": 7, "id": "g-1" }`)}
	before := append([]byte(nil), e.Data...)
	first, err := n.Normalize(e)
	if err != nil {
		t.Fatal(err)
	}
	second, err := n.Normalize(e)
	if err != nil {
		t.Fatal(err)
	}
	if first.Dataset != "games" || first.ID != "g-1" || !first.UpdatedAt.Equal(e.Time) {
		t.Fatalf("record = %#v", first)
	}
	if got, want := string(first.Data), `{"game_id":"g-1","game_name":"Final"}`; got != want {
		t.Errorf("data = %s, want %s", got, want)
	}
	if !bytes.Equal(first.Data, second.Data) {
		t.Errorf("same input yielded different data: %s vs %s", first.Data, second.Data)
	}
	if !bytes.Equal(e.Data, before) {
		t.Errorf("Normalize mutated event data: got %s, want %s", e.Data, before)
	}
}

func TestNormalizeRejectsMissingKeyAndSource(t *testing.T) {
	n, err := New(spec())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, data, want string }{
		{"key", `{"name":"Final"}`, `normalization.key: source field "id" is missing`},
		{"mapped source", `{"id":"g-1"}`, `normalization.fields: source field "name" is missing`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := n.Normalize(event.Event{Data: []byte(tc.data)})
			if err == nil || err.Error() != tc.want {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNormalizeRejectsNonObjectPayload(t *testing.T) {
	n, err := New(spec())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`[]`, `null`, `not-json`} {
		if _, err := n.Normalize(event.Event{Data: []byte(data)}); err == nil {
			t.Errorf("Normalize(%s) error = nil", data)
		}
	}
}

func TestNormalizeSchemasDefaultsConversionsAndDeterminism(t *testing.T) {
	dir := t.TempDir()
	raw := filepath.Join(dir, "raw.json")
	normalized := filepath.Join(dir, "normalized.json")
	if err := os.WriteFile(raw, []byte(`{"type":"object","required":["id","points"],"properties":{"id":{"type":"string"},"points":{"type":"string"}},"additionalProperties":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(normalized, []byte(`{"type":"object","required":["game_id","points","active"],"properties":{"game_id":{"type":"string"},"points":{"type":"integer"},"active":{"type":"boolean"}},"additionalProperties":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	s := spec()
	s.Normalization.Fields = []dataset.FieldMapping{{Source: "id", Target: "game_id"}, {Source: "points", Target: "points", Transform: "integer"}, {Source: "active", Target: "active", Default: true, Transform: "boolean"}}
	s.API.Fields = []string{"game_id", "points", "active"}
	s.Schemas = dataset.Schemas{Raw: dataset.SchemaReference{Path: raw, Revision: "raw-1"}, Normalized: dataset.SchemaReference{Path: normalized, Revision: "normalized-1"}}
	n, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	e := event.Event{Data: []byte(`{"id":"g1","points":"12"}`)}
	a, err := n.Normalize(e)
	if err != nil {
		t.Fatal(err)
	}
	b, err := n.Normalize(e)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(a.Data), `{"active":true,"game_id":"g1","points":12}`; got != want {
		t.Fatalf("data = %s, want %s", got, want)
	}
	if !bytes.Equal(a.Data, b.Data) {
		t.Fatal("normalization was not deterministic")
	}
	if _, err := n.Normalize(event.Event{Data: []byte(`{"id":"g1","points":12}`)}); err == nil {
		t.Fatal("raw schema type drift was accepted")
	}
}

func TestNormalizeCanOmitExplicitlyOptionalField(t *testing.T) {
	s := spec()
	optional := false
	s.Normalization.Fields = append(s.Normalization.Fields, dataset.FieldMapping{Source: "venue", Target: "venue", Required: &optional})
	n, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	record, err := n.Normalize(event.Event{Time: time.Now(), Data: []byte(`{"id":"g1","name":"Final"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(record.Data, []byte("venue")) {
		t.Fatalf("optional missing field was emitted: %s", record.Data)
	}
}

func TestNumericConversionsPreserveExactJSONNumberSpelling(t *testing.T) {
	for _, tc := range []struct {
		kind, input, want string
	}{
		{"number", `"9007199254740993"`, `9007199254740993`},
		{"number", `"1.2300e+4"`, `1.2300e+4`},
		{"integer", `"9223372036854775808"`, `9223372036854775808`},
		{"integer", `"1e3"`, `1e3`},
		{"number", `1.2300e+4`, `1.2300e+4`},
	} {
		t.Run(tc.kind+"/"+tc.input, func(t *testing.T) {
			got, err := convert([]byte(tc.input), tc.kind)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("convert() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestNumericConversionsRejectInvalidAndNonFiniteStrings(t *testing.T) {
	for _, kind := range []string{"number", "integer"} {
		for _, value := range []string{"NaN", "Infinity", "-Infinity", "1e", "01", "+1"} {
			t.Run(kind+"/"+value, func(t *testing.T) {
				if _, err := convert([]byte(`"`+value+`"`), kind); err == nil {
					t.Errorf("convert accepted %q", value)
				}
			})
		}
	}
	if _, err := convert([]byte(`"1.5"`), "integer"); err == nil {
		t.Fatal("integer conversion accepted fractional string")
	}
}

func TestMappingDigestIsOrderIndependentAndContentSensitive(t *testing.T) {
	a := dataset.Normalization{Key: "id", Fields: []dataset.FieldMapping{
		{Source: "id", Target: "id"}, {Source: "score", Target: "score", Transform: "integer"},
	}}
	b := dataset.Normalization{Key: "id", Fields: []dataset.FieldMapping{
		{Source: "score", Target: "score", Transform: "integer"}, {Source: "id", Target: "id"},
	}}
	if MappingDigest(a) != MappingDigest(b) {
		t.Fatalf("equivalent mapping order changed digest: %s != %s", MappingDigest(a), MappingDigest(b))
	}
	b.Fields[0].Transform = "number"
	if MappingDigest(a) == MappingDigest(b) {
		t.Fatal("mapping change did not alter digest")
	}
}
