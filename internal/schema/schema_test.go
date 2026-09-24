package schema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateFixtureAndUnsupportedKeywords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.json")
	if err := os.WriteFile(path, []byte(`{"type":"object","required":["id"],"properties":{"id":{"type":"string","minLength":1},"score":{"type":"integer"}},"additionalProperties":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	d, err := Load(path, "raw-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Validate([]byte(`{"id":"g1","score":2}`)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"score":2}`, `{"id":"","score":2}`, `{"id":"g1","extra":true}`, `{"id":"g1","score":2.5}`} {
		if err := d.Validate([]byte(body)); err == nil {
			t.Errorf("Validate(%s) = nil", body)
		}
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"$ref":"https://example.test/schema"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad, "bad"); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("Load unsupported = %v", err)
	}
}

func TestEnumPreservesLargeJSONNumbers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large-enum.json")
	if err := os.WriteFile(path, []byte(`{"type":"integer","enum":[9007199254740993]}`), 0600); err != nil {
		t.Fatal(err)
	}
	document, err := Load(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := document.Validate([]byte(`9007199254740993`)); err != nil {
		t.Fatalf("matching large integer rejected: %v", err)
	}
	if err := document.Validate([]byte(`9007199254740992`)); err == nil {
		t.Fatal("different large integer matched enum")
	}
}

func TestMinimumUsesExactJSONNumberComparison(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "minimum.json")
	if err := os.WriteFile(path, []byte(`{"minimum":9007199254740993}`), 0600); err != nil {
		t.Fatal(err)
	}
	document, err := Load(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := document.Validate([]byte(`9007199254740993`)); err != nil {
		t.Fatalf("minimum boundary rejected: %v", err)
	}
	if err := document.Validate([]byte(`9007199254740992`)); err == nil {
		t.Fatal("value below an exact large minimum was accepted")
	}

	path = filepath.Join(dir, "decimal-minimum.json")
	if err := os.WriteFile(path, []byte(`{"minimum":1e-1}`), 0600); err != nil {
		t.Fatal(err)
	}
	document, err = Load(path, "v2")
	if err != nil {
		t.Fatal(err)
	}
	if err := document.Validate([]byte(`0.10`)); err != nil {
		t.Fatalf("equivalent decimal/exponent rejected: %v", err)
	}
	if err := document.Validate([]byte(`0.099`)); err == nil {
		t.Fatal("value below decimal minimum was accepted")
	}
}

func TestNumbersAndIntegersAreNotLimitedToInt64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "integer.json")
	if err := os.WriteFile(path, []byte(`{"type":"integer"}`), 0600); err != nil {
		t.Fatal(err)
	}
	document, err := Load(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{`9007199254740993`, `9223372036854775808`, `1e3`} {
		if err := document.Validate([]byte(value)); err != nil {
			t.Errorf("integer %s rejected: %v", value, err)
		}
	}
	if err := document.Validate([]byte(`1.5`)); err == nil {
		t.Fatal("fractional value accepted as integer")
	}

	for _, body := range []string{`{"minimum":"1"}`, `{"minimum":NaN}`, `{"minimum":Infinity}`} {
		bad := filepath.Join(dir, "bad-number.json")
		if err := os.WriteFile(bad, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(bad, "bad"); err == nil {
			t.Errorf("Load(%s) accepted invalid minimum", body)
		}
	}
}

func TestEmptyEnumAndNilDocumentAreRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-enum.json")
	if err := os.WriteFile(path, []byte(`{"enum":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, "v1"); err == nil {
		t.Fatal("empty enum was accepted")
	}
	var document *Document
	if err := document.Validate([]byte(`{}`)); err == nil {
		t.Fatal("nil document was accepted")
	}
}

func TestLoadPinnedRejectsChangedBytesUnderSameRevision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(path, []byte(`{"type":"object"}`), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := Load(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPinned(path, "v1", first.Digest); err != nil {
		t.Fatalf("matching pin rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"object","additionalProperties":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPinned(path, "v1", first.Digest); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("changed bytes accepted under same revision: %v", err)
	}
}
