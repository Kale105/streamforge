// Package schema implements a deliberately bounded, local JSON Schema subset.
// It supports object/array/scalar type checks, properties, required,
// additionalProperties, enum, minimum and minLength. Remote references, $ref,
// composition, patterns, formats and unknown keywords are rejected at load time.
package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"sort"
)

const MaximumBytes = 1 << 20

// jsonNumberPattern is deliberately narrower than big.Rat's input grammar: it
// accepts exactly the number tokens permitted by JSON.
var jsonNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

// ParseJSONNumber validates a JSON number token and returns its exact value.
// The caller can retain the original string when its spelling matters.
func ParseJSONNumber(s string) (*big.Rat, error) {
	if !jsonNumberPattern.MatchString(s) {
		return nil, fmt.Errorf("invalid JSON number %q", s)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid JSON number %q", s)
	}
	return r, nil
}

// IsJSONInteger reports whether s is a valid JSON number with an integral
// value. It does not impose an int64-sized limit.
func IsJSONInteger(s string) bool {
	r, err := ParseJSONNumber(s)
	return err == nil && r.IsInt()
}

type Document struct {
	root     node
	Revision string
	// Digest is the lowercase SHA-256 hex digest of the bytes that were
	// parsed. It is retained with the compiled schema for pipeline identity.
	Digest string
}
type node struct {
	Type       string
	Properties map[string]node
	Required   map[string]bool
	Additional *bool
	Enum       []json.RawMessage
	Minimum    *json.Number
	MinLength  *int
	Items      *node
}

func Load(path, revision string) (*Document, error) {
	if path == "" || revision == "" {
		return nil, errors.New("schema path and revision are required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open schema: %w", err)
	}
	defer f.Close()
	b, err := readBounded(f)
	if err != nil {
		return nil, err
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode schema: %w", err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("schema must contain one JSON value")
	}
	n, err := compile(raw, "$")
	if err != nil {
		return nil, err
	}
	return &Document{root: n, Revision: revision, Digest: digest(b)}, nil
}

// LoadPinned verifies the SHA-256 digest before compiling a schema. It is the
// configuration activation path: an edited file cannot silently retain an
// old revision label. Load remains for legacy personal-mode callers that do
// not activate a provider pipeline.
func LoadPinned(path, revision, expectedDigest string) (*Document, error) {
	if expectedDigest == "" {
		return nil, errors.New("schema SHA-256 digest is required")
	}
	document, err := Load(path, revision)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalDigest(expectedDigest)
	if err != nil {
		return nil, err
	}
	if document.Digest != canonical {
		return nil, fmt.Errorf("schema SHA-256 digest mismatch: configured %s, loaded %s", canonical, document.Digest)
	}
	return document, nil
}

func digest(bytes []byte) string {
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func canonicalDigest(value string) (string, error) {
	if len(value) != sha256.Size*2 {
		return "", errors.New("schema SHA-256 digest must be 64 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.New("schema SHA-256 digest must be 64 hexadecimal characters")
	}
	return hex.EncodeToString(decoded), nil
}
func readBounded(r io.Reader) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, MaximumBytes+1))
	if e != nil {
		return nil, e
	}
	if len(b) > MaximumBytes {
		return nil, fmt.Errorf("schema exceeds %d bytes", MaximumBytes)
	}
	return b, nil
}

func compile(raw map[string]json.RawMessage, path string) (node, error) {
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "additionalProperties": true, "enum": true, "minimum": true, "minLength": true, "items": true, "$schema": true, "title": true, "description": true}
	for k := range raw {
		if !allowed[k] {
			return node{}, fmt.Errorf("%s.%s: unsupported schema keyword", path, k)
		}
	}
	var n node
	if v := raw["type"]; v != nil {
		if err := json.Unmarshal(v, &n.Type); err != nil {
			return n, fmt.Errorf("%s.type: must be string", path)
		}
		if n.Type != "object" && n.Type != "array" && n.Type != "string" && n.Type != "integer" && n.Type != "number" && n.Type != "boolean" && n.Type != "null" {
			return n, fmt.Errorf("%s.type: unsupported type", path)
		}
	}
	if v := raw["required"]; v != nil {
		var a []string
		if err := json.Unmarshal(v, &a); err != nil {
			return n, fmt.Errorf("%s.required: must be strings", path)
		}
		n.Required = map[string]bool{}
		for _, x := range a {
			n.Required[x] = true
		}
	}
	if v := raw["additionalProperties"]; v != nil {
		var b bool
		if err := json.Unmarshal(v, &b); err != nil {
			return n, fmt.Errorf("%s.additionalProperties: schemas are unsupported", path)
		}
		n.Additional = &b
	}
	if v := raw["properties"]; v != nil {
		var p map[string]json.RawMessage
		if err := json.Unmarshal(v, &p); err != nil {
			return n, fmt.Errorf("%s.properties: must be object", path)
		}
		n.Properties = map[string]node{}
		for k, v := range p {
			var child map[string]json.RawMessage
			if err := json.Unmarshal(v, &child); err != nil {
				return n, fmt.Errorf("%s.properties.%s: must be object", path, k)
			}
			c, e := compile(child, path+".properties."+k)
			if e != nil {
				return n, e
			}
			n.Properties[k] = c
		}
	}
	if v := raw["items"]; v != nil {
		var child map[string]json.RawMessage
		if err := json.Unmarshal(v, &child); err != nil {
			return n, fmt.Errorf("%s.items: must be object", path)
		}
		c, e := compile(child, path+".items")
		if e != nil {
			return n, e
		}
		n.Items = &c
	}
	if v := raw["enum"]; v != nil {
		if err := json.Unmarshal(v, &n.Enum); err != nil {
			return n, fmt.Errorf("%s.enum: must be array", path)
		}
		if len(n.Enum) == 0 {
			return n, fmt.Errorf("%s.enum: must contain at least one value", path)
		}
	}
	if v := raw["minimum"]; v != nil {
		x, err := rawJSONNumber(v)
		if err != nil {
			return n, fmt.Errorf("%s.minimum: must be number", path)
		}
		n.Minimum = &x
	}
	if v := raw["minLength"]; v != nil {
		var x int
		if err := json.Unmarshal(v, &x); err != nil || x < 0 {
			return n, fmt.Errorf("%s.minLength: must be non-negative integer", path)
		}
		n.MinLength = &x
	}
	return n, nil
}

func rawJSONNumber(raw json.RawMessage) (json.Number, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return "", err
	}
	if err := ensureEOF(dec); err != nil {
		return "", err
	}
	n, ok := value.(json.Number)
	if !ok {
		return "", errors.New("not a number")
	}
	if _, err := ParseJSONNumber(n.String()); err != nil {
		return "", err
	}
	return n, nil
}

func ensureEOF(dec *json.Decoder) error {
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (d *Document) Validate(data []byte) error {
	if d == nil {
		return errors.New("schema document is required")
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("$: invalid JSON: %w", err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("$: multiple JSON values")
	}
	return validate(d.root, v, "$")
}
func validate(n node, v any, path string) error {
	if n.Type != "" && !matches(n.Type, v) {
		return fmt.Errorf("%s: expected %s", path, n.Type)
	}
	if len(n.Enum) > 0 {
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%s: encode value for enum comparison: %w", path, err)
		}
		ok := false
		for _, x := range n.Enum {
			var y any
			decoder := json.NewDecoder(bytes.NewReader(x))
			decoder.UseNumber()
			if err := decoder.Decode(&y); err != nil {
				return fmt.Errorf("%s: decode enum value: %w", path, err)
			}
			yb, err := json.Marshal(y)
			if err != nil {
				return fmt.Errorf("%s: encode enum value: %w", path, err)
			}
			if bytes.Equal(b, yb) {
				ok = true
			}
		}
		if !ok {
			return fmt.Errorf("%s: must match enum", path)
		}
	}
	if n.Minimum != nil {
		x, ok := v.(json.Number)
		if !ok {
			return fmt.Errorf("%s: expected number", path)
		}
		a, err := ParseJSONNumber(x.String())
		if err != nil {
			return fmt.Errorf("%s: expected number", path)
		}
		b, err := ParseJSONNumber(n.Minimum.String())
		if err != nil {
			return fmt.Errorf("%s.minimum: invalid number", path)
		}
		if a.Cmp(b) < 0 {
			return fmt.Errorf("%s: must be >= %s", path, n.Minimum)
		}
	}
	if n.MinLength != nil {
		if s, ok := v.(string); ok && len([]rune(s)) < *n.MinLength {
			return fmt.Errorf("%s: length must be >= %d", path, *n.MinLength)
		}
	}
	if o, ok := v.(map[string]any); ok {
		for k := range n.Required {
			if _, yes := o[k]; !yes {
				return fmt.Errorf("%s.%s: is required", path, k)
			}
		}
		keys := make([]string, 0, len(o))
		for k := range o {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			c, yes := n.Properties[k]
			if !yes {
				if n.Additional != nil && !*n.Additional {
					return fmt.Errorf("%s.%s: additional property is not allowed", path, k)
				}
				continue
			}
			if err := validate(c, o[k], path+"."+k); err != nil {
				return err
			}
		}
	}
	if a, ok := v.([]any); ok && n.Items != nil {
		for i, x := range a {
			if err := validate(*n.Items, x, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}
func matches(t string, v any) bool {
	switch t {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	case "number":
		x, ok := v.(json.Number)
		if !ok {
			return false
		}
		_, err := ParseJSONNumber(x.String())
		return err == nil
	case "integer":
		x, ok := v.(json.Number)
		if !ok {
			return false
		}
		return IsJSONInteger(x.String())
	}
	return false
}
