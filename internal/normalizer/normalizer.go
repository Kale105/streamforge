// Package normalizer maps admitted event payloads to storage-neutral records.
package normalizer

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/schema"
)

// Normalizer is immutable after construction and safe for concurrent callers.
type Normalizer struct {
	datasetName      string
	keySource        string
	fields           []dataset.FieldMapping
	rawSchema        *schema.Document
	normalizedSchema *schema.Document
	contract         event.Contract
}

// New validates spec before constructing a normalizer.
func New(spec dataset.Dataset) (*Normalizer, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	fields, err := cloneFields(spec.Normalization.Fields)
	if err != nil {
		return nil, err
	}
	n := &Normalizer{
		datasetName: spec.Metadata.Name,
		keySource:   spec.Normalization.Key,
		fields:      fields,
	}
	if spec.Schemas.Raw.Path != "" {
		n.rawSchema, err = loadSchema(spec.Schemas.Raw)
		if err != nil {
			return nil, fmt.Errorf("schemas.raw: %w", err)
		}
	}
	if spec.Schemas.Normalized.Path != "" {
		n.normalizedSchema, err = loadSchema(spec.Schemas.Normalized)
		if err != nil {
			return nil, fmt.Errorf("schemas.normalized: %w", err)
		}
	}
	n.contract = event.Contract{
		RawSchema:        identity(spec.Schemas.Raw, n.rawSchema),
		NormalizedSchema: identity(spec.Schemas.Normalized, n.normalizedSchema),
		Transform: event.TransformIdentity{
			Revision: spec.Normalization.Revision,
			Digest:   MappingDigest(spec.Normalization),
		},
	}
	return n, nil
}

func loadSchema(ref dataset.SchemaReference) (*schema.Document, error) {
	if ref.Digest != "" {
		return schema.LoadPinned(ref.Path, ref.Revision, ref.Digest)
	}
	return schema.Load(ref.Path, ref.Revision)
}

func identity(ref dataset.SchemaReference, document *schema.Document) event.SchemaIdentity {
	if document == nil {
		return event.SchemaIdentity{}
	}
	return event.SchemaIdentity{Revision: ref.Revision, Digest: document.Digest}
}

// Contract returns the immutable schema and transform identity calculated at
// construction. It is deliberately copied so callers cannot mutate a shared
// normalizer's contract between goroutines.
func (n *Normalizer) Contract() event.Contract { return n.contract }

// MappingDigest returns a SHA-256 digest for the semantic mapping, independent
// of YAML field order. Array order is not operationally meaningful because
// targets are unique; sorting makes equivalent declarations identically
// replayable. Defaults retain exact JSON number spelling through UseNumber.
func MappingDigest(mapping dataset.Normalization) string {
	type entry struct {
		Source    string          `json:"source"`
		Target    string          `json:"target"`
		Transform string          `json:"transform"`
		Required  *bool           `json:"required"`
		Default   json.RawMessage `json:"default,omitempty"`
	}
	entries := make([]entry, 0, len(mapping.Fields))
	for _, field := range mapping.Fields {
		var defaultValue json.RawMessage
		if field.Default != nil {
			encoded, err := json.Marshal(field.Default)
			if err == nil {
				var decoded any
				decoder := json.NewDecoder(bytes.NewReader(encoded))
				decoder.UseNumber()
				if decoder.Decode(&decoded) == nil {
					defaultValue, _ = json.Marshal(decoded)
				}
			}
		}
		var required *bool
		if field.Required != nil {
			value := *field.Required
			required = &value
		}
		entries = append(entries, entry{Source: field.Source, Target: field.Target, Transform: field.Transform, Required: required, Default: defaultValue})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Target != entries[j].Target {
			return entries[i].Target < entries[j].Target
		}
		if entries[i].Source != entries[j].Source {
			return entries[i].Source < entries[j].Source
		}
		return entries[i].Transform < entries[j].Transform
	})
	payload, _ := json.Marshal(struct {
		Key    string  `json:"key"`
		Fields []entry `json:"fields"`
	}{Key: mapping.Key, Fields: entries})
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:])
}

func cloneFields(source []dataset.FieldMapping) ([]dataset.FieldMapping, error) {
	fields := make([]dataset.FieldMapping, len(source))
	for i, mapping := range source {
		fields[i] = mapping
		if mapping.Required != nil {
			required := *mapping.Required
			fields[i].Required = &required
		}
		if mapping.Default != nil {
			encoded, err := json.Marshal(mapping.Default)
			if err != nil {
				return nil, fmt.Errorf("normalization.fields[%d].default: must be JSON-compatible: %w", i, err)
			}
			var cloned any
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.UseNumber()
			if err := decoder.Decode(&cloned); err != nil {
				return nil, fmt.Errorf("normalization.fields[%d].default: decode copy: %w", i, err)
			}
			fields[i].Default = cloned
		}
	}
	return fields, nil
}

// Normalize selects configured top-level fields and creates a canonical record.
// It does not mutate the event nor retain its raw payload bytes.
func (n *Normalizer) Normalize(e event.Event) (recordstore.Record, error) {
	if n.rawSchema != nil {
		if err := n.rawSchema.Validate(e.Data); err != nil {
			return recordstore.Record{}, fmt.Errorf("schemas.raw: %w", err)
		}
	}
	var source map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &source); err != nil || source == nil {
		return recordstore.Record{}, fmt.Errorf("event data: must be a JSON object")
	}
	keyValue, found := source[n.keySource]
	if !found {
		return recordstore.Record{}, fmt.Errorf("normalization.key: source field %q is missing", n.keySource)
	}
	var id string
	if err := json.Unmarshal(keyValue, &id); err != nil || id == "" {
		return recordstore.Record{}, fmt.Errorf("normalization.key: source field %q must be a non-empty JSON string", n.keySource)
	}
	if len(id) > recordstore.MaxRecordIDBytes {
		return recordstore.Record{}, fmt.Errorf("normalization.key: source field %q exceeds %d bytes", n.keySource, recordstore.MaxRecordIDBytes)
	}

	output := make(map[string]json.RawMessage, len(n.fields))
	for _, mapping := range n.fields {
		value, found := source[mapping.Source]
		if !found {
			if mapping.Default != nil {
				b, err := json.Marshal(mapping.Default)
				if err != nil {
					return recordstore.Record{}, fmt.Errorf("normalization.fields.%s.default: %w", mapping.Target, err)
				}
				value = b
			} else if mapping.Required == nil {
				return recordstore.Record{}, fmt.Errorf("normalization.fields: source field %q is missing", mapping.Source)
			} else if *mapping.Required {
				return recordstore.Record{}, fmt.Errorf("normalization.fields: source field %q is required", mapping.Source)
			} else {
				continue
			}
		}
		if mapping.Transform != "" && mapping.Transform != "select" {
			var err error
			value, err = convert(value, mapping.Transform)
			if err != nil {
				return recordstore.Record{}, fmt.Errorf("normalization.fields.%s: %w", mapping.Target, err)
			}
		}
		// Copy prevents a record from sharing caller-owned event bytes.
		output[mapping.Target] = append(json.RawMessage(nil), value...)
	}
	data, err := json.Marshal(output)
	if err != nil {
		return recordstore.Record{}, fmt.Errorf("canonicalize normalized record: %w", err)
	}
	if len(data) > recordstore.MaxRecordDataBytes {
		return recordstore.Record{}, fmt.Errorf("normalized record exceeds %d bytes", recordstore.MaxRecordDataBytes)
	}
	if n.normalizedSchema != nil {
		if err := n.normalizedSchema.Validate(data); err != nil {
			return recordstore.Record{}, fmt.Errorf("schemas.normalized: %w", err)
		}
	}
	return recordstore.Record{Dataset: n.datasetName, ID: id, Data: data, UpdatedAt: e.Time}, nil
}

func convert(raw json.RawMessage, kind string) (json.RawMessage, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid source JSON: %w", err)
	}
	var out any
	switch kind {
	case "string":
		switch x := v.(type) {
		case string:
			out = x
		case json.Number:
			out = x.String()
		case bool:
			out = strconv.FormatBool(x)
		default:
			return nil, fmt.Errorf("cannot convert %T to string", v)
		}
	case "integer":
		switch x := v.(type) {
		case json.Number:
			if !schema.IsJSONInteger(x.String()) {
				return nil, fmt.Errorf("cannot convert %q to integer", x)
			}
			out = x
		case string:
			if !schema.IsJSONInteger(x) {
				return nil, fmt.Errorf("cannot convert %q to integer", x)
			}
			out = json.Number(x)
		default:
			return nil, fmt.Errorf("cannot convert %T to integer", v)
		}
	case "number":
		switch x := v.(type) {
		case json.Number:
			if _, e := schema.ParseJSONNumber(x.String()); e != nil {
				return nil, fmt.Errorf("cannot convert %q to number", x)
			}
			out = x
		case string:
			if _, e := schema.ParseJSONNumber(x); e != nil {
				return nil, fmt.Errorf("cannot convert %q to number", x)
			}
			out = json.Number(x)
		default:
			return nil, fmt.Errorf("cannot convert %T to number", v)
		}
	case "boolean":
		switch x := v.(type) {
		case bool:
			out = x
		case string:
			b, e := strconv.ParseBool(x)
			if e != nil {
				return nil, fmt.Errorf("cannot convert %q to boolean", x)
			}
			out = b
		default:
			return nil, fmt.Errorf("cannot convert %T to boolean", v)
		}
	default:
		return nil, fmt.Errorf("unsupported transform %q", kind)
	}
	b, err := json.Marshal(out)
	return b, err
}
