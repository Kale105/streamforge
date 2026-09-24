// Package openapi generates the bounded public contract for configured datasets.
// When a normalized schema cannot be safely loaded, record data is represented
// as a bounded generic JSON object; the document metadata records that
// limitation rather than guessing field types.
package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/recordstore"
)

const (
	defaultTitle        = "Streamforge Dataset API"
	defaultAPIVersion   = "1.0.0"
	defaultAPIKeyHeader = "X-API-Key"
	maximumSchemaBytes  = 1 << 20
)

// Options controls document-level OpenAPI details. No credentials are accepted
// or included in the generated document.
type Options struct {
	Title         string
	Version       string
	APIKeyEnabled bool
	APIKeyHeader  string
}

// Generate returns deterministic OpenAPI 3.1 JSON for datasets. Each dataset
// must be independently valid and exposes one literal GET endpoint.
func Generate(datasets []dataset.Dataset, options Options) ([]byte, error) {
	if len(datasets) == 0 {
		return nil, errors.New("at least one dataset is required")
	}
	if options.Title == "" {
		options.Title = defaultTitle
	}
	if strings.TrimSpace(options.Title) == "" {
		return nil, errors.New("OpenAPI title must not be blank")
	}
	if options.Version == "" {
		options.Version = defaultAPIVersion
	}
	if strings.TrimSpace(options.Version) == "" {
		return nil, errors.New("OpenAPI version must not be blank")
	}
	if options.APIKeyEnabled {
		if options.APIKeyHeader == "" {
			options.APIKeyHeader = defaultAPIKeyHeader
		}
		if !validHeaderName(options.APIKeyHeader) {
			return nil, fmt.Errorf("API key header %q is invalid", options.APIKeyHeader)
		}
	}

	paths := map[string]any{}
	metadata := make([]any, 0, len(datasets))
	seenPaths := map[string]string{}
	seenOperations := map[string]string{}
	seenDatasets := map[string]bool{}
	for _, spec := range datasets {
		if err := spec.Validate(); err != nil {
			return nil, fmt.Errorf("validate dataset %q: %w", spec.Metadata.Name, err)
		}
		if seenDatasets[spec.Metadata.Name] {
			return nil, fmt.Errorf("duplicate dataset name %q", spec.Metadata.Name)
		}
		seenDatasets[spec.Metadata.Name] = true
		if prior, exists := seenPaths[spec.API.Path]; exists {
			return nil, fmt.Errorf("duplicate API path %q for datasets %q and %q", spec.API.Path, prior, spec.Metadata.Name)
		}
		operationID := operationID(spec.Metadata.Name)
		if prior, exists := seenOperations[operationID]; exists {
			return nil, fmt.Errorf("duplicate operation ID %q for datasets %q and %q", operationID, prior, spec.Metadata.Name)
		}
		seenPaths[spec.API.Path] = spec.Metadata.Name
		seenOperations[operationID] = spec.Metadata.Name

		dataSchema, limitation := normalizedDataSchema(spec)
		paths[spec.API.Path] = map[string]any{"get": getOperation(spec, operationID, dataSchema, options.APIKeyEnabled)}
		entry := map[string]any{"name": spec.Metadata.Name, "version": spec.Metadata.Version, "path": spec.API.Path}
		if limitation != "" {
			entry["schemaLimitation"] = limitation
		}
		metadata = append(metadata, entry)
	}
	sort.Slice(metadata, func(i, j int) bool {
		return metadata[i].(map[string]any)["name"].(string) < metadata[j].(map[string]any)["name"].(string)
	})

	doc := map[string]any{
		"openapi":                "3.1.0",
		"info":                   map[string]any{"title": options.Title, "version": options.Version},
		"paths":                  paths,
		"components":             map[string]any{"schemas": map[string]any{"Problem": problemSchema()}},
		"x-streamforge-datasets": metadata,
	}
	if options.APIKeyEnabled {
		doc["components"].(map[string]any)["securitySchemes"] = map[string]any{
			"ApiKeyAuth": map[string]any{"type": "apiKey", "in": "header", "name": options.APIKeyHeader},
		}
	}
	return json.MarshalIndent(doc, "", "  ")
}

func getOperation(spec dataset.Dataset, id string, dataSchema map[string]any, protected bool) map[string]any {
	response := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"records"},
		"properties": map[string]any{
			"records":     map[string]any{"type": "array", "maxItems": min(spec.API.Pagination.Maximum, recordstore.MaxPageItems), "items": recordSchema(dataSchema)},
			"next_cursor": map[string]any{"type": "string", "maxLength": 4096, "description": "Opaque cursor for the next page."},
		},
	}
	parameters := []any{
		map[string]any{"name": "limit", "in": "query", "description": fmt.Sprintf("Maximum records to return (hard cap: %d).", recordstore.MaxPageItems), "schema": map[string]any{"type": "integer", "minimum": 1, "maximum": min(spec.API.Pagination.Maximum, recordstore.MaxPageItems), "default": spec.API.Pagination.Default}},
		map[string]any{"name": "cursor", "in": "query", "description": "Opaque cursor bound to this revision, sort, and filter set.", "schema": map[string]any{"type": "string", "maxLength": 4096}},
	}
	if len(spec.API.Sorts) > 0 {
		parameters = append(parameters,
			map[string]any{"name": "sort", "in": "query", "description": "Configured stable sort field.", "schema": map[string]any{"type": "string", "enum": querySortValues(spec)}},
			map[string]any{"name": "direction", "in": "query", "description": "Sort direction for a configured sort.", "schema": map[string]any{"type": "string", "enum": []string{"asc", "desc"}, "default": "asc"}},
		)
	}
	for _, field := range spec.API.Filters {
		parameters = append(parameters, map[string]any{"name": "filter." + field, "in": "query", "description": "Exact JSON-scalar equality filter for " + field + ".", "schema": map[string]any{"type": "string", "maxLength": 4096}})
	}
	op := map[string]any{
		"operationId":           id,
		"summary":               "List " + spec.Metadata.Name + " records",
		"x-streamforge-dataset": map[string]any{"name": spec.Metadata.Name, "version": spec.Metadata.Version},
		"parameters":            parameters,
		"responses": map[string]any{
			"200": responseContent("Records returned.", "application/json", response),
			"400": problemResponse("Invalid query parameter."),
			"500": problemResponse("Records could not be read."),
		},
	}
	op["x-streamforge-max-response-bytes"] = recordstore.MaxResponseBytes
	if protected {
		op["security"] = []any{map[string]any{"ApiKeyAuth": []any{}}}
		op["responses"].(map[string]any)["401"] = problemResponse("Authentication required.")
		op["responses"].(map[string]any)["403"] = problemResponse("Access denied.")
		op["responses"].(map[string]any)["429"] = problemResponse("Rate limit exceeded.")
	}
	return op
}

func querySortValues(spec dataset.Dataset) []string {
	values := append([]string(nil), spec.API.Sorts...)
	sort.Strings(values)
	return values
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func responseContent(description, contentType string, value map[string]any) map[string]any {
	return map[string]any{"description": description, "content": map[string]any{contentType: map[string]any{"schema": value}}}
}

func problemResponse(description string) map[string]any {
	return map[string]any{"description": description, "content": map[string]any{"application/problem+json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Problem"}}}}
}

func problemSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"status", "title", "detail"}, "properties": map[string]any{
		"status": map[string]any{"type": "integer", "minimum": 100, "maximum": 599},
		"title":  map[string]any{"type": "string", "maxLength": 256}, "detail": map[string]any{"type": "string", "maxLength": 4096},
	}}
}

func recordSchema(data map[string]any) map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"dataset", "dataset_version", "id", "data", "updated_at"}, "properties": map[string]any{
		"dataset": map[string]any{"type": "string"}, "dataset_version": map[string]any{"type": "string"},
		"id": map[string]any{"type": "string"}, "data": data, "updated_at": map[string]any{"type": "string", "format": "date-time"},
	}}
}

func normalizedDataSchema(spec dataset.Dataset) (map[string]any, string) {
	properties := map[string]any{}
	for _, field := range spec.API.Fields {
		properties[field] = map[string]any{}
	}
	fallback := func(reason string) (map[string]any, string) {
		return map[string]any{
			"type": "object", "additionalProperties": false,
			"minProperties": len(properties), "maxProperties": len(properties),
			"required": append([]string(nil), spec.API.Fields...), "properties": properties,
		}, reason
	}
	if spec.Schemas.Normalized.Path == "" {
		return fallback("No normalized schema is configured; published values are unconstrained JSON values.")
	}
	root, err := loadSchema(spec.Schemas.Normalized.Path)
	if err != nil {
		return fallback("The configured normalized schema could not be safely read; published values are unconstrained JSON values.")
	}
	if root.Type != "object" {
		return fallback("The configured normalized schema is not an object; published values are unconstrained JSON values.")
	}
	for _, field := range spec.API.Fields {
		if node, ok := root.Properties[field]; ok {
			properties[field] = node.openapi()
		}
	}
	return map[string]any{"type": "object", "additionalProperties": false, "required": append([]string(nil), spec.API.Fields...), "properties": properties}, ""
}

type schemaNode struct {
	Type       string
	Properties map[string]schemaNode
	Items      *schemaNode
	Required   []string
	Additional *bool
	Enum       []any
	Minimum    any
	MinLength  any
}

func (n schemaNode) openapi() map[string]any {
	out := map[string]any{}
	if n.Type != "" {
		out["type"] = n.Type
	}
	if len(n.Enum) > 0 {
		out["enum"] = n.Enum
	}
	if n.Minimum != nil {
		out["minimum"] = n.Minimum
	}
	if n.MinLength != nil {
		out["minLength"] = n.MinLength
	}
	if n.Items != nil {
		out["items"] = n.Items.openapi()
	}
	if n.Properties != nil {
		p := map[string]any{}
		for k, v := range n.Properties {
			p[k] = v.openapi()
		}
		out["properties"] = p
	}
	if n.Required != nil {
		out["required"] = n.Required
	}
	if n.Additional != nil {
		out["additionalProperties"] = *n.Additional
	}
	return out
}

func loadSchema(path string) (schemaNode, error) {
	f, err := os.Open(path)
	if err != nil {
		return schemaNode{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maximumSchemaBytes+1))
	if err != nil || len(b) > maximumSchemaBytes {
		return schemaNode{}, errors.New("schema is unavailable or exceeds size limit")
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return schemaNode{}, err
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return schemaNode{}, errors.New("schema contains multiple JSON values")
	}
	return parseNode(raw)
}

func parseNode(raw map[string]json.RawMessage) (schemaNode, error) {
	allowed := map[string]bool{"$schema": true, "title": true, "description": true, "type": true, "properties": true, "required": true, "additionalProperties": true, "enum": true, "minimum": true, "minLength": true, "items": true}
	for key := range raw {
		if !allowed[key] {
			return schemaNode{}, errors.New("unsupported normalized schema keyword")
		}
	}
	var n schemaNode
	if value := raw["type"]; value != nil {
		if err := json.Unmarshal(value, &n.Type); err != nil {
			return n, err
		}
		switch n.Type {
		case "object", "array", "string", "integer", "number", "boolean", "null":
		default:
			return n, errors.New("unsupported normalized schema type")
		}
	}
	if value := raw["enum"]; value != nil {
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		if err := decoder.Decode(&n.Enum); err != nil || len(n.Enum) == 0 {
			return n, errors.New("invalid normalized schema enum")
		}
	}
	if value := raw["required"]; value != nil {
		if err := json.Unmarshal(value, &n.Required); err != nil {
			return n, errors.New("invalid normalized schema required")
		}
		sort.Strings(n.Required)
	}
	if value := raw["additionalProperties"]; value != nil {
		var allowed bool
		if err := json.Unmarshal(value, &allowed); err != nil {
			return n, errors.New("invalid normalized schema additionalProperties")
		}
		n.Additional = &allowed
	}
	if value := raw["minimum"]; value != nil {
		var number json.Number
		if err := json.Unmarshal(value, &number); err != nil {
			return n, err
		}
		n.Minimum = number
	}
	if value := raw["minLength"]; value != nil {
		var x int
		if err := json.Unmarshal(value, &x); err != nil || x < 0 {
			return n, errors.New("invalid normalized schema minLength")
		}
		n.MinLength = x
	}
	if value := raw["items"]; value != nil {
		var child map[string]json.RawMessage
		if err := json.Unmarshal(value, &child); err != nil {
			return n, err
		}
		parsed, err := parseNode(child)
		if err != nil {
			return n, err
		}
		n.Items = &parsed
	}
	if value := raw["properties"]; value != nil {
		var properties map[string]json.RawMessage
		if err := json.Unmarshal(value, &properties); err != nil {
			return n, err
		}
		n.Properties = map[string]schemaNode{}
		for name, child := range properties {
			var object map[string]json.RawMessage
			if err := json.Unmarshal(child, &object); err != nil {
				return n, err
			}
			parsed, err := parseNode(object)
			if err != nil {
				return n, err
			}
			n.Properties[name] = parsed
		}
	}
	return n, nil
}

func operationID(name string) string {
	var result strings.Builder
	result.WriteString("list")
	for _, part := range strings.Split(name, "-") {
		if part == "" {
			continue
		}
		result.WriteByte(part[0] - ('a' - 'A'))
		result.WriteString(part[1:])
	}
	return result.String()
}

func validHeaderName(header string) bool {
	for _, r := range header {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return false
		}
	}
	return header != ""
}
