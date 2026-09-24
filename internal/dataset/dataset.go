// Package dataset defines the deliberately small personal-mode dataset contract.
package dataset

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/Kale105/streamforge/internal/recordstore"
)

const (
	APIVersionV1        = "streamforge.dev/v1alpha1"
	KindDataset         = "Dataset"
	CollectorHTTPJSON   = "http-json"
	CollectorSSE        = "sse"
	CollectorSubprocess = "subprocess"
	CollectorWebhook    = "webhook"
	StoragePostgres     = "postgres"
	PaginationCursor    = "cursor"
)

// Dataset is the desired state for one personal-mode, HTTP JSON-backed dataset.
// It is intentionally not a general transformation or API language.
type Dataset struct {
	APIVersion    string        `yaml:"apiVersion" json:"apiVersion"`
	Kind          string        `yaml:"kind" json:"kind"`
	Metadata      Metadata      `yaml:"metadata" json:"metadata"`
	Collector     HTTPCollector `yaml:"collector" json:"collector"`
	Normalization Normalization `yaml:"normalization" json:"normalization"`
	Schemas       Schemas       `yaml:"schemas,omitempty" json:"schemas,omitempty"`
	Storage       Storage       `yaml:"storage" json:"storage"`
	API           API           `yaml:"api" json:"api"`
}

type Metadata struct {
	Name    string `yaml:"name" json:"name"`
	Version string `yaml:"version" json:"version"`
}

type HTTPCollector struct {
	Type       string               `yaml:"type" json:"type"`
	HTTPJSON   *HTTPJSONCollector   `yaml:"httpJson,omitempty" json:"httpJson,omitempty"`
	SSE        *SSECollector        `yaml:"sse,omitempty" json:"sse,omitempty"`
	Subprocess *SubprocessCollector `yaml:"subprocess,omitempty" json:"subprocess,omitempty"`
	Retry      Retry                `yaml:"retry,omitempty" json:"retry,omitempty"`

	// Deprecated Go-only compatibility fields keep existing programmatic callers
	// compiling. They are never accepted from YAML; migrate to collector.httpJson.
	URL            string        `yaml:"-" json:"-"`
	PollInterval   time.Duration `yaml:"-" json:"-"`
	RequestTimeout time.Duration `yaml:"-" json:"-"`
	MaxBodyBytes   int64         `yaml:"-" json:"-"`
}

type HTTPJSONCollector struct {
	URL            string            `yaml:"url" json:"url"`
	PollInterval   time.Duration     `yaml:"pollInterval" json:"pollInterval"`
	RequestTimeout time.Duration     `yaml:"requestTimeout" json:"requestTimeout"`
	MaxBodyBytes   int64             `yaml:"maxBodyBytes" json:"maxBodyBytes"`
	Headers        map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
}

type SSECollector struct {
	URL             string            `yaml:"url" json:"url"`
	RequestTimeout  time.Duration     `yaml:"requestTimeout" json:"requestTimeout"`
	MaxMessageBytes int               `yaml:"maxMessageBytes" json:"maxMessageBytes"`
	MaxLineBytes    int               `yaml:"maxLineBytes" json:"maxLineBytes"`
	Headers         map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
}

type SubprocessCollector struct {
	Path             string            `yaml:"path" json:"path"`
	Args             []string          `yaml:"args,omitempty" json:"args,omitempty"`
	Environment      map[string]string `yaml:"environment,omitempty" json:"environment,omitempty"`
	MaxStderrBytes   int               `yaml:"maxStderrBytes" json:"maxStderrBytes"`
	HandshakeTimeout time.Duration     `yaml:"handshakeTimeout" json:"handshakeTimeout"`
	AckTimeout       time.Duration     `yaml:"ackTimeout" json:"ackTimeout"`
	ShutdownTimeout  time.Duration     `yaml:"shutdownTimeout" json:"shutdownTimeout"`
}

// Retry is intentionally bounded. A zero-value block disables retry; a partially
// specified block is invalid rather than silently choosing an unbounded policy.
type Retry struct {
	InitialBackoff         time.Duration `yaml:"initialBackoff,omitempty" json:"initialBackoff,omitempty"`
	MaxBackoff             time.Duration `yaml:"maxBackoff,omitempty" json:"maxBackoff,omitempty"`
	MaxConsecutiveFailures int           `yaml:"maxConsecutiveFailures,omitempty" json:"maxConsecutiveFailures,omitempty"`
}

func (r Retry) Enabled() bool {
	return r.InitialBackoff != 0 || r.MaxBackoff != 0 || r.MaxConsecutiveFailures != 0
}

func (c HTTPCollector) HTTP() *HTTPJSONCollector {
	if c.HTTPJSON != nil {
		return c.HTTPJSON
	}
	if c.URL == "" && c.PollInterval == 0 && c.RequestTimeout == 0 && c.MaxBodyBytes == 0 {
		return nil
	}
	return &HTTPJSONCollector{URL: c.URL, PollInterval: c.PollInterval, RequestTimeout: c.RequestTimeout, MaxBodyBytes: c.MaxBodyBytes}
}

// Normalization supports only selecting a top-level JSON property and optionally
// naming it differently in the resulting record.
type Normalization struct {
	Key string `yaml:"key" json:"key"`
	// Revision is an operator-chosen immutable label for this mapping. It is
	// optional for the original personal-mode contract, but provider workers
	// require it before they accept a pipeline contract.
	Revision string         `yaml:"revision,omitempty" json:"revision,omitempty"`
	Fields   []FieldMapping `yaml:"fields" json:"fields"`
}

type FieldMapping struct {
	Source    string `yaml:"source" json:"source"`
	Target    string `yaml:"target" json:"target"`
	Transform string `yaml:"transform,omitempty" json:"transform,omitempty"`
	// Required defaults to true when omitted, preserving the original mapping
	// contract. Set it explicitly to false to omit a missing source field.
	Required *bool `yaml:"required,omitempty" json:"required,omitempty"`
	Default  any   `yaml:"default,omitempty" json:"default,omitempty"`
}

// Schemas pins local JSON Schema documents. Paths are resolved relative to the
// dataset YAML by config.LoadDataset; remote schemas and references are never fetched.
type Schemas struct {
	Raw        SchemaReference `yaml:"raw" json:"raw"`
	Normalized SchemaReference `yaml:"normalized" json:"normalized"`
}

type SchemaReference struct {
	Path     string `yaml:"path" json:"path"`
	Revision string `yaml:"revision" json:"revision"`
	// Digest is the SHA-256 digest of the exact schema file bytes. A revision
	// is a human label; this pin prevents a file being edited in place under
	// the same label.
	Digest string `yaml:"digest,omitempty" json:"digest,omitempty"`
}

type Storage struct {
	Engine string `yaml:"engine" json:"engine"`
}

type API struct {
	Enabled    bool       `yaml:"enabled" json:"enabled"`
	Path       string     `yaml:"path" json:"path"`
	Fields     []string   `yaml:"fields" json:"fields"`
	Filters    []string   `yaml:"filters" json:"filters"`
	Sorts      []string   `yaml:"sorts" json:"sorts"`
	Pagination Pagination `yaml:"pagination" json:"pagination"`
}

type Pagination struct {
	Type    string `yaml:"type" json:"type"`
	Default int    `yaml:"default" json:"default"`
	Maximum int    `yaml:"maximum" json:"maximum"`
}

// FieldError identifies a configuration error at its source field path.
type FieldError struct{ Path, Message string }

func (e FieldError) Error() string { return e.Path + ": " + e.Message }

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$`)
var fieldPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Validate performs semantic validation. All independently discoverable errors are returned.
func (d Dataset) Validate() error {
	var errs []error
	add := func(path, message string) { errs = append(errs, FieldError{path, message}) }
	if d.APIVersion != APIVersionV1 {
		add("apiVersion", "must be "+APIVersionV1)
	}
	if d.Kind != KindDataset {
		add("kind", "must be "+KindDataset)
	}
	if !namePattern.MatchString(d.Metadata.Name) {
		add("metadata.name", "must be a lowercase kebab-case identifier")
	} else if len(d.Metadata.Name) > 63 {
		add("metadata.name", "must not exceed 63 bytes")
	}
	if strings.TrimSpace(d.Metadata.Version) == "" {
		add("metadata.version", "is required")
	} else if len(d.Metadata.Version) > 128 {
		add("metadata.version", "must not exceed 128 bytes")
	}
	validateCollector(d.Collector, add)
	targets := make(map[string]struct{}, len(d.Normalization.Fields))
	optionalTargets := make(map[string]struct{})
	keyMapped := false
	if !fieldPattern.MatchString(d.Normalization.Key) {
		add("normalization.key", "must name a top-level source field")
	}
	if len(d.Normalization.Fields) == 0 {
		add("normalization.fields", "must contain at least one field mapping")
	}
	for i, mapping := range d.Normalization.Fields {
		prefix := fmt.Sprintf("normalization.fields[%d]", i)
		if !fieldPattern.MatchString(mapping.Source) {
			add(prefix+".source", "must name a top-level JSON field")
		}
		if !fieldPattern.MatchString(mapping.Target) {
			add(prefix+".target", "must be a record field name")
		}
		if mapping.Transform != "" && mapping.Transform != "select" && mapping.Transform != "string" && mapping.Transform != "integer" && mapping.Transform != "number" && mapping.Transform != "boolean" {
			add(prefix+".transform", "must be select, string, integer, number, or boolean")
		}
		if _, exists := targets[mapping.Target]; exists {
			add(prefix+".target", "duplicates an earlier target")
		}
		targets[mapping.Target] = struct{}{}
		if mapping.Required != nil && !*mapping.Required && mapping.Default == nil {
			optionalTargets[mapping.Target] = struct{}{}
		}
		if mapping.Source == d.Normalization.Key {
			keyMapped = true
		}
	}
	for _, entry := range []struct {
		path string
		ref  SchemaReference
	}{{"schemas.raw", d.Schemas.Raw}, {"schemas.normalized", d.Schemas.Normalized}} {
		if (entry.ref.Path == "") != (entry.ref.Revision == "") {
			add(entry.path, "path and revision must be supplied together")
		}
		if entry.ref.Digest != "" && (entry.ref.Path == "" || entry.ref.Revision == "") {
			add(entry.path, "SHA-256 digest requires both path and revision")
		}
	}
	if d.Normalization.Key != "" && !keyMapped {
		add("normalization.key", "must be selected by one normalization field mapping")
	}
	if d.Storage.Engine != StoragePostgres {
		add("storage.engine", "must be "+StoragePostgres)
	}
	if !d.API.Enabled {
		add("api.enabled", "must be true in personal mode")
	} else {
		if err := ValidateAPIPath(d.API.Path); err != nil {
			add("api.path", err.Error())
		}
		validateAllowList(add, "api.fields", d.API.Fields, targets)
		for i, field := range d.API.Fields {
			if _, optional := optionalTargets[field]; optional {
				add(fmt.Sprintf("api.fields[%d]", i), "cannot publish an optional mapping without a default")
			}
		}
		if len(d.API.Fields) == 0 {
			add("api.fields", "must publish at least one normalized field")
		}
		if len(d.API.Filters) > recordstore.MaxFilters {
			add("api.filters", fmt.Sprintf("cannot exceed %d fields", recordstore.MaxFilters))
		}
		// Filters are intentionally checked against normalized targets rather
		// than only API.Fields. This permits a stable, non-public normalized
		// field to be used for server-side selection without making it part of
		// the response payload.
		validateAllowList(add, "api.filters", d.API.Filters, targets)
		validateSortAllowList(add, d.API.Sorts)
		if d.API.Pagination.Type != PaginationCursor {
			add("api.pagination.type", "must be "+PaginationCursor)
		}
		if d.API.Pagination.Default <= 0 {
			add("api.pagination.default", "must be greater than zero")
		}
		if d.API.Pagination.Default > recordstore.MaxPageItems {
			add("api.pagination.default", fmt.Sprintf("cannot exceed hard page cap of %d", recordstore.MaxPageItems))
		}
		if d.API.Pagination.Maximum <= 0 {
			add("api.pagination.maximum", "must be greater than zero")
		}
		if d.API.Pagination.Maximum > recordstore.MaxPageItems {
			add("api.pagination.maximum", fmt.Sprintf("cannot exceed hard page cap of %d", recordstore.MaxPageItems))
		}
		if d.API.Pagination.Default > d.API.Pagination.Maximum {
			add("api.pagination.default", "must not exceed api.pagination.maximum")
		}
	}
	return errors.Join(errs...)
}

func validateCollector(c HTTPCollector, add func(string, string)) {
	if c.Type != CollectorHTTPJSON && c.Type != CollectorSSE && c.Type != CollectorSubprocess {
		if c.Type == CollectorWebhook {
			add("collector.type", "webhook is not supported in v1")
		} else {
			add("collector.type", "must be http-json, sse, or subprocess")
		}
	}
	count := 0
	if c.HTTPJSON != nil {
		count++
	}
	if c.SSE != nil {
		count++
	}
	if c.Subprocess != nil {
		count++
	}
	legacyHTTP := c.HTTP() != nil && c.HTTPJSON == nil
	if legacyHTTP {
		count++
	}
	if count != 1 {
		add("collector", "must contain exactly one type-matching configuration block")
	}
	if c.Type == CollectorHTTPJSON {
		h := c.HTTP()
		if h == nil {
			add("collector.httpJson", "is required for type http-json")
		} else {
			validateHTTP("collector.httpJson", h.URL, h.RequestTimeout, h.MaxBodyBytes, h.Headers, add)
			if h.PollInterval <= 0 {
				add("collector.httpJson.pollInterval", "must be greater than zero")
			}
		}
	} else if c.Type == CollectorSSE {
		if c.SSE == nil {
			add("collector.sse", "is required for type sse")
		} else {
			validateHTTP("collector.sse", c.SSE.URL, c.SSE.RequestTimeout, int64(c.SSE.MaxMessageBytes), c.SSE.Headers, add)
			if c.SSE.MaxLineBytes <= 0 || c.SSE.MaxLineBytes > c.SSE.MaxMessageBytes {
				add("collector.sse.maxLineBytes", "must be positive and not exceed maxMessageBytes")
			}
		}
	} else if c.Type == CollectorSubprocess {
		if c.Subprocess == nil {
			add("collector.subprocess", "is required for type subprocess")
		} else {
			validateSubprocess(c.Subprocess, add)
		}
	}
	if c.Retry.Enabled() {
		if c.Retry.InitialBackoff <= 0 || c.Retry.MaxBackoff <= 0 || c.Retry.MaxConsecutiveFailures <= 0 || c.Retry.MaxBackoff < c.Retry.InitialBackoff {
			add("collector.retry", "requires positive initialBackoff, maxBackoff, maxConsecutiveFailures, with maxBackoff >= initialBackoff")
		}
	}
}

func validateHTTP(path, rawURL string, timeout time.Duration, maximum int64, headers map[string]string, add func(string, string)) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		add(path+".url", "must be an absolute http or https URL without userinfo")
	}
	if timeout <= 0 {
		add(path+".requestTimeout", "must be greater than zero")
	}
	if maximum <= 0 {
		add(path+".maxBodyBytes", "must be greater than zero")
	}
	for name, ref := range headers {
		if forbiddenHeader(name) {
			add(path+".headers."+name, "must not set host, hop-by-hop, or framing headers")
		}
		if !validEnvReference(ref) {
			add(path+".headers."+name, "must be an env:NAME reference")
		}
	}
}

func validateSubprocess(c *SubprocessCollector, add func(string, string)) {
	if c.Path == "" || strings.IndexByte(c.Path, 0) >= 0 || !filepath.IsAbs(c.Path) {
		add("collector.subprocess.path", "must be an absolute path without NUL")
	}
	for i, arg := range c.Args {
		if strings.IndexByte(arg, 0) >= 0 || strings.Contains(arg, "${") || strings.Contains(arg, "$(") || strings.Contains(arg, "%") || strings.HasPrefix(arg, "env:") {
			add(fmt.Sprintf("collector.subprocess.args[%d]", i), "must not interpolate secrets or contain NUL")
		}
	}
	if c.MaxStderrBytes < 0 || c.HandshakeTimeout < 0 || c.AckTimeout < 0 || c.ShutdownTimeout < 0 {
		add("collector.subprocess", "bounds and timeouts must not be negative")
	}
	for name, ref := range c.Environment {
		if !fieldPattern.MatchString(name) || reservedEnv(name) {
			add("collector.subprocess.environment."+name, "is reserved or invalid")
		}
		if !validEnvReference(ref) {
			add("collector.subprocess.environment."+name, "must be an env:NAME reference")
		}
	}
}

func validEnvReference(value string) bool {
	if !strings.HasPrefix(value, "env:") {
		return false
	}
	name := strings.TrimPrefix(value, "env:")
	return fieldPattern.MatchString(name) && name == strings.ToUpper(name)
}

func reservedEnv(name string) bool {
	switch strings.ToUpper(name) {
	case "PATH", "HOME", "USER", "USERNAME", "SHELL", "COMSPEC", "SYSTEMROOT", "WINDIR", "PATHEXT", "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "TMP", "TEMP":
		return true
	}
	return false
}

func forbiddenHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "content-length":
		return true
	}
	return false
}

func validateSortAllowList(add func(string, string), sorts []string) {
	seen := make(map[string]struct{}, len(sorts))
	for i, sort := range sorts {
		entry := fmt.Sprintf("api.sorts[%d]", i)
		if sort != string(recordstore.SortUpdatedAt) && sort != string(recordstore.SortID) {
			add(entry, "must be updated_at or id")
		}
		if _, exists := seen[sort]; exists {
			add(entry, "duplicates an earlier sort")
		}
		seen[sort] = struct{}{}
	}
}

// ValidateAPIPath accepts only canonical, literal endpoint paths. This keeps the
// dataset contract independent of net/http ServeMux pattern syntax.
func ValidateAPIPath(path string) error {
	if path == "" || path[0] != '/' {
		return errors.New("must begin with /")
	}
	if path == "/" {
		return errors.New("must not be the root path")
	}
	if strings.HasSuffix(path, "/") {
		return errors.New("must not have a trailing slash")
	}
	if strings.Contains(path, "//") {
		return errors.New("must not contain duplicate slashes")
	}
	if strings.ContainsAny(path, "%{}*?#\\") {
		return errors.New("must be a canonical literal path")
	}
	if strings.IndexFunc(path, unicode.IsSpace) >= 0 {
		return errors.New("must not contain whitespace")
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if segment == "." || segment == ".." {
			return errors.New("must not contain dot segments")
		}
	}
	if path == "/healthz" || path == "/readyz" || path == "/openapi.json" {
		return errors.New("is reserved")
	}
	return nil
}

func validateAllowList(add func(string, string), path string, fields []string, known map[string]struct{}) {
	seen := make(map[string]struct{}, len(fields))
	for i, field := range fields {
		entry := fmt.Sprintf("%s[%d]", path, i)
		if _, exists := known[field]; !exists {
			add(entry, "must reference a normalized record field")
		}
		if _, exists := seen[field]; exists {
			add(entry, "duplicates an earlier field")
		}
		seen[field] = struct{}{}
	}
}
