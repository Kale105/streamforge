// Package api exposes bounded read-only dataset endpoints.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/recordstore"
)

const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 15 * time.Second
	defaultIdleTimeout  = 60 * time.Second
	defaultHeaderLimit  = 1 << 20
	maximumCursorLength = 4096
)

type Readiness interface {
	Ready(context.Context) error
}

type Config struct {
	Datasets        map[string]DatasetPolicy
	DefaultPageSize int
	MaximumPageSize int
}

type DatasetPolicy struct {
	Path    string
	Version string
	Fields  []string
	Filters []string
	Sorts   []recordstore.SortField
}

type API struct {
	reader    recordstore.Reader
	readiness Readiness
	config    Config
	mux       *http.ServeMux
}

func New(reader recordstore.Reader, readiness Readiness, config Config) (*API, error) {
	if reader == nil {
		return nil, errors.New("API record reader is required")
	}
	if readiness == nil {
		return nil, errors.New("API readiness checker is required")
	}
	if len(config.Datasets) == 0 {
		return nil, errors.New("API requires at least one allowed dataset")
	}
	if config.DefaultPageSize <= 0 {
		return nil, errors.New("API default page size must be positive")
	}
	if config.DefaultPageSize > recordstore.MaxPageItems {
		return nil, fmt.Errorf("API default page size cannot exceed hard page cap of %d", recordstore.MaxPageItems)
	}
	if config.MaximumPageSize < config.DefaultPageSize {
		return nil, errors.New("API maximum page size must be at least the default")
	}
	if config.MaximumPageSize > recordstore.MaxPageItems {
		return nil, fmt.Errorf("API maximum page size cannot exceed hard page cap of %d", recordstore.MaxPageItems)
	}

	paths := make(map[string]string, len(config.Datasets))
	for name, policy := range config.Datasets {
		if err := dataset.ValidateAPIPath(policy.Path); err != nil {
			return nil, fmt.Errorf("API path for dataset %q %q: %w", name, policy.Path, err)
		}
		if previous, exists := paths[policy.Path]; exists {
			return nil, fmt.Errorf("API path %q is shared by datasets %q and %q", policy.Path, previous, name)
		}
		if len(policy.Fields) == 0 {
			return nil, fmt.Errorf("API fields for dataset %q cannot be empty", name)
		}
		if policy.Version == "" {
			return nil, fmt.Errorf("API version for dataset %q cannot be empty", name)
		}
		if len(policy.Filters) > recordstore.MaxFilters {
			return nil, fmt.Errorf("API filters for dataset %q cannot exceed %d", name, recordstore.MaxFilters)
		}
		if err := validateFilters(policy.Filters); err != nil {
			return nil, fmt.Errorf("API %s for dataset %q", err, name)
		}
		if err := validateSorts(policy.Sorts); err != nil {
			return nil, fmt.Errorf("API %s for dataset %q", err, name)
		}
		paths[policy.Path] = name
	}

	api := &API{
		reader:    reader,
		readiness: readiness,
		config:    config,
		mux:       http.NewServeMux(),
	}
	api.mux.HandleFunc("GET /healthz", api.health)
	api.mux.HandleFunc("GET /readyz", api.ready)
	for name, policy := range config.Datasets {
		datasetName, datasetPolicy := name, policy
		api.mux.HandleFunc("GET "+policy.Path, func(w http.ResponseWriter, r *http.Request) {
			api.listRecords(datasetName, datasetPolicy, w, r)
		})
	}
	return api, nil
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

// NewServer applies defensive HTTP timeouts around an API handler.
func NewServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: defaultReadTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    defaultHeaderLimit,
	}
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	if err := a.readiness.Ready(r.Context()); err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "not ready", "a required dependency is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *API) listRecords(dataset string, policy DatasetPolicy, w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r, a.config.DefaultPageSize, a.config.MaximumPageSize)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid limit", err.Error())
		return
	}
	query, err := parseQuery(r, dataset, policy)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid query", err.Error())
		return
	}
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid cursor", "cursor is malformed or unsupported")
		return
	}

	if cursor != nil {
		if cursor.DatasetVersion != policy.Version || cursor.Sort != string(query.Sort) || cursor.Direction != string(query.Direction) || cursor.FilterFingerprint != query.FilterFingerprint {
			writeProblem(w, http.StatusBadRequest, "invalid cursor", "cursor does not match this dataset query")
			return
		}
		query.After = cursor
	}
	var page recordstore.Page
	if advanced, ok := a.reader.(recordstore.QueryReader); ok {
		page, err = advanced.Query(r.Context(), query, limit)
	} else if len(query.Filters) == 0 && query.Sort == recordstore.SortUpdatedAt && query.Direction == recordstore.SortAscending {
		page, err = a.reader.List(r.Context(), dataset, policy.Version, cursor, limit)
	} else {
		writeProblem(w, http.StatusNotImplemented, "query unsupported", "configured storage does not support filters or custom sorting")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "query failed", "records could not be read")
		return
	}
	if err := projectRecords(page.Records, policy.Fields); err != nil {
		writeProblem(w, http.StatusInternalServerError, "query failed", "stored records do not match the published API contract")
		return
	}

	encoded, err := encodeBoundedListResponse(page, policy.Version, query)
	if err != nil {
		writeProblem(w, http.StatusRequestEntityTooLarge, "response too large", "a single record exceeds the API response byte budget")
		return
	}
	writeEncodedJSON(w, http.StatusOK, encoded, "application/json")
}

// encodeBoundedListResponse emits a complete response before any HTTP headers
// are sent. If a legal page is too large, it keeps the longest fitting prefix
// and creates a cursor from that prefix's final record so omitted records are
// neither skipped nor duplicated on the next request.
func encodeBoundedListResponse(page recordstore.Page, version string, query recordstore.Query) ([]byte, error) {
	for count := len(page.Records); count >= 0; count-- {
		next, err := responseCursor(page, count, version, query)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(listResponse{Records: page.Records[:count], NextCursor: next})
		if err != nil {
			return nil, fmt.Errorf("encode response: %w", err)
		}
		if len(encoded)+1 <= recordstore.MaxResponseBytes { // writeEncodedJSON adds a newline.
			return append(encoded, '\n'), nil
		}
	}
	return nil, errors.New("response exceeds byte budget")
}

func responseCursor(page recordstore.Page, count int, version string, query recordstore.Query) (string, error) {
	var cursor *recordstore.Cursor
	if count < len(page.Records) {
		if count == 0 {
			return "", errors.New("first record exceeds byte budget")
		}
		last := page.Records[count-1]
		cursor = &recordstore.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
	} else if page.Next != nil {
		copy := *page.Next
		cursor = &copy
	}
	if cursor == nil {
		return "", nil
	}
	cursor.DatasetVersion = version
	cursor.Sort = string(query.Sort)
	cursor.Direction = string(query.Direction)
	cursor.FilterFingerprint = query.FilterFingerprint
	return encodeCursor(*cursor)
}

func parseQuery(r *http.Request, dataset string, policy DatasetPolicy) (recordstore.Query, error) {
	query := recordstore.Query{Dataset: dataset, DatasetVersion: policy.Version, AllowedFields: append([]string(nil), policy.Filters...), Sort: recordstore.SortUpdatedAt, Direction: recordstore.SortAscending}
	values := r.URL.Query()
	explicitSort := false
	explicitDirection := false
	if len(values) > recordstore.MaxFilters+4 {
		return query, errors.New("too many query parameters")
	}
	for name, entries := range values {
		switch name {
		case "limit", "cursor":
			if len(entries) != 1 {
				return query, fmt.Errorf("query parameter %q must appear once", name)
			}
		case "sort":
			if len(entries) != 1 {
				return query, errors.New("sort must appear once")
			}
			query.Sort = recordstore.SortField(entries[0])
			explicitSort = true
		case "direction":
			if len(entries) != 1 {
				return query, errors.New("direction must appear once")
			}
			query.Direction = recordstore.SortDirection(entries[0])
			explicitDirection = true
		default:
			if !strings.HasPrefix(name, "filter.") {
				return query, fmt.Errorf("unsupported query parameter %q", name)
			}
			field := strings.TrimPrefix(name, "filter.")
			if field == "" || !contains(policy.Filters, field) {
				return query, fmt.Errorf("filter field %q is not allowed", field)
			}
			if len(entries) != 1 {
				return query, fmt.Errorf("filter %q must appear once", field)
			}
			if len(entries[0]) == 0 || len(entries[0]) > recordstore.MaxFilterValueBytes {
				return query, fmt.Errorf("filter %q value is invalid or too large", field)
			}
			value := json.RawMessage(entries[0])
			if !isJSONScalar(value) {
				return query, fmt.Errorf("filter %q must be a JSON scalar", field)
			}
			query.Filters = append(query.Filters, recordstore.Filter{Field: field, Value: append(json.RawMessage(nil), value...)})
		}
	}
	if query.Sort != recordstore.SortUpdatedAt && query.Sort != recordstore.SortID {
		return query, errors.New("sort must be updated_at or id")
	}
	// An omitted sort retains the legacy, deterministic updated_at ascending
	// default. Any client-requested sort (including a direction override for
	// the default field) must appear in the configured allow-list.
	if (explicitSort || explicitDirection) && !containsSort(policy.Sorts, query.Sort) {
		return query, fmt.Errorf("sort field %q is not allowed", query.Sort)
	}
	if query.Direction != recordstore.SortAscending && query.Direction != recordstore.SortDescending {
		return query, errors.New("direction must be asc or desc")
	}
	if len(query.Filters) > recordstore.MaxFilters {
		return query, fmt.Errorf("filters cannot exceed %d", recordstore.MaxFilters)
	}
	sort.Slice(query.Filters, func(i, j int) bool { return query.Filters[i].Field < query.Filters[j].Field })
	query.FilterFingerprint = filterFingerprint(query.Filters)
	return query, nil
}

func isJSONScalar(value json.RawMessage) bool {
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	if decoder.Decode(&decoded) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false
	}
	switch decoded.(type) {
	case nil, bool, string, json.Number:
		return true
	default:
		return false
	}
}

func filterFingerprint(filters []recordstore.Filter) string {
	h := sha256.New()
	for _, filter := range filters {
		h.Write([]byte(filter.Field))
		h.Write([]byte{0})
		h.Write(filter.Value)
		h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func containsSort(values []recordstore.SortField, want recordstore.SortField) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func validateFilters(values []string) error {
	seen := map[string]bool{}
	for _, value := range values {
		if value == "" || len(value) > 128 {
			return fmt.Errorf("filter field %q is invalid", value)
		}
		if seen[value] {
			return fmt.Errorf("filter field %q is duplicated", value)
		}
		seen[value] = true
	}
	return nil
}
func validateSorts(values []recordstore.SortField) error {
	seen := map[recordstore.SortField]bool{}
	for _, value := range values {
		if value != recordstore.SortID && value != recordstore.SortUpdatedAt {
			return fmt.Errorf("sort %q is unsupported", value)
		}
		if seen[value] {
			return fmt.Errorf("sort %q is duplicated", value)
		}
		seen[value] = true
	}
	return nil
}

func projectRecords(records []recordstore.Record, fields []string) error {
	for i := range records {
		var source map[string]json.RawMessage
		if err := json.Unmarshal(records[i].Data, &source); err != nil || source == nil {
			return fmt.Errorf("decode record %q", records[i].ID)
		}
		projected := make(map[string]json.RawMessage, len(fields))
		for _, field := range fields {
			value, exists := source[field]
			if !exists {
				return fmt.Errorf("record %q is missing field %q", records[i].ID, field)
			}
			projected[field] = value
		}
		encoded, err := json.Marshal(projected)
		if err != nil {
			return fmt.Errorf("encode record %q projection: %w", records[i].ID, err)
		}
		records[i].Data = encoded
	}
	return nil
}

type listResponse struct {
	Records    []recordstore.Record `json:"records"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

type problem struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	writeJSONStatus(w, status, problem{Status: status, Title: title, Detail: detail}, "application/problem+json")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	writeJSONStatus(w, status, value, "application/json")
}

func writeJSONStatus(w http.ResponseWriter, status int, value any, contentType string) {
	encoded, err := json.Marshal(value)
	if err != nil {
		// All built-in responses are marshalable. Do not send a partial success
		// header if a future handler accidentally passes an unsupported value.
		encoded = []byte(`{"status":500,"title":"internal server error","detail":"response encoding failed"}`)
		status = http.StatusInternalServerError
		contentType = "application/problem+json"
	}
	encoded = append(encoded, '\n')
	writeEncodedJSON(w, status, encoded, contentType)
}

func writeEncodedJSON(w http.ResponseWriter, status int, encoded []byte, contentType string) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func parseLimit(r *http.Request, defaultLimit, maximumLimit int) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	cap := maximumLimit
	if cap > recordstore.MaxPageItems {
		cap = recordstore.MaxPageItems
	}
	if limit > cap {
		return 0, fmt.Errorf("limit cannot exceed %d", cap)
	}
	return limit, nil
}

type cursorPayload struct {
	Version           int    `json:"v"`
	UpdatedAt         string `json:"updated_at"`
	ID                string `json:"id"`
	Revision          string `json:"revision,omitempty"`
	Sort              string `json:"sort,omitempty"`
	Direction         string `json:"direction,omitempty"`
	FilterFingerprint string `json:"filter_fingerprint,omitempty"`
}

func encodeCursor(cursor recordstore.Cursor) (string, error) {
	if cursor.UpdatedAt.IsZero() || cursor.ID == "" {
		return "", errors.New("cursor requires updated_at and id")
	}
	encoded, err := json.Marshal(cursorPayload{
		Version:           2,
		UpdatedAt:         cursor.UpdatedAt.UTC().Format(time.RFC3339Nano),
		ID:                cursor.ID,
		Revision:          cursor.DatasetVersion,
		Sort:              cursor.Sort,
		Direction:         cursor.Direction,
		FilterFingerprint: cursor.FilterFingerprint,
	})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	result := base64.RawURLEncoding.EncodeToString(encoded)
	if len(result) > maximumCursorLength {
		return "", errors.New("cursor exceeds maximum size")
	}
	return result, nil
}

func decodeCursor(raw string) (*recordstore.Cursor, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > maximumCursorLength {
		return nil, errors.New("cursor exceeds maximum size")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	var payload cursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, fmt.Errorf("decode cursor payload: %w", err)
	}
	if (payload.Version != 1 && payload.Version != 2) || payload.ID == "" {
		return nil, errors.New("unsupported cursor")
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, payload.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("decode cursor timestamp: %w", err)
	}
	return &recordstore.Cursor{UpdatedAt: updatedAt, ID: payload.ID, DatasetVersion: payload.Revision, Sort: payload.Sort, Direction: payload.Direction, FilterFingerprint: payload.FilterFingerprint}, nil
}

func unknownQueryParameter(r *http.Request, allowed ...string) string {
	allow := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allow[name] = struct{}{}
	}
	for name := range r.URL.Query() {
		if _, ok := allow[name]; !ok {
			return name
		}
	}
	return ""
}
