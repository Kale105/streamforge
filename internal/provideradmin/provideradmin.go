// Package provideradmin exposes the provider-mode DLQ administrative surface.
// It is deliberately separate from the personal-mode replay ledger.
package provideradmin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/providerdlq"
)

const (
	Scope             = "provider-admin"
	IdempotencyHeader = "Idempotency-Key"
	defaultPageSize   = 50
)

var (
	ErrUnauthorized = errors.New("provider admin authentication required")
	ErrForbidden    = errors.New("provider admin access denied")
)

// Authorizer authenticates a provider administrator for the requested scope.
// The returned actor is recorded on replay requests, never taken from a
// request header or body.
type Authorizer interface {
	Authorize(context.Context, string) (actor string, err error)
}

// Config configures one provider DLQ cluster. ClusterID is intentionally
// server-side so callers cannot inspect or replay another cluster's records.
type Config struct {
	ClusterID       string
	DefaultPageSize int
	MaximumPageSize int
}

type Service struct {
	store      providerdlq.Store
	authorizer Authorizer
	config     Config
	mux        *http.ServeMux
}

func New(store providerdlq.Store, authorizer Authorizer, config Config) (*Service, error) {
	if store == nil {
		return nil, errors.New("provider DLQ store is required")
	}
	if authorizer == nil {
		return nil, errors.New("provider admin authorizer is required")
	}
	if strings.TrimSpace(config.ClusterID) == "" || len(config.ClusterID) > providerdlq.MaxClusterIDBytes {
		return nil, errors.New("provider admin cluster ID is invalid")
	}
	if config.DefaultPageSize == 0 {
		config.DefaultPageSize = defaultPageSize
	}
	if config.MaximumPageSize == 0 {
		config.MaximumPageSize = providerdlq.MaxPageLimit
	}
	if config.DefaultPageSize <= 0 || config.MaximumPageSize < config.DefaultPageSize || config.MaximumPageSize > providerdlq.MaxPageLimit {
		return nil, errors.New("provider admin page size configuration is invalid")
	}
	s := &Service{store: store, authorizer: authorizer, config: config, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /admin/v1/provider-dlq", s.list)
	s.mux.HandleFunc("POST /admin/v1/provider-dlq/{failure_id}/replays", s.requestReplay)
	s.mux.HandleFunc("GET /admin/v1/provider-replays/{request_id}", s.getReplay)
	return s, nil
}

type requestContextKey struct{}

// RequestFromContext is for Authorizer adapters that authenticate the request
// header against an existing key store. The service installs it immediately
// before dispatch; callers cannot forge it across an HTTP boundary.
func RequestFromContext(ctx context.Context) (*http.Request, bool) {
	r, ok := ctx.Value(requestContextKey{}).(*http.Request)
	return r, ok
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestContextKey{}, r)))
}

func (s *Service) authorize(w http.ResponseWriter, r *http.Request) (string, bool) {
	actor, err := s.authorizer.Authorize(r.Context(), Scope)
	if err != nil || strings.TrimSpace(actor) == "" {
		status, detail := http.StatusForbidden, "access denied"
		if errors.Is(err, ErrUnauthorized) {
			status, detail = http.StatusUnauthorized, "authentication required"
		}
		writeProblem(w, status, detail)
		return "", false
	}
	return actor, true
}

func (s *Service) withAuthorization(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := s.authorize(w, r)
		if !ok {
			return
		}
		next(w, r, actor)
	}
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	s.withAuthorization(s.listAuthorized)(w, r)
}

func (s *Service) listAuthorized(w http.ResponseWriter, r *http.Request, _ string) {
	if unknownQuery(r, "limit", "cursor") {
		writeProblem(w, http.StatusBadRequest, "invalid query")
		return
	}
	limit, err := parseLimit(r.URL.Query().Get("limit"), s.config.DefaultPageSize, s.config.MaximumPageSize)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid limit")
		return
	}
	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	page, err := s.store.List(r.Context(), providerdlq.ListRequest{ClusterID: s.config.ClusterID, Limit: limit, After: after})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	resp := listResponse{Items: make([]failureView, 0, len(page.Items))}
	for _, failure := range page.Items {
		resp.Items = append(resp.Items, safeFailure(failure))
	}
	if page.Next != nil {
		resp.NextCursor, err = encodeCursor(*page.Next)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "query failed")
			return
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) requestReplay(w http.ResponseWriter, r *http.Request) {
	s.withAuthorization(s.requestReplayAuthorized)(w, r)
}

type replayInput struct {
	Reason         string `json:"reason"`
	TargetRevision string `json:"target_revision"`
}

func (s *Service) requestReplayAuthorized(w http.ResponseWriter, r *http.Request, actor string) {
	values := r.Header.Values(IdempotencyHeader)
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" || len(values[0]) > providerdlq.MaxReplayRequestIDBytes {
		writeProblem(w, http.StatusBadRequest, "idempotency key is required")
		return
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	var input replayInput
	if err := decoder.Decode(&input); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid replay request")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, http.StatusBadRequest, "invalid replay request")
		return
	}
	result, err := s.store.RequestReplay(r.Context(), providerdlq.ReplayRequest{ClusterID: s.config.ClusterID, FailureID: r.PathValue("failure_id"), RequestID: values[0], Actor: actor, Reason: input.Reason, TargetRevision: input.TargetRevision})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	status := http.StatusAccepted
	if !result.Accepted {
		status = http.StatusOK
	}
	writeJSON(w, status, replayView{RequestID: result.Request.RequestID, FailureID: result.Request.FailureID, SourceRevision: result.SourceRevision, TargetRevision: result.Request.TargetRevision, State: result.State, Accepted: result.Accepted})
}

func (s *Service) getReplay(w http.ResponseWriter, r *http.Request) {
	s.withAuthorization(func(w http.ResponseWriter, r *http.Request, _ string) {
		requestID := r.PathValue("request_id")
		if strings.TrimSpace(requestID) == "" || len(requestID) > providerdlq.MaxReplayRequestIDBytes {
			writeProblem(w, http.StatusBadRequest, "invalid replay request")
			return
		}
		status, err := s.store.GetReplay(r.Context(), s.config.ClusterID, requestID)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, replayStatusView{RequestID: status.Request.RequestID, FailureID: status.Request.FailureID, SourceRevision: status.SourceRevision, TargetRevision: status.Request.TargetRevision, State: status.State, Attempts: status.Attempts, RequestedAt: status.RequestedAt, StartedAt: status.StartedAt, FinishedAt: status.FinishedAt})
	})(w, r)
}

type failureView struct {
	FailureID       string            `json:"failure_id"`
	Dataset         string            `json:"dataset"`
	SourceRevision  string            `json:"source_revision"`
	Stage           providerdlq.Stage `json:"stage"`
	Class           providerdlq.Class `json:"class"`
	SourceTopic     string            `json:"source_topic"`
	SourcePartition int32             `json:"source_partition"`
	SourceOffset    int64             `json:"source_offset"`
	FailedAt        time.Time         `json:"failed_at"`
}
type listResponse struct {
	Items      []failureView `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}
type replayView struct {
	RequestID      string                  `json:"request_id"`
	FailureID      string                  `json:"failure_id"`
	SourceRevision string                  `json:"source_revision"`
	TargetRevision string                  `json:"target_revision"`
	State          providerdlq.ReplayState `json:"state"`
	Accepted       bool                    `json:"accepted"`
}

type replayStatusView struct {
	RequestID      string                  `json:"request_id"`
	FailureID      string                  `json:"failure_id"`
	SourceRevision string                  `json:"source_revision"`
	TargetRevision string                  `json:"target_revision"`
	State          providerdlq.ReplayState `json:"state"`
	Attempts       int                     `json:"attempts"`
	RequestedAt    time.Time               `json:"requested_at"`
	StartedAt      *time.Time              `json:"started_at,omitempty"`
	FinishedAt     *time.Time              `json:"finished_at,omitempty"`
}

func safeFailure(f providerdlq.Failure) failureView {
	return failureView{f.FailureID, f.Dataset, f.SourceRevision, f.Stage, f.Class, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.FailedAt}
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, providerdlq.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, providerdlq.ErrInvalidFailure), errors.Is(err, providerdlq.ErrInvalidReplayRequest), errors.Is(err, providerdlq.ErrCrossRevisionUnsupported):
		writeProblem(w, http.StatusBadRequest, "invalid replay request")
	case errors.Is(err, providerdlq.ErrReplayRequestFailed), errors.Is(err, providerdlq.ErrReplayClaimLost):
		writeProblem(w, http.StatusConflict, "replay request conflicts with current state")
	default:
		writeProblem(w, http.StatusInternalServerError, "provider admin operation failed")
	}
}
func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "detail": detail})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func unknownQuery(r *http.Request, allow ...string) bool {
	m := map[string]bool{}
	for _, k := range allow {
		m[k] = true
	}
	for k := range r.URL.Query() {
		if !m[k] {
			return true
		}
	}
	return false
}
func parseLimit(raw string, def, max int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, e := strconv.Atoi(raw)
	if e != nil || n < 1 || n > max {
		return 0, errors.New("limit")
	}
	return n, nil
}

type cursorWire struct {
	FailedAt  string `json:"failed_at"`
	FailureID string `json:"failure_id"`
}

func encodeCursor(c providerdlq.Cursor) (string, error) {
	if c.FailedAt.IsZero() || c.FailureID == "" {
		return "", errors.New("cursor")
	}
	b, e := json.Marshal(cursorWire{c.FailedAt.UTC().Format(time.RFC3339Nano), c.FailureID})
	return base64.RawURLEncoding.EncodeToString(b), e
}
func decodeCursor(raw string) (*providerdlq.Cursor, error) {
	if raw == "" {
		return nil, nil
	}
	b, e := base64.RawURLEncoding.DecodeString(raw)
	if e != nil {
		return nil, e
	}
	var w cursorWire
	if e = json.Unmarshal(b, &w); e != nil {
		return nil, e
	}
	t, e := time.Parse(time.RFC3339Nano, w.FailedAt)
	if e != nil || w.FailureID == "" {
		return nil, fmt.Errorf("cursor")
	}
	return &providerdlq.Cursor{FailedAt: t, FailureID: w.FailureID}, nil
}
