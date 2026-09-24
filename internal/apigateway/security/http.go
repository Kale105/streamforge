package security

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

const APIKeyHeader = "X-API-Key"

// RequestScope resolves the dataset and action required by a request. Returning
// known=false leaves the route unprotected, which is useful for health checks.
type RequestScope interface {
	Scope(*http.Request) (dataset string, action Action, known bool)
}

// RequestScopeFunc adapts a function to RequestScope.
type RequestScopeFunc func(*http.Request) (string, Action, bool)

func (f RequestScopeFunc) Scope(r *http.Request) (string, Action, bool) { return f(r) }

// Middleware authenticates protected requests, checks their scope, and applies
// a per-key rate limit. It neither logs nor puts API-key material in responses.
type Middleware struct {
	Keys         KeyStore
	Scopes       RequestScope
	Limiter      *Limiter // Deprecated compatibility adapter for local mode.
	QuotaChecker QuotaChecker
	Usage        UsageRecorder
}

func (m Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.Scopes == nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		dataset, action, protected := m.Scopes.Scope(r)
		if !protected {
			next.ServeHTTP(w, r)
			return
		}
		// Reject ambiguous credentials rather than silently choosing one value.
		values := r.Header.Values(APIKeyHeader)
		if len(values) != 1 {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		key, err := HashAPIKey(values[0])
		if err != nil || m.Keys == nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		principal, found, err := m.Keys.LookupKey(r.Context(), key)
		if err != nil || !found || principal.ID == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !Authorized(principal, dataset, action) {
			writeError(w, http.StatusForbidden, "access denied")
			return
		}
		checker := m.QuotaChecker
		if checker == nil && m.Limiter != nil {
			checker = m.Limiter
		}
		if checker != nil {
			decision, err := checker.CheckQuota(r.Context(), QuotaRequest{PrincipalID: principal.ID, Dataset: dataset})
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, "quota service unavailable")
				return
			}
			if !decision.Allowed {
				w.Header().Set("Retry-After", strconv.FormatInt(retrySeconds(decision.RetryAfter), 10))
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
		}
		request := r.WithContext(context.WithValue(r.Context(), principalContextKey{}, clonePrincipal(principal)))
		if m.Usage == nil {
			next.ServeHTTP(w, request)
			return
		}
		tracked := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(tracked, request)
		// Usage is intentionally non-billing and best-effort. It cannot expose
		// keys because Usage has no credential fields.
		_ = m.Usage.RecordUsage(request.Context(), Usage{PrincipalID: principal.ID, Dataset: dataset, Status: tracked.status})
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

type principalContextKey struct{}

// PrincipalFromContext returns the authenticated principal, if the middleware
// accepted the request. It never contains an API key or digest.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey{}).(Principal)
	return clonePrincipal(p), ok
}

type errorResponse struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Status: status, Title: http.StatusText(status), Detail: message})
}

func retrySeconds(d time.Duration) int64 {
	seconds := int64((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
