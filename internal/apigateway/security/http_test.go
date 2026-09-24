package security

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMiddlewareResponsesAndContext(t *testing.T) {
	store := NewMemoryKeyStore()
	digest, _ := HashAPIKey("top-secret")
	if err := store.PutDigest(digest, Principal{ID: "client", Scopes: []Scope{{Dataset: "games", Action: ActionRead}}}); err != nil {
		t.Fatal(err)
	}
	limiter, _ := NewLimiter(RateLimit{Rate: 1, Burst: 1}, &fakeClock{now: time.Unix(0, 0)})
	h := Middleware{Keys: store, Limiter: limiter, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "games", ActionRead, true })}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok || p.ID != "client" {
			t.Error("missing principal")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	assertHTTPError(t, h, "", http.StatusUnauthorized)
	assertHTTPError(t, h, "wrong", http.StatusUnauthorized)
	multiple := httptest.NewRequest(http.MethodGet, "/v1/games", nil)
	multiple.Header.Add(APIKeyHeader, "top-secret")
	multiple.Header.Add(APIKeyHeader, "another-secret")
	multipleResponse := httptest.NewRecorder()
	h.ServeHTTP(multipleResponse, multiple)
	if multipleResponse.Code != http.StatusUnauthorized {
		t.Fatalf("multiple keys = %d", multipleResponse.Code)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/games", nil)
	r.Header.Set(APIKeyHeader, "top-secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("authorized = %d", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/games", nil)
	r.Header.Set(APIKeyHeader, "top-secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("limited = %d retry %q", w.Code, w.Header().Get("Retry-After"))
	}
}

func TestMiddlewareForbiddenAndUnprotected(t *testing.T) {
	store := NewMemoryKeyStore()
	digest, _ := HashAPIKey("secret")
	_ = store.PutDigest(digest, Principal{ID: "c"})
	protected := Middleware{Keys: store, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "games", ActionRead, true })}.Wrap(http.NotFoundHandler())
	assertHTTPError(t, protected, "secret", http.StatusForbidden)
	open := Middleware{Keys: store, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "", "", false })}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	w := httptest.NewRecorder()
	open.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unprotected = %d", w.Code)
	}
}

func TestMiddlewareConcurrent(t *testing.T) {
	store := NewMemoryKeyStore()
	d, _ := HashAPIKey("secret")
	_ = store.PutDigest(d, Principal{ID: "c", Scopes: []Scope{{Dataset: "games", Action: ActionRead}}})
	limiter, _ := NewLimiter(RateLimit{Rate: 1, Burst: 100}, systemClock{})
	h := Middleware{Keys: store, Limiter: limiter, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "games", ActionRead, true })}.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	var wg sync.WaitGroup
	statuses := make(chan int, 200)
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set(APIKeyHeader, "secret")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			statuses <- w.Code
		}()
	}
	wg.Wait()
	close(statuses)
	ok := 0
	for status := range statuses {
		if status == http.StatusOK {
			ok++
		}
	}
	if ok != 100 {
		t.Fatalf("ok = %d", ok)
	}
}

func TestMiddlewareQuotaFailsClosedAndDoesNotChargeUnauthorized(t *testing.T) {
	store := NewMemoryKeyStore()
	digest, _ := HashAPIKey("secret")
	_ = store.PutDigest(digest, Principal{ID: "client", Scopes: []Scope{{Dataset: "games", Action: ActionRead}}})
	checker := &quotaStub{err: context.DeadlineExceeded}
	h := Middleware{Keys: store, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "games", ActionRead, true }), QuotaChecker: checker}.Wrap(http.NotFoundHandler())
	assertHTTPError(t, h, "wrong", http.StatusUnauthorized)
	if checker.calls != 0 {
		t.Fatalf("unauthorized calls = %d", checker.calls)
	}
	assertHTTPError(t, h, "secret", http.StatusServiceUnavailable)
	if checker.calls != 1 {
		t.Fatalf("quota calls = %d", checker.calls)
	}
}

func TestMiddlewareRecordsOnlySafeAuthorizedUsage(t *testing.T) {
	store := NewMemoryKeyStore()
	digest, _ := HashAPIKey("secret")
	_ = store.PutDigest(digest, Principal{ID: "client", Scopes: []Scope{{Dataset: "games", Action: ActionRead}}})
	usage := &usageStub{}
	h := Middleware{Keys: store, Scopes: RequestScopeFunc(func(*http.Request) (string, Action, bool) { return "games", ActionRead, true }), Usage: usage}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) }))
	assertHTTPError(t, h, "wrong", http.StatusUnauthorized)
	r := httptest.NewRequest(http.MethodGet, "/v1/games?secret=leak", nil)
	r.Header.Set(APIKeyHeader, "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d", w.Code)
	}
	if len(usage.usages) != 1 || usage.usages[0] != (Usage{PrincipalID: "client", Dataset: "games", Status: http.StatusCreated}) {
		t.Fatalf("usage = %#v", usage.usages)
	}
}

type quotaStub struct {
	calls int
	err   error
}

func (s *quotaStub) CheckQuota(context.Context, QuotaRequest) (QuotaDecision, error) {
	s.calls++
	return QuotaDecision{}, s.err
}

type usageStub struct{ usages []Usage }

func (s *usageStub) RecordUsage(_ context.Context, usage Usage) error {
	s.usages = append(s.usages, usage)
	return nil
}

func assertHTTPError(t *testing.T, h http.Handler, key string, want int) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if key != "" {
		r.Header.Set(APIKeyHeader, key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("status = %d; want %d", w.Code, want)
	}
	if w.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatal("non-JSON error")
	}
	if key != "" && json.Valid(w.Body.Bytes()) && strings.Contains(w.Body.String(), key) {
		t.Fatal("key leaked")
	}
}
