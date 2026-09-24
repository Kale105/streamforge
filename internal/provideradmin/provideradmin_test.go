package provideradmin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/providerdlq"
)

type testAuthorizer struct {
	actor string
	err   error
	scope string
}

func (a *testAuthorizer) Authorize(_ context.Context, scope string) (string, error) {
	a.scope = scope
	return a.actor, a.err
}

type testStore struct {
	list       providerdlq.Page
	listErr    error
	gotList    providerdlq.ListRequest
	replay     providerdlq.ReplayRequestResult
	replayErr  error
	gotReplay  providerdlq.ReplayRequest
	status     providerdlq.ReplayStatus
	statusErr  error
	gotCluster string
	gotStatus  string
}

func (s *testStore) Upsert(context.Context, providerdlq.Failure) (providerdlq.Failure, error) {
	panic("unexpected")
}
func (s *testStore) List(_ context.Context, r providerdlq.ListRequest) (providerdlq.Page, error) {
	s.gotList = r
	return s.list, s.listErr
}
func (s *testStore) RequestReplay(_ context.Context, r providerdlq.ReplayRequest) (providerdlq.ReplayRequestResult, error) {
	s.gotReplay = r
	return s.replay, s.replayErr
}
func (s *testStore) GetReplay(_ context.Context, clusterID, id string) (providerdlq.ReplayStatus, error) {
	s.gotCluster = clusterID
	s.gotStatus = id
	return s.status, s.statusErr
}
func (s *testStore) ClaimReplay(context.Context, providerdlq.ClaimRequest) (*providerdlq.ReplayClaim, error) {
	panic("unexpected")
}
func (s *testStore) CompleteReplay(context.Context, string, string) error    { panic("unexpected") }
func (s *testStore) FailReplay(context.Context, string, string, error) error { panic("unexpected") }

func newTestService(t *testing.T, store *testStore, auth *testAuthorizer) *Service {
	t.Helper()
	s, err := New(store, auth, Config{ClusterID: "local", DefaultPageSize: 2, MaximumPageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProviderAdminAuthorizationAndPagination(t *testing.T) {
	for _, tc := range []struct {
		name string
		auth *testAuthorizer
		url  string
		want int
	}{
		{"unauthenticated", &testAuthorizer{err: ErrUnauthorized}, "/admin/v1/provider-dlq", http.StatusUnauthorized},
		{"forbidden", &testAuthorizer{err: ErrForbidden}, "/admin/v1/provider-dlq", http.StatusForbidden},
		{"invalid limit", &testAuthorizer{actor: "ops"}, "/admin/v1/provider-dlq?limit=11", http.StatusBadRequest},
		{"unknown query", &testAuthorizer{actor: "ops"}, "/admin/v1/provider-dlq?debug=yes", http.StatusBadRequest},
		{"valid page", &testAuthorizer{actor: "ops"}, "/admin/v1/provider-dlq?limit=3", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &testStore{}
			a := tc.auth
			s := newTestService(t, st, a)
			rr := httptest.NewRecorder()
			s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.url, nil))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			if a.scope != Scope {
				t.Fatalf("scope=%q", a.scope)
			}
			if tc.want == http.StatusOK && (st.gotList.ClusterID != "local" || st.gotList.Limit != 3) {
				t.Fatalf("list=%+v", st.gotList)
			}
		})
	}
}

func TestRequestReplayUsesAuthenticatedActorAndIdempotencyKey(t *testing.T) {
	st := &testStore{replay: providerdlq.ReplayRequestResult{Request: providerdlq.ReplayRequest{RequestID: "req-1", FailureID: "failure-1", TargetRevision: "v2"}, SourceRevision: "v1", State: providerdlq.ReplayRequested, Accepted: true}}
	s := newTestService(t, st, &testAuthorizer{actor: "operator-7"})
	r := httptest.NewRequest(http.MethodPost, "/admin/v1/provider-dlq/failure-1/replays", strings.NewReader(`{"reason":"fixed parser","target_revision":"v2"}`))
	r.Header.Set(IdempotencyHeader, "req-1")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, r)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d %s", rr.Code, rr.Body.String())
	}
	if got := st.gotReplay; got.Actor != "operator-7" || got.RequestID != "req-1" || got.ClusterID != "local" || got.TargetRevision != "v2" || got.Reason != "fixed parser" {
		t.Fatalf("request=%+v", got)
	}
	if !strings.Contains(rr.Body.String(), `"source_revision":"v1"`) {
		t.Fatalf("missing source revision: %s", rr.Body.String())
	}
}

func TestRequestReplayInputAndStoreErrors(t *testing.T) {
	for _, tc := range []struct {
		name, key, body string
		err             error
		want            int
	}{
		{"missing key", "", `{"reason":"x","target_revision":"v1"}`, nil, http.StatusBadRequest},
		{"unknown field", "key", `{"reason":"x","target_revision":"v1","nope":true}`, nil, http.StatusBadRequest},
		{"not found", "key", `{"reason":"x","target_revision":"v1"}`, providerdlq.ErrNotFound, http.StatusNotFound},
		{"revision conflict", "key", `{"reason":"x","target_revision":"v1"}`, providerdlq.ErrCrossRevisionUnsupported, http.StatusBadRequest},
		{"failed conflict", "key", `{"reason":"x","target_revision":"v1"}`, providerdlq.ErrReplayRequestFailed, http.StatusConflict},
		{"internal does not leak", "key", `{"reason":"x","target_revision":"v1"}`, errors.New("password=secret"), http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &testStore{replayErr: tc.err}
			s := newTestService(t, st, &testAuthorizer{actor: "ops"})
			r := httptest.NewRequest(http.MethodPost, "/admin/v1/provider-dlq/f/replays", strings.NewReader(tc.body))
			if tc.key != "" {
				r.Header.Set(IdempotencyHeader, tc.key)
			}
			rr := httptest.NewRecorder()
			s.ServeHTTP(rr, r)
			if rr.Code != tc.want {
				t.Fatalf("status=%d %s", rr.Code, rr.Body.String())
			}
			if strings.Contains(rr.Body.String(), "secret") {
				t.Fatal("leaked store diagnostic")
			}
		})
	}
}

func TestGetReplayDoesNotExposeDiagnostics(t *testing.T) {
	now := time.Now().UTC()
	st := &testStore{status: providerdlq.ReplayStatus{Request: providerdlq.ReplayRequest{RequestID: "req-9", FailureID: "f", TargetRevision: "v2"}, SourceRevision: "v1", State: providerdlq.ReplayFailed, Attempts: 2, Diagnostic: "token=secret", RequestedAt: now}}
	s := newTestService(t, st, &testAuthorizer{actor: "ops"})
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/v1/provider-replays/req-9", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	if st.gotStatus != "req-9" || st.gotCluster != "local" || strings.Contains(rr.Body.String(), "secret") || !strings.Contains(rr.Body.String(), `"source_revision":"v1"`) {
		t.Fatalf("body=%s cluster=%q got=%q", rr.Body.String(), st.gotCluster, st.gotStatus)
	}
}
