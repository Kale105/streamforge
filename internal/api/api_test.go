package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/recordstore"
)

func TestListRecordsUsesBoundedCursorPagination(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.September, 16, 1, 2, 3, 0, time.UTC)
	reader := &readerStub{page: recordstore.Page{
		Records: []recordstore.Record{{Dataset: "games", ID: "game-1", Data: json.RawMessage(`{"score":10}`), UpdatedAt: updatedAt}},
		Next:    &recordstore.Cursor{UpdatedAt: updatedAt, ID: "game-1"},
	}}
	api := newTestAPI(t, reader, readinessStub{})

	request := httptest.NewRequest(http.MethodGet, "/v1/games?limit=25", nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if reader.dataset != "games" || reader.version != "v1" || reader.limit != 25 || reader.cursor != nil {
		t.Fatalf("List args = dataset %q, version %q, limit %d, cursor %#v", reader.dataset, reader.version, reader.limit, reader.cursor)
	}
	var body listResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Records) != 1 || body.NextCursor == "" {
		t.Fatalf("response = %#v; want one record and next cursor", body)
	}
	if string(body.Records[0].Data) != `{"score":10}` {
		t.Errorf("projected data = %s; want only published score field", body.Records[0].Data)
	}
	decoded, err := decodeCursor(body.NextCursor)
	if err != nil {
		t.Fatalf("decode next cursor: %v", err)
	}
	if decoded.ID != "game-1" || !decoded.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("decoded cursor = %#v", decoded)
	}
}

func TestListRecordsRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want int
	}{
		{name: "unknown dataset", url: "/v1/unknown", want: http.StatusNotFound},
		{name: "zero limit", url: "/v1/games?limit=0", want: http.StatusBadRequest},
		{name: "excessive limit", url: "/v1/games?limit=101", want: http.StatusBadRequest},
		{name: "bad cursor", url: "/v1/games?cursor=not-base64!", want: http.StatusBadRequest},
		{name: "oversized cursor", url: "/v1/games?cursor=" + strings.Repeat("a", maximumCursorLength+1), want: http.StatusBadRequest},
		{name: "unconfigured sort", url: "/v1/games?sort=id", want: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI(t, &readerStub{}, readinessStub{})
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.url, nil))
			if response.Code != test.want {
				t.Fatalf("status = %d; want %d; body=%s", response.Code, test.want, response.Body.String())
			}
			if test.want != http.StatusNotFound && response.Header().Get("Content-Type") != "application/problem+json" {
				contentType := response.Header().Get("Content-Type")
				t.Errorf("Content-Type = %q; want application/problem+json", contentType)
			}
		})
	}
}

func TestSortPolicyMakesExplicitSortsAuthoritative(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, url string
		policy    DatasetPolicy
		want      int
	}{
		{name: "deny id outside allowlist", url: "/v1/games?sort=id", policy: DatasetPolicy{Path: "/v1/games", Version: "v1", Fields: []string{"score"}, Sorts: []recordstore.SortField{recordstore.SortUpdatedAt}}, want: http.StatusBadRequest},
		{name: "allow configured id", url: "/v1/games?sort=id&direction=desc", policy: DatasetPolicy{Path: "/v1/games", Version: "v1", Fields: []string{"score"}, Sorts: []recordstore.SortField{recordstore.SortID}}, want: http.StatusOK},
		{name: "empty policy keeps implicit legacy default", url: "/v1/games", policy: DatasetPolicy{Path: "/v1/games", Version: "v1", Fields: []string{"score"}}, want: http.StatusOK},
		{name: "empty policy rejects explicit default direction", url: "/v1/games?direction=desc", policy: DatasetPolicy{Path: "/v1/games", Version: "v1", Fields: []string{"score"}}, want: http.StatusBadRequest},
		{name: "empty policy rejects explicit updated sort", url: "/v1/games?sort=updated_at", policy: DatasetPolicy{Path: "/v1/games", Version: "v1", Fields: []string{"score"}}, want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &queryReaderStub{page: recordstore.Page{Records: []recordstore.Record{{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: json.RawMessage(`{"score":1}`), UpdatedAt: time.Now()}}}}
			api, err := New(reader, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": test.policy}, DefaultPageSize: 2, MaximumPageSize: 10})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, test.url, nil))
			if w.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, test.want, w.Body.String())
			}
			if test.want == http.StatusOK && (reader.query.Sort == "" || reader.query.Direction == "") {
				t.Fatalf("missing query ordering: %#v", reader.query)
			}
		})
	}
}

func TestHiddenConfiguredFilterDoesNotBecomePublicField(t *testing.T) {
	t.Parallel()
	reader := &queryReaderStub{page: recordstore.Page{Records: []recordstore.Record{{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: json.RawMessage(`{"score":10,"internal_state":"accepted"}`), UpdatedAt: time.Now()}}}}
	api, err := New(reader, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}, Filters: []string{"internal_state"}}}, DefaultPageSize: 2, MaximumPageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/games?filter.internal_state=%22accepted%22", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if len(reader.query.Filters) != 1 || reader.query.Filters[0].Field != "internal_state" {
		t.Fatalf("query=%#v", reader.query)
	}
	var body listResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Records) != 1 || string(body.Records[0].Data) != `{"score":10}` {
		t.Fatalf("projected records=%#v", body.Records)
	}
}

func TestListRecordsBuildsBoundedAdvancedQuery(t *testing.T) {
	t.Parallel()
	updatedAt := time.Date(2026, 9, 17, 1, 2, 3, 0, time.UTC)
	reader := &queryReaderStub{page: recordstore.Page{Records: []recordstore.Record{{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: json.RawMessage(`{"score":10}`), UpdatedAt: updatedAt}}, Next: &recordstore.Cursor{UpdatedAt: updatedAt, ID: "a"}}}
	api, err := New(reader, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}, Filters: []string{"score"}, Sorts: []recordstore.SortField{recordstore.SortID}}}, DefaultPageSize: 2, MaximumPageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/games?filter.score=10&sort=id&direction=desc", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	if reader.query.Sort != recordstore.SortID || reader.query.Direction != recordstore.SortDescending || len(reader.query.Filters) != 1 || string(reader.query.Filters[0].Value) != "10" {
		t.Fatalf("query = %#v", reader.query)
	}
	var response listResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.NextCursor == "" {
		t.Fatal("missing next cursor")
	}
	cursor, err := decodeCursor(response.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.DatasetVersion != "v1" || cursor.Sort != "id" || cursor.Direction != "desc" || cursor.FilterFingerprint == "" {
		t.Fatalf("cursor = %#v", cursor)
	}
}

func TestAdvancedQueryRejectsUnsafeShapesAndCursorMismatch(t *testing.T) {
	t.Parallel()
	api, err := New(&queryReaderStub{}, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score", "state"}, Filters: []string{"score", "state"}, Sorts: []recordstore.SortField{recordstore.SortID}}}, DefaultPageSize: 2, MaximumPageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := encodeCursor(recordstore.Cursor{UpdatedAt: time.Now().UTC(), ID: "a", DatasetVersion: "other", Sort: "updated_at", Direction: "asc", FilterFingerprint: filterFingerprint(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"/v1/games?filter.unknown=1", "/v1/games?filter.score=1&filter.score=2", "/v1/games?filter.score={}",
		"/v1/games?filter.score=" + strings.Repeat("1", recordstore.MaxFilterValueBytes+1), "/v1/games?filter.score=1&cursor=" + seed,
	} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, raw, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d body=%s", raw[:min(len(raw), 80)], w.Code, w.Body.String())
		}
	}
}

func TestAdvancedCursorPaginationHasNoGapsOrDuplicatesInEitherDirection(t *testing.T) {
	t.Parallel()
	for _, direction := range []string{"asc", "desc"} {
		t.Run(direction, func(t *testing.T) {
			reader := &pagingReader{records: []recordstore.Record{
				{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: json.RawMessage(`{"score":1}`), UpdatedAt: time.Unix(1, 0)},
				{Dataset: "games", DatasetVersion: "v1", ID: "b", Data: json.RawMessage(`{"score":2}`), UpdatedAt: time.Unix(2, 0)},
				{Dataset: "games", DatasetVersion: "v1", ID: "c", Data: json.RawMessage(`{"score":3}`), UpdatedAt: time.Unix(3, 0)},
			}}
			api, err := New(reader, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}, Sorts: []recordstore.SortField{recordstore.SortID}}}, DefaultPageSize: 2, MaximumPageSize: 2})
			if err != nil {
				t.Fatal(err)
			}
			url := "/v1/games?sort=id&direction=" + direction
			var got []string
			for url != "" {
				w := httptest.NewRecorder()
				api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
				if w.Code != http.StatusOK {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
				var page listResponse
				if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
					t.Fatal(err)
				}
				for _, record := range page.Records {
					got = append(got, record.ID)
				}
				if page.NextCursor == "" {
					url = ""
				} else {
					url = "/v1/games?sort=id&direction=" + direction + "&cursor=" + page.NextCursor
				}
			}
			want := []string{"a", "b", "c"}
			if direction == "desc" {
				want = []string{"c", "b", "a"}
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("records=%v want=%v", got, want)
			}
		})
	}
}

func TestHealthAndReadiness(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, &readerStub{}, readinessStub{err: errors.New("database down")})

	health := httptest.NewRecorder()
	api.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d; want %d", health.Code, http.StatusOK)
	}

	ready := httptest.NewRecorder()
	api.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d; want %d", ready.Code, http.StatusServiceUnavailable)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	want := recordstore.Cursor{UpdatedAt: time.Now().UTC().Round(0), ID: "record/with spaces"}
	raw, err := encodeCursor(want)
	if err != nil {
		t.Fatalf("encodeCursor() error = %v", err)
	}
	got, err := decodeCursor(raw)
	if err != nil {
		t.Fatalf("decodeCursor() error = %v", err)
	}
	if got.ID != want.ID || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("cursor = %#v; want %#v", got, want)
	}
}

func TestNewRejectsNonCanonicalDatasetPaths(t *testing.T) {
	t.Parallel()

	paths := []string{
		"", "/", "/v1/games/", "/v1//games", "/v1/./games", "/v1/%67ames",
		"/v1/{game}", "/v1/*", "/v1/game name", "/v1/games?limit=1", "/v1/games#x",
		"/healthz", "/readyz", "GET /v1/games", "/v1/{$}",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			_, err := New(&readerStub{}, readinessStub{}, Config{
				Datasets:        map[string]DatasetPolicy{"games": {Path: path, Version: "v1", Fields: []string{"score"}}},
				DefaultPageSize: 20,
				MaximumPageSize: 100,
			})
			if err == nil {
				t.Fatalf("New() error = nil for path %q", path)
			}
		})
	}
}

func TestNewRejectsPageSizesAboveHardCapBeforeServing(t *testing.T) {
	t.Parallel()
	for _, config := range []Config{
		{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}}}, DefaultPageSize: recordstore.MaxPageItems + 1, MaximumPageSize: recordstore.MaxPageItems + 1},
		{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}}}, DefaultPageSize: 1, MaximumPageSize: recordstore.MaxPageItems + 1},
	} {
		if _, err := New(&readerStub{}, readinessStub{}, config); err == nil {
			t.Fatal("New accepted page size above hard cap")
		}
	}
}

func TestListResponseByteBudgetTruncatesWithSafeCursor(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	data := json.RawMessage(`{"score":"` + strings.Repeat("x", recordstore.MaxResponseBytes/2+1024) + `"}`)
	reader := &queryReaderStub{page: recordstore.Page{Records: []recordstore.Record{
		{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: append(json.RawMessage(nil), data...), UpdatedAt: stamp},
		{Dataset: "games", DatasetVersion: "v1", ID: "b", Data: append(json.RawMessage(nil), data...), UpdatedAt: stamp.Add(time.Second)},
		{Dataset: "games", DatasetVersion: "v1", ID: "c", Data: append(json.RawMessage(nil), data...), UpdatedAt: stamp.Add(2 * time.Second)},
	}}}
	api, err := New(reader, readinessStub{}, Config{Datasets: map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}}}, DefaultPageSize: 3, MaximumPageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/games?limit=3", nil))
	if w.Code != http.StatusOK || w.Body.Len() > recordstore.MaxResponseBytes {
		t.Fatalf("status=%d bytes=%d", w.Code, w.Body.Len())
	}
	var response listResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != 1 || response.NextCursor == "" {
		t.Fatalf("response record count/cursor = %d/%q", len(response.Records), response.NextCursor)
	}
	cursor, err := decodeCursor(response.NextCursor)
	if err != nil || cursor.ID != "a" {
		t.Fatalf("cursor=%#v err=%v; want a", cursor, err)
	}
}

func TestListResponseByteBudgetRejectsOversizedSingleRecord(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"score":"` + strings.Repeat("x", recordstore.MaxResponseBytes+1) + `"}`)
	reader := &queryReaderStub{page: recordstore.Page{Records: []recordstore.Record{{Dataset: "games", DatasetVersion: "v1", ID: "a", Data: data, UpdatedAt: time.Now()}}}}
	api := newTestAPI(t, reader, readinessStub{})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/games", nil))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func newTestAPI(t *testing.T, reader recordstore.Reader, readiness Readiness) *API {
	t.Helper()
	api, err := New(reader, readiness, Config{
		Datasets:        map[string]DatasetPolicy{"games": {Path: "/v1/games", Version: "v1", Fields: []string{"score"}}},
		DefaultPageSize: 20,
		MaximumPageSize: 100,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return api
}

type readerStub struct {
	page    recordstore.Page
	err     error
	dataset string
	version string
	cursor  *recordstore.Cursor
	limit   int
}

type queryReaderStub struct {
	readerStub
	page  recordstore.Page
	query recordstore.Query
	err   error
}

type pagingReader struct{ records []recordstore.Record }

func (s *pagingReader) List(context.Context, string, string, *recordstore.Cursor, int) (recordstore.Page, error) {
	return recordstore.Page{}, errors.New("advanced query expected")
}
func (s *pagingReader) Query(_ context.Context, query recordstore.Query, limit int) (recordstore.Page, error) {
	if query.DatasetVersion != "v1" {
		return recordstore.Page{}, errors.New("revision leaked")
	}
	records := append([]recordstore.Record(nil), s.records...)
	if query.Direction == recordstore.SortDescending {
		for left, right := 0, len(records)-1; left < right; left, right = left+1, right-1 {
			records[left], records[right] = records[right], records[left]
		}
	}
	start := 0
	if query.After != nil {
		for i, record := range records {
			if record.ID == query.After.ID {
				start = i + 1
				break
			}
		}
	}
	if start >= len(records) {
		return recordstore.Page{}, nil
	}
	end := start + limit
	if end > len(records) {
		end = len(records)
	}
	page := recordstore.Page{Records: append([]recordstore.Record(nil), records[start:end]...)}
	if end < len(records) {
		last := page.Records[len(page.Records)-1]
		page.Next = &recordstore.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
	}
	return page, nil
}

func (s *queryReaderStub) Query(_ context.Context, query recordstore.Query, _ int) (recordstore.Page, error) {
	s.query = query
	return s.page, s.err
}

func (s *readerStub) List(_ context.Context, dataset, version string, cursor *recordstore.Cursor, limit int) (recordstore.Page, error) {
	s.dataset = dataset
	s.version = version
	s.cursor = cursor
	s.limit = limit
	return s.page, s.err
}

type readinessStub struct{ err error }

func (s readinessStub) Ready(context.Context) error { return s.err }
