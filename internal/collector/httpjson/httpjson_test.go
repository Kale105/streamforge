package httpjson

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func testConfig(url string) Config {
	return Config{URL: url, Interval: time.Hour, Source: "urn:test:source", Type: "test.observed.v1", MaxResponseBytes: 1024, RequestTimeout: time.Second}
}

func TestCollectSuccessStableIDAndETag(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") == "v1" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", "v1")
		_, _ = w.Write([]byte(`{"game":"one"}`))
	}))
	defer server.Close()

	c, err := New(testConfig(server.URL), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var got event.Event
	err = c.Collect(ctx, func(_ context.Context, e event.Event) error { got = e; cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Collect() error = %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("event validation: %v", err)
	}
	if got.ID != stableID(got.Source, []byte(`{"game":"one"}`)) {
		t.Errorf("ID = %q", got.ID)
	}

	// A second poll receives 304 and must not emit an event.
	if requests.Load() != 1 {
		t.Fatalf("requests = %d; want 1", requests.Load())
	}
	if err := c.poll(context.Background(), func(context.Context, event.Event) error { t.Fatal("emitted for 304"); return nil }); err != nil {
		t.Fatalf("304 poll: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d; want 2", requests.Load())
	}

	c2, err := New(testConfig(server.URL), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var duplicate event.Event
	ctx3, cancel3 := context.WithCancel(context.Background())
	err = c2.Collect(ctx3, func(_ context.Context, e event.Event) error { duplicate = e; cancel3(); return nil })
	if !errors.Is(err, context.Canceled) || duplicate.ID != got.ID {
		t.Fatalf("stable ID mismatch: %q != %q (err %v)", duplicate.ID, got.ID, err)
	}
}

func TestCollectRejectsMalformedAndOversizedJSON(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		body  string
		limit int64
		want  error
	}{
		"malformed": {`{"broken":`, 1024, ErrInvalidResponse},
		"empty":     {`  `, 1024, ErrInvalidResponse},
		"oversized": {`{"value":"this is too long"}`, 4, ErrResponseTooLarge},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(test.body)) }))
			defer server.Close()
			cfg := testConfig(server.URL)
			cfg.MaxResponseBytes = test.limit
			c, err := New(cfg, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			err = c.Collect(context.Background(), func(context.Context, event.Event) error { t.Fatal("emit called"); return nil })
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v; want %v", err, test.want)
			}
		})
	}
}

func TestCollectClassifiesNonSuccess(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			c, err := New(testConfig(server.URL), server.Client())
			if err != nil {
				t.Fatal(err)
			}
			err = c.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
			want := ErrPermanent
			if status >= 500 {
				want = ErrRetryable
			}
			if !errors.Is(err, want) {
				t.Fatalf("error = %v; want %v", err, want)
			}
		})
	}
}

func TestCollectHonorsCancellation(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer server.Close()
	c, err := New(testConfig(server.URL), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Collect(ctx, func(context.Context, event.Event) error { return nil }) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not stop promptly")
	}
}

func TestCollectClassifiesRequestTimeoutAsRetryable(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.RequestTimeout = 20 * time.Millisecond
	c, err := New(cfg, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = c.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("error = %v; want retryable timeout", err)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	_, err := New(Config{}, http.DefaultClient)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}

func TestStableIDIgnoresObjectFormattingAndKeyOrder(t *testing.T) {
	t.Parallel()

	first := stableID("urn:test", []byte(`{"id":"game-1","score":10}`))
	second := stableID("urn:test", []byte("{\n  \"score\": 10,\n  \"id\": \"game-1\"\n}"))
	if first != second {
		t.Fatalf("stable IDs differ for semantically equivalent JSON: %q != %q", first, second)
	}
}
