package sse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func TestSSEParsesAndReconnectsWithLastID(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 2 && r.Header.Get("Last-Event-ID") != "one" {
			t.Errorf("Last-Event-ID = %q", r.Header.Get("Last-Event-ID"))
		}
		_, _ = w.Write([]byte("id: one\nevent: score\nretry: 5\ndata: {\"x\":1}\n\n"))
	}))
	defer s.Close()
	c, err := New(Config{URL: s.URL, Source: "urn:test", Type: "default", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second}, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		var got event.Event
		err = c.Collect(context.Background(), func(_ context.Context, e event.Event) error { got = e; return nil })
		if !errors.Is(err, ErrRetryable) {
			t.Fatal(err)
		}
		if got.ID == "" || got.Type != "score" {
			t.Fatalf("event %#v", got)
		}
	}
	if c.LastEventID() != "one" || c.RetryDelay() != 5*time.Millisecond {
		t.Fatal("state not retained")
	}
}
func TestSSERejectsMalformedAndOversized(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: not-json\n\n"))
	}))
	defer s.Close()
	c, _ := New(Config{URL: s.URL, Source: "s", Type: "t", MaxLineBytes: 64, MaxMessageBytes: 64, RequestTimeout: time.Second}, s.Client())
	if err := c.Collect(context.Background(), func(context.Context, event.Event) error { return nil }); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("%v", err)
	}
	over := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"long\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}\n\n"))
	}))
	defer over.Close()
	c, _ = New(Config{URL: over.URL, Source: "s", Type: "t", MaxLineBytes: 16, MaxMessageBytes: 16, RequestTimeout: time.Second}, over.Client())
	if err := c.Collect(context.Background(), func(context.Context, event.Event) error { return nil }); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("%v", err)
	}
}

func TestSSECancellation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		<-r.Context().Done()
	}))
	defer s.Close()
	c, _ := New(Config{URL: s.URL, Source: "s", Type: "t", MaxLineBytes: 64, MaxMessageBytes: 64, RequestTimeout: time.Second}, s.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Collect(ctx, func(context.Context, event.Event) error { return nil }) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not cancel")
	}
}

func TestSSEReusedResumeIDGetsDistinctPlatformIDs(t *testing.T) {
	c, err := New(Config{URL: "https://example.test/events", Source: "urn:test", Type: "default", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	var got []event.Event
	err = c.parse(context.Background(), strings.NewReader("id: checkpoint\ndata: {\"score\":1}\n\nid: checkpoint\ndata: {\"score\":2}\n\n"), func(_ context.Context, e event.Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID == got[1].ID {
		t.Fatalf("event IDs = %#v", got)
	}
	if c.LastEventID() != "checkpoint" {
		t.Fatalf("LastEventID = %q", c.LastEventID())
	}
}

func TestSSERetryBounds(t *testing.T) {
	var hints []time.Duration
	c, err := New(Config{URL: "https://example.test/events", Source: "urn:test", Type: "default", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second, MaxRetryDelay: time.Second, RetryHint: func(delay time.Duration) { hints = append(hints, delay) }}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	stream := "retry: 1000\n\nretry: 9223372036854775807\n\ndata: {\"ok\":true}\n\n"
	err = c.parse(context.Background(), strings.NewReader(stream), func(context.Context, event.Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if c.RetryDelay() != time.Second {
		t.Fatalf("RetryDelay = %s", c.RetryDelay())
	}
	if len(hints) != 2 || hints[1] != time.Second {
		t.Fatalf("hints = %v", hints)
	}
}

func TestSSEStartsFromCommittedCursorAndCommitsAfterEmit(t *testing.T) {
	var header string
	var order []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("Last-Event-ID")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: opaque\ndata: {\"value\":1}\n\n"))
	}))
	defer s.Close()
	c, err := New(Config{URL: s.URL, Source: "urn:test", Type: "update", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second, InitialCursor: Cursor{Sequence: 7, LastEventID: "committed"}, RequireEventID: true, Commit: func(_ context.Context, sequence uint64, lastID, eventID string) error {
		order = append(order, "commit")
		if sequence != 8 || lastID != "opaque" || eventID == "" {
			t.Fatalf("commit (%d, %q, %q)", sequence, lastID, eventID)
		}
		return nil
	}}, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = c.Collect(context.Background(), func(_ context.Context, e event.Event) error {
		order = append(order, "emit")
		if e.ID == "" {
			t.Fatal("missing ID")
		}
		return nil
	})
	if !errors.Is(err, ErrRetryable) {
		t.Fatal(err)
	}
	if header != "committed" || strings.Join(order, ",") != "emit,commit" {
		t.Fatalf("header=%q order=%v", header, order)
	}
	if got := c.Cursor(); got != (Cursor{Sequence: 8, LastEventID: "opaque"}) {
		t.Fatalf("cursor=%+v", got)
	}
}

func TestSSECommitFailureRetainsCursorAndRetriesSameEventIdentity(t *testing.T) {
	var commits int
	c, err := New(Config{URL: "https://example.test/events", Source: "urn:test", Type: "update", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second, InitialCursor: Cursor{Sequence: 3, LastEventID: "prior"}, RequireEventID: true, Commit: func(context.Context, uint64, string, string) error {
		commits++
		return errors.New("database down")
	}}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	stream := "id: same\ndata: {\"value\":1}\n\n"
	var first, second string
	err = c.parse(context.Background(), strings.NewReader(stream), func(_ context.Context, e event.Event) error { first = e.ID; return nil })
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("first=%v", err)
	}
	if got := c.Cursor(); got != (Cursor{Sequence: 3, LastEventID: "prior"}) {
		t.Fatalf("cursor after failed commit=%+v", got)
	}
	err = c.parse(context.Background(), strings.NewReader(stream), func(_ context.Context, e event.Event) error { second = e.ID; return nil })
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("second=%v", err)
	}
	if first == "" || first != second || commits != 2 {
		t.Fatalf("ids %q %q commits %d", first, second, commits)
	}
}

func TestSSERepeatedOpaqueIDsAdvanceHostSequence(t *testing.T) {
	var checkpoints []Cursor
	c, err := New(Config{URL: "https://example.test/events", Source: "urn:test", Type: "update", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second, RequireEventID: true, Commit: func(_ context.Context, sequence uint64, lastID, _ string) error {
		checkpoints = append(checkpoints, Cursor{Sequence: sequence, LastEventID: lastID})
		return nil
	}}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	err = c.parse(context.Background(), strings.NewReader("id: repeat\ndata: {\"n\":1}\n\nid: repeat\ndata: {\"n\":2}\n\n"), func(_ context.Context, e event.Event) error { ids = append(ids, e.ID); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] == ids[1] || len(checkpoints) != 2 || checkpoints[0].Sequence != 1 || checkpoints[1].Sequence != 2 {
		t.Fatalf("ids=%v checkpoints=%+v", ids, checkpoints)
	}
}

func TestSSERequireEventIDRejectsMissingID(t *testing.T) {
	c, err := New(Config{URL: "https://example.test/events", Source: "urn:test", Type: "update", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second, RequireEventID: true}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	err = c.parse(context.Background(), strings.NewReader("data: {\"n\":1}\n\n"), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("%v", err)
	}
}

func TestSSEClassifiesHTTPStatusForReconnect(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "rate limited", status: http.StatusTooManyRequests, retryable: true},
		{name: "server failure", status: http.StatusBadGateway, retryable: true},
		{name: "bad request", status: http.StatusBadRequest, retryable: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tc.status) }))
			defer s.Close()
			c, err := New(Config{URL: s.URL, Source: "urn:test", Type: "update", MaxLineBytes: 64, MaxMessageBytes: 128, RequestTimeout: time.Second}, s.Client())
			if err != nil {
				t.Fatal(err)
			}
			err = c.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
			if errors.Is(err, ErrRetryable) != tc.retryable || errors.Is(err, ErrPermanent) == tc.retryable {
				t.Fatalf("status=%d err=%v", tc.status, err)
			}
		})
	}
}
