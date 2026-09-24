package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	valid := config{url: "https://example.test/v1/events", duration: time.Second, concurrency: 1, requestTimeout: time.Second}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*config){
		"relative URL":     func(c *config) { c.url = "/events" },
		"URL credentials":  func(c *config) { c.url = "https://user:secret@example.test/events" },
		"duration":         func(c *config) { c.duration = 0 },
		"long duration":    func(c *config) { c.duration = maxDuration + time.Second },
		"concurrency":      func(c *config) { c.concurrency = 0 },
		"high concurrency": func(c *config) { c.concurrency = maxConcurrency + 1 },
		"timeout":          func(c *config) { c.requestTimeout = maxRequestTimeout + time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestMetadataIsBoundedAndUnique(t *testing.T) {
	var metadata values
	if err := metadata.Set("profile=smoke"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Set("profile=again"); err == nil {
		t.Fatal("duplicate metadata key accepted")
	}
	if err := metadata.Set(strings.Repeat("k", maxMetadataKey+1) + "=v"); err == nil {
		t.Fatal("oversized metadata key accepted")
	}
}

func TestAggregateStatusAndPercentiles(t *testing.T) {
	a := newAggregate(100 * time.Millisecond)
	a.add(200, nil, 1*time.Millisecond)
	a.add(200, nil, 2*time.Millisecond)
	a.add(429, nil, 10*time.Millisecond)
	a.add(0, errors.New("timeout"), 200*time.Millisecond)
	if a.requests != 4 || a.errors != 1 || a.status[200] != 2 || a.status[429] != 1 {
		t.Fatalf("aggregate = %#v", a)
	}
	latency := a.latency()
	if latency.P50 != 2 || latency.P95 != 100 || latency.P99 != 100 {
		t.Fatalf("latency = %#v", latency)
	}
}

func TestRunDoesNotEmitAPIKey(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-API-Key"); got != "do-not-print" {
			t.Fatalf("API key header = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	var output bytes.Buffer
	err := run([]string{"-url", target.URL, "-duration", "20ms", "-concurrency", "1", "-request-timeout", "1s", "-api-key", "do-not-print", "-metadata", "profile=smoke"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(output.Bytes(), []byte("do-not-print")) {
		t.Fatalf("output leaked API key: %s", output.String())
	}
	var got report
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Metadata["profile"] != "smoke" || got.Requests == 0 {
		t.Fatalf("report = %#v", got)
	}
}

func TestRunReportCapturesReproducibilityMetadata(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	path := t.TempDir() + "/raw.json"
	var output bytes.Buffer
	if err := run([]string{"-url", target.URL, "-duration", "20ms", "-concurrency", "1", "-request-timeout", "1s", "-api-key", "do-not-print", "-metadata", "profile=smoke", "-output", path}, &output); err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != reportSchemaVersion || got.StartedAt.IsZero() || got.EndedAt.IsZero() || got.RawOutputPath != path || got.Environment.GoVersion == "" || got.Latency.Definition == "" {
		t.Fatalf("incomplete report: %#v", got)
	}
	if strings.Contains(strings.Join(got.Command, " "), "do-not-print") {
		t.Fatalf("command leaked API key: %#v", got.Command)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("raw output not written: %v", err)
	}
}

func TestSummaryRequiresThreeConsistentRawRuns(t *testing.T) {
	dir := t.TempDir()
	makeReport := func(profile string, concurrency int) string {
		t.Helper()
		r := report{
			SchemaVersion:  reportSchemaVersion,
			StartedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EndedAt:        time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
			Configuration:  runConfiguration{URL: "https://example.test/events", Duration: "1s", Concurrency: concurrency, RequestTimeout: "1s", Metadata: map[string]string{"profile": profile}},
			URL:            "https://example.test/events",
			Concurrency:    concurrency,
			RequestTimeout: "1s",
			Metadata:       map[string]string{"profile": profile},
			Requests:       3,
			Status:         map[int]uint64{200: 3},
			Latency:        latencyReport{Unit: "ms", Definition: "test"},
		}
		path := filepath.Join(dir, fmt.Sprintf("%s-%d-%d.json", profile, concurrency, time.Now().UnixNano()))
		encoded, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	one, two := makeReport("baseline", 1), makeReport("baseline", 1)
	if _, err := summarize([]string{one, two}); err == nil {
		t.Fatal("accepted fewer than three raw runs")
	}
	three := makeReport("baseline", 1)
	got, err := summarize([]string{one, two, three})
	if err != nil {
		t.Fatal(err)
	}
	if got.RunCount != 3 || got.Aggregate.Requests != 9 || got.Aggregate.Latency.P95 != 0 || !strings.Contains(got.Aggregate.Latency.Definition, "not computed") {
		t.Fatalf("summary = %#v", got)
	}
	if _, err := summarize([]string{one, two, makeReport("baseline", 2)}); err == nil {
		t.Fatal("accepted inconsistent configuration")
	}
	if _, err := summarize([]string{one, two, makeReport("recovery", 1)}); err == nil {
		t.Fatal("accepted inconsistent profile")
	}
}

func TestExecuteBoundsConcurrencyAndCancelsRequests(t *testing.T) {
	var active, peak atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		for {
			previous := peak.Load()
			if current <= previous || peak.CompareAndSwap(previous, current) {
				break
			}
		}
		defer active.Add(-1)
		<-r.Context().Done()
	}))
	defer target.Close()
	started := time.Now()
	got, err := execute(config{url: target.URL, duration: 30 * time.Millisecond, concurrency: 3, requestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if peak.Load() > 3 || got.Requests > 3 {
		t.Fatalf("workers exceeded configured bound: peak=%d requests=%d", peak.Load(), got.Requests)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancellation was not bounded: %s", elapsed)
	}
}

func TestNewHTTPClientRetainsOneIdleConnectionPerWorker(t *testing.T) {
	client, transport := newHTTPClient(7, 3*time.Second)
	defer transport.CloseIdleConnections()
	if client.Timeout != 3*time.Second {
		t.Fatalf("client timeout = %s", client.Timeout)
	}
	if transport.MaxIdleConns != 7 || transport.MaxIdleConnsPerHost != 7 || transport.MaxConnsPerHost != 7 {
		t.Fatalf("transport connection limits = total:%d per-host:%d active:%d", transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost)
	}
}

func TestExecuteReusesBoundedConnectionsAndDrainsBodies(t *testing.T) {
	const concurrency = 4
	var connections atomic.Int32
	payload := bytes.Repeat([]byte("x"), 32<<10)
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		// The benchmark deadline can cancel an in-flight request while its
		// response is being written. That expected shutdown race is not part of
		// this test's connection-reuse assertion.
		_, _ = w.Write(payload)
	}))
	target.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	target.Start()
	defer target.Close()

	got, err := execute(config{url: target.URL, duration: 100 * time.Millisecond, concurrency: concurrency, requestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got.Requests <= concurrency {
		t.Fatalf("requests = %d; test did not exercise connection reuse", got.Requests)
	}
	if opened := connections.Load(); opened > concurrency {
		t.Fatalf("opened %d connections for %d workers; connection reuse regressed", opened, concurrency)
	}
}

func TestExecuteDrainsAndClosesEverySuccessfulResponse(t *testing.T) {
	var opened, reachedEOF, closed atomic.Int64
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		opened.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &observedBody{reader: strings.NewReader(strings.Repeat("data", 1024)), reachedEOF: &reachedEOF, closed: &closed},
			Header:     make(http.Header),
		}, nil
	})}
	got, err := executeWithClient(config{url: "http://example.test/events", duration: 15 * time.Millisecond, concurrency: 3, requestTimeout: time.Second}, client)
	if err != nil {
		t.Fatal(err)
	}
	if got.Errors != 0 || opened.Load() == 0 {
		t.Fatalf("report = %#v, opened=%d", got, opened.Load())
	}
	if opened.Load() != reachedEOF.Load() || opened.Load() != closed.Load() {
		t.Fatalf("response bodies opened=%d EOF=%d closed=%d", opened.Load(), reachedEOF.Load(), closed.Load())
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type observedBody struct {
	reader     *strings.Reader
	reachedEOF *atomic.Int64
	closed     *atomic.Int64
	once       sync.Once
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.once.Do(func() { b.reachedEOF.Add(1) })
	}
	return n, err
}

func (b *observedBody) Close() error {
	b.closed.Add(1)
	return nil
}
