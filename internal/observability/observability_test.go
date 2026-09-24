package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTextDeterministicAndHistogram(t *testing.T) {
	r := NewRegistry(Config{Collectors: []string{"feed"}})
	r.EventMaterialized()
	done := r.CollectorAttempt("feed")
	done(nil)
	r.RecoveryRun("success", 2, time.Millisecond)
	first, second := r.Text(), r.Text()
	if first != second {
		t.Fatal("metrics output was not deterministic")
	}
	for _, want := range []string{
		"streamforge_collector_attempts_total{collector=\"feed\"} 1",
		"streamforge_collector_duration_seconds_bucket{collector=\"feed\",le=\"+Inf\"} 1",
		"streamforge_recovery_duration_seconds_bucket{le=\"+Inf\"} 1",
	} {
		if !strings.Contains(first, want) {
			t.Fatalf("output missing %q:\n%s", want, first)
		}
	}
}

func TestHandlerContentType(t *testing.T) {
	r := NewRegistry(Config{})
	r.EventAdmitted()
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if got := w.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if !strings.Contains(w.Body.String(), "streamforge_events_admitted_total 1") {
		t.Fatal("handler omitted metric")
	}
}

func TestLabelsAreBoundedAndEscaped(t *testing.T) {
	r := NewRegistry(Config{Collectors: []string{"safe\"name", "bad\nname", "bad,name", "bad{name"}})
	for i := 0; i < 100; i++ {
		r.CollectorAttempt("untrusted-" + string(rune(i)))(nil)
		r.APIRequest("TRACE", 999)
	}
	r.CollectorAttempt("safe\"name")(nil)
	text := r.Text()
	if strings.Count(text, "streamforge_collector_attempts_total") != 2 {
		t.Fatalf("unexpected collector series:\n%s", text)
	}
	if !strings.Contains(text, "collector=\"safe\\\"name\"") {
		t.Fatalf("collector label was not escaped:\n%s", text)
	}
	if strings.Contains(text, "untrusted-") || strings.Contains(text, "bad\\nname") || strings.Contains(text, "bad,name") || strings.Contains(text, "bad{name") {
		t.Fatalf("untrusted labels were retained:\n%s", text)
	}
	if strings.Count(text, "streamforge_api_requests_total") != 1 {
		t.Fatalf("API labels grew unexpectedly:\n%s", text)
	}
}

func TestConcurrentRecording(t *testing.T) {
	r := NewRegistry(Config{Collectors: []string{"feed"}})
	const workers, iterations = 16, 1000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				r.EventAdmitted()
				r.SetQueueDepth(j)
				r.APIRequest(http.MethodGet, http.StatusOK)
				r.CollectorAttempt("feed")(nil)
			}
		}()
	}
	wg.Wait()
	text := r.Text()
	if !strings.Contains(text, "streamforge_events_admitted_total 16000") {
		t.Fatalf("lost concurrent increments:\n%s", text)
	}
	if !strings.Contains(text, "streamforge_collector_attempts_total{collector=\"feed\"} 16000") {
		t.Fatalf("lost collector increments:\n%s", text)
	}
}

func TestHTTPDecoratorRecordsImplicitAndExplicitStatus(t *testing.T) {
	r := NewRegistry(Config{})
	h := HTTP(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/teapot" {
			w.WriteHeader(http.StatusTeapot)
		} else {
			_, _ = w.Write([]byte("ok"))
		}
	}), r)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("TRACE", "/teapot", nil))
	text := r.Text()
	for _, want := range []string{"method=\"GET\",status=\"200\"", "method=\"OTHER\",status=\"418\""} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q: %s", want, text)
		}
	}
}

func TestHTTPDecoratorKeepsFirstStatus(t *testing.T) {
	r := NewRegistry(Config{})
	h := HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.WriteHeader(http.StatusInternalServerError)
	}), r)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	text := r.Text()
	if !strings.Contains(text, "status=\"201\"") || strings.Contains(text, "status=\"500\"") {
		t.Fatalf("metrics did not retain first response status: %s", text)
	}
}
