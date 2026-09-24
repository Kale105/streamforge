// Package observability provides bounded, dependency-free operational metrics.
package observability

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config constrains the only potentially user-supplied metric dimension.
// Values outside Collectors are recorded under "other".
type Config struct {
	Collectors []string
}

// Registry is safe for concurrent use. Its metric names, labels, and duration
// buckets are fixed, so recording cannot create unbounded time series.
type Registry struct {
	mu         sync.RWMutex
	collectors map[string]struct{}
	counters   map[string]uint64
	gauges     map[string]float64
	histograms map[string]*histogram
}

type histogram struct {
	counts [len(durationBuckets) + 1]uint64
	sum    float64
}

var durationBuckets = [...]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// NewRegistry creates an empty registry. Duplicate and empty collector names
// are discarded; a collector not in this allowlist is intentionally grouped.
func NewRegistry(config Config) *Registry {
	collectors := make(map[string]struct{}, len(config.Collectors))
	for _, name := range config.Collectors {
		if name != "" && validLabel(name) {
			collectors[name] = struct{}{}
		}
	}
	return &Registry{collectors: collectors, counters: make(map[string]uint64), gauges: make(map[string]float64), histograms: make(map[string]*histogram)}
}

func validLabel(value string) bool {
	// Metric keys are stored internally in a compact, delimiter-separated form.
	// Reject its delimiters up front so configuration can never corrupt parsing
	// or create attacker-controlled label names.
	return len(value) <= 64 && !strings.ContainsAny(value, "\n\r\x00{},=")
}

func (r *Registry) collector(name string) string {
	if _, ok := r.collectors[name]; ok {
		return name
	}
	return "other"
}

func (r *Registry) increment(key string) { r.counters[key]++ }

// CollectorAttempt records an attempted collection run and returns a closure
// that must be called once with the final error.
func (r *Registry) CollectorAttempt(name string) func(error) {
	name = r.collector(name)
	started := time.Now()
	r.mu.Lock()
	r.increment("streamforge_collector_attempts_total{collector=" + name + "}")
	r.mu.Unlock()
	return func(err error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err != nil {
			r.increment("streamforge_collector_failures_total{collector=" + name + "}")
		}
		r.observe("streamforge_collector_duration_seconds{collector="+name+"}", time.Since(started).Seconds())
	}
}

func (r *Registry) EventEnqueued()     { r.add("streamforge_events_enqueued_total") }
func (r *Registry) EventAdmitted()     { r.add("streamforge_events_admitted_total") }
func (r *Registry) EventMaterialized() { r.add("streamforge_events_materialized_total") }
func (r *Registry) EventRejected()     { r.add("streamforge_events_rejected_total") }
func (r *Registry) add(key string)     { r.mu.Lock(); r.increment(key); r.mu.Unlock() }

// WorkerProcessed records successful worker handling. Role and outcome are
// fixed enums, so record contents can never create a metric time series.
func (r *Registry) WorkerProcessed(role string) { r.workerCounter("processed", role) }
func (r *Registry) WorkerRetried(role string)   { r.workerCounter("retried", role) }
func (r *Registry) WorkerDLQ(role string)       { r.workerCounter("dlq", role) }
func (r *Registry) WorkerLeaseLoss()            { r.workerCounter("lease_loss", "collector") }
func (r *Registry) WorkerFailure(role, class string) {
	role, class = workerRole(role), workerClass(class)
	r.mu.Lock()
	r.increment("streamforge_worker_failures_total{role=" + role + ",class=" + class + "}")
	r.mu.Unlock()
}
func (r *Registry) WorkerLatency(role string, elapsed time.Duration) {
	role = workerRole(role)
	r.mu.Lock()
	r.observe("streamforge_worker_operation_duration_seconds{role="+role+"}", elapsed.Seconds())
	r.mu.Unlock()
}
func (r *Registry) workerCounter(kind, role string) {
	r.mu.Lock()
	r.increment("streamforge_worker_" + kind + "_total{role=" + workerRole(role) + "}")
	r.mu.Unlock()
}
func workerRole(role string) string {
	switch role {
	case "collector", "processor", "sink", "dlq-indexer", "replay":
		return role
	}
	return "other"
}
func workerClass(class string) string {
	switch class {
	case "decode", "key", "normalize", "sink", "dependency":
		return class
	}
	return "other"
}

// SetQueueDepth records the current queue depth. Negative depths are clamped.
func (r *Registry) SetQueueDepth(depth int) {
	if depth < 0 {
		depth = 0
	}
	r.mu.Lock()
	r.gauges["streamforge_queue_depth"] = float64(depth)
	r.mu.Unlock()
}

// APIRequest records an HTTP request with fixed method and bounded status labels.
func (r *Registry) APIRequest(method string, status int) {
	method = knownMethod(method)
	if status < 100 || status > 599 {
		status = 0
	}
	r.mu.Lock()
	r.increment("streamforge_api_requests_total{method=" + method + ",status=" + strconv.Itoa(status) + "}")
	r.mu.Unlock()
}

func knownMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

// RecoveryRun records a completed recovery run. outcome must be success,
// failure, or limited; other values are grouped as unknown.
func (r *Registry) RecoveryRun(outcome string, events int, elapsed time.Duration) {
	if events < 0 {
		events = 0
	}
	outcome = recoveryOutcome(outcome)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.increment("streamforge_recovery_runs_total{outcome=" + outcome + "}")
	r.counters["streamforge_recovery_events_total{outcome="+outcome+"}"] += uint64(events)
	r.observe("streamforge_recovery_duration_seconds", elapsed.Seconds())
}

func recoveryOutcome(value string) string {
	switch value {
	case "success", "failure", "limited":
		return value
	default:
		return "unknown"
	}
}

func (r *Registry) observe(key string, seconds float64) {
	if seconds < 0 {
		seconds = 0
	}
	h := r.histograms[key]
	if h == nil {
		h = &histogram{}
		r.histograms[key] = h
	}
	h.sum += seconds
	for i, bucket := range durationBuckets {
		if seconds <= bucket {
			h.counts[i]++
		}
	}
	h.counts[len(durationBuckets)]++
}

// Handler returns a Prometheus text-format endpoint. It does not register a
// global handler, leaving application routing under the caller's control.
func (r *Registry) Handler() http.Handler { return http.HandlerFunc(r.serveHTTP) }

func (r *Registry) serveHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(r.Text()))
}

// Text returns a deterministic Prometheus text exposition snapshot.
func (r *Registry) Text() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	lines := make([]string, 0, len(r.counters)+len(r.gauges)+len(r.histograms)*14)
	for key, value := range r.counters {
		lines = append(lines, renderKey(key)+" "+strconv.FormatUint(value, 10))
	}
	for key, value := range r.gauges {
		lines = append(lines, renderKey(key)+" "+strconv.FormatFloat(value, 'g', -1, 64))
	}
	for key, h := range r.histograms {
		base, labels := splitKey(key)
		for i, bucket := range durationBuckets {
			lines = append(lines, base+"_bucket"+renderLabels(withLabel(labels, "le="+strconv.FormatFloat(bucket, 'g', -1, 64)))+" "+strconv.FormatUint(h.counts[i], 10))
		}
		lines = append(lines, base+"_bucket"+renderLabels(withLabel(labels, "le=+Inf"))+" "+strconv.FormatUint(h.counts[len(durationBuckets)], 10))
		lines = append(lines, base+"_sum"+renderLabels(labels)+" "+strconv.FormatFloat(h.sum, 'g', -1, 64))
		lines = append(lines, base+"_count"+renderLabels(labels)+" "+strconv.FormatUint(h.counts[len(durationBuckets)], 10))
	}
	sort.Strings(lines)
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

func splitKey(key string) (string, string) {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		return key[:i], strings.TrimSuffix(key[i+1:], "}")
	}
	return key, ""
}
func withLabel(labels, extra string) string {
	if labels == "" {
		return extra
	}
	return labels + "," + extra
}
func renderKey(key string) string { base, labels := splitKey(key); return base + renderLabels(labels) }
func renderLabels(labels string) string {
	if labels == "" {
		return ""
	}
	parts := strings.Split(labels, ",")
	for i, p := range parts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			parts[i] = "invalid=\"invalid\""
			continue
		}
		parts[i] = kv[0] + "=\"" + escapeLabel(kv[1]) + "\""
	}
	return "{" + strings.Join(parts, ",") + "}"
}
func escapeLabel(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(value)
}

// HTTP instruments a handler without changing its routes.
func HTTP(next http.Handler, metrics *Registry) http.Handler {
	if metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, req)
		metrics.APIRequest(req.Method, rw.status)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *statusWriter) Write(b []byte) (int, error) { return w.ResponseWriter.Write(b) }
