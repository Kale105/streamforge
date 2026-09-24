// Command loadgen produces bounded, concurrent HTTP GET load and emits one
// machine-readable run report. It is a measurement harness, not a production
// traffic generator or a source of capacity claims.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	reportSchemaVersion  = "streamforge.loadgen.run.v1"
	summarySchemaVersion = "streamforge.loadgen.summary.v1"
	maxRequestTimeout    = time.Minute
	maxDuration          = 24 * time.Hour
	maxConcurrency       = 4096
	maxTargetURLBytes    = 4096
	maxResponseBytes     = 16 << 20
	maxMetadata          = 32
	maxMetadataKey       = 64
	maxMetadataValue     = 256
)

type config struct {
	url            string
	duration       time.Duration
	concurrency    int
	requestTimeout time.Duration
	apiKey         string
	metadata       map[string]string
	outputPath     string
	command        []string
}
type runConfiguration struct {
	URL            string            `json:"url"`
	Duration       string            `json:"duration"`
	Concurrency    int               `json:"concurrency"`
	RequestTimeout string            `json:"request_timeout"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}
type environment struct {
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPUs       int    `json:"cpus"`
	GoVersion  string `json:"go_version"`
	Executable string `json:"executable,omitempty"`
	Module     string `json:"module,omitempty"`
	Version    string `json:"version,omitempty"`
	GitCommit  string `json:"git_commit,omitempty"`
}
type latencyReport struct {
	Unit       string `json:"unit"`
	Definition string `json:"definition"`
	P50        uint64 `json:"p50"`
	P95        uint64 `json:"p95"`
	P99        uint64 `json:"p99"`
}
type report struct {
	SchemaVersion  string            `json:"schema_version"`
	StartedAt      time.Time         `json:"started_at"`
	EndedAt        time.Time         `json:"ended_at"`
	Duration       string            `json:"duration"`
	Command        []string          `json:"command"`
	Configuration  runConfiguration  `json:"configuration"`
	Environment    environment       `json:"environment"`
	RawOutputPath  string            `json:"raw_output_path,omitempty"`
	URL            string            `json:"url"`
	Concurrency    int               `json:"concurrency"`
	RequestTimeout string            `json:"request_timeout"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Requests       uint64            `json:"requests"`
	Errors         uint64            `json:"errors"`
	Status         map[int]uint64    `json:"status"`
	Latency        latencyReport     `json:"latency"`
}
type aggregateReport struct {
	Requests uint64         `json:"requests"`
	Errors   uint64         `json:"errors"`
	Status   map[int]uint64 `json:"status"`
	Latency  latencyReport  `json:"latency"`
}
type summary struct {
	SchemaVersion string           `json:"schema_version"`
	Profile       string           `json:"profile"`
	RawRunPaths   []string         `json:"raw_run_paths"`
	RunCount      int              `json:"run_count"`
	Configuration runConfiguration `json:"configuration"`
	Aggregate     aggregateReport  `json:"aggregate"`
}

// aggregate uses one-millisecond buckets. The fixed-size histogram keeps memory bounded.
type aggregate struct {
	requestTimeout time.Duration
	status         map[int]uint64
	errors         uint64
	requests       uint64
	buckets        []uint64
}

func newAggregate(timeout time.Duration) *aggregate {
	return &aggregate{requestTimeout: timeout, status: make(map[int]uint64), buckets: make([]uint64, int(timeout/time.Millisecond)+1)}
}
func (a *aggregate) add(status int, err error, elapsed time.Duration) {
	a.requests++
	if err != nil {
		a.errors++
	} else {
		a.status[status]++
	}
	ms := elapsed / time.Millisecond
	if ms >= time.Duration(len(a.buckets)) {
		ms = time.Duration(len(a.buckets) - 1)
	}
	a.buckets[int(ms)]++
}
func (a *aggregate) merge(other *aggregate) {
	a.requests += other.requests
	a.errors += other.errors
	for code, count := range other.status {
		a.status[code] += count
	}
	for i, count := range other.buckets {
		a.buckets[i] += count
	}
}
func (a *aggregate) percentile(percent uint64) uint64 {
	if a.requests == 0 {
		return 0
	}
	rank := (a.requests*percent + 99) / 100
	var seen uint64
	for ms, count := range a.buckets {
		seen += count
		if seen >= rank {
			return uint64(ms)
		}
	}
	return uint64(len(a.buckets) - 1)
}
func (a *aggregate) latency() latencyReport {
	return latencyReport{Unit: "ms", Definition: "nearest-rank percentile over one-millisecond request-latency buckets; timeout-or-longer values use the final bucket", P50: a.percentile(50), P95: a.percentile(95), P99: a.percentile(99)}
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}
func run(args []string, output io.Writer) error {
	var metadata values
	var aggregateRuns paths
	flags := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	urlFlag := flags.String("url", "", "target HTTP(S) URL")
	duration := flags.Duration("duration", 30*time.Second, "fixed test duration")
	concurrency := flags.Int("concurrency", 4, "number of concurrent workers")
	timeout := flags.Duration("request-timeout", 5*time.Second, "per-request timeout")
	apiKey := flags.String("api-key", "", "API key; never included in output")
	outputPath := flags.String("output", "", "optional raw JSON report destination")
	flags.Var(&metadata, "metadata", "environment metadata as key=value; repeatable")
	flags.Var(&aggregateRuns, "aggregate-run", "raw report path; repeat three or more times to create a summary")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("loadgen does not accept positional arguments")
	}
	if len(aggregateRuns) > 0 {
		if *urlFlag != "" || *outputPath != "" {
			return errors.New("aggregate-run cannot be combined with a load run or output")
		}
		s, err := summarize(aggregateRuns)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(s)
	}
	key := *apiKey
	if key == "" {
		key = os.Getenv("STREAMFORGE_LOADGEN_API_KEY")
	}
	cfg := config{url: *urlFlag, duration: *duration, concurrency: *concurrency, requestTimeout: *timeout, apiKey: key, metadata: metadata, outputPath: *outputPath, command: sanitizedCommand(args)}
	if err := cfg.validate(); err != nil {
		return err
	}
	r, err := execute(cfg)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if cfg.outputPath != "" {
		if err := writeRawReport(cfg.outputPath, encoded); err != nil {
			return err
		}
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}
func (c config) validate() error {
	u, err := url.Parse(c.url)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("url must be an absolute http or https URL")
	}
	if len(c.url) > maxTargetURLBytes || u.User != nil || hasSensitiveQuery(u) {
		return errors.New("url must not contain credentials or sensitive query parameters")
	}
	if c.duration <= 0 || c.duration > maxDuration {
		return fmt.Errorf("duration must be positive and at most %s", maxDuration)
	}
	if c.concurrency <= 0 || c.concurrency > maxConcurrency {
		return fmt.Errorf("concurrency must be between 1 and %d", maxConcurrency)
	}
	if c.requestTimeout <= 0 || c.requestTimeout > maxRequestTimeout {
		return fmt.Errorf("request-timeout must be positive and at most %s", maxRequestTimeout)
	}
	return nil
}
func execute(cfg config) (report, error) {
	client, transport := newHTTPClient(cfg.concurrency, cfg.requestTimeout)
	defer transport.CloseIdleConnections()
	return executeWithClient(cfg, client)
}

func newHTTPClient(concurrency int, timeout time.Duration) (*http.Client, *http.Transport) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The default transport retains only two idle connections per host. A load
	// run with greater concurrency would therefore churn TCP connections and can
	// benchmark the host's ephemeral-port/thread limits instead of the target.
	transport.MaxIdleConns = concurrency
	transport.MaxIdleConnsPerHost = concurrency
	transport.MaxConnsPerHost = concurrency
	return &http.Client{Timeout: timeout, Transport: transport}, transport
}

func executeWithClient(cfg config, client *http.Client) (report, error) {
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()
	aggregates := make(chan *aggregate, cfg.concurrency)
	var workers sync.WaitGroup
	for range cfg.concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			agg := newAggregate(cfg.requestTimeout)
			for ctx.Err() == nil {
				requestStarted := time.Now()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.url, nil)
				if err == nil && cfg.apiKey != "" {
					req.Header.Set("X-API-Key", cfg.apiKey)
				}
				if err == nil {
					response, requestErr := client.Do(req)
					err = requestErr
					if response != nil {
						read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes+1))
						_ = response.Body.Close()
						if readErr != nil {
							err = readErr
						} else if read > maxResponseBytes {
							err = errors.New("response body exceeds load-generator limit")
						}
						agg.add(response.StatusCode, err, time.Since(requestStarted))
						continue
					}
				}
				agg.add(0, err, time.Since(requestStarted))
			}
			aggregates <- agg
		}()
	}
	workers.Wait()
	close(aggregates)
	total := newAggregate(cfg.requestTimeout)
	for a := range aggregates {
		total.merge(a)
	}
	ended := time.Now().UTC()
	configuration := runConfiguration{URL: cfg.url, Duration: cfg.duration.String(), Concurrency: cfg.concurrency, RequestTimeout: cfg.requestTimeout.String(), Metadata: cfg.metadata}
	return report{SchemaVersion: reportSchemaVersion, StartedAt: started, EndedAt: ended, Duration: ended.Sub(started).String(), Command: cfg.command, Configuration: configuration, Environment: currentEnvironment(), RawOutputPath: cfg.outputPath, URL: cfg.url, Concurrency: cfg.concurrency, RequestTimeout: cfg.requestTimeout.String(), Metadata: cfg.metadata, Requests: total.requests, Errors: total.errors, Status: total.status, Latency: total.latency()}, nil
}
func currentEnvironment() environment {
	e := environment{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version()}
	if executable, err := os.Executable(); err == nil {
		e.Executable = executable
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		e.Module, e.Version = info.Main.Path, info.Main.Version
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output(); err == nil {
		e.GitCommit = strings.TrimSpace(string(output))
	}
	return e
}
func writeRawReport(path string, encoded []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create raw report: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write raw report: %w", err)
	}
	return nil
}

func summarize(runPaths []string) (summary, error) {
	if len(runPaths) < 3 {
		return summary{}, errors.New("aggregate summary requires at least three raw run reports for one profile")
	}
	paths := append([]string(nil), runPaths...)
	sort.Strings(paths)
	reports := make([]report, 0, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			return summary{}, fmt.Errorf("read raw run %q: %w", path, err)
		}
		var r report
		if err := json.Unmarshal(contents, &r); err != nil {
			return summary{}, fmt.Errorf("decode raw run %q: %w", path, err)
		}
		if err := validateReport(r); err != nil {
			return summary{}, fmt.Errorf("invalid raw run %q: %w", path, err)
		}
		reports = append(reports, r)
	}
	first := reports[0]
	profile := first.Metadata["profile"]
	for _, r := range reports[1:] {
		if r.Metadata["profile"] != profile || !sameConfiguration(first.Configuration, r.Configuration) {
			return summary{}, errors.New("aggregate summary requires raw runs with one identical profile and configuration")
		}
	}
	status := make(map[int]uint64)
	var requests, errs uint64
	for _, r := range reports {
		requests += r.Requests
		errs += r.Errors
		for code, count := range r.Status {
			status[code] += count
		}
	}
	return summary{SchemaVersion: summarySchemaVersion, Profile: profile, RawRunPaths: paths, RunCount: len(reports), Configuration: first.Configuration, Aggregate: aggregateReport{Requests: requests, Errors: errs, Status: status, Latency: latencyReport{Unit: "ms", Definition: "not computed: aggregate percentiles require raw latency histograms; inspect each raw run's p50/p95/p99", P50: 0, P95: 0, P99: 0}}}, nil
}
func validateReport(r report) error {
	if r.SchemaVersion != reportSchemaVersion {
		return fmt.Errorf("unsupported schema version %q", r.SchemaVersion)
	}
	if r.StartedAt.IsZero() || r.EndedAt.IsZero() || r.EndedAt.Before(r.StartedAt) {
		return errors.New("invalid start/end timestamps")
	}
	if r.Metadata["profile"] == "" {
		return errors.New("missing profile metadata")
	}
	if r.Requests == 0 || r.Latency.Unit != "ms" || r.Latency.Definition == "" {
		return errors.New("missing measurement fields")
	}
	var accounted uint64
	for _, count := range r.Status {
		accounted += count
	}
	if accounted+r.Errors != r.Requests {
		return errors.New("request count does not equal status counts plus errors")
	}
	if err := (config{url: r.Configuration.URL, duration: parseDuration(r.Configuration.Duration), concurrency: r.Configuration.Concurrency, requestTimeout: parseDuration(r.Configuration.RequestTimeout), metadata: r.Configuration.Metadata}).validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	if r.URL != r.Configuration.URL || r.Concurrency != r.Configuration.Concurrency || r.RequestTimeout != r.Configuration.RequestTimeout || !mapsEqual(r.Metadata, r.Configuration.Metadata) {
		return errors.New("legacy report fields do not match configuration")
	}
	return nil
}
func parseDuration(value string) time.Duration {
	duration, _ := time.ParseDuration(value)
	return duration
}
func sameConfiguration(left, right runConfiguration) bool {
	left.Metadata, right.Metadata = copyWithoutProfile(left.Metadata), copyWithoutProfile(right.Metadata)
	return left.URL == right.URL && left.Duration == right.Duration && left.Concurrency == right.Concurrency && left.RequestTimeout == right.RequestTimeout && mapsEqual(left.Metadata, right.Metadata)
}
func copyWithoutProfile(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for k, v := range source {
		if k != "profile" {
			result[k] = v
		}
	}
	return result
}
func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for k, v := range left {
		if right[k] != v {
			return false
		}
	}
	return true
}
func sanitizedCommand(args []string) []string {
	result := []string{"loadgen"}
	for i := 0; i < len(args); i++ {
		if args[i] == "-api-key" || args[i] == "--api-key" {
			result = append(result, args[i], "[redacted]")
			i++
			continue
		}
		if strings.HasPrefix(args[i], "-api-key=") || strings.HasPrefix(args[i], "--api-key=") {
			result = append(result, "-api-key=[redacted]")
			continue
		}
		result = append(result, args[i])
	}
	return result
}
func hasSensitiveQuery(u *url.URL) bool {
	for key := range u.Query() {
		if sensitiveName(key) {
			return true
		}
	}
	return false
}
func sensitiveName(name string) bool {
	name = strings.ToLower(name)
	return strings.Contains(name, "secret") || strings.Contains(name, "token") || strings.Contains(name, "password") || strings.Contains(name, "apikey") || strings.Contains(name, "api_key") || name == "key" || strings.Contains(name, "authorization")
}

type values map[string]string

func (v *values) String() string { return "" }
func (v *values) Set(value string) error {
	name, value, found := strings.Cut(value, "=")
	name = strings.TrimSpace(name)
	if !found || name == "" || len(name) > maxMetadataKey || len(value) > maxMetadataValue || sensitiveName(name) || strings.IndexFunc(name+value, func(r rune) bool { return r < 0x20 }) >= 0 {
		return errors.New("metadata must use a non-sensitive key=value")
	}
	if *v == nil {
		*v = make(values)
	}
	if len(*v) >= maxMetadata {
		return fmt.Errorf("metadata cannot exceed %d entries", maxMetadata)
	}
	if _, duplicate := (*v)[name]; duplicate {
		return fmt.Errorf("duplicate metadata key %q", name)
	}
	(*v)[name] = value
	return nil
}

type paths []string

func (p *paths) String() string { return "" }
func (p *paths) Set(value string) error {
	if value == "" {
		return errors.New("aggregate-run path cannot be empty")
	}
	*p = append(*p, value)
	return nil
}
