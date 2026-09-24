// Package configured builds safe collectors from a validated dataset package.
package configured

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/collector/httpjson"
	collectorretry "github.com/Kale105/streamforge/internal/collector/retry"
	"github.com/Kale105/streamforge/internal/collector/sse"
	"github.com/Kale105/streamforge/internal/collector/subprocess"
	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
)

type Mode string

const (
	ModePersonal Mode = "personal"
	ModeProvider Mode = "provider"
)

// Capabilities describe what the runtime may safely assume about a built source.
type Capabilities struct {
	Resumable           bool
	Partitionable       bool
	OrderingScope       string
	AcknowledgementMode string
}

type Result struct {
	Collector    collector.Collector
	Capabilities Capabilities
	Source       string
	Type         string
}

type Options struct {
	Mode             Mode
	Contract         event.Contract
	HTTPClient       *http.Client
	LookupEnv        func(string) (string, bool)
	SSEInitialCursor sse.Cursor
	SSECommit        sse.CommitFunc
}

func Source(spec dataset.Dataset) string { return "urn:streamforge:dataset:" + spec.Metadata.Name }
func Type(spec dataset.Dataset) string {
	return "dev.streamforge." + spec.Metadata.Name + ".observed.v1"
}

// Build resolves environment references once, constructs a bounded collector,
// and places the admission boundary around it. Connector-supplied contracts are
// discarded; every admitted event carries the active host contract.
func Build(spec dataset.Dataset, options Options) (Result, error) {
	if err := spec.Validate(); err != nil {
		return Result{}, err
	}
	if options.Mode == "" {
		options.Mode = ModePersonal
	}
	if options.Mode != ModePersonal && options.Mode != ModeProvider {
		return Result{}, errors.New("collector mode must be personal or provider")
	}
	if options.Mode == ModeProvider && !options.Contract.Complete() {
		return Result{}, errors.New("provider collector requires a complete pinned contract")
	}
	if options.LookupEnv == nil {
		options.LookupEnv = os.LookupEnv
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	var built collector.Collector
	var caps Capabilities
	switch spec.Collector.Type {
	case dataset.CollectorHTTPJSON:
		h := spec.Collector.HTTP()
		headers, err := resolveHeaders(h.Headers, options.LookupEnv)
		if err != nil {
			return Result{}, err
		}
		c, err := httpjson.New(httpjson.Config{URL: h.URL, Interval: h.PollInterval, RequestTimeout: h.RequestTimeout, MaxResponseBytes: h.MaxBodyBytes, Source: Source(spec), Type: Type(spec)}, withHeaders(client, headers))
		if err != nil {
			return Result{}, fmt.Errorf("build HTTP JSON collector: %w", err)
		}
		built, caps = c, Capabilities{OrderingScope: "endpoint", AcknowledgementMode: "durable-admission"}
	case dataset.CollectorSSE:
		if options.Mode != ModeProvider {
			return Result{}, errors.New("sse collectors require provider mode")
		}
		s := spec.Collector.SSE
		headers, err := resolveHeaders(s.Headers, options.LookupEnv)
		if err != nil {
			return Result{}, err
		}
		c, err := sse.New(sse.Config{URL: s.URL, RequestTimeout: s.RequestTimeout, MaxMessageBytes: s.MaxMessageBytes, MaxLineBytes: s.MaxLineBytes, Source: Source(spec), Type: Type(spec), InitialCursor: options.SSEInitialCursor, Commit: options.SSECommit, RequireEventID: true}, withHeaders(client, headers))
		if err != nil {
			return Result{}, fmt.Errorf("build SSE collector: %w", err)
		}
		built, caps = c, Capabilities{Resumable: true, OrderingScope: "stream", AcknowledgementMode: "durable-admission"}
	case dataset.CollectorSubprocess:
		if options.Mode != ModeProvider {
			return Result{}, errors.New("subprocess collectors require provider mode")
		}
		s := spec.Collector.Subprocess
		env, allowed, err := resolveEnvironment(s.Environment, options.LookupEnv)
		if err != nil {
			return Result{}, err
		}
		c, err := subprocess.New(subprocess.Config{Path: s.Path, Args: s.Args, Env: env, AllowedEnv: allowed, Dataset: spec.Metadata.Name, DatasetVersion: spec.Metadata.Version, Source: Source(spec), EventType: Type(spec), Contract: options.Contract, MaxStderrBytes: s.MaxStderrBytes, HandshakeTimeout: s.HandshakeTimeout, AckTimeout: s.AckTimeout, ShutdownTimeout: s.ShutdownTimeout})
		if err != nil {
			return Result{}, fmt.Errorf("build subprocess collector: %w", err)
		}
		built, caps = c, Capabilities{Resumable: true, OrderingScope: "connector", AcknowledgementMode: "connector-ack-after-admission"}
	default:
		return Result{}, fmt.Errorf("unsupported collector type %q", spec.Collector.Type)
	}
	if spec.Collector.Retry.Enabled() {
		var err error
		built, err = collectorretry.New(built, retryable, collectorretry.Config{InitialBackoff: spec.Collector.Retry.InitialBackoff, MaxBackoff: spec.Collector.Retry.MaxBackoff, MaxConsecutiveFailures: spec.Collector.Retry.MaxConsecutiveFailures})
		if err != nil {
			return Result{}, err
		}
	}
	return Result{Collector: admission{inner: built, source: Source(spec), typ: Type(spec), contract: options.Contract}, Capabilities: caps, Source: Source(spec), Type: Type(spec)}, nil
}

type admission struct {
	inner       collector.Collector
	source, typ string
	contract    event.Contract
}

func (a admission) Collect(ctx context.Context, emit collector.EmitFunc) error {
	if emit == nil {
		return errors.New("collector emit function is required")
	}
	return a.inner.Collect(ctx, func(ctx context.Context, observed event.Event) error {
		if observed.Source != a.source {
			return fmt.Errorf("collector event source %q does not match assigned source", observed.Source)
		}
		// Type and contract are host authority; a connector cannot select either.
		observed.Source, observed.Type, observed.Contract = a.source, a.typ, a.contract
		return emit(ctx, observed)
	})
}

func retryable(err error) bool {
	return errors.Is(err, httpjson.ErrRetryable) || errors.Is(err, sse.ErrRetryable) || errors.Is(err, subprocess.ErrRetryable)
}

func resolveHeaders(input map[string]string, lookup func(string) (string, bool)) (map[string]string, error) {
	output := make(map[string]string, len(input))
	for name, ref := range input {
		if datasetHeaderForbidden(name) {
			return nil, fmt.Errorf("header %q is forbidden", name)
		}
		value, err := resolveRef(ref, lookup)
		if err != nil {
			return nil, fmt.Errorf("resolve header %q: %w", name, err)
		}
		output[name] = value
	}
	return output, nil
}

func resolveEnvironment(input map[string]string, lookup func(string) (string, bool)) (map[string]string, []string, error) {
	output := make(map[string]string, len(input))
	allowed := make([]string, 0, len(input))
	for name, ref := range input {
		value, err := resolveRef(ref, lookup)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve environment %q: %w", name, err)
		}
		output[name] = value
		allowed = append(allowed, name)
	}
	return output, allowed, nil
}

func resolveRef(ref string, lookup func(string) (string, bool)) (string, error) {
	name, ok := strings.CutPrefix(ref, "env:")
	if !ok || name == "" {
		return "", errors.New("must be an env:NAME reference")
	}
	value, ok := lookup(name)
	if !ok {
		return "", fmt.Errorf("environment variable %q is not set", name)
	}
	return value, nil
}

func datasetHeaderForbidden(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "content-length":
		return true
	}
	return false
}

type headerTransport struct {
	next    http.RoundTripper
	headers map[string]string
}

func (t headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	for k, v := range t.headers {
		copy.Header.Set(k, v)
	}
	return t.next.RoundTrip(copy)
}
func withHeaders(client *http.Client, headers map[string]string) *http.Client {
	copy := *client
	next := client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	copy.Transport = headerTransport{next: next, headers: headers}
	return &copy
}

var _ collector.Collector = admission{}
