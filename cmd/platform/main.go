package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	streamapi "github.com/Kale105/streamforge/internal/api"
	"github.com/Kale105/streamforge/internal/apigateway/security"
	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/collector/configured"
	"github.com/Kale105/streamforge/internal/collector/generator"
	"github.com/Kale105/streamforge/internal/config"
	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/materializer"
	"github.com/Kale105/streamforge/internal/normalizer"
	"github.com/Kale105/streamforge/internal/observability"
	openapidoc "github.com/Kale105/streamforge/internal/openapi"
	"github.com/Kale105/streamforge/internal/pipeline"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/recovery"
	"github.com/Kale105/streamforge/internal/sink"
	stdoutsink "github.com/Kale105/streamforge/internal/sink/stdout"
	"github.com/Kale105/streamforge/internal/storage/postgres"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("streamforge stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: platform <validate|server|query|demo|worker|key|provider-admin|failures|replay> [options]")
	}
	switch args[0] {
	case "validate":
		return runValidate(args[1:])
	case "server":
		return runServerCommand(args[1:])
	case "query":
		return runQueryCommand(args[1:])
	case "demo":
		return runDemo(args[1:])
	case "key":
		return runKey(args[1:])
	case "failures":
		return runFailures(args[1:])
	case "replay":
		return runReplay(args[1:])
	case "worker":
		return runWorker(args[1:])
	case "provider-admin":
		return runProviderAdmin(args[1:])
	default:
		return fmt.Errorf("unknown command %q; expected validate, server, query, demo, key, provider-admin, failures, replay, or worker", args[0])
	}
}

func runQueryCommand(args []string) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	listenAddress := flags.String("listen", ":8080", "HTTP listen address")
	metricsAddress := flags.String("metrics-listen", "127.0.0.1:9090", "metrics listen address; empty disables metrics HTTP")
	requireAPIKey := flags.Bool("require-api-key", true, "require an API key for dataset reads")
	ratePerSecond := flags.Float64("rate-per-second", 20, "per-process, per-principal request refill rate")
	rateBurst := flags.Int("rate-burst", 40, "per-process, per-principal request burst")
	drainTimeout := flags.Duration("drain-timeout", 15*time.Second, "maximum graceful HTTP drain")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("query does not accept positional arguments")
	}
	if *databaseURL == "" {
		return errors.New("database URL is required through -database-url or STREAMFORGE_DATABASE_URL")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveQuery(ctx, serverOptions{
		configPath: *configPath, databaseURL: *databaseURL, listenAddress: *listenAddress,
		metricsAddress: *metricsAddress, requireAPIKey: *requireAPIKey,
		ratePerSecond: *ratePerSecond, rateBurst: *rateBurst, drainTimeout: *drainTimeout,
	})
}

func runValidate(args []string) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	path := flags.String("config", "dataset.yaml", "dataset YAML file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("validate does not accept positional arguments")
	}
	if _, err := config.LoadDataset(*path); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%s is valid\n", *path)
	return nil
}

func runServerCommand(args []string) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	listenAddress := flags.String("listen", ":8080", "HTTP listen address")
	metricsAddress := flags.String("metrics-listen", "127.0.0.1:9090", "metrics listen address; empty disables metrics HTTP")
	requireAPIKey := flags.Bool("require-api-key", false, "require an API key for dataset reads")
	ratePerSecond := flags.Float64("rate-per-second", 20, "per-principal request refill rate")
	rateBurst := flags.Int("rate-burst", 40, "per-principal request burst")
	bufferSize := flags.Int("buffer", 64, "maximum in-memory events awaiting persistence")
	drainTimeout := flags.Duration("drain-timeout", 15*time.Second, "maximum graceful pipeline drain")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("server does not accept positional arguments")
	}
	if *databaseURL == "" {
		return errors.New("database URL is required through -database-url or STREAMFORGE_DATABASE_URL")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, serverOptions{
		configPath: *configPath, databaseURL: *databaseURL, listenAddress: *listenAddress,
		metricsAddress: *metricsAddress, requireAPIKey: *requireAPIKey,
		ratePerSecond: *ratePerSecond, rateBurst: *rateBurst,
		bufferSize: *bufferSize, drainTimeout: *drainTimeout,
	})
}

type serverOptions struct {
	configPath     string
	databaseURL    string
	listenAddress  string
	metricsAddress string
	requireAPIKey  bool
	ratePerSecond  float64
	rateBurst      int
	bufferSize     int
	drainTimeout   time.Duration
}

func serve(ctx context.Context, options serverOptions) error {
	spec, err := config.LoadDataset(options.configPath)
	if err != nil {
		return err
	}
	// Validate and construct the source before acquiring external resources or
	// starting listeners. Personal mode must remain a Kafka-free HTTP JSON path.
	built, err := configured.Build(spec, configured.Options{Mode: configured.ModePersonal, HTTPClient: &http.Client{}})
	if err != nil {
		return fmt.Errorf("construct personal collector: %w", err)
	}
	store, err := postgres.New(ctx, options.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	normalize, err := normalizer.New(spec)
	if err != nil {
		return fmt.Errorf("construct normalizer: %w", err)
	}
	destination, err := materializer.New(normalize, store, spec.Metadata.Name, spec.Metadata.Version)
	if err != nil {
		return fmt.Errorf("construct materializer: %w", err)
	}
	sourceID := configured.Source(spec)
	if err := recovery.Drain(ctx, recovery.Config{
		Dataset: spec.Metadata.Name, Version: spec.Metadata.Version,
		Source: sourceID, BatchSize: 100, MaxBatches: 1000,
	}, store, destination); err != nil {
		return fmt.Errorf("recover durable materializations: %w", err)
	}
	metrics := observability.NewRegistry(observability.Config{Collectors: []string{spec.Metadata.Name}})
	var instrumentedSource collector.Collector = observability.Collector{Next: built.Collector, Metrics: metrics, Name: spec.Metadata.Name}
	var instrumentedDestination sink.Sink = observability.Sink{
		Next: destination, Metrics: metrics,
		Rejected: func(err error) bool { return errors.Is(err, materializer.ErrRejected) },
	}

	publicHandler, err := newPublicHandler(spec, store, options, metrics)
	if err != nil {
		return err
	}
	server := streamapi.NewServer(options.listenAddress, publicHandler)
	var metricsServer *http.Server
	if options.metricsAddress != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", metrics.Handler())
		metricsServer = streamapi.NewServer(options.metricsAddress, metricsMux)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	componentCount := 2
	if metricsServer != nil {
		componentCount++
	}
	results := make(chan componentResult, componentCount)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		results <- componentResult{component: "API server", err: err}
	}()
	go func() {
		err := pipeline.Run(runCtx, pipeline.Config{BufferSize: options.bufferSize, DrainTimeout: options.drainTimeout}, instrumentedSource, instrumentedDestination)
		results <- componentResult{component: "ingestion pipeline", err: err}
	}()
	if metricsServer != nil {
		go func() {
			err := metricsServer.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			results <- componentResult{component: "metrics server", err: err}
		}()
	}

	var firstErr error
	remaining := componentCount
	select {
	case first := <-results:
		remaining--
		if first.err != nil && !isCancellation(first.err) {
			firstErr = fmt.Errorf("%s: %w", first.component, first.err)
		}
	case <-ctx.Done():
	}
	cancel()

	shutdownDeadline := time.Now().Add(options.drainTimeout)
	shutdownErr := shutdownHTTPUntil(shutdownDeadline, server, metricsServer)
	if shutdownErr != nil && firstErr == nil {
		firstErr = shutdownErr
	}
	firstErr = collectComponentResults(results, remaining, time.Until(shutdownDeadline), firstErr)
	return firstErr
}

func newPublicHandler(spec dataset.Dataset, store *postgres.Store, options serverOptions, metrics *observability.Registry) (http.Handler, error) {
	handler, err := streamapi.New(store, store, streamapi.Config{
		Datasets: map[string]streamapi.DatasetPolicy{
			spec.Metadata.Name: publicDatasetPolicy(spec),
		},
		DefaultPageSize: spec.API.Pagination.Default,
		MaximumPageSize: spec.API.Pagination.Maximum,
	})
	if err != nil {
		return nil, fmt.Errorf("construct API: %w", err)
	}
	openAPI, err := openapidoc.Generate([]dataset.Dataset{spec}, openapidoc.Options{APIKeyEnabled: options.requireAPIKey})
	if err != nil {
		return nil, fmt.Errorf("generate OpenAPI document: %w", err)
	}
	publicMux := http.NewServeMux()
	publicMux.Handle("/", handler)
	publicMux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.oai.openapi+json")
		_, _ = w.Write(openAPI)
	})
	var publicHandler http.Handler = publicMux
	if options.requireAPIKey {
		limiter, err := security.NewLimiter(security.RateLimit{Rate: options.ratePerSecond, Burst: options.rateBurst}, nil)
		if err != nil {
			return nil, fmt.Errorf("construct API rate limiter: %w", err)
		}
		publicHandler = security.Middleware{
			Keys: store, Limiter: limiter,
			Scopes: security.RequestScopeFunc(func(r *http.Request) (string, security.Action, bool) {
				return datasetReadScope(r, spec.API.Path, spec.Metadata.Name)
			}),
		}.Wrap(publicHandler)
	}
	publicHandler = observability.HTTP(publicHandler, metrics)
	return publicHandler, nil
}

// publicDatasetPolicy is the narrow configuration-to-runtime boundary for
// query controls. Dataset.Validate has already established that filter and
// sort names are bounded allow-lists; this copy keeps later mutations of a
// loaded configuration from changing the live API policy.
func publicDatasetPolicy(spec dataset.Dataset) streamapi.DatasetPolicy {
	sorts := make([]recordstore.SortField, len(spec.API.Sorts))
	for i, sort := range spec.API.Sorts {
		sorts[i] = recordstore.SortField(sort)
	}
	return streamapi.DatasetPolicy{
		Path:    spec.API.Path,
		Version: spec.Metadata.Version,
		Fields:  append([]string(nil), spec.API.Fields...),
		Filters: append([]string(nil), spec.API.Filters...),
		Sorts:   sorts,
	}
}

func serveQuery(ctx context.Context, options serverOptions) error {
	spec, err := config.LoadDataset(options.configPath)
	if err != nil {
		return err
	}
	store, err := postgres.New(ctx, options.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}
	metrics := observability.NewRegistry(observability.Config{Collectors: []string{spec.Metadata.Name}})
	handler, err := newPublicHandler(spec, store, options, metrics)
	if err != nil {
		return err
	}
	server := streamapi.NewServer(options.listenAddress, handler)
	var metricsServer *http.Server
	if options.metricsAddress != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", metrics.Handler())
		metricsServer = streamapi.NewServer(options.metricsAddress, metricsMux)
	}

	componentCount := 1
	if metricsServer != nil {
		componentCount++
	}
	results := make(chan componentResult, componentCount)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		results <- componentResult{component: "API server", err: err}
	}()
	if metricsServer != nil {
		go func() {
			err := metricsServer.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			results <- componentResult{component: "metrics server", err: err}
		}()
	}

	var firstErr error
	remaining := componentCount
	select {
	case first := <-results:
		remaining--
		if first.err != nil && !isCancellation(first.err) {
			firstErr = fmt.Errorf("%s: %w", first.component, first.err)
		}
	case <-ctx.Done():
	}
	shutdownDeadline := time.Now().Add(options.drainTimeout)
	shutdownErr := shutdownHTTPUntil(shutdownDeadline, server, metricsServer)
	if shutdownErr != nil && firstErr == nil {
		firstErr = shutdownErr
	}
	firstErr = collectComponentResults(results, remaining, time.Until(shutdownDeadline), firstErr)
	return firstErr
}

type componentResult struct {
	component string
	err       error
}

// shutdownHTTP gives all HTTP servers the same deadline, rather than allowing
// each server to consume a full drain interval serially. If that deadline is
// exhausted every listener is force-closed and the caller can continue its
// own teardown without an unbounded wait.
func shutdownHTTP(timeout time.Duration, servers ...*http.Server) error {
	return shutdownHTTPUntil(time.Now().Add(timeout), servers...)
}

func shutdownHTTPUntil(deadline time.Time, servers ...*http.Server) error {
	active := make([]*http.Server, 0, len(servers))
	for _, server := range servers {
		if server != nil {
			active = append(active, server)
		}
	}
	if len(active) == 0 {
		return nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	errs := make(chan error, len(active))
	for _, server := range active {
		go func(s *http.Server) { errs <- s.Shutdown(ctx) }(server)
	}
	var joined error
	for range active {
		select {
		case err := <-errs:
			if err != nil {
				joined = errors.Join(joined, err)
			}
		case <-ctx.Done():
			for _, server := range active {
				_ = server.Close()
			}
			return errors.Join(joined, fmt.Errorf("HTTP graceful shutdown deadline exceeded; forced termination requested: %w", ctx.Err()))
		}
	}
	return joined
}

// collectComponentResults is intentionally bounded. A component which ignored
// cancellation must be reported, never allowed to pin process shutdown.
func collectComponentResults(results <-chan componentResult, remaining int, timeout time.Duration, firstErr error) error {
	if timeout <= 0 {
		for remaining > 0 {
			select {
			case result := <-results:
				remaining--
				if result.err != nil && !isCancellation(result.err) && firstErr == nil {
					firstErr = fmt.Errorf("%s: %w", result.component, result.err)
				}
			default:
				if firstErr == nil {
					firstErr = fmt.Errorf("component shutdown did not complete before HTTP deadline; %d component(s) remain", remaining)
				}
				return firstErr
			}
		}
		return firstErr
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.err != nil && !isCancellation(result.err) && firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", result.component, result.err)
			}
		case <-timer.C:
			if firstErr == nil {
				firstErr = fmt.Errorf("component shutdown deadline exceeded; %d component(s) did not terminate", remaining)
			}
			return firstErr
		}
	}
	return firstErr
}

func datasetReadScope(r *http.Request, path, datasetName string) (string, security.Action, bool) {
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.URL.Path == path {
		return datasetName, security.ActionRead, true
	}
	return "", "", false
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func runDemo(args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	count := flags.Int("count", 10, "number of events; zero runs until interrupted")
	interval := flags.Duration("interval", time.Second, "time between generated events")
	buffer := flags.Int("buffer", 4, "maximum queued events")
	if err := flags.Parse(args); err != nil {
		return err
	}
	source, err := generator.New(*interval, *count)
	if err != nil {
		return fmt.Errorf("configure generator: %w", err)
	}
	destination, err := stdoutsink.New(os.Stdout)
	if err != nil {
		return fmt.Errorf("configure stdout sink: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = pipeline.Run(ctx, pipeline.Config{BufferSize: *buffer, DrainTimeout: 5 * time.Second}, source, destination)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
