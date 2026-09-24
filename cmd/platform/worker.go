package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	rand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/Kale105/streamforge/internal/collector/configured"
	"github.com/Kale105/streamforge/internal/collector/httpjson"
	"github.com/Kale105/streamforge/internal/collector/sse"
	"github.com/Kale105/streamforge/internal/collector/subprocess"
	"github.com/Kale105/streamforge/internal/config"
	"github.com/Kale105/streamforge/internal/coordination"
	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/normalizer"
	"github.com/Kale105/streamforge/internal/observability"
	"github.com/Kale105/streamforge/internal/providerdlq"
	"github.com/Kale105/streamforge/internal/providerflow"
	"github.com/Kale105/streamforge/internal/telemetry"
	"github.com/Kale105/streamforge/internal/transport"
	transportkafka "github.com/Kale105/streamforge/internal/transport/kafka"
	"github.com/Kale105/streamforge/internal/workerretry"
	"github.com/Kale105/streamforge/internal/worklease"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	defaultKafkaMessageBytes = 2 << 20
	defaultKafkaFetchBytes   = 16 << 20
)

// workerAdmin owns the one optional admin listener for a worker process.
// Liveness describes only this listener's serving loop; readiness is raised
// only by the role after it has initialized its required dependencies.
type workerAdmin struct {
	metrics  *observability.Registry
	server   *http.Server
	listener net.Listener
	ready    atomic.Bool
}

func startWorkerAdmin(address string) (*workerAdmin, error) {
	admin := &workerAdmin{metrics: observability.NewRegistry(observability.Config{})}
	if strings.TrimSpace(address) == "" {
		return admin, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("start worker admin listener: %w", err)
	}
	admin.listener = listener
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if admin.ready.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "worker dependencies are not ready", http.StatusServiceUnavailable)
		}
	})
	mux.Handle("/metrics", admin.metrics.Handler())
	admin.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { _ = admin.server.Serve(listener) }()
	return admin, nil
}

func (a *workerAdmin) SetReady() {
	if a != nil {
		a.ready.Store(true)
	}
}

func (a *workerAdmin) SetNotReady() {
	if a != nil {
		a.ready.Store(false)
	}
}

func (a *workerAdmin) Close() error {
	if a == nil || a.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.server.Shutdown(ctx)
	if err != nil {
		_ = a.server.Close()
	}
	return err
}

func runWorker(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: platform worker <collector|processor|sink|dlq-indexer|replay> [options]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "collector":
		return runCollectorWorker(ctx, args[1:])
	case "processor":
		return runProcessorWorker(ctx, args[1:])
	case "sink":
		return runSinkWorker(ctx, args[1:])
	case "dlq-indexer":
		return runDLQIndexerWorker(ctx, args[1:])
	case "replay":
		return runProviderReplayWorker(ctx, args[1:])
	default:
		return fmt.Errorf("unknown worker role %q; expected collector, processor, sink, dlq-indexer, or replay", args[0])
	}
}

type kafkaFlags struct {
	brokers       string
	rawTopic      string
	normalized    string
	dlqTopic      string
	replayTopic   string
	maxMessage    int
	fetchMaxBytes int
	tlsEnabled    string
	tlsCAPEM      string
	tlsCAFile     string
	tlsServerName string
	tlsCertFile   string
	tlsKeyFile    string
	saslMechanism string
	saslUsername  string
	saslPassword  string
	adminAddr     string
}

func addKafkaFlags(flags *flag.FlagSet, values *kafkaFlags) {
	flags.StringVar(&values.brokers, "brokers", os.Getenv("STREAMFORGE_KAFKA_BROKERS"), "comma-separated Kafka broker addresses")
	flags.StringVar(&values.rawTopic, "raw-topic", "", "raw topic; defaults from dataset name")
	flags.StringVar(&values.normalized, "normalized-topic", "", "normalized topic; defaults from dataset name")
	flags.StringVar(&values.dlqTopic, "dlq-topic", "", "dead-letter topic; defaults from dataset name")
	flags.StringVar(&values.replayTopic, "replay-topic", "", "revision-scoped replay topic; defaults from dataset name")
	flags.IntVar(&values.maxMessage, "max-message-bytes", defaultKafkaMessageBytes, "maximum Kafka message bytes")
	flags.IntVar(&values.fetchMaxBytes, "fetch-max-bytes", defaultKafkaFetchBytes, "maximum bytes per Kafka fetch")
	// Keep TLS enablement as text until validation so an invalid environment
	// value fails explicitly instead of silently disabling transport security.
	flags.StringVar(&values.tlsEnabled, "kafka-tls-enabled", os.Getenv("STREAMFORGE_KAFKA_TLS_ENABLED"), "enable verified Kafka TLS")
	// Security values cannot be flag defaults: flag help prints non-empty
	// defaults, which would expose credentials or PEM material. security()
	// resolves their STREAMFORGE_* counterparts after parsing instead.
	flags.StringVar(&values.tlsCAPEM, "kafka-tls-ca-pem", "", "Kafka TLS CA PEM (prefer environment injection)")
	flags.StringVar(&values.tlsCAFile, "kafka-tls-ca-file", "", "Kafka TLS CA PEM file")
	flags.StringVar(&values.tlsServerName, "kafka-tls-server-name", "", "Kafka TLS server name")
	flags.StringVar(&values.tlsCertFile, "kafka-tls-client-cert-file", "", "Kafka TLS client certificate file")
	flags.StringVar(&values.tlsKeyFile, "kafka-tls-client-key-file", "", "Kafka TLS client key file")
	flags.StringVar(&values.saslMechanism, "kafka-sasl-mechanism", "", "Kafka SASL mechanism: PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512")
	flags.StringVar(&values.saslUsername, "kafka-sasl-username", "", "Kafka SASL username")
	flags.StringVar(&values.saslPassword, "kafka-sasl-password", "", "Kafka SASL password (prefer environment injection)")
	flags.StringVar(&values.adminAddr, "admin-addr", os.Getenv("STREAMFORGE_WORKER_ADMIN_ADDR"), "worker admin listen address; empty disables admin endpoint")
}

func (values *kafkaFlags) normalize(datasetName, datasetVersion string) {
	prefix := "streamforge." + datasetName
	revision := revisionSuffix(datasetVersion)
	if values.rawTopic == "" {
		values.rawTopic = prefix + ".raw"
	}
	if values.normalized == "" {
		values.normalized = prefix + ".normalized." + revision
	}
	if values.dlqTopic == "" {
		values.dlqTopic = prefix + ".dlq." + revision
	}
	if values.replayTopic == "" {
		values.replayTopic = prefix + ".raw.replay." + revision
	}
}

func (values kafkaFlags) limits() transport.Limits {
	return transport.Limits{
		MaxMessageBytes: values.maxMessage, SendTimeout: 10 * time.Second,
		FetchMaxBytes: values.fetchMaxBytes, FetchMaxWait: 500 * time.Millisecond, MaxInFlight: 1,
	}
}

func (values kafkaFlags) brokerList() ([]string, error) {
	parts := strings.Split(values.brokers, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return nil, errors.New("Kafka brokers are required through -brokers or STREAMFORGE_KAFKA_BROKERS")
	}
	return brokers, nil
}

func (values kafkaFlags) security() (transportkafka.SecurityConfig, error) {
	value := func(flagValue, environmentName string) string {
		if flagValue != "" {
			return flagValue
		}
		return os.Getenv(environmentName)
	}
	enabled := false
	if strings.TrimSpace(values.tlsEnabled) != "" {
		parsed, err := strconv.ParseBool(values.tlsEnabled)
		if err != nil {
			return transportkafka.SecurityConfig{}, errors.New("Kafka TLS enabled must be a boolean")
		}
		enabled = parsed
	}
	security := transportkafka.SecurityConfig{
		TLS:  transportkafka.TLSConfig{Enabled: enabled, CAPEM: value(values.tlsCAPEM, "STREAMFORGE_KAFKA_TLS_CA_PEM"), CAFile: value(values.tlsCAFile, "STREAMFORGE_KAFKA_TLS_CA_FILE"), ServerName: value(values.tlsServerName, "STREAMFORGE_KAFKA_TLS_SERVER_NAME"), ClientCertFile: value(values.tlsCertFile, "STREAMFORGE_KAFKA_TLS_CLIENT_CERT_FILE"), ClientKeyFile: value(values.tlsKeyFile, "STREAMFORGE_KAFKA_TLS_CLIENT_KEY_FILE")},
		SASL: transportkafka.SASLConfig{Mechanism: value(values.saslMechanism, "STREAMFORGE_KAFKA_SASL_MECHANISM"), Username: value(values.saslUsername, "STREAMFORGE_KAFKA_SASL_USERNAME"), Password: value(values.saslPassword, "STREAMFORGE_KAFKA_SASL_PASSWORD")},
	}
	// Validation intentionally produces generic errors; configuration errors
	// must never reflect a username, password, or PEM material back to logs.
	if err := (transportkafka.ProducerConfig{Brokers: []string{"validation"}, Limits: values.limits(), Security: security}).Validate(); err != nil {
		return transportkafka.SecurityConfig{}, err
	}
	return security, nil
}

func providerContract(spec dataset.Dataset) (*normalizer.Normalizer, event.Contract, error) {
	for _, item := range []struct {
		name string
		ref  dataset.SchemaReference
	}{{"raw", spec.Schemas.Raw}, {"normalized", spec.Schemas.Normalized}} {
		if item.ref.Path == "" || item.ref.Revision == "" || item.ref.Digest == "" {
			return nil, event.Contract{}, fmt.Errorf("provider workers require a pinned %s schema path, revision, and digest", item.name)
		}
	}
	if strings.TrimSpace(spec.Normalization.Revision) == "" {
		return nil, event.Contract{}, errors.New("provider workers require normalization.revision")
	}
	n, err := normalizer.New(spec)
	if err != nil {
		return nil, event.Contract{}, err
	}
	contract := n.Contract()
	if !contract.Complete() {
		return nil, event.Contract{}, errors.New("provider workers require a complete immutable data contract")
	}
	return n, contract, nil
}

func runCollectorWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker collector", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL for collector ownership")
	holder := flags.String("holder", "", "unique worker identity; generated when omitted")
	leaseDuration := flags.Duration("lease-duration", 30*time.Second, "collector ownership lease duration")
	var kafkaConfig kafkaFlags
	addKafkaFlags(flags, &kafkaConfig)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker collector does not accept positional arguments")
	}
	admin, err := startWorkerAdmin(kafkaConfig.adminAddr)
	if err != nil {
		return err
	}
	defer admin.Close()
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	kafkaConfig.normalize(spec.Metadata.Name, spec.Metadata.Version)
	brokers, err := kafkaConfig.brokerList()
	if err != nil {
		return err
	}
	security, err := kafkaConfig.security()
	if err != nil {
		return err
	}
	_, contract, err := providerContract(spec)
	if err != nil {
		return err
	}
	sourceID := configured.Source(spec)
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if *holder == "" {
		hostname, _ := os.Hostname()
		identifier, err := randomID(8)
		if err != nil {
			return err
		}
		*holder = generatedHolder(hostname, identifier)
	}
	supervisor := worklease.Supervisor{Config: worklease.Config{
		Store:      store,
		Assignment: coordination.Assignment{Dataset: spec.Metadata.Name, Source: sourceID, Partition: "default"},
		Holder:     *holder, Duration: *leaseDuration, CleanupTimeout: 5 * time.Second,
	}}
	for {
		admin.SetNotReady()
		err = runWithWorkerRetryMetrics(ctx, admin.metrics, "collector", func(attemptCtx context.Context) error {
			return supervisor.RunWithLease(attemptCtx, func(leaseCtx context.Context, lease coordination.Lease) error {
				options := configured.Options{Mode: configured.ModeProvider, Contract: contract, HTTPClient: &http.Client{}}
				if spec.Collector.Type == dataset.CollectorSSE {
					progress, found, err := store.Checkpoint(leaseCtx, lease.Assignment)
					if err != nil {
						return fmt.Errorf("load SSE checkpoint: %w", err)
					}
					if found {
						options.SSEInitialCursor = sse.Cursor{Sequence: progress.Checkpoint.Sequence, LastEventID: string(progress.Checkpoint.Cursor)}
					}
					// SSE invokes Commit only after the emit callback returns. The emit
					// callback below waits for its Kafka transaction, so Advance records
					// a cursor only after the raw event is durable at the broker.
					options.SSECommit = func(commitCtx context.Context, sequence uint64, lastID, eventID string) error {
						return store.Advance(commitCtx, lease, coordination.Progress{
							Checkpoint:      coordination.Checkpoint{Sequence: sequence, Cursor: []byte(lastID)},
							Acknowledgement: []byte(eventID),
						})
					}
				}
				// Build occurs inside the acquired lease. A successor does not retain
				// an old SSE cursor or subprocess session from the previous owner.
				built, err := configured.Build(spec, options)
				if err != nil {
					return fmt.Errorf("build provider collector: %w", err)
				}
				// This identity intentionally excludes the holder and fencing token.
				// A successor for the same logical assignment uses the same Kafka
				// transactional ID and therefore fences this producer at the broker.
				producer, err := transportkafka.NewProducer(transportkafka.ProducerConfig{Brokers: brokers, Limits: kafkaConfig.limits(), TransactionalID: collectorTransactionalID(spec.Metadata.Name, spec.Metadata.Version, lease.Assignment), Security: security})
				if err != nil {
					return err
				}
				defer producer.Close()
				admin.SetReady()
				return built.Collector.Collect(leaseCtx, func(ctx context.Context, observed event.Event) error {
					body, err := transport.MarshalCanonicalEvent(observed)
					if err != nil {
						return err
					}
					ctx = telemetry.EnsureTraceparent(ctx)
					err = producer.PublishBytesWithHeaders(ctx, kafkaConfig.rawTopic, sourcePartitionKey(observed, spec.Normalization.Key), body, observed.Time, telemetry.Headers(ctx))
					if err == nil {
						admin.metrics.WorkerProcessed("collector")
					}
					return err
				})
			})
		})
		if errors.Is(err, coordination.ErrLeaseLost) {
			admin.metrics.WorkerLeaseLoss()
			admin.SetNotReady()
			// A lease loss is expected during failover. Do not reuse the source or
			// producer: the next pass will acquire a new lease and rebuild both.
			continue
		}
		if errors.Is(err, worklease.ErrContended) {
			admin.SetNotReady()
			timer := time.NewTimer(contentionDelay())
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
				continue
			}
		}
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() != nil {
			admin.SetNotReady()
			return nil
		}
		if err == nil {
			admin.SetNotReady()
			return nil
		}
		admin.SetNotReady()
		return err
	}
}

func runProcessorWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker processor", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	group := flags.String("group", "", "Kafka consumer group; defaults from dataset revision")
	clusterID := flags.String("cluster-id", "local", "provider Kafka cluster identity")
	var kafkaConfig kafkaFlags
	addKafkaFlags(flags, &kafkaConfig)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker processor does not accept positional arguments")
	}
	admin, err := startWorkerAdmin(kafkaConfig.adminAddr)
	if err != nil {
		return err
	}
	defer admin.Close()
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	kafkaConfig.normalize(spec.Metadata.Name, spec.Metadata.Version)
	if *group == "" {
		*group = "streamforge-processor-" + spec.Metadata.Name + "-" + revisionSuffix(spec.Metadata.Version)
	}
	brokers, err := kafkaConfig.brokerList()
	if err != nil {
		return err
	}
	security, err := kafkaConfig.security()
	if err != nil {
		return err
	}
	normalize, _, err := providerContract(spec)
	if err != nil {
		return err
	}
	return runWithWorkerRetryMetrics(ctx, admin.metrics, "processor", func(attemptCtx context.Context) error {
		admin.SetNotReady()
		consumer, err := transportkafka.NewConsumer(transportkafka.ConsumerConfig{Brokers: brokers, Topics: []string{kafkaConfig.rawTopic, kafkaConfig.replayTopic}, Group: *group, Limits: kafkaConfig.limits(), StartFromEarliest: true, Security: security})
		if err != nil {
			return err
		}
		defer consumer.Close()
		producer, err := transportkafka.NewProducer(transportkafka.ProducerConfig{Brokers: brokers, Limits: kafkaConfig.limits(), Security: security})
		if err != nil {
			return err
		}
		defer producer.Close()
		processor, err := providerflow.NewProcessorFromNormalizer(normalize, providerflow.KafkaPublisher{Producer: producer, Topic: kafkaConfig.normalized, MaxBytes: kafkaConfig.maxMessage}, spec.Metadata.Version)
		if err != nil {
			return err
		}
		dlq := kafkaFailurePublisher{producer: producer, topic: kafkaConfig.dlqTopic, maxBytes: kafkaConfig.maxMessage}
		admin.SetReady()
		return consumer.RunRecords(attemptCtx, func(recordCtx context.Context, record transportkafka.RawRecord) error {
			return instrumentProcessorRecord(recordCtx, record, admin.metrics, *clusterID, spec.Metadata.Name, spec.Metadata.Version, spec.Normalization.Key, kafkaConfig.replayTopic, processor, dlq)
		})
	})
}

func runSinkWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker sink", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	group := flags.String("group", "", "Kafka consumer group; defaults from dataset")
	clusterID := flags.String("cluster-id", "local", "provider Kafka cluster identity")
	var kafkaConfig kafkaFlags
	addKafkaFlags(flags, &kafkaConfig)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker sink does not accept positional arguments")
	}
	admin, err := startWorkerAdmin(kafkaConfig.adminAddr)
	if err != nil {
		return err
	}
	defer admin.Close()
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	kafkaConfig.normalize(spec.Metadata.Name, spec.Metadata.Version)
	if *group == "" {
		*group = "streamforge-sink-" + spec.Metadata.Name + "-" + revisionSuffix(spec.Metadata.Version)
	}
	brokers, err := kafkaConfig.brokerList()
	if err != nil {
		return err
	}
	security, err := kafkaConfig.security()
	if err != nil {
		return err
	}
	_, contract, err := providerContract(spec)
	if err != nil {
		return err
	}
	return runWithWorkerRetryMetrics(ctx, admin.metrics, "sink", func(attemptCtx context.Context) error {
		admin.SetNotReady()
		store, err := openMigratedStore(attemptCtx, *databaseURL)
		if err != nil {
			return err
		}
		defer store.Close()
		consumer, err := transportkafka.NewConsumer(transportkafka.ConsumerConfig{Brokers: brokers, Topic: kafkaConfig.normalized, Group: *group, Limits: kafkaConfig.limits(), StartFromEarliest: true, Security: security})
		if err != nil {
			return err
		}
		defer consumer.Close()
		producer, err := transportkafka.NewProducer(transportkafka.ProducerConfig{Brokers: brokers, Limits: kafkaConfig.limits(), Security: security})
		if err != nil {
			return err
		}
		defer producer.Close()
		dlq := kafkaFailurePublisher{producer: producer, topic: kafkaConfig.dlqTopic, maxBytes: kafkaConfig.maxMessage}
		strictSink, err := providerflow.NewStrictSink(store, contract)
		if err != nil {
			return err
		}
		admin.SetReady()
		return consumer.RunRecords(attemptCtx, func(recordCtx context.Context, record transportkafka.RawRecord) error {
			return instrumentSinkRecord(recordCtx, record, admin.metrics, *clusterID, spec.Metadata.Name, spec.Metadata.Version, kafkaConfig.normalized, strictSink, dlq, kafkaConfig.maxMessage)
		})
	})
}

func runDLQIndexerWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker dlq-indexer", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	group := flags.String("group", "", "Kafka consumer group; defaults from dataset revision")
	clusterID := flags.String("cluster-id", "local", "provider Kafka cluster identity")
	var kafkaConfig kafkaFlags
	addKafkaFlags(flags, &kafkaConfig)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker dlq-indexer does not accept positional arguments")
	}
	admin, err := startWorkerAdmin(kafkaConfig.adminAddr)
	if err != nil {
		return err
	}
	defer admin.Close()
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	kafkaConfig.normalize(spec.Metadata.Name, spec.Metadata.Version)
	if *group == "" {
		*group = "streamforge-dlq-indexer-" + spec.Metadata.Name + "-" + revisionSuffix(spec.Metadata.Version)
	}
	brokers, err := kafkaConfig.brokerList()
	if err != nil {
		return err
	}
	security, err := kafkaConfig.security()
	if err != nil {
		return err
	}
	if _, _, err := providerContract(spec); err != nil {
		return err
	}
	return runWithWorkerRetryMetrics(ctx, admin.metrics, "dlq-indexer", func(attemptCtx context.Context) error {
		admin.SetNotReady()
		store, err := openMigratedStore(attemptCtx, *databaseURL)
		if err != nil {
			return err
		}
		defer store.Close()
		consumer, err := transportkafka.NewConsumer(transportkafka.ConsumerConfig{Brokers: brokers, Topic: kafkaConfig.dlqTopic, Group: *group, Limits: kafkaConfig.limits(), StartFromEarliest: true, Security: security})
		if err != nil {
			return err
		}
		defer consumer.Close()
		admin.SetReady()
		return consumer.RunRecords(attemptCtx, func(recordCtx context.Context, record transportkafka.RawRecord) error {
			failure, err := providerdlq.UnmarshalFailure(record.Value, kafkaConfig.maxMessage)
			if err != nil {
				return err
			} // corrupt DLQ records require operator intervention; never skip them.
			if failure.ClusterID != *clusterID {
				return fmt.Errorf("provider DLQ cluster mismatch")
			}
			_, err = store.ProviderDLQ().Upsert(recordCtx, failure)
			return err
		})
	})
}

func runProviderReplayWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker replay", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	workerID := flags.String("worker-id", "", "stable replay worker identity")
	clusterID := flags.String("cluster-id", "local", "provider Kafka cluster identity")
	lease := flags.Duration("claim-lease", time.Minute, "replay claim lease")
	poll := flags.Duration("poll-interval", time.Second, "idle claim polling interval")
	var kafkaConfig kafkaFlags
	addKafkaFlags(flags, &kafkaConfig)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker replay does not accept positional arguments")
	}
	admin, err := startWorkerAdmin(kafkaConfig.adminAddr)
	if err != nil {
		return err
	}
	defer admin.Close()
	if *workerID == "" {
		return errors.New("worker replay requires -worker-id")
	}
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	kafkaConfig.normalize(spec.Metadata.Name, spec.Metadata.Version)
	brokers, err := kafkaConfig.brokerList()
	if err != nil {
		return err
	}
	security, err := kafkaConfig.security()
	if err != nil {
		return err
	}
	if _, _, err := providerContract(spec); err != nil {
		return err
	}
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	admin.SetReady()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		claim, err := store.ProviderDLQ().ClaimReplay(ctx, providerdlq.ClaimRequest{ClusterID: *clusterID, Worker: *workerID, Lease: *lease})
		if err != nil {
			if workerRetryable(err) {
				if err := waitWorker(ctx, *poll); err != nil {
					return nil
				}
				continue
			}
			return err
		}
		if claim == nil {
			if err := waitWorker(ctx, *poll); err != nil {
				return nil
			}
			continue
		}
		// The transaction ID is request-stable, so a replacement worker fences a
		// predecessor before it can publish a second successful replay.
		producer, err := transportkafka.NewProducer(transportkafka.ProducerConfig{Brokers: brokers, Limits: kafkaConfig.limits(), TransactionalID: replayTransactionalID(claim.Request.RequestID), Security: security})
		if err == nil {
			traceCtx, ok := telemetry.WithTraceparent(ctx, claim.Failure.Traceparent)
			if !ok {
				traceCtx = telemetry.EnsureTraceparent(ctx)
			}
			err = producer.PublishOpaqueWithHeaders(traceCtx, claim.Failure.ReplayTopic, claim.Failure.Key, claim.Failure.Value, claim.Failure.SourceTimestamp, telemetry.Headers(traceCtx))
			_ = producer.Close()
		}
		if err != nil {
			if workerRetryable(err) {
				return err
			} // claim expiry permits another attempt; do not terminally fail transient transport.
			if finishErr := store.ProviderDLQ().FailReplay(ctx, claim.Request.RequestID, claim.ClaimToken, err); finishErr != nil {
				return finishErr
			}
			continue
		}
		if err := store.ProviderDLQ().CompleteReplay(ctx, claim.Request.RequestID, claim.ClaimToken); err != nil {
			return err
		}
	}
}

func waitWorker(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return errors.New("worker poll interval must be positive")
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func replayTransactionalID(requestID string) string {
	sum := sha256.Sum256([]byte(requestID))
	return "streamforge.replay." + hex.EncodeToString(sum[:16])
}

// failurePublisher is deliberately small so acknowledgement behavior can be
// tested without a Kafka broker. A handler returns nil only after Publish
// returns nil; RunRecords performs the corresponding offset commit.
type failurePublisher interface {
	PublishFailure(context.Context, providerdlq.Failure) error
}

type kafkaFailurePublisher struct {
	producer *transportkafka.Producer
	topic    string
	maxBytes int
}

func (p kafkaFailurePublisher) PublishFailure(ctx context.Context, failure providerdlq.Failure) error {
	body, err := providerdlq.MarshalFailure(failure)
	if err != nil {
		return err
	}
	if len(body) > p.maxBytes {
		return transport.ErrMessageTooLarge
	}
	return p.producer.PublishBytesWithHeaders(ctx, p.topic, failure.FailureID, body, failure.FailedAt, telemetry.Headers(telemetry.EnsureTraceparent(ctx)))
}

type workerMetricsKey struct{}

func withWorkerMetrics(ctx context.Context, metrics *observability.Registry) context.Context {
	return context.WithValue(ctx, workerMetricsKey{}, metrics)
}
func workerMetrics(ctx context.Context) *observability.Registry {
	metrics, _ := ctx.Value(workerMetricsKey{}).(*observability.Registry)
	return metrics
}

func instrumentProcessorRecord(ctx context.Context, record transportkafka.RawRecord, metrics *observability.Registry, args ...any) error {
	started := time.Now()
	ctx = withWorkerMetrics(telemetry.ContextFromHeaders(ctx, record.Headers), metrics)
	err := handleProcessorRecord(ctx, record, args[0].(string), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(*providerflow.Processor), args[6].(failurePublisher))
	metrics.WorkerLatency("processor", time.Since(started))
	if err == nil {
		metrics.WorkerProcessed("processor")
	} else {
		metrics.WorkerFailure("processor", "dependency")
	}
	return err
}

func instrumentSinkRecord(ctx context.Context, record transportkafka.RawRecord, metrics *observability.Registry, args ...any) error {
	started := time.Now()
	ctx = withWorkerMetrics(telemetry.ContextFromHeaders(ctx, record.Headers), metrics)
	err := handleSinkRecord(ctx, record, args[0].(string), args[1].(string), args[2].(string), args[3].(string), args[4].(providerflow.Sink), args[5].(failurePublisher), args[6].(int))
	metrics.WorkerLatency("sink", time.Since(started))
	if err == nil {
		metrics.WorkerProcessed("sink")
	} else {
		metrics.WorkerFailure("sink", "dependency")
	}
	return err
}

func handleProcessorRecord(ctx context.Context, record transportkafka.RawRecord, clusterID, dataset, revision, keyField, replayTopic string, processor *providerflow.Processor, dlq failurePublisher) error {
	if len(record.Key) == 0 {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageProcessor, providerdlq.ClassKey, "raw record has no key", replayTopic, processor.Contract, event.Contract{}))
	}
	var observed event.Event
	if err := json.Unmarshal(record.Value, &observed); err != nil || observed.Validate() != nil {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageProcessor, providerdlq.ClassDecode, "invalid raw event envelope", replayTopic, processor.Contract, observed.Contract))
	}
	if sourcePartitionKey(observed, keyField) != string(record.Key) {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageProcessor, providerdlq.ClassKey, "raw record key does not match event key", replayTopic, processor.Contract, observed.Contract))
	}
	err := processor.HandleRaw(ctx, transport.Envelope{Topic: record.Topic, PartitionKey: string(record.Key), Event: observed})
	if err == nil {
		return nil
	}
	if errors.Is(err, providerflow.ErrRejected) {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageProcessor, providerdlq.ClassNormalize, boundedDiagnostic(err.Error()), replayTopic, processor.Contract, observed.Contract))
	}
	// Broker delivery failure is not poison: leave the offset uncommitted for a
	// fresh client attempt. Configuration and contract errors likewise surface
	// rather than silently pretending the record was handled.
	return err
}

func handleSinkRecord(ctx context.Context, record transportkafka.RawRecord, clusterID, dataset, revision, replayTopic string, sink providerflow.Sink, dlq failurePublisher, maxBytes int) error {
	if len(record.Key) == 0 {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageSink, providerdlq.ClassKey, "normalized record has no key", replayTopic, sink.Contract, bestEffortNormalizedContract(record.Value)))
	}
	message, err := providerflow.Unmarshal(record.Value, maxBytes)
	if err != nil {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageSink, providerdlq.ClassDecode, "invalid normalized record contract", replayTopic, sink.Contract, bestEffortNormalizedContract(record.Value)))
	}
	if message.IdempotencyKey() != string(record.Key) {
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageSink, providerdlq.ClassKey, "normalized record key does not match source identity", replayTopic, sink.Contract, message.Contract))
	}
	if err := sink.Handle(ctx, message); err != nil {
		if workerRetryable(err) {
			return err
		}
		return publishProviderFailure(ctx, dlq, providerFailure(record, clusterID, dataset, revision, providerdlq.StageSink, providerdlq.ClassSink, boundedDiagnostic(err.Error()), replayTopic, sink.Contract, message.Contract))
	}
	return nil
}

func publishProviderFailure(ctx context.Context, dlq failurePublisher, failure providerdlq.Failure) error {
	if dlq == nil {
		return errors.New("provider DLQ publisher is required")
	}
	failure.Traceparent = telemetry.Traceparent(ctx)
	if err := dlq.PublishFailure(ctx, failure); err != nil {
		return fmt.Errorf("durably publish provider dead letter: %w", err)
	}
	if metrics := workerMetrics(ctx); metrics != nil {
		metrics.WorkerDLQ(string(failure.Stage))
		metrics.WorkerFailure(string(failure.Stage), string(failure.Class))
	}
	return nil
}

func providerFailure(record transportkafka.RawRecord, clusterID, dataset, revision string, stage providerdlq.Stage, class providerdlq.Class, diagnostic, replayTopic string, expected, original event.Contract) providerdlq.Failure {
	f := providerdlq.Failure{ClusterID: clusterID, Dataset: dataset, SourceRevision: revision, Stage: stage, Class: class,
		Diagnostic: boundedDiagnostic(diagnostic), SourceTopic: record.Topic, SourcePartition: record.Partition, SourceOffset: record.Offset,
		SourceTimestamp: record.Timestamp.UTC(), Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...), ExpectedContract: expected, OriginalContract: original, ReplayTopic: replayTopic, FailedAt: time.Now().UTC()}
	f.FailureID = providerdlq.DeterministicFailureID(f.ClusterID, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.Stage)
	return f
}

func bestEffortNormalizedContract(value []byte) event.Contract {
	var wire struct {
		Contract event.Contract `json:"contract"`
	}
	if json.Unmarshal(value, &wire) != nil {
		return event.Contract{}
	}
	return wire.Contract
}

func runWithWorkerRetry(ctx context.Context, attempt func(context.Context) error) error {
	return workerretry.Run(ctx, workerretry.Config{InitialBackoff: 250 * time.Millisecond, MaxBackoff: 5 * time.Second, MaxAttempts: 8}, attempt, workerRetryable)
}

func runWithWorkerRetryMetrics(ctx context.Context, metrics *observability.Registry, role string, attempt func(context.Context) error) error {
	attempts := 0
	return runWithWorkerRetry(ctx, func(attemptCtx context.Context) error {
		if attempts > 0 && metrics != nil {
			metrics.WorkerRetried(role)
		}
		attempts++
		return attempt(attemptCtx)
	})
}

func workerRetryable(err error) bool {
	if transportkafka.IsRetryable(err) || errors.Is(err, transportkafka.ErrRetryable) ||
		errors.Is(err, httpjson.ErrRetryable) || errors.Is(err, sse.ErrRetryable) || errors.Is(err, subprocess.ErrRetryable) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "40001" || pgErr.Code == "40P01"
}

func collectorTransactionalID(dataset, revision string, assignment coordination.Assignment) string {
	// Kafka limits transactional IDs to 255 bytes. Hashing inputs gives a
	// stable, non-secret, bounded ID for every logical source assignment.
	sum := sha256.Sum256([]byte(dataset + "\x00" + revision + "\x00" + assignment.Source + "\x00" + assignment.Partition))
	return "streamforge.collector." + hex.EncodeToString(sum[:16])
}

func sourcePartitionKey(observed event.Event, keyField string) string {
	var payload map[string]json.RawMessage
	if json.Unmarshal(observed.Data, &payload) == nil {
		var key string
		if json.Unmarshal(payload[keyField], &key) == nil && key != "" {
			return key
		}
	}
	return observed.ID
}

// stampCollectedEvent makes the raw topic self-describing. The built-in
// collector is the provider trust boundary, so downstream workers never need
// to infer which schema or mapping admitted a record.
func stampCollectedEvent(observed event.Event, contract event.Contract) event.Event {
	observed.Contract = contract
	return observed
}

func revisionSuffix(version string) string {
	digest := sha256.Sum256([]byte(version))
	return hex.EncodeToString(digest[:6])
}

func boundedDiagnostic(value string) string {
	if len(value) <= providerflow.MaxDiagnosticBytes {
		return value
	}
	end := providerflow.MaxDiagnosticBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end]
}

func generatedHolder(hostname, identifier string) string {
	suffix := "-" + identifier
	maxHost := coordination.MaxIdentifierBytes - len(suffix)
	if maxHost < 0 {
		return identifier[:coordination.MaxIdentifierBytes]
	}
	if len(hostname) > maxHost {
		hostname = hostname[:maxHost]
	}
	return hostname + suffix
}

func contentionDelay() time.Duration {
	return 4*time.Second + time.Duration(rand.Int64N(int64(2*time.Second)+1))
}
