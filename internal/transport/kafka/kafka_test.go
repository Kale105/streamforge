package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/transport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
func TestConfigValidation(t *testing.T) {
	l := transport.Limits{MaxMessageBytes: 1, SendTimeout: time.Second, FetchMaxBytes: 1, FetchMaxWait: time.Second, MaxInFlight: 1}
	if (ProducerConfig{Limits: l}).Validate() == nil {
		t.Fatal("expected brokers error")
	}
	if (ConsumerConfig{Brokers: []string{"x"}, Group: "g", Topic: "__bad", Limits: l}).Validate() == nil {
		t.Fatal("expected topic error")
	}
}

func TestTransactionalIDValidation(t *testing.T) {
	l := transport.Limits{MaxMessageBytes: 1, SendTimeout: time.Second, FetchMaxBytes: 1, FetchMaxWait: time.Second, MaxInFlight: 1}
	if (ProducerConfig{Brokers: []string{"x"}, Limits: l, TransactionalID: " \t"}).Validate() == nil {
		t.Fatal("expected transactional ID error")
	}
}

func TestTransactionAbortsAfterProduceFailure(t *testing.T) {
	calls := []kgo.TransactionEndTry{}
	p := &Producer{limits: transport.Limits{MaxMessageBytes: 10, SendTimeout: time.Second}, transactional: true,
		begin:   func() error { return nil },
		produce: func(context.Context, *kgo.Record) error { return errors.New("send failed") },
		end:     func(_ context.Context, how kgo.TransactionEndTry) error { calls = append(calls, how); return nil },
	}
	err := p.publishTransaction(context.Background(), &kgo.Record{})
	if err == nil || len(calls) != 1 || calls[0] != kgo.TryAbort {
		t.Fatalf("err=%v transaction ends=%v", err, calls)
	}
}

func TestProducerFencingIsTypedFatal(t *testing.T) {
	err := classifyPublish(context.Background(), nil, kerr.ProducerFenced)
	var fenced *ProducerFencedError
	if !errors.As(err, &fenced) || !errors.Is(err, ErrProducerFenced) || errors.Is(err, ErrRetryable) {
		t.Fatalf("unexpected classification: %v", err)
	}
}

func TestChildDeadlineIsRetryableButParentCancellationIsClean(t *testing.T) {
	if !errors.Is(classifyPublish(context.Background(), context.DeadlineExceeded, context.DeadlineExceeded), ErrRetryable) {
		t.Fatal("child deadline must be retryable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(classifyPublish(ctx, context.DeadlineExceeded, context.DeadlineExceeded), context.Canceled) {
		t.Fatal("parent cancellation must remain clean")
	}
}
func TestRetryClassification(t *testing.T) {
	var ne net.Error = timeoutError{}
	if !IsRetryable(ne) {
		t.Fatal("timeout must be retryable")
	}
	if IsRetryable(errors.New("bad request")) {
		t.Fatal("plain error must be fatal")
	}
}

func TestHandlerFailureDoesNotCommit(t *testing.T) {
	committed := false
	err := handleRecordAndCommit(context.Background(), func(context.Context, RawRecord) error { return errors.New("downstream unavailable") }, RawRecord{}, func(context.Context) error { committed = true; return nil }, time.Second)
	if err == nil || committed {
		t.Fatal("failed handler must not be committed")
	}
}

func TestHandlerSuccessCommitsOnce(t *testing.T) {
	commits := 0
	err := handleRecordAndCommit(context.Background(), func(context.Context, RawRecord) error { return nil }, RawRecord{}, func(context.Context) error { commits++; return nil }, time.Second)
	if err != nil || commits != 1 {
		t.Fatalf("err=%v commits=%d", err, commits)
	}
}

func TestRawRecordCopiesKafkaMetadata(t *testing.T) {
	timestamp := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	record := &kgo.Record{
		Topic: "raw.input", Partition: 4, Offset: 19, Timestamp: timestamp,
		Key: []byte("key"), Value: []byte(`{"malformed":`),
		Headers: []kgo.RecordHeader{{Key: "content-type", Value: []byte("application/json")}},
	}
	raw := rawRecord(record)
	if raw.Topic != record.Topic || raw.Partition != record.Partition || raw.Offset != record.Offset || !raw.Timestamp.Equal(timestamp) {
		t.Fatalf("metadata mismatch: %#v", raw)
	}
	if string(raw.Key) != "key" || string(raw.Value) != `{"malformed":` || len(raw.Headers) != 1 || raw.Headers[0].Key != "content-type" || string(raw.Headers[0].Value) != "application/json" {
		t.Fatalf("payload mismatch: %#v", raw)
	}
	record.Key[0] = 'X'
	record.Value[0] = 'X'
	record.Headers[0].Value[0] = 'X'
	if string(raw.Key) != "key" || string(raw.Value) != `{"malformed":` || string(raw.Headers[0].Value) != "application/json" {
		t.Fatal("raw record must copy Kafka-owned bytes")
	}
}

func TestRawRecordKeepsMalformedBytesForCaller(t *testing.T) {
	raw := rawRecord(&kgo.Record{Topic: "raw.input", Key: []byte("source"), Value: []byte("not json")})
	if string(raw.Value) != "not json" {
		t.Fatalf("raw bytes changed: %q", raw.Value)
	}
	if _, err := decode(raw.Topic, raw.Key, raw.Value); err == nil {
		t.Fatal("test fixture should fail the optional envelope decoder")
	}
}

func TestHeaderValidationAndCopies(t *testing.T) {
	headers := []Header{{Key: "trace-id", Value: []byte("abc")}}
	if err := validateHeaders(headers); err != nil {
		t.Fatalf("validate headers: %v", err)
	}
	kafka := kafkaHeaders(headers)
	headers[0].Value[0] = 'X'
	if string(kafka[0].Value) != "abc" {
		t.Fatal("producer headers must copy caller bytes")
	}
	tooMany := make([]Header, MaxHeaderCount+1)
	for i := range tooMany {
		tooMany[i] = Header{Key: "x"}
	}
	if !errors.Is(validateHeaders(tooMany), ErrHeadersTooLarge) {
		t.Fatal("expected header-count limit")
	}
	if !errors.Is(validateHeaders([]Header{{Key: " "}}), ErrInvalidHeader) {
		t.Fatal("expected invalid header key")
	}
	if !errors.Is(validateHeaders([]Header{{Key: "x", Value: make([]byte, MaxHeaderBytes)}}), ErrHeadersTooLarge) {
		t.Fatal("expected aggregate header-size limit")
	}
}

func TestKafkaSecurityOptionsAreSharedByProducerAndConsumer(t *testing.T) {
	security := SecurityConfig{
		TLS:  TLSConfig{Enabled: true, ServerName: "kafka.internal"},
		SASL: SASLConfig{Mechanism: SASLSCRAMSHA512, Username: "worker", Password: "not-in-errors"},
	}
	producer, err := security.clientOptions()
	if err != nil {
		t.Fatalf("producer security options: %v", err)
	}
	consumer, err := security.clientOptions()
	if err != nil {
		t.Fatalf("consumer security options: %v", err)
	}
	if producer.TLS == nil || consumer.TLS == nil || producer.TLS.MinVersion != tls.VersionTLS12 || consumer.TLS.MinVersion != tls.VersionTLS12 || producer.TLS.ServerName != consumer.TLS.ServerName {
		t.Fatalf("TLS options differ: producer=%#v consumer=%#v", producer.TLS, consumer.TLS)
	}
	if producer.SASL == nil || consumer.SASL == nil || producer.SASL.Name() != consumer.SASL.Name() || producer.SASL.Name() != SASLSCRAMSHA512 {
		t.Fatalf("SASL options differ: producer=%v consumer=%v", producer.SASL, consumer.SASL)
	}
	if len(producer.kgoOptions()) != len(consumer.kgoOptions()) {
		t.Fatal("producer and consumer must install the same security options")
	}
}

func TestKafkaTLSValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  TLSConfig
	}{
		{name: "disabled settings", cfg: TLSConfig{CAFile: "ca.pem"}},
		{name: "both CA sources", cfg: TLSConfig{Enabled: true, CAPEM: "pem", CAFile: "ca.pem"}},
		{name: "certificate without key", cfg: TLSConfig{Enabled: true, ClientCertFile: "client.pem"}},
		{name: "key without certificate", cfg: TLSConfig{Enabled: true, ClientKeyFile: "client.key"}},
		{name: "invalid CA", cfg: TLSConfig{Enabled: true, CAPEM: "not a certificate"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.cfg.config(); err == nil {
				t.Fatal("expected TLS validation error")
			}
		})
	}
	config, err := (TLSConfig{Enabled: true, ServerName: "broker.local"}).config()
	if err != nil || config == nil || config.MinVersion != tls.VersionTLS12 || config.InsecureSkipVerify {
		t.Fatalf("verified TLS config=%#v err=%v", config, err)
	}
}

func TestKafkaSASLMechanismsAndSecretRedaction(t *testing.T) {
	for _, mechanism := range []string{SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512} {
		t.Run(mechanism, func(t *testing.T) {
			actual, err := (SASLConfig{Mechanism: mechanism, Username: "user", Password: "secret-value"}).mechanism()
			if err != nil || actual == nil || actual.Name() != mechanism {
				t.Fatalf("mechanism=%v err=%v", actual, err)
			}
		})
	}
	for _, cfg := range []SASLConfig{
		{Username: "account-token", Password: "secret-value"},
		{Mechanism: SASLPlain, Username: "account-token"},
		{Mechanism: "unknown", Username: "account-token", Password: "secret-value"},
	} {
		_, err := cfg.mechanism()
		if err == nil {
			t.Fatal("expected SASL validation error")
		}
		if strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "account-token") {
			t.Fatalf("credential leaked in error: %v", err)
		}
	}
}

func TestKafkaSecurityDisabledPreservesDefault(t *testing.T) {
	options, err := (SecurityConfig{}).clientOptions()
	if err != nil || options.TLS != nil || options.SASL != nil || len(options.kgoOptions()) != 0 {
		t.Fatalf("options=%#v err=%v", options, err)
	}
}
