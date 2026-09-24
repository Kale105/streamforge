// Package httpjson collects JSON documents from a single HTTP endpoint.
package httpjson

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

var (
	// ErrInvalidConfig identifies invalid collector configuration.
	ErrInvalidConfig = errors.New("invalid HTTP JSON collector configuration")
	// ErrInvalidResponse identifies a response that cannot be admitted as an event.
	ErrInvalidResponse = errors.New("invalid HTTP JSON response")
	// ErrResponseTooLarge identifies a body which exceeded MaxResponseBytes.
	ErrResponseTooLarge = errors.New("HTTP JSON response exceeds maximum size")
	// ErrRetryable identifies failures a caller may retry using its own bounded policy.
	ErrRetryable = errors.New("retryable HTTP JSON collection failure")
	// ErrPermanent identifies failures a caller should surface rather than automatically retry.
	ErrPermanent = errors.New("permanent HTTP JSON collection failure")
)

// Config describes one polling endpoint. Source and Type become event metadata.
type Config struct {
	URL              string
	Interval         time.Duration
	Source           string
	Type             string
	MaxResponseBytes int64
	RequestTimeout   time.Duration
}

// Collector polls one endpoint. It is intended for one Collect call at a time.
type Collector struct {
	config Config
	client *http.Client
	etag   string
}

// New validates config and constructs a collector using client for all requests.
func New(config Config, client *http.Client) (*Collector, error) {
	if strings.TrimSpace(config.URL) == "" || config.Interval <= 0 ||
		strings.TrimSpace(config.Source) == "" || strings.TrimSpace(config.Type) == "" ||
		config.MaxResponseBytes <= 0 || config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("%w: URL, positive interval, source, type, positive maximum response bytes, and positive request timeout are required", ErrInvalidConfig)
	}
	if client == nil {
		return nil, fmt.Errorf("%w: HTTP client is required", ErrInvalidConfig)
	}
	request, err := http.NewRequest(http.MethodGet, config.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: URL: %v", ErrInvalidConfig, err)
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
		return nil, fmt.Errorf("%w: URL must use HTTP or HTTPS", ErrInvalidConfig)
	}
	return &Collector{config: config, client: client}, nil
}

// Collect polls immediately, then once per interval, until context cancellation
// or a fetch/emit error. It never performs retries itself.
func (c *Collector) Collect(ctx context.Context, emit collector.EmitFunc) error {
	if emit == nil {
		return fmt.Errorf("%w: emit function is required", ErrInvalidConfig)
	}
	if err := c.poll(ctx, emit); err != nil {
		return err
	}

	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.poll(ctx, emit); err != nil {
				return err
			}
		}
	}
}

func (c *Collector) poll(ctx context.Context, emit collector.EmitFunc) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, c.config.URL, nil)
	if err != nil {
		return fmt.Errorf("%w: build request: %v", ErrPermanent, err)
	}
	if c.etag != "" {
		req.Header.Set("If-None-Match", c.etag)
	}

	response, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: request: %v", ErrRetryable, err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotModified {
		return nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		category := ErrPermanent
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			category = ErrRetryable
		}
		return fmt.Errorf("%w: unexpected status %d", category, response.StatusCode)
	}

	payload, err := readJSON(response.Body, c.config.MaxResponseBytes)
	if err != nil {
		return err
	}
	// Copy the payload into the event so no buffer owned by HTTP handling can mutate it.
	data := append(json.RawMessage(nil), payload...)
	e := event.Event{
		SpecVersion: event.SpecVersion,
		ID:          stableID(c.config.Source, data),
		Source:      c.config.Source,
		Type:        c.config.Type,
		Time:        time.Now().UTC(),
		Data:        data,
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("%w: constructed event: %v", ErrInvalidResponse, err)
	}
	if err := emit(ctx, e); err != nil {
		return fmt.Errorf("emit HTTP JSON event: %w", err)
	}
	c.etag = response.Header.Get("ETag")
	return nil
}

func readJSON(body io.Reader, maximum int64) (json.RawMessage, error) {
	bytes, err := io.ReadAll(io.LimitReader(body, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read body: %v", ErrRetryable, err)
	}
	if int64(len(bytes)) > maximum {
		return nil, ErrResponseTooLarge
	}
	if len(strings.TrimSpace(string(bytes))) == 0 {
		return nil, fmt.Errorf("%w: empty body", ErrInvalidResponse)
	}
	if !json.Valid(bytes) {
		return nil, fmt.Errorf("%w: malformed JSON", ErrInvalidResponse)
	}
	return json.RawMessage(bytes), nil
}

func stableID(source string, payload []byte) string {
	canonical := payload
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err == nil {
		if encoded, err := json.Marshal(value); err == nil {
			canonical = encoded
		}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(source))
	_, _ = hash.Write([]byte{'\n'})
	_, _ = hash.Write(canonical)
	return fmt.Sprintf("httpjson-%x", hash.Sum(nil))
}

var _ collector.Collector = (*Collector)(nil)
