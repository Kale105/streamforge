// Package webhook admits authenticated JSON webhook requests as events.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
)

var (
	ErrInvalidConfig   = errors.New("invalid webhook collector configuration")
	ErrRequestTooLarge = errors.New("webhook request exceeds maximum size")
	ErrInvalidRequest  = errors.New("invalid webhook request")
	ErrRetryable       = errors.New("retryable webhook collection failure")
)

// Config limits every untrusted part of an incoming request.  When Secret is
// set, SignatureHeader must carry a hex HMAC-SHA256 of the raw request body.
// IdempotencyHeader, when present, is used as the event ID; otherwise an ID is
// derived deterministically from Source and the canonical JSON payload.
type Config struct {
	Address           string
	Source            string
	Type              string
	MaxBodyBytes      int64
	MaxHeaderBytes    int
	RequestTimeout    time.Duration
	Secret            []byte
	SignatureHeader   string
	IdempotencyHeader string
}

// Collector runs one HTTP server for the lifetime of Collect. It is not safe
// to call Collect concurrently.
type Collector struct {
	config Config
	mu     sync.RWMutex
	addr   string
}

func New(config Config) (*Collector, error) {
	if strings.TrimSpace(config.Address) == "" || strings.TrimSpace(config.Source) == "" || strings.TrimSpace(config.Type) == "" || config.MaxBodyBytes <= 0 || config.MaxBodyBytes > event.MaxDataBytes || config.MaxHeaderBytes <= 0 || config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("%w: address, source, type, positive bounded body/header sizes, and timeout are required", ErrInvalidConfig)
	}
	if len(config.Secret) > 0 && strings.TrimSpace(config.SignatureHeader) == "" {
		config.SignatureHeader = "X-Webhook-Signature"
	}
	if config.IdempotencyHeader == "" {
		config.IdempotencyHeader = "Idempotency-Key"
	}
	// Retain no caller-owned secret memory. Callers commonly reuse or zero the
	// configuration after construction.
	config.Secret = append([]byte(nil), config.Secret...)
	return &Collector{config: config}, nil
}

// Addr returns the bound address after Collect begins listening.
func (c *Collector) Addr() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.addr }

// Collect blocks while serving. Cancellation gracefully stops accepting new
// requests and waits only up to RequestTimeout for active requests.
func (c *Collector) Collect(ctx context.Context, emit collector.EmitFunc) error {
	if emit == nil {
		return fmt.Errorf("%w: emit function is required", ErrInvalidConfig)
	}
	ln, err := net.Listen("tcp", c.config.Address)
	if err != nil {
		return fmt.Errorf("%w: listen: %v", ErrRetryable, err)
	}
	c.mu.Lock()
	c.addr = ln.Addr().String()
	c.mu.Unlock()
	server := &http.Server{Handler: c.handler(emit), ReadHeaderTimeout: c.config.RequestTimeout, ReadTimeout: c.config.RequestTimeout, WriteTimeout: c.config.RequestTimeout, IdleTimeout: c.config.RequestTimeout, MaxHeaderBytes: c.config.MaxHeaderBytes}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ln) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("%w: serve: %v", ErrRetryable, err)
	case <-ctx.Done():
		stop, cancel := context.WithTimeout(context.Background(), c.config.RequestTimeout)
		err := server.Shutdown(stop)
		cancel()
		if err != nil {
			_ = server.Close()
		}
		<-done
		return ctx.Err()
	}
}

func (c *Collector) handler(emit collector.EmitFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		body, err := readBody(r.Body, c.config.MaxBodyBytes)
		if err != nil {
			if errors.Is(err, ErrRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "invalid request", http.StatusBadRequest)
			}
			return
		}
		if len(c.config.Secret) > 0 && !validSignature(c.config.Secret, r.Header.Get(c.config.SignatureHeader), body) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := r.Header.Get(c.config.IdempotencyHeader)
		if id == "" {
			id = stableID(c.config.Source, body)
		}
		if len(id) > event.MaxIDBytes {
			http.Error(w, "idempotency key too large", http.StatusBadRequest)
			return
		}
		reqCtx, cancel := context.WithTimeout(r.Context(), c.config.RequestTimeout)
		defer cancel()
		e := event.Event{SpecVersion: event.SpecVersion, ID: id, Source: c.config.Source, Type: c.config.Type, Time: time.Now().UTC(), Data: append(json.RawMessage(nil), body...)}
		if err := e.Validate(); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := emit(reqCtx, e); err != nil {
			if reqCtx.Err() != nil {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			} else {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			}
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func readBody(body io.Reader, maximum int64) (json.RawMessage, error) {
	b, err := io.ReadAll(io.LimitReader(body, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRetryable, err)
	}
	if int64(len(b)) > maximum {
		return nil, ErrRequestTooLarge
	}
	if len(strings.TrimSpace(string(b))) == 0 || !json.Valid(b) {
		return nil, ErrInvalidRequest
	}
	return json.RawMessage(b), nil
}
func validSignature(secret []byte, supplied string, body []byte) bool {
	supplied = strings.TrimPrefix(strings.TrimSpace(supplied), "sha256=")
	got, err := hex.DecodeString(supplied)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}
func stableID(source string, data []byte) string {
	var value any
	canonical := data
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.UseNumber()
	if d.Decode(&value) == nil {
		if b, err := json.Marshal(value); err == nil {
			canonical = b
		}
	}
	sum := sha256.Sum256(append(append([]byte(source), '\n'), canonical...))
	return fmt.Sprintf("webhook-%x", sum)
}

var _ collector.Collector = (*Collector)(nil)
