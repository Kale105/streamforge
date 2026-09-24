// Package telemetry contains small, dependency-free telemetry contracts used
// at StreamForge's process and broker boundaries.
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	transportkafka "github.com/Kale105/streamforge/internal/transport/kafka"
)

const TraceparentHeader = "traceparent"

type traceKey struct{}

// Traceparent returns the W3C trace context carried by ctx, if any.
func Traceparent(ctx context.Context) string {
	value, _ := ctx.Value(traceKey{}).(string)
	return value
}

// WithTraceparent validates a W3C traceparent before putting it in ctx. A
// malformed upstream header is never propagated: callers can safely use
// EnsureTraceparent at a trust boundary to replace it with a fresh trace.
func WithTraceparent(ctx context.Context, value string) (context.Context, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if !ValidTraceparent(value) {
		return ctx, false
	}
	return context.WithValue(ctx, traceKey{}, value), true
}

// EnsureTraceparent preserves a valid parent or creates a new sampled W3C
// trace. It is the safe rule for malformed/missing broker headers.
func EnsureTraceparent(ctx context.Context) context.Context {
	if Traceparent(ctx) != "" {
		return ctx
	}
	traceID := randomHex(16)
	spanID := randomHex(8)
	return context.WithValue(ctx, traceKey{}, "00-"+traceID+"-"+spanID+"-01")
}

// FromHeaders extracts the only header StreamForge treats as trace context.
// Duplicate or malformed values are rejected and callers should replace them.
func FromHeaders(headers []transportkafka.Header) string {
	var value string
	for _, header := range headers {
		if !strings.EqualFold(header.Key, TraceparentHeader) {
			continue
		}
		if value != "" {
			return ""
		}
		value = string(header.Value)
	}
	if !ValidTraceparent(strings.ToLower(value)) {
		return ""
	}
	return strings.ToLower(value)
}

func Headers(ctx context.Context) []transportkafka.Header {
	if parent := Traceparent(ctx); parent != "" {
		return []transportkafka.Header{{Key: TraceparentHeader, Value: []byte(parent)}}
	}
	return nil
}

func ContextFromHeaders(ctx context.Context, headers []transportkafka.Header) context.Context {
	if next, ok := WithTraceparent(ctx, FromHeaders(headers)); ok {
		return next
	}
	return EnsureTraceparent(ctx)
}

// ValidTraceparent reports whether value is a canonical W3C traceparent.
// It deliberately requires lower-case hexadecimal and no surrounding
// whitespace so durable records have one stable representation.
func ValidTraceparent(value string) bool {
	if value != strings.TrimSpace(value) || value != strings.ToLower(value) {
		return false
	}
	parts := strings.Split(value, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return false
	}
	if parts[0] == "ff" || allZero(parts[1]) || allZero(parts[2]) {
		return false
	}
	for _, part := range parts {
		if _, err := hex.DecodeString(part); err != nil {
			return false
		}
	}
	return true
}

func allZero(value string) bool { return strings.Trim(value, "0") == "" }

func randomHex(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is exceptional. Deterministic non-zero fallback
		// still produces a syntactically valid context instead of dropping it.
		for i := range b {
			b[i] = byte(i + 1)
		}
	}
	return hex.EncodeToString(b)
}
