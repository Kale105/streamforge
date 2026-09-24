package telemetry

import (
	"context"
	"testing"

	transportkafka "github.com/Kale105/streamforge/internal/transport/kafka"
)

const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestTraceparentRoundTrip(t *testing.T) {
	if !ValidTraceparent(parent) {
		t.Fatal("canonical traceparent rejected")
	}
	if ValidTraceparent("00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01") {
		t.Fatal("non-canonical traceparent accepted")
	}
	ctx, ok := WithTraceparent(context.Background(), parent)
	if !ok {
		t.Fatal("valid traceparent rejected")
	}
	if got := FromHeaders(Headers(ctx)); got != parent {
		t.Fatalf("traceparent=%q", got)
	}
}

func TestMalformedOrDuplicateTraceparentIsReplaced(t *testing.T) {
	for _, headers := range [][]transportkafka.Header{
		{{Key: TraceparentHeader, Value: []byte("bad")}},
		{{Key: TraceparentHeader, Value: []byte(parent)}, {Key: TraceparentHeader, Value: []byte(parent)}},
	} {
		ctx := ContextFromHeaders(context.Background(), headers)
		if got := Traceparent(ctx); got == "" || got == parent {
			t.Fatalf("unsafe trace replacement %q", got)
		}
	}
}
