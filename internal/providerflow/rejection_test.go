package providerflow

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRejectionRoundTripRetainsReplayContext(t *testing.T) {
	raw := raw().Event
	rejection := NewRejection("games", "schema-v2", time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC), "missing home_score", raw)
	body, err := MarshalRejection(rejection, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalRejection(body, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Dataset != "games" || decoded.DatasetVersion != "schema-v2" || decoded.Diagnostic != "missing home_score" || decoded.Event.ID != raw.ID {
		t.Fatalf("decoded rejection lost replay context: %#v", decoded)
	}
	if decoded.IdempotencyKey() != raw.Source+"\x00"+raw.ID {
		t.Fatalf("unexpected key %q", decoded.IdempotencyKey())
	}
}

func TestRejectionIsBounded(t *testing.T) {
	rejection := NewRejection("games", "v1", time.Now(), strings.Repeat("x", MaxDiagnosticBytes+1), raw().Event)
	if _, err := MarshalRejection(rejection, 1<<20); !errors.Is(err, ErrInvalidRejection) {
		t.Fatalf("error = %v, want invalid rejection", err)
	}
	rejection.Diagnostic = "bad payload"
	body, err := MarshalRejection(rejection, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalRejection(body, len(body)-1); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("error = %v, want message too large", err)
	}
}
