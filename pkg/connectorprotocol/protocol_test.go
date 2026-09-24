package connectorprotocol

import (
	"errors"
	"strings"
	"testing"
)

func TestParseRejectsOversizedTerminalErrorFields(t *testing.T) {
	for name, frame := range map[string]string{
		"code":    `{"kind":"error","code":"` + strings.Repeat("c", MaxErrorCodeBytes+1) + `","message":"failed","retryable":false}`,
		"message": `{"kind":"error","code":"failed","message":"` + strings.Repeat("m", MaxErrorMessageBytes+1) + `","retryable":false}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(frame))
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("error = %v, want protocol error", err)
			}
		})
	}
}

func TestCheckpointRequiresOrderedSequence(t *testing.T) {
	for _, frame := range []string{
		`{"kind":"checkpoint","token":"cursor"}`,
		`{"kind":"checkpoint","sequence":0,"token":"cursor"}`,
	} {
		if _, err := Parse([]byte(frame)); !errors.Is(err, ErrProtocol) {
			t.Fatalf("Parse(%s) error = %v, want protocol error", frame, err)
		}
	}
	parsed, err := Parse([]byte(`{"kind":"checkpoint","sequence":7,"token":"cursor"}`))
	if err != nil || parsed.Checkpoint == nil || parsed.Checkpoint.Sequence != 7 {
		t.Fatalf("valid checkpoint = %#v, %v", parsed, err)
	}
}
