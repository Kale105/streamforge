// Package connectorprotocol defines the versioned, newline-delimited JSON
// protocol used by operator-installed collector programs.
//
// A collector writes exactly one hello frame first, followed by event and
// checkpoint frames. The host replies on stdin with acknowledgements. An event
// acknowledgement is written only after the host has durably accepted it. A
// collector may therefore replay an unacknowledged event after a restart;
// delivery is explicitly at-least-once. Cancellation closes the session and
// does not imply an acknowledgement for an in-flight event.
package connectorprotocol

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Kale105/streamforge/internal/event"
)

const (
	Version       = "streamforge.collector.v1"
	MaxFrameBytes = 20 << 20 // enough for the platform's 16 MiB event payload
	MaxTokenBytes = 4096
	// Terminal errors are operational metadata, not a payload transport. Keep
	// their fields bounded so a failed connector cannot consume unbounded host
	// memory or log storage.
	MaxErrorCodeBytes    = 256
	MaxErrorMessageBytes = 4096
)

var (
	ErrMalformedFrame = errors.New("malformed connector protocol frame")
	ErrProtocol       = errors.New("connector protocol violation")
)

// Capabilities is intentionally additive: unknown fields are ignored so a
// newer collector can describe features to an older host safely.
type Capabilities struct {
	Checkpoints bool `json:"checkpoints"`
}

type Hello struct {
	Kind         string       `json:"kind"`
	Protocol     string       `json:"protocol"`
	Capabilities Capabilities `json:"capabilities"`
}

type EventFrame struct {
	Kind     string      `json:"kind"`
	Sequence uint64      `json:"sequence"`
	Event    event.Event `json:"event"`
}

type CheckpointFrame struct {
	Kind     string `json:"kind"`
	Sequence uint64 `json:"sequence"`
	Token    string `json:"token"`
}

// Ack acknowledges either an event sequence or a checkpoint token. Exactly
// one of Sequence and Token is set.
type Ack struct {
	Kind     string `json:"kind"`
	Sequence uint64 `json:"sequence,omitempty"`
	Token    string `json:"token,omitempty"`
}

// TerminalError is a structured, terminal error reported by the collector.
// Message is for operators only and must never contain event payloads/secrets.
type TerminalError struct {
	Kind      string `json:"kind"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type Frame struct {
	Hello      *Hello
	Event      *EventFrame
	Checkpoint *CheckpointFrame
	Error      *TerminalError
}

func (a Ack) Validate() error {
	if a.Kind != "ack" || (a.Sequence == 0 && a.Token == "") || (a.Sequence != 0 && a.Token != "") || len(a.Token) > MaxTokenBytes {
		return fmt.Errorf("%w: invalid acknowledgement", ErrProtocol)
	}
	return nil
}

func (h Hello) Validate() error {
	if h.Kind != "hello" || h.Protocol != Version {
		return fmt.Errorf("%w: expected hello for %s", ErrProtocol, Version)
	}
	return nil
}

// Parse validates a complete NDJSON record. It rejects unknown frame kinds and
// rejects trailing fields only insofar as the JSON decoder rejects malformed
// JSON; additive fields are reserved for forward-compatible protocol changes.
func Parse(line []byte) (Frame, error) {
	if len(line) == 0 || len(line) > MaxFrameBytes {
		return Frame{}, fmt.Errorf("%w: invalid frame length", ErrMalformedFrame)
	}
	var envelope struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
	}
	switch envelope.Kind {
	case "hello":
		var h Hello
		if err := json.Unmarshal(line, &h); err != nil {
			return Frame{}, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		if err := h.Validate(); err != nil {
			return Frame{}, err
		}
		return Frame{Hello: &h}, nil
	case "event":
		var e EventFrame
		if err := json.Unmarshal(line, &e); err != nil {
			return Frame{}, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		if e.Kind != "event" || e.Sequence == 0 {
			return Frame{}, fmt.Errorf("%w: invalid event frame", ErrProtocol)
		}
		if err := e.Event.Validate(); err != nil {
			return Frame{}, fmt.Errorf("%w: invalid event: %v", ErrProtocol, err)
		}
		return Frame{Event: &e}, nil
	case "checkpoint":
		var c CheckpointFrame
		if err := json.Unmarshal(line, &c); err != nil {
			return Frame{}, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		if c.Kind != "checkpoint" || c.Sequence == 0 || c.Token == "" || len(c.Token) > MaxTokenBytes {
			return Frame{}, fmt.Errorf("%w: invalid checkpoint", ErrProtocol)
		}
		return Frame{Checkpoint: &c}, nil
	case "error":
		var e TerminalError
		if err := json.Unmarshal(line, &e); err != nil {
			return Frame{}, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		if e.Kind != "error" || e.Code == "" || e.Message == "" || len(e.Code) > MaxErrorCodeBytes || len(e.Message) > MaxErrorMessageBytes {
			return Frame{}, fmt.Errorf("%w: invalid terminal error", ErrProtocol)
		}
		return Frame{Error: &e}, nil
	default:
		return Frame{}, fmt.Errorf("%w: unknown frame kind", ErrProtocol)
	}
}

func EncodeAck(ack Ack) ([]byte, error) {
	if err := ack.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(ack)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxFrameBytes {
		return nil, fmt.Errorf("%w: acknowledgement too large", ErrProtocol)
	}
	return append(b, '\n'), nil
}
