package subprocess

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/pkg/connectorprotocol"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_COLLECTOR_HELPER") != "1" {
		return
	}
	mode := os.Getenv("COLLECTOR_HELPER_MODE")
	if mode == "exit" {
		os.Exit(7)
	}
	if mode == "oversize" {
		fmt.Fprint(os.Stdout, string(make([]byte, connectorprotocol.MaxFrameBytes+1)), "\n")
		os.Exit(0)
	}
	if mode == "malformed" {
		fmt.Fprintln(os.Stdout, "not-json")
		os.Exit(0)
	}
	if mode == "stderr" {
		fmt.Fprint(os.Stderr, "too-much-stderr")
		select {}
	}
	checkpoints := mode == "checkpoint"
	fmt.Fprintf(os.Stdout, `{"kind":"hello","protocol":%q,"capabilities":{"checkpoints":%t}}`+"\n", connectorprotocol.Version, checkpoints)
	if mode == "cancel" {
		select {}
	}
	if mode == "terminal" {
		fmt.Fprintln(os.Stdout, `{"kind":"error","code":"upstream_down","message":"unavailable","retryable":true}`)
		os.Exit(0)
	}
	e := event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "urn:test", Type: "test.event", Time: time.Unix(1, 0).UTC(), Data: json.RawMessage(`{"ok":true}`)}
	b, _ := json.Marshal(struct {
		Kind     string      `json:"kind"`
		Sequence uint64      `json:"sequence"`
		Event    event.Event `json:"event"`
	}{"event", 1, e})
	fmt.Fprintln(os.Stdout, string(b))
	if mode == "failedemit" {
		select {}
	}
	line, err := bufio.NewReader(os.Stdin).ReadBytes('\n')
	if err != nil {
		os.Exit(8)
	}
	var ack connectorprotocol.Ack
	if json.Unmarshal(line, &ack) != nil || ack.Sequence != 1 || ack.Kind != "ack" {
		os.Exit(9)
	}
	os.Exit(0)
}

func helper(t *testing.T, mode string) *Runner {
	t.Helper()
	r, err := New(Config{Path: os.Args[0], Args: []string{"-test.run=TestHelperProcess", "--"}, AllowedEnv: []string{"GO_WANT_COLLECTOR_HELPER", "COLLECTOR_HELPER_MODE"}, Env: map[string]string{"GO_WANT_COLLECTOR_HELPER": "1", "COLLECTOR_HELPER_MODE": mode}, ShutdownTimeout: 20 * time.Millisecond, Checkpoint: func(context.Context, connectorprotocol.CheckpointFrame) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRunnerRejectsCheckpointCapabilityWithoutCallback(t *testing.T) {
	r, err := New(Config{Path: os.Args[0], Args: []string{"-test.run=TestHelperProcess", "--"}, AllowedEnv: []string{"GO_WANT_COLLECTOR_HELPER", "COLLECTOR_HELPER_MODE"}, Env: map[string]string{"GO_WANT_COLLECTOR_HELPER": "1", "COLLECTOR_HELPER_MODE": "checkpoint"}})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, connectorprotocol.ErrProtocol) {
		t.Fatalf("error = %v, want protocol error", err)
	}
}

func TestCollectFramesRejectsCheckpointWithoutAcknowledging(t *testing.T) {
	r := &Runner{}
	var acks bytes.Buffer
	err := r.collectFrames(context.Background(), strings.NewReader("{\"kind\":\"checkpoint\",\"sequence\":1,\"token\":\"cursor-1\"}\n"), &acks, func(context.Context, event.Event) error { return nil }, true)
	if !errors.Is(err, connectorprotocol.ErrProtocol) || !errors.Is(err, ErrPermanent) {
		t.Fatalf("error = %v, want permanent protocol error", err)
	}
	if acks.Len() != 0 {
		t.Fatalf("unexpected acknowledgement %q", acks.String())
	}
}

func TestCollectFramesRejectsNonIncreasingCheckpointSequences(t *testing.T) {
	r := &Runner{config: Config{Checkpoint: func(context.Context, connectorprotocol.CheckpointFrame) error { return nil }}}
	frames := "{\"kind\":\"checkpoint\",\"sequence\":1,\"token\":\"a\"}\n{\"kind\":\"checkpoint\",\"sequence\":1,\"token\":\"b\"}\n"
	err := r.collectFrames(context.Background(), strings.NewReader(frames), &bytes.Buffer{}, func(context.Context, event.Event) error { return nil }, true)
	if !errors.Is(err, connectorprotocol.ErrProtocol) {
		t.Fatalf("error = %v, want protocol error", err)
	}
}

func TestReadLineAcceptsMaximumBodyPlusNewline(t *testing.T) {
	body := strings.Repeat("x", 8)
	got, err := readLine(strings.NewReader(body+"\n"), len(body))
	if err != nil || string(got) != body {
		t.Fatalf("readLine = %q, %v", got, err)
	}
}

func TestCollectFramesRejectsNonIncreasingEventSequences(t *testing.T) {
	e := event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "urn:test", Type: "test.event", Time: time.Unix(1, 0).UTC(), Data: json.RawMessage(`{"ok":true}`)}
	line, err := json.Marshal(struct {
		Kind     string      `json:"kind"`
		Sequence uint64      `json:"sequence"`
		Event    event.Event `json:"event"`
	}{"event", 1, e})
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{}
	err = r.collectFrames(context.Background(), strings.NewReader(string(line)+"\n"+string(line)+"\n"), &bytes.Buffer{}, func(context.Context, event.Event) error { return nil }, false)
	if !errors.Is(err, connectorprotocol.ErrProtocol) {
		t.Fatalf("error = %v, want protocol error", err)
	}
}

func TestCollectFramesRejectsUnterminatedFrame(t *testing.T) {
	r := &Runner{}
	err := r.collectFrames(context.Background(), strings.NewReader(`{"kind":"error","code":"failed","message":"bad","retryable":false}`), &bytes.Buffer{}, func(context.Context, event.Event) error { return nil }, false)
	if !errors.Is(err, connectorprotocol.ErrMalformedFrame) {
		t.Fatalf("error = %v, want malformed frame", err)
	}
}

type checkpointWriter struct {
	bytes.Buffer
	persisted *bool
}

func (w *checkpointWriter) Write(p []byte) (int, error) {
	if !*w.persisted {
		return 0, errors.New("acknowledged before checkpoint persistence")
	}
	return w.Buffer.Write(p)
}

func TestRunnerValidEventAndAcknowledgement(t *testing.T) {
	var got int
	err := helper(t, "valid").Collect(context.Background(), func(context.Context, event.Event) error { got++; return nil })
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("error = %v, want retryable unexpected exit", err)
	}
	if got != 1 {
		t.Fatalf("emitted %d events, want 1", got)
	}
}

func TestRunnerDoesNotAcknowledgeFailedEmit(t *testing.T) {
	want := errors.New("sink unavailable")
	err := helper(t, "failedemit").Collect(context.Background(), func(context.Context, event.Event) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want emit error", err)
	}
}

func TestRunnerRejectsBadFrames(t *testing.T) {
	for _, mode := range []string{"malformed", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			err := helper(t, mode).Collect(context.Background(), func(context.Context, event.Event) error { return nil })
			if err == nil {
				t.Fatal("expected protocol error")
			}
		})
	}
}

func TestRunnerBoundsStderrAsPermanentFailure(t *testing.T) {
	r := helper(t, "stderr")
	r.config.MaxStderrBytes = 4
	err := r.Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, ErrStderrTooLarge) || !errors.Is(err, ErrPermanent) {
		t.Fatalf("error = %v, want permanent stderr bound failure", err)
	}
}

func TestRunnerCancellationAndExitError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := helper(t, "cancel").Collect(ctx, func(context.Context, event.Event) error { return nil })
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	err = helper(t, "exit").Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if err == nil {
		t.Fatal("expected exit error")
	}
}

func TestCollectFramesCancellationDuringBlockedAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	r := &Runner{config: Config{AckTimeout: time.Second}}
	writer := blockingWriter{started: started, release: release}
	e := event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "urn:test", Type: "test.event", Time: time.Unix(1, 0).UTC(), Data: json.RawMessage(`{"ok":true}`)}
	line, err := json.Marshal(struct {
		Kind     string      `json:"kind"`
		Sequence uint64      `json:"sequence"`
		Event    event.Event `json:"event"`
	}{"event", 1, e})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- r.collectFrames(ctx, strings.NewReader(string(line)+"\n"), writer, func(context.Context, event.Event) error { return nil }, false)
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	close(release)
}

func TestReadHandshakeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	reader := &blockingReader{started: started, release: release}
	done := make(chan error, 1)
	go func() { _, err := readHandshake(ctx, bufio.NewReader(reader), time.Second); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	close(release)
}

type blockingWriter struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (w blockingWriter) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(p), nil
}

type blockingReader struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r *blockingReader) Read(p []byte) (int, error) { close(r.started); <-r.release; return 0, io.EOF }

func TestRunnerStructuredTerminalError(t *testing.T) {
	err := helper(t, "terminal").Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	var remote RemoteError
	if !errors.As(err, &remote) || remote.Code != "upstream_down" || !remote.Retryable {
		t.Fatalf("error = %#v, want remote error", err)
	}
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("error = %v, want retryable classification", err)
	}
}

func TestRemoteErrorClassificationFollowsConnectorRetryableFlag(t *testing.T) {
	if !errors.Is(RemoteError{Retryable: true}, ErrRetryable) {
		t.Fatal("retryable remote error did not match ErrRetryable")
	}
	if !errors.Is(RemoteError{Retryable: false}, ErrPermanent) {
		t.Fatal("permanent remote error did not match ErrPermanent")
	}
	if errors.Is(RemoteError{Retryable: false}, ErrRetryable) {
		t.Fatal("permanent remote error matched ErrRetryable")
	}
}

func TestRunnerRejectsCheckpointCapability(t *testing.T) {
	err := helper(t, "checkpoint").Collect(context.Background(), func(context.Context, event.Event) error { return nil })
	if !errors.Is(err, ErrPermanent) || !errors.Is(err, connectorprotocol.ErrProtocol) {
		t.Fatalf("error = %v, want permanent protocol error", err)
	}
}

func TestEnvironmentIsCleanAndHostMetadataCannotBeOverridden(t *testing.T) {
	t.Setenv("PATH", "must-not-be-inherited")
	r, err := New(Config{
		Path: os.Args[0], AllowedEnv: []string{"API_TOKEN"}, Env: map[string]string{"API_TOKEN": "resolved-secret"},
		Dataset: "games", DatasetVersion: "v2", Source: "urn:host", EventType: "host.observed",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(r.environment(), "\n")
	for _, want := range []string{
		"API_TOKEN=resolved-secret", "STREAMFORGE_DATASET=games", "STREAMFORGE_DATASET_VERSION=v2",
		"STREAMFORGE_SOURCE=urn:host", "STREAMFORGE_EVENT_TYPE=host.observed", "STREAMFORGE_PROTOCOL=" + connectorprotocol.Version,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("environment %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "PATH=") {
		t.Fatalf("environment inherited PATH: %q", got)
	}
	if _, err := New(Config{Path: os.Args[0], AllowedEnv: []string{"STREAMFORGE_SOURCE"}, Env: map[string]string{"STREAMFORGE_SOURCE": "forged"}}); err == nil {
		t.Fatal("reserved host metadata override was accepted")
	}
	if _, err := New(Config{Path: "relative-collector"}); err == nil {
		t.Fatal("relative executable path was accepted")
	}
}

func TestCollectFramesRejectsSourceAndContractForgery(t *testing.T) {
	base := event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "urn:host", Type: "host.observed", Time: time.Unix(1, 0).UTC(), Data: json.RawMessage(`{"ok":true}`)}
	frame := func(e event.Event) string {
		b, err := json.Marshal(struct {
			Kind     string      `json:"kind"`
			Sequence uint64      `json:"sequence"`
			Event    event.Event `json:"event"`
		}{"event", 1, e})
		if err != nil {
			t.Fatal(err)
		}
		return string(b) + "\n"
	}
	for name, mutate := range map[string]func(*event.Event){
		"source": func(e *event.Event) { e.Source = "urn:forged" },
		"contract": func(e *event.Event) {
			e.Contract = event.Contract{RawSchema: event.SchemaIdentity{Revision: "v1", Digest: strings.Repeat("a", 64)}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := base
			mutate(&e)
			var acks bytes.Buffer
			r := &Runner{config: Config{Source: "urn:host", EventType: "host.observed"}}
			err := r.collectFrames(context.Background(), strings.NewReader(frame(e)), &acks, func(context.Context, event.Event) error { t.Fatal("forged event reached emit"); return nil }, false)
			if !errors.Is(err, ErrPermanent) || !errors.Is(err, connectorprotocol.ErrProtocol) {
				t.Fatalf("error = %v", err)
			}
			if acks.Len() != 0 {
				t.Fatalf("forged event was acknowledged: %q", acks.String())
			}
		})
	}
}

func TestCollectFramesStampsHostContractOnlyAfterSuccessfulAdmission(t *testing.T) {
	contract := event.Contract{
		RawSchema:        event.SchemaIdentity{Revision: "raw-v1", Digest: strings.Repeat("a", 64)},
		NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("b", 64)},
		Transform:        event.TransformIdentity{Revision: "transform-v1", Digest: strings.Repeat("c", 64)},
	}
	e := event.Event{SpecVersion: event.SpecVersion, ID: "id", Source: "urn:host", Type: "host.observed", Time: time.Unix(1, 0).UTC(), Data: json.RawMessage(`{"ok":true}`)}
	b, err := json.Marshal(struct {
		Kind     string      `json:"kind"`
		Sequence uint64      `json:"sequence"`
		Event    event.Event `json:"event"`
	}{"event", 1, e})
	if err != nil {
		t.Fatal(err)
	}
	var acks bytes.Buffer
	r := &Runner{config: Config{Source: "urn:host", EventType: "host.observed", Contract: contract}}
	err = r.collectFrames(context.Background(), strings.NewReader(string(b)+"\n"), &acks, func(_ context.Context, got event.Event) error {
		if got.Contract != contract {
			t.Fatalf("contract = %#v, want host contract", got.Contract)
		}
		return nil
	}, false)
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("error = %v, want clean EOF retryable", err)
	}
	if got := acks.String(); got != "{\"kind\":\"ack\",\"sequence\":1}\n" {
		t.Fatalf("acks = %q", got)
	}
}
