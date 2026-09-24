// Package subprocess runs trusted, operator-installed collector executables.
package subprocess

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Kale105/streamforge/internal/collector"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/pkg/connectorprotocol"
)

const (
	DefaultMaxStderrBytes   = 1 << 20
	defaultHandshakeTimeout = 10 * time.Second
	defaultAckTimeout       = 5 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
)

var (
	ErrInvalidConfig  = errors.New("invalid subprocess collector configuration")
	ErrFrameTooLarge  = errors.New("connector protocol frame exceeds maximum size")
	ErrStderrTooLarge = errors.New("collector stderr exceeds maximum size")
	// ErrRetryable identifies an interrupted connector session which may be
	// retried by a bounded collector retry policy.
	ErrRetryable = errors.New("retryable subprocess collector failure")
	// ErrPermanent identifies a connector protocol or configuration failure.
	ErrPermanent = errors.New("permanent subprocess collector failure")
)

var reservedEnvironment = map[string]struct{}{
	"STREAMFORGE_DATASET": {}, "STREAMFORGE_DATASET_VERSION": {},
	"STREAMFORGE_SOURCE": {}, "STREAMFORGE_EVENT_TYPE": {},
	"STREAMFORGE_PROTOCOL": {},
}

// Config uses an executable and argument vector directly; no shell is ever
// invoked. The child receives a clean environment: Env is already resolved by
// the configuration boundary and is the only operator-provided environment.
// The STREAMFORGE_* fields are host authority and cannot be replaced by Env.
type Config struct {
	Path           string
	Args           []string
	AllowedEnv     []string
	Env            map[string]string
	Dataset        string
	DatasetVersion string
	Source         string
	EventType      string
	Contract       event.Contract
	MaxStderrBytes int
	// HandshakeTimeout, AckTimeout, and ShutdownTimeout bound every operation
	// which depends on a plugin. Zero selects conservative defaults.
	HandshakeTimeout time.Duration
	AckTimeout       time.Duration
	ShutdownTimeout  time.Duration
	// Checkpoint persists a collector checkpoint before the runner acknowledges
	// it. It is required when the collector declares checkpoint support.
	Checkpoint func(context.Context, connectorprotocol.CheckpointFrame) error
}

type Runner struct{ config Config }

func New(config Config) (*Runner, error) {
	if strings.TrimSpace(config.Path) == "" || !filepath.IsAbs(config.Path) || strings.IndexByte(config.Path, 0) >= 0 {
		return nil, fmt.Errorf("%w: executable path must be absolute and non-empty", ErrInvalidConfig)
	}
	if config.MaxStderrBytes < 0 || config.HandshakeTimeout < 0 || config.AckTimeout < 0 || config.ShutdownTimeout < 0 {
		return nil, fmt.Errorf("%w: negative stderr bound", ErrInvalidConfig)
	}
	allowed := make(map[string]bool, len(config.AllowedEnv))
	for _, name := range config.AllowedEnv {
		if name == "" || strings.Contains(name, "=") || strings.IndexByte(name, 0) >= 0 {
			return nil, fmt.Errorf("%w: invalid environment name", ErrInvalidConfig)
		}
		if _, reserved := reservedEnvironment[name]; reserved {
			return nil, fmt.Errorf("%w: environment %q is host-reserved", ErrInvalidConfig, name)
		}
		allowed[name] = true
	}
	for name, value := range config.Env {
		if !allowed[name] || name == "" || strings.Contains(name, "=") || strings.IndexByte(name, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("%w: environment %q is not allow-listed", ErrInvalidConfig, name)
		}
		if _, reserved := reservedEnvironment[name]; reserved {
			return nil, fmt.Errorf("%w: environment %q is host-reserved", ErrInvalidConfig, name)
		}
	}
	if !config.Contract.Empty() && !config.Contract.Complete() {
		return nil, fmt.Errorf("%w: host contract must be complete", ErrInvalidConfig)
	}
	config.Args = append([]string(nil), config.Args...)
	config.AllowedEnv = append([]string(nil), config.AllowedEnv...)
	config.Env = clone(config.Env)
	return &Runner{config: config}, nil
}

func (r *Runner) Collect(ctx context.Context, emit collector.EmitFunc) error {
	if emit == nil {
		return fmt.Errorf("%w: emit is required", ErrInvalidConfig)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.Command(r.config.Path, r.config.Args...)
	cmd.Env = r.environment()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("open collector stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open collector stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("open collector stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start collector: %w", err)
	}
	// Closing the pipes is the cancellation primitive for blocked protocol reads
	// and writes. It also lets this method return when an untrusted connector
	// stops consuming stdin or producing stdout.
	var closeOnce sync.Once
	closePipes := func() { closeOnce.Do(func() { _ = stdin.Close(); _ = stdout.Close(); _ = stderr.Close() }) }
	done := make(chan struct{})
	go func() {
		select {
		case <-runCtx.Done():
			closePipes()
		case <-done:
		}
	}()
	defer close(done)
	limit := r.config.MaxStderrBytes
	if limit == 0 {
		limit = DefaultMaxStderrBytes
	}
	stderrDone := make(chan error, 1)
	go func() {
		err := drain(stderr, limit)
		if err != nil {
			cancel()
		}
		stderrDone <- err
	}()

	var result error
	stdoutReader := bufio.NewReaderSize(stdout, 32<<10)
	line, err := readHandshake(runCtx, stdoutReader, r.handshakeTimeout())
	if ctx.Err() != nil {
		result = ctx.Err()
	} else if err != nil {
		result = protocolReadError(err)
	} else {
		frame, parseErr := connectorprotocol.Parse(line)
		if parseErr != nil || frame.Hello == nil {
			result = permanentProtocol(fmt.Errorf("%w: startup handshake", connectorprotocol.ErrProtocol))
		} else if frame.Hello.Capabilities.Checkpoints {
			// The v1 protocol has no restore handshake. Advertising a checkpoint
			// would imply durability semantics the host cannot provide safely.
			result = permanentProtocol(fmt.Errorf("%w: checkpoint capability is unsupported without restore handshake", connectorprotocol.ErrProtocol))
		} else {
			result = r.collectFrames(runCtx, stdoutReader, stdin, emit, false)
		}
	}
	// Stopping the child on every non-successful protocol/session outcome avoids
	// leaving a trusted plugin running after the host has stopped reading it.
	if result != nil {
		cancel()
	}
	closePipes()
	waitErr := waitForProcess(runCtx, cmd, r.shutdownTimeout())
	stderrErr := waitFor(stderrDone, r.shutdownTimeout(), "stderr drain")
	if result != nil {
		return errors.Join(result, stderrErr)
	}
	if stderrErr != nil {
		return stderrErr
	}
	if waitErr != nil {
		return fmt.Errorf("collector exited: %w", waitErr)
	}
	// A connector that closes stdout while the host is still alive did not
	// complete a finite collection contract; treating it as success produces a
	// silent ingestion outage. It is retryable because no protocol violation was
	// observed and an event may be replayed after its missing acknowledgement.
	return UnexpectedExitError{}
}

func (r *Runner) handshakeTimeout() time.Duration {
	if r.config.HandshakeTimeout > 0 {
		return r.config.HandshakeTimeout
	}
	return defaultHandshakeTimeout
}

func (r *Runner) ackTimeout() time.Duration {
	if r.config.AckTimeout > 0 {
		return r.config.AckTimeout
	}
	return defaultAckTimeout
}

func (r *Runner) shutdownTimeout() time.Duration {
	if r.config.ShutdownTimeout > 0 {
		return r.config.ShutdownTimeout
	}
	return defaultShutdownTimeout
}

func readHandshake(ctx context.Context, r *bufio.Reader, timeout time.Duration) ([]byte, error) {
	type readResult struct {
		line []byte
		err  error
	}
	result := make(chan readResult, 1)
	go func() { line, err := readLine(r, connectorprotocol.MaxFrameBytes); result <- readResult{line, err} }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case got := <-result:
		return got.line, got.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("%w: startup handshake timed out", context.DeadlineExceeded)
	}
}

func waitForProcess(ctx context.Context, cmd *exec.Cmd, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return terminateAndWait(cmd, done, timeout)
	case <-timer.C:
		return terminateAndWait(cmd, done, timeout)
	}
}

func terminateAndWait(cmd *exec.Cmd, done <-chan error, timeout time.Duration) error {
	// Interrupt is best-effort (and unsupported for some Windows children).
	_ = cmd.Process.Signal(os.Interrupt)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		_ = cmd.Process.Kill()
		forced := time.NewTimer(timeout)
		defer forced.Stop()
		select {
		case err := <-done:
			return errors.Join(fmt.Errorf("collector graceful shutdown timed out"), err)
		case <-forced.C:
			return fmt.Errorf("collector wait timed out after forced termination")
		}
	}
}

func waitFor(done <-chan error, timeout time.Duration, operation string) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("%s timed out", operation)
	}
}

func (r *Runner) collectFrames(ctx context.Context, stdout io.Reader, stdin io.Writer, emit collector.EmitFunc, checkpoints bool) error {
	var lastSequence uint64
	var lastCheckpoint uint64
	reader, ok := stdout.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReaderSize(stdout, 32<<10)
	}
	for {
		line, err := readLine(reader, connectorprotocol.MaxFrameBytes)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, io.EOF) {
			return UnexpectedExitError{}
		}
		if err != nil {
			return protocolReadError(err)
		}
		frame, err := connectorprotocol.Parse(line)
		if err != nil {
			return permanentProtocol(err)
		}
		switch {
		case frame.Event != nil:
			if frame.Event.Sequence <= lastSequence {
				return permanentProtocol(fmt.Errorf("%w: event sequence must strictly increase", connectorprotocol.ErrProtocol))
			}
			if err := r.admit(frame.Event.Event, emit, ctx); err != nil {
				return fmt.Errorf("emit collector event: %w", err)
			}
			if err := r.writeAck(ctx, stdin, connectorprotocol.Ack{Kind: "ack", Sequence: frame.Event.Sequence}); err != nil {
				return retryableOperation("acknowledge collector event", err)
			}
			lastSequence = frame.Event.Sequence
		case frame.Checkpoint != nil:
			// A checkpoint acknowledgement would make the connector believe the
			// host can restore it after ownership failover. V1 cannot yet perform
			// that restore handshake, so reject the frame before any persistence or
			// acknowledgement side effect.
			_ = checkpoints
			_ = lastCheckpoint
			return permanentProtocol(fmt.Errorf("%w: checkpoints are unsupported without restore handshake", connectorprotocol.ErrProtocol))
		case frame.Error != nil:
			return RemoteError{Code: frame.Error.Code, Message: frame.Error.Message, Retryable: frame.Error.Retryable}
		default:
			return permanentProtocol(fmt.Errorf("%w: hello is only valid at startup", connectorprotocol.ErrProtocol))
		}
	}
}

func (r *Runner) writeAck(ctx context.Context, w io.Writer, ack connectorprotocol.Ack) error {
	done := make(chan error, 1)
	go func() { done <- writeAck(w, ack) }()
	timer := time.NewTimer(r.ackTimeout())
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w: acknowledgement timed out", context.DeadlineExceeded)
	}
}

type RemoteError struct {
	Code, Message string
	Retryable     bool
}

func (e RemoteError) Error() string {
	return fmt.Sprintf("collector terminal error (%s, retryable=%t): %s", e.Code, e.Retryable, e.Message)
}

// Unwrap gives callers a sentinel classification without requiring them to
// inspect the operator-provided message. Remote errors are retryable only when
// the connector explicitly says so.
func (e RemoteError) Unwrap() error {
	if e.Retryable {
		return ErrRetryable
	}
	return ErrPermanent
}

// UnexpectedExitError is a clean child termination before host cancellation.
// It is distinct from malformed protocol input and is safe to retry.
type UnexpectedExitError struct{}

func (UnexpectedExitError) Error() string { return "collector exited before host cancellation" }
func (UnexpectedExitError) Unwrap() error { return ErrRetryable }

func (r *Runner) admit(observed event.Event, emit collector.EmitFunc, ctx context.Context) error {
	if r.config.Source != "" && observed.Source != r.config.Source {
		return permanentProtocol(fmt.Errorf("%w: event source %q does not match assigned source", connectorprotocol.ErrProtocol, observed.Source))
	}
	if r.config.EventType != "" && observed.Type != r.config.EventType {
		return permanentProtocol(fmt.Errorf("%w: event type %q does not match assigned event type", connectorprotocol.ErrProtocol, observed.Type))
	}
	// Contract identity is host authority. Connector output must not carry an
	// arbitrary contract that could bypass the active schema/transform pins.
	if !observed.Contract.Empty() {
		return permanentProtocol(fmt.Errorf("%w: connector event must not declare a contract", connectorprotocol.ErrProtocol))
	}
	observed.Contract = r.config.Contract
	return emit(ctx, observed)
}

func writeAck(w io.Writer, ack connectorprotocol.Ack) error {
	b, err := connectorprotocol.EncodeAck(ack)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
func protocolReadError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrRetryable, err)
	}
	if errors.Is(err, ErrFrameTooLarge) {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	return permanentProtocol(fmt.Errorf("%w: %v", connectorprotocol.ErrMalformedFrame, err))
}

func permanentProtocol(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

func retryableOperation(operation string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s: %w", ErrRetryable, operation, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func readLine(r io.Reader, max int) ([]byte, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 32<<10)
	}
	var out []byte
	for {
		part, err := br.ReadSlice('\n')
		bodyBytes := len(part)
		if err == nil {
			bodyBytes--
		}
		if len(out)+bodyBytes > max {
			return nil, ErrFrameTooLarge
		}
		out = append(out, part...)
		if err == nil {
			return out[:len(out)-1], nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(out) == 0 {
				return nil, io.EOF
			}
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
}

func drain(r io.Reader, max int) error {
	n, err := io.Copy(io.Discard, io.LimitReader(r, int64(max)+1))
	if err != nil {
		return err
	}
	if n > int64(max) {
		return fmt.Errorf("%w: %w", ErrPermanent, ErrStderrTooLarge)
	}
	return nil
}
func (r *Runner) environment() []string {
	values := make(map[string]string, len(r.config.Env)+len(reservedEnvironment))
	for k, v := range r.config.Env {
		values[k] = v
	}
	values["STREAMFORGE_DATASET"] = r.config.Dataset
	values["STREAMFORGE_DATASET_VERSION"] = r.config.DatasetVersion
	values["STREAMFORGE_SOURCE"] = r.config.Source
	values["STREAMFORGE_EVENT_TYPE"] = r.config.EventType
	values["STREAMFORGE_PROTOCOL"] = connectorprotocol.Version
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, k := range keys {
		env = append(env, k+"="+values[k])
	}
	return env
}
func clone(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ collector.Collector = (*Runner)(nil)
