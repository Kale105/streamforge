package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyStorageError(t *testing.T) {
	base := errors.New("database failed")
	for name, test := range map[string]struct {
		err       error
		transient bool
	}{
		"safe to retry":         {err: retryableError{base}, transient: true},
		"network timeout":       {err: testNetError{err: base, timeout: true}, transient: true},
		"network temporary":     {err: testNetError{err: base, temporary: true}, transient: true},
		"connection exception":  {err: &pgconn.PgError{Code: "08006"}, transient: true},
		"serialization":         {err: &pgconn.PgError{Code: "40001"}, transient: true},
		"deadlock":              {err: &pgconn.PgError{Code: "40P01"}, transient: true},
		"resource exhausted":    {err: &pgconn.PgError{Code: "53300"}, transient: true},
		"lock unavailable":      {err: &pgconn.PgError{Code: "55P03"}, transient: true},
		"admin shutdown":        {err: &pgconn.PgError{Code: "57P01"}, transient: true},
		"crash shutdown":        {err: &pgconn.PgError{Code: "57P02"}, transient: true},
		"cannot connect now":    {err: &pgconn.PgError{Code: "57P03"}, transient: true},
		"data exception":        {err: &pgconn.PgError{Code: "22001"}},
		"constraint violation":  {err: &pgconn.PgError{Code: "23505"}},
		"syntax error":          {err: &pgconn.PgError{Code: "42601"}},
		"unclassified postgres": {err: &pgconn.PgError{Code: "XX000"}},
	} {
		t.Run(name, func(t *testing.T) {
			wrapped := fmt.Errorf("outer: %w", test.err)
			got := classifyStorageError(context.Background(), wrapped)
			if IsTransient(got) != test.transient {
				t.Fatalf("IsTransient(%v) = %v, want %v", got, IsTransient(got), test.transient)
			}
			if !errors.Is(got, test.err) {
				t.Fatalf("classified error lost causal error: %v", got)
			}
		})
	}
}

func TestClassifyStorageErrorContextOwnership(t *testing.T) {
	t.Run("operation deadline is transient", func(t *testing.T) {
		got := classifyStorageError(context.Background(), fmt.Errorf("operation: %w", context.DeadlineExceeded))
		if !IsTransient(got) || !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("operation deadline classification = %v", got)
		}
	})
	t.Run("caller cancellation is terminal", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		got := classifyStorageError(parent, context.Canceled)
		if IsTransient(got) || !errors.Is(got, context.Canceled) {
			t.Fatalf("caller cancellation classification = %v", got)
		}
	})
	t.Run("caller deadline is terminal", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		got := classifyStorageError(parent, context.DeadlineExceeded)
		if IsTransient(got) || !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("caller deadline classification = %v", got)
		}
	})
}

func TestProviderWriteErrorPreservesCauseAndClassification(t *testing.T) {
	cause := fmt.Errorf("commit socket failure: %w", testNetError{err: errors.New("reset"), timeout: true})
	got := providerWriteError(context.Background(), "commit provider sink write", cause)
	if !IsTransient(got) || !errors.Is(got, cause) {
		t.Fatalf("provider error = %v", got)
	}
	if strings.Contains(got.Error(), "socket failure") || strings.Contains(got.Error(), "reset") {
		t.Fatalf("provider error leaked driver diagnostics: %q", got.Error())
	}
	constraint := providerWriteError(context.Background(), "upsert normalized record", &pgconn.PgError{Code: "23505"})
	if IsTransient(constraint) {
		// Explicitly retain a terminal constraint violation as non-retryable.
		t.Fatalf("constraint violation incorrectly classified as transient: %v", constraint)
	}
}

type retryableError struct{ error }

func (retryableError) SafeToRetry() bool { return true }

type testNetError struct {
	err       error
	timeout   bool
	temporary bool
}

func (e testNetError) Error() string   { return e.err.Error() }
func (e testNetError) Unwrap() error   { return e.err }
func (e testNetError) Timeout() bool   { return e.timeout }
func (e testNetError) Temporary() bool { return e.temporary }
