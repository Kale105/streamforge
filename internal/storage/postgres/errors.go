package postgres

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrTransient marks a storage failure for which retrying the complete
// idempotent operation may succeed. Callers should still bound retries and
// preserve the wrapped cause for logs and diagnostics.
var ErrTransient = errors.New("transient postgres error")

// IsTransient reports whether err was classified at the PostgreSQL boundary as
// safe to retry. It deliberately does not guess from error text.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient)
}

// classifyStorageError separates a caller ending the request from a database
// operation that exceeded its own timeout. A canceled caller is terminal (and
// commonly expected during shutdown); an operation-owned deadline is a
// transient infrastructure failure. The original error remains in the chain.
func classifyStorageError(parent context.Context, err error) error {
	if err == nil || IsTransient(err) {
		return err
	}
	if parent != nil && parent.Err() != nil {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || pgconn.SafeToRetry(err) || isTransientNetError(err) || isTransientPGError(err) {
		return errors.Join(ErrTransient, err)
	}
	return err
}

func isTransientNetError(err error) bool {
	var networkErr net.Error
	if !errors.As(err, &networkErr) {
		return false
	}
	return networkErr.Timeout() || networkErr.Temporary()
}

func isTransientPGError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr == nil {
		return false
	}
	code := strings.ToUpper(pgErr.Code)
	if len(code) < 2 {
		return false
	}
	// These application, integrity, and SQL syntax classes are deliberately
	// terminal even if future classification rules grow more permissive.
	switch code[:2] {
	case "22", "23", "42":
		return false
	case "08", "53":
		return true
	}
	switch code {
	case "40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03": // cannot_connect_now
		return true
	default:
		return false
	}
}
