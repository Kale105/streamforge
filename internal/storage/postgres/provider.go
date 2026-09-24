package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Kale105/streamforge/internal/providerflow"
)

var ErrProviderIdempotencyConflict = errors.New("source event identity was reused for different normalized content")

var _ providerflow.Writer = (*Store)(nil)

// Write atomically applies a provider-mode normalized message and records its
// source identity. Duplicate Kafka deliveries become no-ops; a conflicting
// payload under the same identity is rejected instead of silently discarded.
func (s *Store) Write(ctx context.Context, message providerflow.Message) error {
	parent := ctx
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return classifyStorageError(parent, err)
	}
	digest, err := providerflow.SemanticHash(message)
	if err != nil {
		return fmt.Errorf("validate provider message: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return providerWriteError(parent, "begin provider sink write", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `INSERT INTO provider_sink_receipts
(source, event_id, dataset, dataset_version, record_id, message_hash)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (source, event_id, dataset, dataset_version) DO NOTHING`,
		message.SourceEvent.Source, message.SourceEvent.ID, message.Record.Dataset,
		message.Record.DatasetVersion, message.Record.ID, digest[:])
	if err != nil {
		return providerWriteError(parent, "record provider sink receipt", err)
	}
	if tag.RowsAffected() == 0 {
		var existing []byte
		if err := tx.QueryRow(ctx, `SELECT message_hash FROM provider_sink_receipts
WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4`,
			message.SourceEvent.Source, message.SourceEvent.ID, message.Record.Dataset,
			message.Record.DatasetVersion).Scan(&existing); err != nil {
			return providerWriteError(parent, "load provider sink receipt", err)
		}
		if !bytes.Equal(existing, digest[:]) {
			return ErrProviderIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return providerWriteError(parent, "commit duplicate provider sink write", err)
		}
		return nil
	}
	if err := put(ctx, tx, message.Record); err != nil {
		return providerWriteError(parent, "upsert normalized record", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return providerWriteError(parent, "commit provider sink write", err)
	}
	return nil
}

func providerWriteError(parent context.Context, operation string, err error) error {
	// Do not surface PostgreSQL error text to API or worker callers: it can
	// contain a query fragment, database object, or driver diagnostic. Unwrap
	// still retains the complete cause for errors.Is/errors.As and structured
	// logging at a trusted boundary.
	return providerOperationError{operation: operation, cause: classifyStorageError(parent, err)}
}

type providerOperationError struct {
	operation string
	cause     error
}

func (e providerOperationError) Error() string { return e.operation }
func (e providerOperationError) Unwrap() error { return e.cause }
