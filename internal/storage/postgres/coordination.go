package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Kale105/streamforge/internal/coordination"
	"github.com/jackc/pgx/v5"
)

var _ coordination.LeaseStore = (*Store)(nil)

// Acquire obtains an unheld or expired assignment. The database issues the
// fencing token and computes expiry, so collector clocks cannot affect safety.
func (s *Store) Acquire(ctx context.Context, assignment coordination.Assignment, holder string, duration time.Duration) (coordination.Lease, bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return coordination.Lease{}, false, err
	}
	if err := assignment.Validate(); err != nil {
		return coordination.Lease{}, false, err
	}
	if err := coordination.ValidateHolder(holder); err != nil {
		return coordination.Lease{}, false, err
	}
	if err := coordination.ValidateDuration(duration); err != nil {
		return coordination.Lease{}, false, err
	}

	lease, err := scanLease(s.pool.QueryRow(ctx, `
INSERT INTO collector_assignments (dataset, source, partition, holder, fencing_token, lease_expires_at)
VALUES ($1, $2, $3, $4, 1, now() + ($5::bigint * interval '1 microsecond'))
ON CONFLICT (dataset, source, partition) DO UPDATE
SET holder = EXCLUDED.holder,
    fencing_token = collector_assignments.fencing_token + 1,
    lease_expires_at = now() + ($5::bigint * interval '1 microsecond')
WHERE collector_assignments.lease_expires_at <= now()
RETURNING holder, fencing_token, lease_expires_at`, assignment.Dataset, assignment.Source, assignment.Partition, holder, duration.Microseconds()), assignment)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.Lease{}, false, nil
	}
	if err != nil {
		return coordination.Lease{}, false, fmt.Errorf("acquire collector lease: %w", err)
	}
	return lease, true, nil
}

// Renew extends a live lease only when its holder and fencing token still match.
func (s *Store) Renew(ctx context.Context, lease coordination.Lease, duration time.Duration) (coordination.Lease, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return coordination.Lease{}, err
	}
	if err := validateCoordinationLease(lease); err != nil {
		return coordination.Lease{}, err
	}
	if err := coordination.ValidateDuration(duration); err != nil {
		return coordination.Lease{}, err
	}

	updated, err := scanLease(s.pool.QueryRow(ctx, `
UPDATE collector_assignments
SET lease_expires_at = now() + ($6::bigint * interval '1 microsecond')
WHERE dataset=$1 AND source=$2 AND partition=$3 AND holder=$4
  AND fencing_token=$5 AND lease_expires_at > now()
RETURNING holder, fencing_token, lease_expires_at`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition, lease.Holder, int64(lease.Token), duration.Microseconds()), lease.Assignment)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.Lease{}, coordination.ErrLeaseLost
	}
	if err != nil {
		return coordination.Lease{}, fmt.Errorf("renew collector lease: %w", err)
	}
	return updated, nil
}

// Release ends a live lease. A stale lease cannot release another holder's work.
func (s *Store) Release(ctx context.Context, lease coordination.Lease) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := validateCoordinationLease(lease); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE collector_assignments
SET holder=NULL, lease_expires_at=now()
WHERE dataset=$1 AND source=$2 AND partition=$3 AND holder=$4
  AND fencing_token=$5 AND lease_expires_at > now()`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition, lease.Holder, int64(lease.Token))
	if err != nil {
		return fmt.Errorf("release collector lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return coordination.ErrLeaseLost
	}
	return nil
}

// Advance atomically stores source acknowledgement data and a strictly newer
// checkpoint. The assignment lock checks the live fencing token before either
// value can be written.
func (s *Store) Advance(ctx context.Context, lease coordination.Lease, progress coordination.Progress) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := validateCoordinationLease(lease); err != nil {
		return err
	}
	if err := progress.Validate(); err != nil {
		return err
	}
	if progress.Checkpoint.Sequence > math.MaxInt64 {
		return errors.New("checkpoint sequence exceeds storage range")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var token int64
	err = tx.QueryRow(ctx, `SELECT fencing_token FROM collector_assignments
WHERE dataset=$1 AND source=$2 AND partition=$3 AND holder=$4
  AND fencing_token=$5 AND lease_expires_at > now() FOR UPDATE`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition, lease.Holder, int64(lease.Token)).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("validate collector lease for checkpoint: %w", err)
	}

	var sequence int64
	var cursor, acknowledgement []byte
	err = tx.QueryRow(ctx, `SELECT sequence, cursor, acknowledgement FROM collector_checkpoints
WHERE dataset=$1 AND source=$2 AND partition=$3 FOR UPDATE`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition).Scan(&sequence, &cursor, &acknowledgement)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO collector_checkpoints (dataset, source, partition, sequence, cursor, acknowledgement)
VALUES ($1, $2, $3, $4, $5, $6)`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition, int64(progress.Checkpoint.Sequence), progress.Checkpoint.Cursor, progress.Acknowledgement)
	} else if err == nil {
		switch {
		case progress.Checkpoint.Sequence < uint64(sequence):
			return coordination.ErrCheckpointOrder
		case progress.Checkpoint.Sequence == uint64(sequence):
			if !bytes.Equal(progress.Checkpoint.Cursor, cursor) || !bytes.Equal(progress.Acknowledgement, acknowledgement) {
				return coordination.ErrCheckpointOrder
			}
			return tx.Commit(ctx)
		default:
			_, err = tx.Exec(ctx, `UPDATE collector_checkpoints SET sequence=$4, cursor=$5, acknowledgement=$6, updated_at=now()
WHERE dataset=$1 AND source=$2 AND partition=$3`, lease.Assignment.Dataset, lease.Assignment.Source, lease.Assignment.Partition, int64(progress.Checkpoint.Sequence), progress.Checkpoint.Cursor, progress.Acknowledgement)
		}
	}
	if err != nil {
		return fmt.Errorf("advance collector checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit collector checkpoint: %w", err)
	}
	return nil
}

// Checkpoint returns the latest transactionally persisted progress.
func (s *Store) Checkpoint(ctx context.Context, assignment coordination.Assignment) (coordination.Progress, bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return coordination.Progress{}, false, err
	}
	if err := assignment.Validate(); err != nil {
		return coordination.Progress{}, false, err
	}
	var sequence int64
	var cursor, acknowledgement []byte
	err := s.pool.QueryRow(ctx, `SELECT sequence, cursor, acknowledgement FROM collector_checkpoints WHERE dataset=$1 AND source=$2 AND partition=$3`, assignment.Dataset, assignment.Source, assignment.Partition).Scan(&sequence, &cursor, &acknowledgement)
	if errors.Is(err, pgx.ErrNoRows) {
		return coordination.Progress{}, false, nil
	}
	if err != nil {
		return coordination.Progress{}, false, fmt.Errorf("load collector checkpoint: %w", err)
	}
	return coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: uint64(sequence), Cursor: cursor}, Acknowledgement: acknowledgement}, true, nil
}

func validateCoordinationLease(lease coordination.Lease) error {
	if err := coordination.ValidateLease(lease); err != nil {
		return err
	}
	if lease.Token > math.MaxInt64 {
		return errors.New("fencing token exceeds storage range")
	}
	return nil
}

type leaseRow interface{ Scan(...any) error }

func scanLease(row leaseRow, assignment coordination.Assignment) (coordination.Lease, error) {
	var holder string
	var token int64
	var expiresAt time.Time
	if err := row.Scan(&holder, &token, &expiresAt); err != nil {
		return coordination.Lease{}, err
	}
	if token <= 0 {
		return coordination.Lease{}, errors.New("invalid fencing token returned by storage")
	}
	return coordination.Lease{Assignment: assignment, Holder: holder, Token: coordination.FencingToken(token), ExpiresAt: expiresAt}, nil
}
