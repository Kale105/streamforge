// Package coordination defines storage-neutral ownership and progress contracts
// for distributed collectors.
package coordination

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidAssignment = errors.New("dataset, source, and partition are required")
	ErrInvalidHolder     = errors.New("lease holder is required")
	ErrInvalidLease      = errors.New("lease is invalid")
	ErrLeaseLost         = errors.New("lease is no longer held")
	ErrInvalidCheckpoint = errors.New("checkpoint sequence and cursor are required")
	ErrCheckpointOrder   = errors.New("checkpoint cannot move backwards")
)

const (
	MaxIdentifierBytes = 256
	MaxCursorBytes     = 64 * 1024
	MinLeaseDuration   = time.Second
	MaxLeaseDuration   = time.Hour
)

// Assignment uniquely identifies a independently collectible source partition.
type Assignment struct {
	Dataset   string
	Source    string
	Partition string
}

// FencingToken is issued by storage and is intentionally not meaningful to a
// collector beyond equality. A newer successful acquisition always receives a
// larger token.
type FencingToken uint64

// Lease grants temporary exclusive access to an Assignment. ExpiresAt is set
// from authoritative storage time, never a collector's wall clock.
type Lease struct {
	Assignment Assignment
	Holder     string
	Token      FencingToken
	ExpiresAt  time.Time
}

// Checkpoint records progress. Cursor is opaque source data; Sequence is its
// explicit ordering/version and is the only value used to compare progress.
type Checkpoint struct {
	Sequence uint64
	Cursor   []byte
}

// Progress combines a checkpoint and the durable acknowledgement payload sent
// to (or recorded for) the source. Implementations must persist both together.
type Progress struct {
	Checkpoint      Checkpoint
	Acknowledgement []byte
}

// LeaseStore is the minimal persistence contract required by collector workers.
// A false acquired result is ordinary contention, not an error. ErrLeaseLost
// means a token is stale, released, or expired.
type LeaseStore interface {
	Acquire(context.Context, Assignment, string, time.Duration) (lease Lease, acquired bool, err error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	Advance(context.Context, Lease, Progress) error
	Checkpoint(context.Context, Assignment) (Progress, bool, error)
}

func (a Assignment) Validate() error {
	if a.Dataset == "" || a.Source == "" || a.Partition == "" {
		return ErrInvalidAssignment
	}
	if len(a.Dataset) > MaxIdentifierBytes || len(a.Source) > MaxIdentifierBytes || len(a.Partition) > MaxIdentifierBytes {
		return fmt.Errorf("assignment identifiers must not exceed %d bytes", MaxIdentifierBytes)
	}
	return nil
}

func ValidateHolder(holder string) error {
	if holder == "" {
		return ErrInvalidHolder
	}
	if len(holder) > MaxIdentifierBytes {
		return fmt.Errorf("lease holder must not exceed %d bytes", MaxIdentifierBytes)
	}
	return nil
}

func ValidateLease(lease Lease) error {
	if err := lease.Assignment.Validate(); err != nil {
		return err
	}
	if err := ValidateHolder(lease.Holder); err != nil {
		return err
	}
	if lease.Token == 0 {
		return ErrInvalidLease
	}
	return nil
}

func ValidateDuration(duration time.Duration) error {
	if duration < MinLeaseDuration || duration > MaxLeaseDuration {
		return fmt.Errorf("lease duration must be between %s and %s", MinLeaseDuration, MaxLeaseDuration)
	}
	return nil
}

func (p Progress) Validate() error {
	if p.Checkpoint.Sequence == 0 || len(p.Checkpoint.Cursor) == 0 {
		return ErrInvalidCheckpoint
	}
	if len(p.Checkpoint.Cursor) > MaxCursorBytes || len(p.Acknowledgement) > MaxCursorBytes {
		return fmt.Errorf("checkpoint cursor and acknowledgement must not exceed %d bytes", MaxCursorBytes)
	}
	return nil
}
