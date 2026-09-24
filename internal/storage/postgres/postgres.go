// Package postgres provides the PostgreSQL implementation of StreamForge storage.
package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/replay"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrEmptyDataset        = errors.New("dataset is required")
	ErrEmptyRecord         = errors.New("record id is required")
	ErrInvalidRecord       = errors.New("record data must be valid JSON")
	ErrReplayNotFailed     = errors.New("materialization is not failed")
	ErrReplayInProgress    = errors.New("materialization replay is already requested")
	ErrReplayRequestFailed = errors.New("replay request already finished with failure; use a new request id")
	// ErrMigrationHistoryMismatch means the persisted migration ledger does not
	// describe the exact embedded history this binary would execute. Continuing
	// in this state could silently run a renamed or modified migration.
	ErrMigrationHistoryMismatch = errors.New("migration history does not match embedded migrations")
	ErrMigrationHistoryGap      = errors.New("migration history has a gap")
	ErrMigrationLedgerSchema    = errors.New("migration ledger schema is incompatible")
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store is a concurrency-safe PostgreSQL-backed record store.
type Store struct {
	pool             *pgxpool.Pool
	operationTimeout time.Duration
	migrationTimeout time.Duration
}

var _ replay.Store = (*Store)(nil)

const (
	defaultOperationTimeout = 10 * time.Second
	defaultMigrationTimeout = 60 * time.Second
	// Every StreamForge process uses the same transaction-scoped advisory lock
	// before inspecting migration state. This makes concurrent role startup safe.
	migrationAdvisoryLockID int64 = 0x5354524D464F5247
)

// Config bounds database work even when a caller supplies context.Background.
// Zero values select safe defaults; positive values may be tuned for slow hosts.
type Config struct {
	OperationTimeout time.Duration
	MigrationTimeout time.Duration
}

// New constructs a connection pool. Call Migrate before accepting traffic.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	return NewWithConfig(ctx, databaseURL, Config{})
}

// NewWithConfig constructs a pool with bounded connection establishment.
func NewWithConfig(ctx context.Context, databaseURL string, config Config) (*Store, error) {
	if databaseURL == "" {
		return nil, errors.New("database URL is required")
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	config = normalizedConfig(config)
	opCtx, cancel := withTimeout(ctx, config.operationTimeout())
	defer cancel()
	pool, err := pgxpool.New(opCtx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	return &Store{pool: pool, operationTimeout: config.operationTimeout(), migrationTimeout: config.migrationTimeout()}, nil
}

// Ready verifies that a connection can be acquired. Its duration is bounded by ctx.
func (s *Store) Ready(ctx context.Context) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

// Close releases all pool resources. It is safe to call more than once.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Migrate applies embedded migrations exactly once, in version order.
func (s *Store) Migrate(ctx context.Context) error {
	ctx, cancel := s.migrationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	entries, err := migrationFiles()
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if err := ensureMigrationLedger(ctx, tx); err != nil {
		return err
	}
	if err := verifyMigrationLedger(ctx, tx, entries); err != nil {
		return err
	}
	for _, migration := range entries {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM streamforge_schema_migrations WHERE version = $1)`, migration.version).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", migration.name, err)
		}
		if exists {
			continue
		}
		if _, err := tx.Exec(ctx, migration.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO streamforge_schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`, migration.version, migration.name, migration.checksum); err != nil {
			return fmt.Errorf("record migration %s: %w", migration.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

// Admit stores a raw event once. It returns true only for the first admission.
func (s *Store) Admit(ctx context.Context, e event.Event) (bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return false, err
	}
	if err := e.Validate(); err != nil {
		return false, fmt.Errorf("validate event: %w", err)
	}
	return admit(ctx, s.pool, e)
}

// List returns records for exactly one dataset revision, sorted by
// (updated_at, id), using an exclusive keyset cursor.
func (s *Store) List(ctx context.Context, dataset, version string, after *recordstore.Cursor, limit int) (recordstore.Page, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return recordstore.Page{}, err
	}
	if dataset == "" {
		return recordstore.Page{}, ErrEmptyDataset
	}
	if version == "" {
		return recordstore.Page{}, errors.New("dataset version is required")
	}
	if limit <= 0 {
		return recordstore.Page{}, errors.New("list limit must be positive")
	}
	var timestamp *time.Time
	var id *string
	if after != nil {
		if after.UpdatedAt.IsZero() || after.ID == "" {
			return recordstore.Page{}, errors.New("cursor requires updated_at and id")
		}
		timestamp, id = &after.UpdatedAt, &after.ID
	}
	rows, err := s.pool.Query(ctx, `
SELECT dataset, dataset_version, id, data, updated_at
FROM normalized_records
WHERE dataset = $1
  AND dataset_version = $2
  AND ($3::timestamptz IS NULL OR (updated_at, id) > ($3, $4))
ORDER BY updated_at ASC, id ASC
LIMIT $5`, dataset, version, timestamp, id, limit+1)
	if err != nil {
		return recordstore.Page{}, fmt.Errorf("list records: %w", err)
	}
	defer rows.Close()
	page := recordstore.Page{Records: make([]recordstore.Record, 0, limit)}
	for rows.Next() {
		var r recordstore.Record
		if err := rows.Scan(&r.Dataset, &r.DatasetVersion, &r.ID, &r.Data, &r.UpdatedAt); err != nil {
			return recordstore.Page{}, fmt.Errorf("scan record: %w", err)
		}
		if len(page.Records) == limit {
			last := page.Records[len(page.Records)-1]
			page.Next = &recordstore.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
			break
		}
		page.Records = append(page.Records, r)
	}
	if err := rows.Err(); err != nil {
		return recordstore.Page{}, fmt.Errorf("iterate records: %w", err)
	}
	return page, nil
}

// PrepareMaterialization creates (or reopens) the ledger entry for a dataset
// revision. A succeeded or failed entry is immutable; failed work can only be
// reopened by an explicit replay request.
// Personal mode has one materializer owner, so reopening pending work is the
// process-restart recovery rule. Provider mode will replace this with leases.
func (s *Store) PrepareMaterialization(ctx context.Context, e event.Event, dataset, version string) (bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return false, err
	}
	if err := e.Validate(); err != nil {
		return false, fmt.Errorf("validate event: %w", err)
	}
	if err := validateMaterializationRevision(dataset, version); err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin materialization preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
INSERT INTO materialization_ledger (source, event_id, dataset, dataset_version, state, attempts, started_at, updated_at)
VALUES ($1, $2, $3, $4, 'pending', 1, now(), now())
ON CONFLICT (source, event_id, dataset, dataset_version) DO UPDATE
SET state = 'pending', attempts = materialization_ledger.attempts + 1,
    diagnostic_error = NULL, started_at = now(), updated_at = now(), succeeded_at = NULL
	WHERE materialization_ledger.state IN ('pending', 'replay_requested')`, e.Source, e.ID, dataset, version)
	if err != nil {
		return false, fmt.Errorf("prepare materialization: %w", err)
	}
	if tag.RowsAffected() == 1 {
		if _, err := tx.Exec(ctx, `UPDATE materialization_replay_attempts
SET state = 'running', started_at = now()
WHERE source = $1 AND event_id = $2 AND dataset = $3 AND dataset_version = $4 AND state = 'requested'`, e.Source, e.ID, dataset, version); err != nil {
			return false, fmt.Errorf("start replay attempt: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit materialization preparation: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecoverableEvents returns raw events which have not yet been materialized for
// a dataset revision.  It intentionally excludes failed entries: a failed
// deterministic normalization must only be retried under a new revision (or by
// an explicit redelivery), rather than repeatedly during startup recovery.
//
// Pending entries are returned so a single personal-mode materializer can
// reopen them through PrepareMaterialization after a process restart.
func (s *Store) RecoverableEvents(ctx context.Context, dataset, version, source string, limit int) ([]event.Event, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return nil, err
	}
	if err := validateRecoveryRequest(dataset, version, source, limit); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
SELECT r.id, r.source, r.type, r.event_time, r.data
FROM raw_events AS r
LEFT JOIN materialization_ledger AS l
  ON l.source = r.source
 AND l.event_id = r.id
 AND l.dataset = $1
 AND l.dataset_version = $2
WHERE r.source = $3
  AND (l.source IS NULL OR l.state = 'pending')
ORDER BY r.received_at ASC, r.source ASC, r.id ASC
LIMIT $4`, dataset, version, source, limit)
	if err != nil {
		return nil, fmt.Errorf("query recoverable events: %w", err)
	}
	defer rows.Close()

	events := make([]event.Event, 0, limit)
	for rows.Next() {
		e := event.Event{SpecVersion: event.SpecVersion}
		if err := rows.Scan(&e.ID, &e.Source, &e.Type, &e.Time, &e.Data); err != nil {
			return nil, fmt.Errorf("scan recoverable event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recoverable events: %w", err)
	}
	return events, nil
}

// ListFailedMaterializations returns failed ledger entries (the durable DLQ)
// in stable keyset order. It never returns pending or replay-requested work.
func (s *Store) ListFailedMaterializations(ctx context.Context, request replay.ListRequest) (replay.Page, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return replay.Page{}, err
	}
	if err := validateReplayListRequest(request); err != nil {
		return replay.Page{}, err
	}
	var cursorUpdatedAt *time.Time
	var cursorSource, cursorEventID, cursorDataset, cursorVersion *string
	if request.After != nil {
		cursorUpdatedAt = &request.After.UpdatedAt
		cursorSource, cursorEventID = &request.After.Source, &request.After.EventID
		cursorDataset, cursorVersion = &request.After.Dataset, &request.After.DatasetVersion
	}
	rows, err := s.pool.Query(ctx, `
SELECT source, event_id, dataset, dataset_version, attempts, diagnostic_error, failed_at, updated_at
FROM materialization_ledger
WHERE state = 'failed'
  AND ($1::timestamptz IS NULL OR (updated_at, source, event_id, dataset, dataset_version) > ($1, $2, $3, $4, $5))
ORDER BY updated_at ASC, source ASC, event_id ASC, dataset ASC, dataset_version ASC
LIMIT $6`, cursorUpdatedAt, cursorSource, cursorEventID, cursorDataset, cursorVersion, request.Limit+1)
	if err != nil {
		return replay.Page{}, fmt.Errorf("list failed materializations: %w", err)
	}
	defer rows.Close()
	page := replay.Page{Items: make([]replay.FailedMaterialization, 0, request.Limit)}
	for rows.Next() {
		var item replay.FailedMaterialization
		if err := rows.Scan(&item.Source, &item.EventID, &item.Dataset, &item.DatasetVersion, &item.Attempts, &item.Diagnostic, &item.FailedAt, &item.UpdatedAt); err != nil {
			return replay.Page{}, fmt.Errorf("scan failed materialization: %w", err)
		}
		if len(page.Items) == request.Limit {
			last := page.Items[len(page.Items)-1]
			page.Next = &replay.Cursor{UpdatedAt: last.UpdatedAt, Source: last.Source, EventID: last.EventID, Dataset: last.Dataset, DatasetVersion: last.DatasetVersion}
			break
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return replay.Page{}, fmt.Errorf("iterate failed materializations: %w", err)
	}
	return page, nil
}

// RequestMaterializationReplay records an explicit, idempotent administrative
// request and returns the original raw event. It only transitions failed work
// to replay_requested; callers must explicitly submit Result.Event for work.
func (s *Store) RequestMaterializationReplay(ctx context.Context, request replay.Request) (replay.Result, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return replay.Result{}, err
	}
	if err := validateReplayRequest(request); err != nil {
		return replay.Result{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return replay.Result{}, fmt.Errorf("begin replay request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	var originalDiagnostic string
	var originalFailedAt time.Time
	var e event.Event
	e.SpecVersion = event.SpecVersion
	err = tx.QueryRow(ctx, `
SELECT l.state, COALESCE(l.diagnostic_error, ''), COALESCE(l.failed_at, l.updated_at),
       r.id, r.source, r.type, r.event_time, r.data
FROM materialization_ledger AS l
JOIN raw_events AS r ON r.source = l.source AND r.id = l.event_id
WHERE l.source = $1 AND l.event_id = $2 AND l.dataset = $3 AND l.dataset_version = $4
FOR UPDATE`, request.Source, request.EventID, request.Dataset, request.DatasetVersion).Scan(
		&state, &originalDiagnostic, &originalFailedAt, &e.ID, &e.Source, &e.Type, &e.Time, &e.Data,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return replay.Result{}, ErrReplayNotFailed
		}
		return replay.Result{}, fmt.Errorf("load replay target: %w", err)
	}
	var existingState string
	err = tx.QueryRow(ctx, `SELECT state FROM materialization_replay_attempts
WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4 AND request_id=$5`, request.Source, request.EventID, request.Dataset, request.DatasetVersion, request.RequestID).Scan(&existingState)
	if err == nil {
		if existingState == "failed" {
			return replay.Result{}, ErrReplayRequestFailed
		}
		if err := tx.Commit(ctx); err != nil {
			return replay.Result{}, fmt.Errorf("commit duplicate replay request: %w", err)
		}
		return replay.Result{Event: e, Accepted: false}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return replay.Result{}, fmt.Errorf("check replay idempotency: %w", err)
	}
	if state != "failed" {
		if state == "replay_requested" {
			return replay.Result{}, ErrReplayInProgress
		}
		return replay.Result{}, ErrReplayNotFailed
	}
	if _, err := tx.Exec(ctx, `INSERT INTO materialization_replay_attempts
(source, event_id, dataset, dataset_version, request_id, actor, reason,
 original_diagnostic_error, original_failed_at, state)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'requested')`,
		request.Source, request.EventID, request.Dataset, request.DatasetVersion,
		request.RequestID, request.Actor, request.Reason, originalDiagnostic, originalFailedAt); err != nil {
		return replay.Result{}, fmt.Errorf("record replay request: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE materialization_ledger
SET state='replay_requested', replay_requested_at=now(), updated_at=now()
WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4 AND state='failed'`, request.Source, request.EventID, request.Dataset, request.DatasetVersion); err != nil {
		return replay.Result{}, fmt.Errorf("mark replay requested: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return replay.Result{}, fmt.Errorf("commit replay request: %w", err)
	}
	return replay.Result{Event: e, Accepted: true}, nil
}

// FailMaterialization retains a bounded diagnostic while leaving raw input
// intact for replay. Only a pending entry may become failed.
func (s *Store) FailMaterialization(ctx context.Context, e event.Event, dataset, version string, cause error) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if cause == nil {
		return errors.New("materialization failure cause is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin materialization failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `UPDATE materialization_ledger
SET state = 'failed', diagnostic_error = left($5, 1024), failed_at = now(), updated_at = now()
WHERE source = $1 AND event_id = $2 AND dataset = $3 AND dataset_version = $4 AND state = 'pending'`, e.Source, e.ID, dataset, version, cause.Error())
	if err != nil {
		return fmt.Errorf("record materialization failure: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE materialization_replay_attempts
SET state = 'failed', finished_at = now(), diagnostic_error = left($5, 1024)
WHERE source = $1 AND event_id = $2 AND dataset = $3 AND dataset_version = $4
  AND state IN ('requested', 'running')`, e.Source, e.ID, dataset, version, cause.Error())
	if err != nil {
		return fmt.Errorf("mark replay attempt failed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit materialization failure: %w", err)
	}
	return nil
}

// CompleteMaterialization atomically writes the record and marks its ledger
// entry succeeded. Record conflict resolution is last-write-wins by updated_at;
// equal timestamps keep the existing value, preventing ambiguous observations
// from overwriting one another.
func (s *Store) CompleteMaterialization(ctx context.Context, e event.Event, r recordstore.Record, dataset, version string) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	if err := validateRecord(r); err != nil {
		return err
	}
	if err := validateMaterializationRevision(dataset, version); err != nil {
		return err
	}
	if r.Dataset != dataset {
		return errors.New("materialization dataset does not match record dataset")
	}
	if r.DatasetVersion != version {
		return errors.New("materialization version does not match record version")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin materialization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := put(ctx, tx, r); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE materialization_ledger
SET state = 'succeeded', diagnostic_error = NULL, succeeded_at = now(), updated_at = now()
WHERE source = $1 AND event_id = $2 AND dataset = $3 AND dataset_version = $4 AND state = 'pending'`, e.Source, e.ID, dataset, version)
	if err != nil {
		return fmt.Errorf("mark materialization succeeded: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("materialization is not pending")
	}
	if _, err := tx.Exec(ctx, `UPDATE materialization_replay_attempts
SET state = 'succeeded', finished_at = now(), diagnostic_error = NULL
WHERE source = $1 AND event_id = $2 AND dataset = $3 AND dataset_version = $4
  AND state IN ('requested', 'running')`, e.Source, e.ID, dataset, version); err != nil {
		return fmt.Errorf("mark replay attempt succeeded: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit materialization: %w", err)
	}
	return nil
}

// Both Pool and Tx satisfy this internal execution contract.
type admissionExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func admit(ctx context.Context, db admissionExecer, e event.Event) (bool, error) {
	tag, err := db.Exec(ctx, `INSERT INTO raw_events (id, source, type, event_time, data) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (source, id) DO NOTHING`, e.ID, e.Source, e.Type, e.Time, e.Data)
	if err != nil {
		return false, fmt.Errorf("admit raw event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func put(ctx context.Context, db admissionExecer, r recordstore.Record) error {
	_, err := db.Exec(ctx, `
INSERT INTO normalized_records (dataset, dataset_version, id, data, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (dataset, dataset_version, id) DO UPDATE
SET data = EXCLUDED.data,
    updated_at = EXCLUDED.updated_at
WHERE normalized_records.updated_at < EXCLUDED.updated_at`, r.Dataset, r.DatasetVersion, r.ID, r.Data, r.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert normalized record: %w", err)
	}
	return nil
}

func validateRecord(r recordstore.Record) error {
	if r.Dataset == "" {
		return ErrEmptyDataset
	}
	if len(r.Dataset) > recordstore.MaxDatasetBytes {
		return errors.New("record dataset exceeds maximum size")
	}
	if r.DatasetVersion == "" {
		return errors.New("record dataset_version is required")
	}
	if len(r.DatasetVersion) > recordstore.MaxDatasetVersionBytes {
		return errors.New("record dataset_version exceeds maximum size")
	}
	if r.ID == "" {
		return ErrEmptyRecord
	}
	if len(r.ID) > recordstore.MaxRecordIDBytes {
		return errors.New("record id exceeds maximum size")
	}
	if r.UpdatedAt.IsZero() {
		return errors.New("record updated_at is required")
	}
	if !json.Valid(r.Data) {
		return ErrInvalidRecord
	}
	if len(r.Data) > recordstore.MaxRecordDataBytes {
		return errors.New("record data exceeds maximum size")
	}
	return nil
}

func validateMaterializationRevision(dataset, version string) error {
	if dataset == "" || version == "" {
		return errors.New("materialization dataset and version are required")
	}
	return nil
}

func validateRecoveryRequest(dataset, version, source string, limit int) error {
	if err := validateMaterializationRevision(dataset, version); err != nil {
		return err
	}
	if source == "" {
		return errors.New("recovery source is required")
	}
	if limit <= 0 {
		return errors.New("recovery limit must be positive")
	}
	return nil
}

func validateReplayListRequest(request replay.ListRequest) error {
	if request.Limit <= 0 {
		return errors.New("failed materialization list limit must be positive")
	}
	if request.After == nil {
		return nil
	}
	cursor := request.After
	if cursor.UpdatedAt.IsZero() || cursor.Source == "" || cursor.EventID == "" || cursor.Dataset == "" || cursor.DatasetVersion == "" {
		return errors.New("failed materialization cursor is incomplete")
	}
	return nil
}

func validateReplayRequest(request replay.Request) error {
	if err := validateMaterializationRevision(request.Dataset, request.DatasetVersion); err != nil {
		return err
	}
	if request.Source == "" || request.EventID == "" || request.RequestID == "" || request.Actor == "" || request.Reason == "" {
		return errors.New("replay source, event id, request id, actor, and reason are required")
	}
	if len(request.RequestID) > 256 || len(request.Actor) > 256 || len(request.Reason) > 1024 {
		return errors.New("replay request id and actor must not exceed 256 bytes; reason must not exceed 1024 bytes")
	}
	return nil
}

func (s *Store) valid(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("postgres store is not initialized")
	}
	return contextErr(ctx)
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	return ctx.Err()
}

func normalizedConfig(config Config) Config {
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if config.MigrationTimeout <= 0 {
		config.MigrationTimeout = defaultMigrationTimeout
	}
	return config
}

func (c Config) operationTimeout() time.Duration { return normalizedConfig(c).OperationTimeout }
func (c Config) migrationTimeout() time.Duration { return normalizedConfig(c).MigrationTimeout }

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *Store) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return withTimeout(ctx, s.operationTimeout)
}
func (s *Store) migrationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return withTimeout(ctx, s.migrationTimeout)
}

type migration struct {
	version   int64
	name, sql string
	checksum  string
}

type appliedMigration struct {
	version  int64
	name     string
	checksum *string
}

// migrationExecer is deliberately small so the ledger bootstrap can be tested
// independently of a pool. A missing checksum is the only supported legacy
// form: after its version and exact filename are verified, it is backfilled.
// That one-time upgrade cannot prove what SQL an older binary executed before
// checksums existed; every subsequent startup verifies the stored digest.
type migrationExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func ensureMigrationLedger(ctx context.Context, tx migrationExecer) error {
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS streamforge_schema_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	// The original ledger shipped without checksum. This additive change is safe
	// for existing installations and deliberately does not assume a table with a
	// different shape is trustworthy.
	if _, err := tx.Exec(ctx, `ALTER TABLE streamforge_schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT`); err != nil {
		return fmt.Errorf("add migration checksum column: %w", err)
	}
	rows, err := tx.Query(ctx, `SELECT column_name
FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'streamforge_schema_migrations'`)
	if err != nil {
		return fmt.Errorf("inspect migration ledger schema: %w", err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return fmt.Errorf("scan migration ledger column: %w", err)
		}
		columns[column] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration ledger columns: %w", err)
	}
	for _, required := range []string{"version", "name", "checksum", "applied_at"} {
		if !columns[required] {
			return fmt.Errorf("%w: streamforge_schema_migrations is missing %q", ErrMigrationLedgerSchema, required)
		}
	}
	return nil
}

func verifyMigrationLedger(ctx context.Context, tx migrationExecer, embedded []migration) error {
	rows, err := tx.Query(ctx, `SELECT version, name, checksum FROM streamforge_schema_migrations ORDER BY version ASC`)
	if err != nil {
		return fmt.Errorf("read migration ledger: %w", err)
	}
	defer rows.Close()
	applied := make([]appliedMigration, 0)
	for rows.Next() {
		var item appliedMigration
		if err := rows.Scan(&item.version, &item.name, &item.checksum); err != nil {
			return fmt.Errorf("scan migration ledger: %w", err)
		}
		applied = append(applied, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration ledger: %w", err)
	}
	backfill, err := validateMigrationHistory(embedded, applied)
	if err != nil {
		return err
	}
	for _, item := range backfill {
		if _, err := tx.Exec(ctx, `UPDATE streamforge_schema_migrations SET checksum = $2 WHERE version = $1 AND checksum IS NULL`, item.version, item.checksum); err != nil {
			return fmt.Errorf("backfill migration checksum for %s: %w", item.name, err)
		}
	}
	return nil
}

// validateMigrationHistory makes the ledger a prefix of the embedded sequence.
// It returns the legacy entries whose missing checksums may safely be
// backfilled only after exact version/name validation.
func validateMigrationHistory(embedded []migration, applied []appliedMigration) ([]migration, error) {
	if len(applied) > len(embedded) {
		return nil, fmt.Errorf("%w: ledger has more entries than this binary", ErrMigrationHistoryMismatch)
	}
	backfill := make([]migration, 0)
	for index, got := range applied {
		want := embedded[index]
		if got.version != want.version {
			if got.version > want.version {
				return nil, fmt.Errorf("%w: expected version %d before recorded version %d", ErrMigrationHistoryGap, want.version, got.version)
			}
			return nil, fmt.Errorf("%w: recorded version %d is not embedded", ErrMigrationHistoryMismatch, got.version)
		}
		if got.name != want.name {
			return nil, fmt.Errorf("%w: version %d name is %q, want %q", ErrMigrationHistoryMismatch, got.version, got.name, want.name)
		}
		if got.checksum == nil || *got.checksum == "" {
			backfill = append(backfill, want)
			continue
		}
		if *got.checksum != want.checksum {
			return nil, fmt.Errorf("%w: version %d checksum differs", ErrMigrationHistoryMismatch, got.version)
		}
	}
	return backfill, nil
}

func migrationFiles() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	files := make([]migration, 0, len(entries))
	seen := map[int64]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q must begin with a numeric version and underscore", entry.Name())
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in %q", entry.Name())
		}
		if seen[version] {
			return nil, fmt.Errorf("duplicate migration version %d", version)
		}
		contents, err := migrationFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		digest := sha256.Sum256(contents)
		files, seen[version] = append(files, migration{version: version, name: entry.Name(), sql: string(contents), checksum: fmt.Sprintf("%x", digest)}), true
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}
