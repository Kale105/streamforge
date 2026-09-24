package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/providerdlq"
	"github.com/jackc/pgx/v5"
)

// ProviderDLQStore is a provider-mode view over the shared PostgreSQL pool.
// It has a distinct type because Store already exposes the personal-mode List
// method with a different contract.
type ProviderDLQStore struct{ store *Store }

var _ providerdlq.Store = (*ProviderDLQStore)(nil)

func (s *Store) ProviderDLQ() *ProviderDLQStore { return &ProviderDLQStore{store: s} }

// Upsert records a provider poison message once per cluster/topic/partition/
// offset/stage. The first durable observation is immutable: it is the replay
// material and the cursor ordering key. Duplicate Kafka delivery must never
// rewrite it, even if the worker's clock or diagnostic has changed.
func (s *ProviderDLQStore) Upsert(ctx context.Context, failure providerdlq.Failure) (providerdlq.Failure, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return providerdlq.Failure{}, err
	}
	if failure.FailureID == "" {
		failure.FailureID = providerdlq.DeterministicFailureID(failure.ClusterID, failure.SourceTopic, failure.SourcePartition, failure.SourceOffset, failure.Stage)
	}
	if err := failure.Validate(); err != nil {
		return providerdlq.Failure{}, err
	}
	row := s.store.pool.QueryRow(ctx, `
INSERT INTO provider_dead_letters (
 cluster_id, failure_id, dataset, source_revision, stage, class, diagnostic,
 source_topic, source_partition, source_offset, source_timestamp, message_key,
 message_value, traceparent, expected_contract, original_contract, replay_topic, failed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17,$18)
ON CONFLICT (cluster_id, failure_id) DO UPDATE
SET failure_id = provider_dead_letters.failure_id
RETURNING cluster_id, failure_id, dataset, source_revision, stage, class,
 diagnostic, source_topic, source_partition, source_offset, source_timestamp,
 message_key, message_value, COALESCE(traceparent, ''), COALESCE(expected_contract, '{}'::jsonb)::text,
 COALESCE(original_contract, '{}'::jsonb)::text, replay_topic, failed_at`,
		failure.ClusterID, failure.FailureID, failure.Dataset, failure.SourceRevision,
		failure.Stage, failure.Class, failure.Diagnostic, failure.SourceTopic,
		failure.SourcePartition, failure.SourceOffset, failure.SourceTimestamp.UTC(),
		failure.Key, failure.Value, nullableTraceparent(failure.Traceparent), nullableContract(failure.ExpectedContract), nullableContract(failure.OriginalContract),
		failure.ReplayTopic, failure.FailedAt.UTC())
	stored, err := scanProviderFailure(row)
	if err != nil {
		return providerdlq.Failure{}, fmt.Errorf("upsert provider dead letter: %w", err)
	}
	return stored, nil
}

func (s *ProviderDLQStore) List(ctx context.Context, request providerdlq.ListRequest) (providerdlq.Page, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return providerdlq.Page{}, err
	}
	if err := request.Validate(); err != nil {
		return providerdlq.Page{}, err
	}
	var afterAt *time.Time
	var afterID *string
	if request.After != nil {
		afterAt, afterID = &request.After.FailedAt, &request.After.FailureID
	}
	rows, err := s.store.pool.Query(ctx, `
SELECT cluster_id, failure_id, dataset, source_revision, stage, class,
 diagnostic, source_topic, source_partition, source_offset, source_timestamp,
 message_key, message_value, COALESCE(traceparent, ''), COALESCE(expected_contract, '{}'::jsonb)::text,
 COALESCE(original_contract, '{}'::jsonb)::text, replay_topic, failed_at
FROM provider_dead_letters
WHERE cluster_id = $1
 AND ($2::timestamptz IS NULL OR (failed_at, failure_id) > ($2, $3))
ORDER BY failed_at ASC, failure_id ASC
LIMIT $4`, request.ClusterID, afterAt, afterID, request.Limit+1)
	if err != nil {
		return providerdlq.Page{}, fmt.Errorf("list provider dead letters: %w", err)
	}
	defer rows.Close()
	page := providerdlq.Page{Items: make([]providerdlq.Failure, 0, request.Limit)}
	for rows.Next() {
		failure, err := scanProviderFailure(rows)
		if err != nil {
			return providerdlq.Page{}, fmt.Errorf("scan provider dead letter: %w", err)
		}
		if len(page.Items) == request.Limit {
			last := page.Items[len(page.Items)-1]
			page.Next = &providerdlq.Cursor{FailedAt: last.FailedAt, FailureID: last.FailureID}
			break
		}
		page.Items = append(page.Items, failure)
	}
	if err := rows.Err(); err != nil {
		return providerdlq.Page{}, fmt.Errorf("iterate provider dead letters: %w", err)
	}
	return page, nil
}

// RequestReplay durably snapshots the dataset and source revision from the
// DLQ row. Replays are strictly same-revision recovery: cross-revision replay
// is a schema migration concern, not a safe way to republish opaque records.
func (s *ProviderDLQStore) RequestReplay(ctx context.Context, request providerdlq.ReplayRequest) (providerdlq.ReplayRequestResult, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return providerdlq.ReplayRequestResult{}, err
	}
	if err := request.Validate(); err != nil {
		return providerdlq.ReplayRequestResult{}, err
	}
	tx, err := s.store.pool.Begin(ctx)
	if err != nil {
		return providerdlq.ReplayRequestResult{}, fmt.Errorf("begin provider replay request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var sourceRevision, dataset string
	err = tx.QueryRow(ctx, `SELECT source_revision, dataset FROM provider_dead_letters
WHERE cluster_id=$1 AND failure_id=$2 FOR UPDATE`, request.ClusterID, request.FailureID).Scan(&sourceRevision, &dataset)
	if errors.Is(err, pgx.ErrNoRows) {
		return providerdlq.ReplayRequestResult{}, providerdlq.ErrNotFound
	}
	if err != nil {
		return providerdlq.ReplayRequestResult{}, fmt.Errorf("load provider replay failure: %w", err)
	}
	if dataset != request.Dataset || request.TargetRevision != sourceRevision {
		return providerdlq.ReplayRequestResult{}, providerdlq.ErrCrossRevisionUnsupported
	}
	var state string
	var existingCluster, existingFailure, existingDataset, existingSource, existingTarget, existingActor, existingReason string
	err = tx.QueryRow(ctx, `SELECT cluster_id, failure_id, dataset, source_revision, target_revision, actor, reason, state
FROM provider_replay_requests WHERE request_id=$1 FOR UPDATE`, request.RequestID).Scan(
		&existingCluster, &existingFailure, &existingDataset, &existingSource, &existingTarget, &existingActor, &existingReason, &state)
	if err == nil {
		if existingCluster != request.ClusterID || existingFailure != request.FailureID ||
			existingDataset != request.Dataset || existingTarget != request.TargetRevision || existingActor != request.Actor || existingReason != request.Reason {
			return providerdlq.ReplayRequestResult{}, providerdlq.ErrInvalidReplayRequest
		}
		if state == "failed" {
			return providerdlq.ReplayRequestResult{}, providerdlq.ErrReplayRequestFailed
		}
		if err := tx.Commit(ctx); err != nil {
			return providerdlq.ReplayRequestResult{}, fmt.Errorf("commit duplicate provider replay request: %w", err)
		}
		return providerdlq.ReplayRequestResult{Request: request, SourceRevision: existingSource, State: providerdlq.ReplayState(state), Accepted: false}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return providerdlq.ReplayRequestResult{}, fmt.Errorf("check provider replay idempotency: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO provider_replay_requests
(request_id, cluster_id, failure_id, dataset, source_revision, target_revision, actor, reason, state)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'requested')`, request.RequestID, request.ClusterID,
		request.FailureID, request.Dataset, sourceRevision, request.TargetRevision, request.Actor, request.Reason); err != nil {
		return providerdlq.ReplayRequestResult{}, fmt.Errorf("insert provider replay request: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return providerdlq.ReplayRequestResult{}, fmt.Errorf("commit provider replay request: %w", err)
	}
	return providerdlq.ReplayRequestResult{Request: request, SourceRevision: sourceRevision, State: providerdlq.ReplayRequested, Accepted: true}, nil
}

// GetReplay returns the durable administrative status of one replay request.
func (s *ProviderDLQStore) GetReplay(ctx context.Context, clusterID, requestID string) (providerdlq.ReplayStatus, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return providerdlq.ReplayStatus{}, err
	}
	if strings.TrimSpace(clusterID) == "" || len(clusterID) > providerdlq.MaxClusterIDBytes || providerdlq.ValidateRequestID(requestID) != nil {
		return providerdlq.ReplayStatus{}, providerdlq.ErrInvalidReplayRequest
	}
	var status providerdlq.ReplayStatus
	var startedAt, finishedAt *time.Time
	err := s.store.pool.QueryRow(ctx, `SELECT request_id, cluster_id, failure_id, dataset,
 source_revision, target_revision, actor, reason, state, attempts,
 COALESCE(final_diagnostic, ''), requested_at, started_at, finished_at
FROM provider_replay_requests WHERE cluster_id=$1 AND request_id=$2`, clusterID, requestID).Scan(
		&status.Request.RequestID, &status.Request.ClusterID, &status.Request.FailureID, &status.Request.Dataset,
		&status.SourceRevision, &status.Request.TargetRevision, &status.Request.Actor,
		&status.Request.Reason, &status.State, &status.Attempts, &status.Diagnostic,
		&status.RequestedAt, &startedAt, &finishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return providerdlq.ReplayStatus{}, providerdlq.ErrNotFound
	}
	if err != nil {
		return providerdlq.ReplayStatus{}, fmt.Errorf("get provider replay: %w", err)
	}
	status.StartedAt, status.FinishedAt = startedAt, finishedAt
	return status, nil
}

// ClaimReplay leases one requested (or expired) replay. FOR UPDATE SKIP LOCKED
// lets any number of workers contend without waiting behind a slow worker.
func (s *ProviderDLQStore) ClaimReplay(ctx context.Context, request providerdlq.ClaimRequest) (*providerdlq.ReplayClaim, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	token, err := randomClaimToken()
	if err != nil {
		return nil, err
	}
	tx, err := s.store.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin provider replay claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row := tx.QueryRow(ctx, `
WITH candidate AS (
 SELECT request_id FROM provider_replay_requests
WHERE cluster_id=$4 AND dataset=$5 AND target_revision=$6
  AND (state = 'requested' OR (state = 'running' AND claim_until <= now()))
 ORDER BY requested_at ASC, request_id ASC
 FOR UPDATE SKIP LOCKED
 LIMIT 1
), claimed AS (
 UPDATE provider_replay_requests AS r
 SET state='running', claim_token=$1, claimed_by=$2,
     claim_until=now() + ($3::bigint * interval '1 microsecond'),
     attempts=r.attempts+1, started_at=now(), finished_at=NULL, final_diagnostic=NULL
 FROM candidate c WHERE r.request_id=c.request_id
 RETURNING r.request_id, r.cluster_id, r.failure_id, r.dataset, r.source_revision,
           r.target_revision, r.actor, r.reason, r.claim_token, r.claim_until,
           r.attempts
)
SELECT c.request_id, c.cluster_id, c.failure_id, c.dataset, c.source_revision,
 c.target_revision, c.actor, c.reason, c.claim_token, c.claim_until, c.attempts,
 f.cluster_id, f.failure_id, f.dataset, f.source_revision, f.stage, f.class,
 f.diagnostic, f.source_topic, f.source_partition, f.source_offset,
 f.source_timestamp, f.message_key, f.message_value, COALESCE(f.traceparent, ''),
 COALESCE(f.expected_contract, '{}'::jsonb)::text, COALESCE(f.original_contract, '{}'::jsonb)::text,
 f.replay_topic, f.failed_at
FROM claimed c JOIN provider_dead_letters f
 ON f.cluster_id=c.cluster_id AND f.failure_id=c.failure_id`, token, request.Worker, request.Lease.Microseconds(), request.ClusterID, request.Dataset, request.TargetRevision)
	claim, err := scanReplayClaim(row)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty provider replay claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim provider replay: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit provider replay claim: %w", err)
	}
	return &claim, nil
}

func (s *ProviderDLQStore) CompleteReplay(ctx context.Context, requestID, claimToken string) error {
	return s.finishProviderReplay(ctx, requestID, claimToken, "succeeded", "")
}

func (s *ProviderDLQStore) FailReplay(ctx context.Context, requestID, claimToken string, cause error) error {
	if cause == nil {
		return errors.New("provider replay failure cause is required")
	}
	diagnostic := cause.Error()
	if len(diagnostic) > providerdlq.MaxDiagnosticBytes {
		diagnostic = diagnostic[:providerdlq.MaxDiagnosticBytes]
	}
	return s.finishProviderReplay(ctx, requestID, claimToken, "failed", diagnostic)
}

func (s *ProviderDLQStore) finishProviderReplay(ctx context.Context, requestID, claimToken, state, diagnostic string) error {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if err := s.store.valid(ctx); err != nil {
		return err
	}
	if err := providerdlq.ValidateClaimOwnership(requestID, claimToken); err != nil {
		return err
	}
	tag, err := s.store.pool.Exec(ctx, `UPDATE provider_replay_requests
SET state=$3, finished_at=now(), claim_until=NULL, final_diagnostic=$4
WHERE request_id=$1 AND claim_token=$2 AND state='running' AND claim_until > now()`, requestID, claimToken, state, nullableDiagnostic(diagnostic))
	if err != nil {
		return fmt.Errorf("finish provider replay: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return providerdlq.ErrReplayClaimLost
	}
	return nil
}

type providerFailureScanner interface{ Scan(...any) error }

func scanProviderFailure(row providerFailureScanner) (providerdlq.Failure, error) {
	var f providerdlq.Failure
	var expectedContract, originalContract string
	err := row.Scan(&f.ClusterID, &f.FailureID, &f.Dataset, &f.SourceRevision,
		&f.Stage, &f.Class, &f.Diagnostic, &f.SourceTopic, &f.SourcePartition,
		&f.SourceOffset, &f.SourceTimestamp, &f.Key, &f.Value, &f.Traceparent, &expectedContract,
		&originalContract, &f.ReplayTopic, &f.FailedAt)
	if err != nil {
		return f, err
	}
	if err := decodeOptionalContract(expectedContract, &f.ExpectedContract); err != nil {
		return f, err
	}
	if err := decodeOptionalContract(originalContract, &f.OriginalContract); err != nil {
		return f, err
	}
	return f, nil
}

func scanReplayClaim(row providerFailureScanner) (providerdlq.ReplayClaim, error) {
	var claim providerdlq.ReplayClaim
	var failure providerdlq.Failure
	var expectedContract, originalContract string
	err := row.Scan(&claim.Request.RequestID, &claim.Request.ClusterID, &claim.Request.FailureID, &claim.Request.Dataset,
		&claim.SourceRevision, &claim.Request.TargetRevision, &claim.Request.Actor,
		&claim.Request.Reason, &claim.ClaimToken, &claim.ClaimUntil, &claim.Attempts,
		&failure.ClusterID, &failure.FailureID, &failure.Dataset, &failure.SourceRevision,
		&failure.Stage, &failure.Class, &failure.Diagnostic, &failure.SourceTopic,
		&failure.SourcePartition, &failure.SourceOffset, &failure.SourceTimestamp,
		&failure.Key, &failure.Value, &failure.Traceparent, &expectedContract, &originalContract,
		&failure.ReplayTopic, &failure.FailedAt)
	if err != nil {
		return claim, err
	}
	if err := decodeOptionalContract(expectedContract, &failure.ExpectedContract); err != nil {
		return claim, err
	}
	if err := decodeOptionalContract(originalContract, &failure.OriginalContract); err != nil {
		return claim, err
	}
	claim.Failure = failure
	return claim, nil
}

func nullableContract(contract event.Contract) any {
	if contract.Empty() {
		return nil
	}
	encoded, err := json.Marshal(contract)
	if err != nil {
		// event.Contract contains only strings, so this cannot happen. Keep the
		// database parameter nullable rather than inventing a malformed value.
		return nil
	}
	return encoded
}

func nullableTraceparent(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func decodeOptionalContract(raw string, target *event.Contract) error {
	if raw == "" || raw == "{}" {
		return nil
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return fmt.Errorf("decode provider dead-letter contract: %w", err)
	}
	if err := target.Validate(); err != nil {
		return fmt.Errorf("validate provider dead-letter contract: %w", err)
	}
	return nil
}

func randomClaimToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate provider replay claim token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func nullableDiagnostic(value string) any {
	if value == "" {
		return nil
	}
	return value
}
