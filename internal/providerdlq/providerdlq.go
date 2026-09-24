// Package providerdlq defines the durable dead-letter and replay contract used
// by provider-mode workers. It is intentionally distinct from the personal
// mode materialization ledger: provider failures are identified by a Kafka
// record position, not by an application event ID.
package providerdlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/telemetry"
)

type Stage string

const (
	StageProcessor Stage = "processor"
	StageSink      Stage = "sink"
)

type Class string

const (
	ClassDecode    Class = "decode"
	ClassKey       Class = "key"
	ClassNormalize Class = "normalize"
	ClassSink      Class = "sink"
)

const (
	MaxClusterIDBytes       = 128
	MaxFailureIDBytes       = 64
	MaxDatasetBytes         = 128
	MaxRevisionBytes        = 128
	MaxDiagnosticBytes      = 4096
	MaxTopicBytes           = 249 // Kafka topic limit
	MaxReplayTopicBytes     = 249
	MaxReplayRequestIDBytes = 128
	MaxActorBytes           = 256
	MaxReasonBytes          = 1024
	MaxMessageBytes         = 1 << 20
	// Contract revisions are operator supplied identifiers. Keep the same
	// bounded shape as dataset revisions, while event.Contract verifies the
	// SHA-256 digest itself.
	MaxContractRevisionBytes = 128
	MaxPageLimit             = 500
)

var (
	ErrInvalidFailure       = errors.New("invalid provider dead-letter failure")
	ErrInvalidReplayRequest = errors.New("invalid provider replay request")
	ErrNotFound             = errors.New("provider dead-letter item not found")
	ErrReplayClaimLost      = errors.New("provider replay claim is no longer owned")
	ErrReplayRequestFailed  = errors.New("provider replay request already failed; use a new request id")
	// ErrCrossRevisionUnsupported prevents a poisoned message from being
	// published through a contract other than the one that produced it. A
	// replay is recovery, not a migration mechanism.
	ErrCrossRevisionUnsupported = errors.New("provider replay cannot target a different revision")
)

// Failure retains enough broker metadata to retry the exact record. FailureID
// is deterministic so re-delivery of the same poison record is an upsert.
type Failure struct {
	ClusterID       string
	FailureID       string
	Dataset         string
	SourceRevision  string
	Stage           Stage
	Class           Class
	Diagnostic      string
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64
	SourceTimestamp time.Time
	Key             []byte
	Value           []byte
	// Traceparent correlates the original worker attempt with durable DLQ
	// inspection and replay. Empty is retained for legacy rows and callers
	// without an accepted trace context.
	Traceparent string `json:"traceparent,omitempty"`
	// ExpectedContract is the immutable contract configured on the worker that
	// rejected the record. OriginalContract is the contract carried by a
	// decodable input record. It is intentionally optional: malformed input has
	// no trustworthy original envelope, but must still retain the expected
	// contract that was used to reject it.
	ExpectedContract event.Contract `json:"expected_contract,omitempty"`
	OriginalContract event.Contract `json:"original_contract,omitempty"`
	ReplayTopic      string
	FailedAt         time.Time
}

// MarshalFailure is the Kafka DLQ wire format. The complete failure, including
// original key/value and source coordinates, is retained so the database
// indexer can be restarted without losing replay material.
func MarshalFailure(f Failure) ([]byte, error) {
	if f.FailureID == "" {
		f.FailureID = DeterministicFailureID(f.ClusterID, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.Stage)
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(f)
}

func UnmarshalFailure(value []byte, maxBytes int) (Failure, error) {
	if len(value) == 0 || (maxBytes > 0 && len(value) > maxBytes) {
		return Failure{}, ErrInvalidFailure
	}
	var failure Failure
	if err := json.Unmarshal(value, &failure); err != nil {
		return Failure{}, fmt.Errorf("decode provider dead-letter: %w", err)
	}
	if err := failure.Validate(); err != nil {
		return Failure{}, err
	}
	return failure, nil
}

func DeterministicFailureID(clusterID, topic string, partition int32, offset int64, stage Stage) string {
	input := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", clusterID, topic, partition, offset, stage)
	digest := sha256.Sum256([]byte(input))
	return hex.EncodeToString(digest[:])
}

func (f Failure) Validate() error {
	if !nonEmptyBounded(f.ClusterID, MaxClusterIDBytes) || !nonEmptyBounded(f.Dataset, MaxDatasetBytes) ||
		!nonEmptyBounded(f.SourceRevision, MaxRevisionBytes) || !nonEmptyBounded(f.Diagnostic, MaxDiagnosticBytes) ||
		!nonEmptyBounded(f.SourceTopic, MaxTopicBytes) || !nonEmptyBounded(f.ReplayTopic, MaxReplayTopicBytes) ||
		f.SourceOffset < 0 || f.SourceTimestamp.IsZero() || f.FailedAt.IsZero() ||
		len(f.Key) > MaxMessageBytes || len(f.Value) > MaxMessageBytes {
		return ErrInvalidFailure
	}
	if f.Stage != StageProcessor && f.Stage != StageSink {
		return ErrInvalidFailure
	}
	if f.Class != ClassDecode && f.Class != ClassKey && f.Class != ClassNormalize && f.Class != ClassSink {
		return ErrInvalidFailure
	}
	if f.Traceparent != "" && !telemetry.ValidTraceparent(f.Traceparent) {
		return ErrInvalidFailure
	}
	if !validOptionalContract(f.ExpectedContract) || !validOptionalContract(f.OriginalContract) {
		return ErrInvalidFailure
	}
	wantID := DeterministicFailureID(f.ClusterID, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.Stage)
	if f.FailureID == "" {
		f.FailureID = wantID
	}
	if f.FailureID != wantID || len(f.FailureID) > MaxFailureIDBytes {
		return ErrInvalidFailure
	}
	return nil
}

// validOptionalContract deliberately permits an absent identity for rows
// created before provider contracts existed, or for a malformed source value
// from which no original envelope can be established. A present identity must
// be complete and uses event.Contract validation for exact SHA-256 checking.
func validOptionalContract(contract event.Contract) bool {
	if contract.Empty() {
		return true
	}
	if !contract.Complete() {
		return false
	}
	return len(contract.RawSchema.Revision) <= MaxContractRevisionBytes &&
		len(contract.NormalizedSchema.Revision) <= MaxContractRevisionBytes &&
		len(contract.Transform.Revision) <= MaxContractRevisionBytes
}

// ListRequest uses an exclusive failed-at/failure-id cursor and is deliberately
// bounded to keep a DLQ inspection endpoint inexpensive.
type ListRequest struct {
	ClusterID string
	Limit     int
	After     *Cursor
}

type Cursor struct {
	FailedAt  time.Time
	FailureID string
}

type Page struct {
	Items []Failure
	Next  *Cursor
}

func (r ListRequest) Validate() error {
	if !nonEmptyBounded(r.ClusterID, MaxClusterIDBytes) || r.Limit <= 0 || r.Limit > MaxPageLimit {
		return ErrInvalidFailure
	}
	if r.After != nil && (r.After.FailedAt.IsZero() || !nonEmptyBounded(r.After.FailureID, MaxFailureIDBytes)) {
		return ErrInvalidFailure
	}
	return nil
}

// ReplayRequest explicitly names a target revision. SourceRevision is copied
// into the durable request by Store.RequestReplay; callers cannot rewrite it.
type ReplayRequest struct {
	ClusterID string
	FailureID string
	// Dataset is repeated in the request so an administrative caller cannot
	// claim a replay for a different configured dataset merely by knowing its
	// request ID. Store.RequestReplay verifies it against the immutable DLQ row.
	Dataset        string
	RequestID      string
	Actor          string
	Reason         string
	TargetRevision string
}

func (r ReplayRequest) Validate() error {
	if !nonEmptyBounded(r.ClusterID, MaxClusterIDBytes) || !nonEmptyBounded(r.FailureID, MaxFailureIDBytes) ||
		!nonEmptyBounded(r.Dataset, MaxDatasetBytes) ||
		!nonEmptyBounded(r.RequestID, MaxReplayRequestIDBytes) || !nonEmptyBounded(r.Actor, MaxActorBytes) ||
		!nonEmptyBounded(r.Reason, MaxReasonBytes) || !nonEmptyBounded(r.TargetRevision, MaxRevisionBytes) {
		return ErrInvalidReplayRequest
	}
	return nil
}

type ReplayRequestResult struct {
	Request        ReplayRequest
	SourceRevision string
	State          ReplayState
	Accepted       bool
}

type ReplayState string

const (
	ReplayRequested ReplayState = "requested"
	ReplayRunning   ReplayState = "running"
	ReplaySucceeded ReplayState = "succeeded"
	ReplayFailed    ReplayState = "failed"
)

// ReplayStatus is the administrative read model. SourceRevision originates in
// the immutable DLQ record, while TargetRevision was chosen by the operator.
type ReplayStatus struct {
	Request        ReplayRequest
	SourceRevision string
	State          ReplayState
	Attempts       int
	Diagnostic     string
	RequestedAt    time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type ClaimRequest struct {
	ClusterID string
	Dataset   string
	// TargetRevision must match the configured worker revision. It narrows the
	// SQL claim predicate, so a worker can never publish a request intended for
	// a different dataset revision.
	TargetRevision string
	Worker         string
	Lease          time.Duration
}

func (r ClaimRequest) Validate() error {
	return validateClaimRequest(r)
}

type ReplayClaim struct {
	Request        ReplayRequest
	Failure        Failure
	ClaimToken     string
	ClaimUntil     time.Time
	Attempts       int
	SourceRevision string
}

// Store is the provider-mode DLQ and replay state machine.
type Store interface {
	Upsert(context.Context, Failure) (Failure, error)
	List(context.Context, ListRequest) (Page, error)
	RequestReplay(context.Context, ReplayRequest) (ReplayRequestResult, error)
	// GetReplay is cluster scoped. Request IDs are idempotency keys supplied by
	// callers and must never become a cross-cluster read capability.
	GetReplay(context.Context, string, string) (ReplayStatus, error)
	ClaimReplay(context.Context, ClaimRequest) (*ReplayClaim, error)
	CompleteReplay(context.Context, string, string) error
	FailReplay(context.Context, string, string, error) error
}

func validateClaimRequest(r ClaimRequest) error {
	if !nonEmptyBounded(r.ClusterID, MaxClusterIDBytes) || !nonEmptyBounded(r.Dataset, MaxDatasetBytes) ||
		!nonEmptyBounded(r.TargetRevision, MaxRevisionBytes) || !nonEmptyBounded(r.Worker, MaxActorBytes) || r.Lease <= 0 || r.Lease > 24*time.Hour {
		return ErrInvalidReplayRequest
	}
	return nil
}

func ValidateClaimOwnership(requestID, claimToken string) error {
	if err := ValidateRequestID(requestID); err != nil || !nonEmptyBounded(claimToken, 128) {
		return ErrInvalidReplayRequest
	}
	return nil
}

func ValidateRequestID(requestID string) error {
	if !nonEmptyBounded(requestID, MaxReplayRequestIDBytes) {
		return ErrInvalidReplayRequest
	}
	return nil
}

func nonEmptyBounded(s string, max int) bool {
	return strings.TrimSpace(s) != "" && len(s) <= max
}
