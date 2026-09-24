package providerdlq

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func testFailure() Failure {
	return Failure{
		ClusterID: "local", Dataset: "games", SourceRevision: "v1",
		Stage: StageProcessor, Class: ClassDecode, Diagnostic: "invalid json",
		SourceTopic: "raw.games", SourcePartition: 2, SourceOffset: 42,
		SourceTimestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Key:             []byte("k"), Value: []byte(`{"bad":`), ReplayTopic: "raw.games.replay",
		FailedAt: time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC),
	}
}

func TestFailureValidationAndDeterministicID(t *testing.T) {
	f := testFailure()
	want := DeterministicFailureID(f.ClusterID, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.Stage)
	if want != DeterministicFailureID(f.ClusterID, f.SourceTopic, f.SourcePartition, f.SourceOffset, f.Stage) {
		t.Fatal("failure IDs are not deterministic")
	}
	f.FailureID = want
	if err := f.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	f.FailureID = "wrong"
	if !errors.Is(f.Validate(), ErrInvalidFailure) {
		t.Fatalf("Validate() = %v, want invalid failure", f.Validate())
	}
}

func TestReplayValidation(t *testing.T) {
	r := ReplayRequest{ClusterID: "local", FailureID: "0123456789", Dataset: "games", RequestID: "replay-1", Actor: "admin", Reason: "schema fix", TargetRevision: "v2"}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if err := (ClaimRequest{ClusterID: "local", Dataset: "games", TargetRevision: "v2", Worker: "worker-a", Lease: time.Minute}).Validate(); err != nil {
		t.Fatalf("claim Validate() = %v", err)
	}
	if err := (ClaimRequest{ClusterID: "local", Dataset: "games", TargetRevision: "v2", Worker: "worker-a", Lease: 25 * time.Hour}).Validate(); !errors.Is(err, ErrInvalidReplayRequest) {
		t.Fatalf("long claim lease = %v", err)
	}
}

func TestReplayAndClaimRequireDatasetRevisionIdentity(t *testing.T) {
	request := ReplayRequest{ClusterID: "local", FailureID: "0123456789", Dataset: "games", RequestID: "replay-1", Actor: "admin", Reason: "retry", TargetRevision: "v1"}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid replay request: %v", err)
	}
	request.Dataset = ""
	if !errors.Is(request.Validate(), ErrInvalidReplayRequest) {
		t.Fatalf("dataset-less replay request = %v", request.Validate())
	}
	claim := ClaimRequest{ClusterID: "local", Dataset: "games", TargetRevision: "v1", Worker: "worker-a", Lease: time.Minute}
	if err := claim.Validate(); err != nil {
		t.Fatalf("valid claim request: %v", err)
	}
	claim.TargetRevision = ""
	if !errors.Is(claim.Validate(), ErrInvalidReplayRequest) {
		t.Fatalf("revision-less claim request = %v", claim.Validate())
	}
}

func TestMalformedFailureRetainsExpectedContractOnKafkaWire(t *testing.T) {
	failure := testFailure()
	failure.ExpectedContract = testContract()
	failure.Traceparent = testTraceparent
	// A malformed raw value has no trusted event envelope to extract an
	// original contract from. The configured, expected contract remains enough
	// to inspect and replay it safely.
	failure.OriginalContract = event.Contract{}
	wire, err := MarshalFailure(failure)
	if err != nil {
		t.Fatalf("MarshalFailure() = %v", err)
	}
	decoded, err := UnmarshalFailure(wire, MaxMessageBytes)
	if err != nil {
		t.Fatalf("UnmarshalFailure() = %v", err)
	}
	if decoded.ExpectedContract != failure.ExpectedContract || !decoded.OriginalContract.Empty() || decoded.Traceparent != testTraceparent {
		t.Fatalf("contracts were not preserved: %#v", decoded)
	}
}

func TestFailureRejectsMalformedTraceparent(t *testing.T) {
	failure := testFailure()
	failure.Traceparent = "00-not-a-trace"
	if !errors.Is(failure.Validate(), ErrInvalidFailure) {
		t.Fatalf("invalid traceparent Validate() = %v", failure.Validate())
	}
	failure.Traceparent = testTraceparent
	if err := failure.Validate(); err != nil {
		t.Fatalf("valid traceparent Validate() = %v", err)
	}
}

func TestFailureRejectsInvalidContractDigestOrRevisionBound(t *testing.T) {
	failure := testFailure()
	failure.ExpectedContract = testContract()
	failure.ExpectedContract.RawSchema.Digest = "bad"
	if !errors.Is(failure.Validate(), ErrInvalidFailure) {
		t.Fatalf("bad digest Validate() = %v", failure.Validate())
	}
	failure.ExpectedContract = testContract()
	failure.ExpectedContract.Transform.Revision = strings.Repeat("x", MaxContractRevisionBytes+1)
	if !errors.Is(failure.Validate(), ErrInvalidFailure) {
		t.Fatalf("long revision Validate() = %v", failure.Validate())
	}
}

func testContract() event.Contract {
	return event.Contract{
		RawSchema:        event.SchemaIdentity{Revision: "raw-v1", Digest: strings.Repeat("a", 64)},
		NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("b", 64)},
		Transform:        event.TransformIdentity{Revision: "mapping-v1", Digest: strings.Repeat("c", 64)},
	}
}

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
