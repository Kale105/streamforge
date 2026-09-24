package postgres

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/providerdlq"
)

func TestProviderReplayIntegrityMigrationProtectsLegacyAndNewRows(t *testing.T) {
	sql, err := migrationFS.ReadFile("migrations/0013_provider_replay_integrity.sql")
	if err != nil {
		t.Fatalf("read replay integrity migration: %v", err)
	}
	text := string(sql)
	for _, want := range []string{
		"legacy cross-revision replay terminalized",
		"CHECK (source_revision = target_revision) NOT VALID",
		"provider_replay_requests_claim_identity_idx",
		"provider_dead_letters.failed_at is immutable",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("integrity migration missing %q", want)
		}
	}
}

func TestProviderDLQFailureValidation(t *testing.T) {
	failure := testProviderFailure()
	failure.FailureID = providerdlq.DeterministicFailureID(failure.ClusterID, failure.SourceTopic, failure.SourcePartition, failure.SourceOffset, failure.Stage)
	if err := failure.Validate(); err != nil {
		t.Fatal(err)
	}
	failure.SourceOffset = -1
	if !errors.Is(failure.Validate(), providerdlq.ErrInvalidFailure) {
		t.Fatalf("negative offset error = %v", failure.Validate())
	}
}

func TestProviderDLQUnknownContractRemainsEmpty(t *testing.T) {
	if value := nullableContract(event.Contract{}); value != nil {
		t.Fatalf("nullableContract(empty) = %#v, want nil", value)
	}
	var decoded event.Contract
	if err := decodeOptionalContract("{}", &decoded); err != nil {
		t.Fatalf("decodeOptionalContract old row = %v", err)
	}
	if !decoded.Empty() {
		t.Fatalf("old row contract = %#v, want unknown/empty", decoded)
	}
	if value := nullableTraceparent(""); value != nil {
		t.Fatalf("nullableTraceparent(empty) = %#v, want nil", value)
	}
}

func providerDLQTestContract() event.Contract {
	return event.Contract{
		RawSchema:        event.SchemaIdentity{Revision: "raw-v1", Digest: strings.Repeat("a", 64)},
		NormalizedSchema: event.SchemaIdentity{Revision: "normalized-v1", Digest: strings.Repeat("b", 64)},
		Transform:        event.TransformIdentity{Revision: "mapping-v1", Digest: strings.Repeat("c", 64)},
	}
}

func testProviderFailure() providerdlq.Failure {
	return providerdlq.Failure{
		ClusterID: "test-cluster", Dataset: "games", SourceRevision: "v1",
		Stage: providerdlq.StageProcessor, Class: providerdlq.ClassDecode,
		Diagnostic: "invalid JSON", SourceTopic: "raw.games", SourcePartition: 0,
		SourceOffset: 1, SourceTimestamp: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
		Key: []byte("key"), Value: []byte(`{"broken":`), ReplayTopic: "raw.games.replay",
		FailedAt: time.Date(2026, 2, 3, 4, 5, 7, 0, time.UTC),
	}
}
