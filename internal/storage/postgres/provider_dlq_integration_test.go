package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/providerdlq"
)

func TestProviderDLQReplayClaimLifecycle(t *testing.T) {
	url := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	dlq := store.ProviderDLQ()
	if _, err := store.pool.Exec(ctx, "TRUNCATE provider_replay_requests, provider_dead_letters"); err != nil {
		t.Fatal(err)
	}

	input := testProviderFailure()
	input.ExpectedContract = providerDLQTestContract()
	input.OriginalContract = providerDLQTestContract()
	input.Traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	failure, err := dlq.Upsert(ctx, input)
	if err != nil {
		t.Fatalf("Upsert() = %v", err)
	}
	if failure.FailureID == "" {
		t.Fatal("Upsert did not assign deterministic failure id")
	}
	page, err := dlq.List(ctx, providerdlq.ListRequest{ClusterID: failure.ClusterID, Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].Traceparent != input.Traceparent {
		t.Fatalf("List() traceparent = %#v, %v", page, err)
	}

	request := providerdlq.ReplayRequest{ClusterID: failure.ClusterID, FailureID: failure.FailureID, Dataset: failure.Dataset, RequestID: "r-1", Actor: "operator", Reason: "retry v1", TargetRevision: "v1"}
	accepted, err := dlq.RequestReplay(ctx, request)
	if err != nil || !accepted.Accepted || accepted.SourceRevision != "v1" {
		t.Fatalf("RequestReplay() = %#v, %v", accepted, err)
	}
	claim, err := dlq.ClaimReplay(ctx, providerdlq.ClaimRequest{ClusterID: failure.ClusterID, Dataset: failure.Dataset, TargetRevision: "v1", Worker: "worker-1", Lease: time.Minute})
	if err != nil || claim == nil || claim.Request.TargetRevision != "v1" || claim.SourceRevision != "v1" {
		t.Fatalf("ClaimReplay() = %#v, %v", claim, err)
	}
	if claim.Failure.ExpectedContract != input.ExpectedContract || claim.Failure.OriginalContract != input.OriginalContract || claim.Failure.Traceparent != input.Traceparent {
		t.Fatalf("ClaimReplay() contract identity not preserved: %#v", claim.Failure)
	}
	if err := dlq.CompleteReplay(ctx, request.RequestID, "stale"); !errors.Is(err, providerdlq.ErrReplayClaimLost) {
		t.Fatalf("stale complete = %v", err)
	}
	if err := dlq.CompleteReplay(ctx, request.RequestID, claim.ClaimToken); err != nil {
		t.Fatalf("CompleteReplay() = %v", err)
	}
	status, err := dlq.GetReplay(ctx, failure.ClusterID, request.RequestID)
	if err != nil || status.State != providerdlq.ReplaySucceeded || status.SourceRevision != "v1" {
		t.Fatalf("GetReplay() = %#v, %v", status, err)
	}
	if _, err := dlq.GetReplay(ctx, "", request.RequestID); !errors.Is(err, providerdlq.ErrInvalidReplayRequest) {
		t.Fatalf("GetReplay invalid cluster = %v", err)
	}
	if _, err := dlq.GetReplay(ctx, "other-cluster", request.RequestID); !errors.Is(err, providerdlq.ErrNotFound) {
		t.Fatalf("GetReplay cross-cluster = %v", err)
	}
}

func TestProviderDLQRejectsAllCrossRevisionReplays(t *testing.T) {
	url := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	dlq := store.ProviderDLQ()
	if _, err := store.pool.Exec(ctx, "TRUNCATE provider_replay_requests, provider_dead_letters"); err != nil {
		t.Fatal(err)
	}
	failure := testProviderFailure()
	failure, err = dlq.Upsert(ctx, failure)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dlq.RequestReplay(ctx, providerdlq.ReplayRequest{ClusterID: failure.ClusterID, FailureID: failure.FailureID, Dataset: failure.Dataset, RequestID: "processor-v2", Actor: "operator", Reason: "wrong", TargetRevision: "v2"})
	if !errors.Is(err, providerdlq.ErrCrossRevisionUnsupported) {
		t.Fatalf("cross revision sink replay = %v", err)
	}
}

func TestProviderDLQReplayClaimIsIdentityScopedAndUsesDatabaseClock(t *testing.T) {
	url := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "TRUNCATE provider_replay_requests, provider_dead_letters"); err != nil {
		t.Fatal(err)
	}
	dlq := store.ProviderDLQ()
	failure, err := dlq.Upsert(ctx, testProviderFailure())
	if err != nil {
		t.Fatal(err)
	}
	request := providerdlq.ReplayRequest{ClusterID: failure.ClusterID, FailureID: failure.FailureID, Dataset: failure.Dataset, RequestID: "claim-scope", Actor: "operator", Reason: "retry", TargetRevision: failure.SourceRevision}
	if _, err := dlq.RequestReplay(ctx, request); err != nil {
		t.Fatal(err)
	}
	if claim, err := dlq.ClaimReplay(ctx, providerdlq.ClaimRequest{ClusterID: failure.ClusterID, Dataset: "other", TargetRevision: failure.SourceRevision, Worker: "worker", Lease: time.Minute}); err != nil || claim != nil {
		t.Fatalf("wrong dataset claim = %#v, %v", claim, err)
	}
	if claim, err := dlq.ClaimReplay(ctx, providerdlq.ClaimRequest{ClusterID: failure.ClusterID, Dataset: failure.Dataset, TargetRevision: "other", Worker: "worker", Lease: time.Minute}); err != nil || claim != nil {
		t.Fatalf("wrong revision claim = %#v, %v", claim, err)
	}
	var before time.Time
	if err := store.pool.QueryRow(ctx, "SELECT now()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	lease := 2 * time.Minute
	claim, err := dlq.ClaimReplay(ctx, providerdlq.ClaimRequest{ClusterID: failure.ClusterID, Dataset: failure.Dataset, TargetRevision: failure.SourceRevision, Worker: "worker", Lease: lease})
	if err != nil || claim == nil {
		t.Fatalf("scoped claim = %#v, %v", claim, err)
	}
	var after time.Time
	if err := store.pool.QueryRow(ctx, "SELECT now()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	// The bound comes from PostgreSQL's now(), not the client wall clock. The
	// range permits only query/transaction elapsed time.
	if claim.ClaimUntil.Before(before.Add(lease)) || claim.ClaimUntil.After(after.Add(lease)) {
		t.Fatalf("claim deadline %s was not derived from database now in [%s,%s]", claim.ClaimUntil, before.Add(lease), after.Add(lease))
	}
	if err := dlq.CompleteReplay(ctx, request.RequestID, "stale-token"); !errors.Is(err, providerdlq.ErrReplayClaimLost) {
		t.Fatalf("stale token complete = %v", err)
	}
}

func TestProviderDLQDuplicateKeepsFailureCursorStable(t *testing.T) {
	url := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "TRUNCATE provider_replay_requests, provider_dead_letters"); err != nil {
		t.Fatal(err)
	}
	dlq := store.ProviderDLQ()
	first, err := dlq.Upsert(ctx, testProviderFailure())
	if err != nil {
		t.Fatal(err)
	}
	duplicate := testProviderFailure()
	duplicate.FailedAt = duplicate.FailedAt.Add(24 * time.Hour)
	duplicate.Diagnostic = "changed diagnostic must not rewrite original"
	stored, err := dlq.Upsert(ctx, duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.FailedAt.Equal(first.FailedAt) || stored.Diagnostic != first.Diagnostic {
		t.Fatalf("duplicate changed immutable material: first=%#v stored=%#v", first, stored)
	}
	page, err := dlq.List(ctx, providerdlq.ListRequest{ClusterID: first.ClusterID, Limit: 1})
	if err != nil || len(page.Items) != 1 || !page.Items[0].FailedAt.Equal(first.FailedAt) {
		t.Fatalf("stable DLQ cursor = %#v, %v", page, err)
	}
}
