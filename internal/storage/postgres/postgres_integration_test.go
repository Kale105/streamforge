package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/apigateway/security"
	"github.com/Kale105/streamforge/internal/coordination"
	"github.com/Kale105/streamforge/internal/event"
	"github.com/Kale105/streamforge/internal/providerflow"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/replay"
)

func TestPostgresMigrationLedgerIntegrity(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	// An isolated schema gives this test a truly fresh installation without
	// mutating the database shared by the rest of the integration suite.
	schema := fmt.Sprintf("streamforge_migration_test_%d", time.Now().UnixNano())
	if _, err := root.pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = root.pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	}()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	store, err := New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("fresh install: %v", err)
	}
	migrations, err := migrationFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		var name, checksum string
		if err := store.pool.QueryRow(ctx, `SELECT name, checksum FROM streamforge_schema_migrations WHERE version=$1`, migration.version).Scan(&name, &checksum); err != nil {
			t.Fatal(err)
		}
		if name != migration.name || checksum != migration.checksum {
			t.Fatalf("fresh ledger version %d = (%q, %q), want (%q, %q)", migration.version, name, checksum, migration.name, migration.checksum)
		}
	}

	// Null checksums represent the pre-checksum ledger shape. They are only
	// upgraded after the persisted version/name has matched exactly.
	first := migrations[0]
	if _, err := store.pool.Exec(ctx, `UPDATE streamforge_schema_migrations SET checksum=NULL WHERE version=$1`, first.version); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("legacy checksum upgrade: %v", err)
	}
	var checksum string
	if err := store.pool.QueryRow(ctx, `SELECT checksum FROM streamforge_schema_migrations WHERE version=$1`, first.version).Scan(&checksum); err != nil || checksum != first.checksum {
		t.Fatalf("legacy checksum = %q, %v", checksum, err)
	}

	assertRejected := func(label, statement string, args ...any) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, statement, args...); err != nil {
			t.Fatal(err)
		}
		if err := store.Migrate(ctx); !errors.Is(err, ErrMigrationHistoryMismatch) && !errors.Is(err, ErrMigrationHistoryGap) {
			t.Fatalf("%s Migrate() error = %v", label, err)
		}
		if _, err := store.pool.Exec(ctx, `DELETE FROM streamforge_schema_migrations WHERE version > $1`, migrations[len(migrations)-1].version); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `UPDATE streamforge_schema_migrations SET name=$2, checksum=$3 WHERE version=$1`, first.version, first.name, first.checksum); err != nil {
			t.Fatal(err)
		}
	}
	assertRejected("renamed", `UPDATE streamforge_schema_migrations SET name='renamed.sql' WHERE version=$1`, first.version)
	assertRejected("changed checksum", `UPDATE streamforge_schema_migrations SET checksum='changed' WHERE version=$1`, first.version)
	assertRejected("unknown future", `INSERT INTO streamforge_schema_migrations(version, name, checksum) VALUES (999999, '999999_future.sql', 'future')`)
	if len(migrations) > 1 {
		if _, err := store.pool.Exec(ctx, `DELETE FROM streamforge_schema_migrations WHERE version=$1`, first.version); err != nil {
			t.Fatal(err)
		}
		if err := store.Migrate(ctx); !errors.Is(err, ErrMigrationHistoryGap) {
			t.Fatalf("gap Migrate() error = %v", err)
		}
		if _, err := store.pool.Exec(ctx, `INSERT INTO streamforge_schema_migrations(version, name, checksum) VALUES ($1, $2, $3)`, first.version, first.name, first.checksum); err != nil {
			t.Fatal(err)
		}
	}
	// A table that only happens to have the ledger name is not trusted. The
	// bootstrap may add checksum, but it must not invent missing history names.
	if _, err := store.pool.Exec(ctx, `ALTER TABLE streamforge_schema_migrations DROP COLUMN name`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); !errors.Is(err, ErrMigrationLedgerSchema) {
		t.Fatalf("incompatible ledger schema Migrate() error = %v", err)
	}
}

func TestPostgresProviderWriteIsIdempotentAndDetectsIdentityConflict(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE provider_sink_receipts, normalized_records"); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	message := providerflow.Message{
		Version:     providerflow.Version,
		SourceEvent: providerflow.SourceEvent{ID: "event-1", Source: "feed", Type: "game.updated", Time: at},
		Record:      recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "game-1", Data: json.RawMessage(`{"score":1}`), UpdatedAt: at},
	}
	if err := store.Write(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(ctx, message); err != nil {
		t.Fatalf("duplicate write: %v", err)
	}
	conflict := message
	conflict.Record.Data = json.RawMessage(`{"score":2}`)
	if err := store.Write(ctx, conflict); !errors.Is(err, ErrProviderIdempotencyConflict) {
		t.Fatalf("conflicting write error = %v", err)
	}
	var receipts int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM provider_sink_receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	page, err := store.List(ctx, "games", "v1", nil, 10)
	if err != nil || receipts != 1 || len(page.Records) != 1 || string(page.Records[0].Data) != `{"score":1}` {
		t.Fatalf("receipts=%d page=%#v err=%v", receipts, page, err)
	}
}

func TestPostgresCollectorLeaseFencingAndCheckpoints(t *testing.T) {
	url := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "TRUNCATE collector_checkpoints, collector_assignments"); err != nil {
		t.Fatal(err)
	}

	a := coordination.Assignment{Dataset: "games", Source: "feed", Partition: "0"}
	first, acquired, err := store.Acquire(ctx, a, "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("first acquire = %#v, %v, %v", first, acquired, err)
	}
	if _, acquired, err := store.Acquire(ctx, a, "worker-b", time.Second); err != nil || acquired {
		t.Fatalf("contention acquire = %v, %v", acquired, err)
	}
	if err := store.Advance(ctx, first, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 1, Cursor: []byte("cursor-1")}, Acknowledgement: []byte("ack-1")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, first, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 0, Cursor: []byte("bad")}}); !errors.Is(err, coordination.ErrInvalidCheckpoint) {
		t.Fatalf("invalid checkpoint = %v", err)
	}
	if err := store.Advance(ctx, first, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 1, Cursor: []byte("different")}, Acknowledgement: []byte("ack-1")}); !errors.Is(err, coordination.ErrCheckpointOrder) {
		t.Fatalf("same sequence change = %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	second, acquired, err := store.Acquire(ctx, a, "worker-b", time.Second)
	if err != nil || !acquired || second.Token <= first.Token {
		t.Fatalf("takeover = %#v, %v, %v", second, acquired, err)
	}
	if _, err := store.Renew(ctx, first, time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("stale renew = %v", err)
	}
	if err := store.Advance(ctx, first, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 2, Cursor: []byte("stale")}}); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("stale advance = %v", err)
	}
	if err := store.Advance(ctx, second, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 2, Cursor: []byte("cursor-2")}, Acknowledgement: []byte("ack-2")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, second, coordination.Progress{Checkpoint: coordination.Checkpoint{Sequence: 1, Cursor: []byte("cursor-1")}}); !errors.Is(err, coordination.ErrCheckpointOrder) {
		t.Fatalf("regression = %v", err)
	}
	progress, found, err := store.Checkpoint(ctx, a)
	if err != nil || !found || progress.Checkpoint.Sequence != 2 || string(progress.Acknowledgement) != "ack-2" {
		t.Fatalf("checkpoint = %#v, %v, %v", progress, found, err)
	}
}

func TestPostgresAPIKeyLifecycle(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE api_key_audit, api_key_scopes, api_keys"); err != nil {
		t.Fatal(err)
	}

	digest, _ := security.HashAPIKey("integration-key-that-is-never-persisted")
	record := security.KeyRecord{Digest: digest, Principal: security.Principal{ID: "principal-a", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionRead}, {Dataset: "teams", Action: security.ActionWrite}}}}
	if err := store.CreateKey(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateKey(ctx, record); !errors.Is(err, ErrKeyAlreadyExists) {
		t.Fatalf("duplicate create = %v", err)
	}
	got, found, err := store.LookupKey(ctx, digest)
	if err != nil || !found || got.ID != record.Principal.ID || !security.Authorized(got, "games", security.ActionRead) || !security.Authorized(got, "teams", security.ActionWrite) || security.Authorized(got, "games", security.ActionWrite) {
		t.Fatalf("lookup = %#v, %v, %v", got, found, err)
	}
	if changed, err := store.RevokeKey(ctx, digest); err != nil || !changed {
		t.Fatalf("revoke = %v, %v", changed, err)
	}
	if _, found, err := store.LookupKey(ctx, digest); err != nil || found {
		t.Fatalf("revoked lookup = %v, %v", found, err)
	}
	if changed, err := store.RevokeKey(ctx, digest); err != nil || changed {
		t.Fatalf("second revoke = %v, %v", changed, err)
	}

	old, _ := security.HashAPIKey("old-key-for-rotation")
	if err := store.CreateKey(ctx, security.KeyRecord{Digest: old, Principal: security.Principal{ID: "principal-b", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionRead}}}}); err != nil {
		t.Fatal(err)
	}
	replacement, _ := security.HashAPIKey("replacement-key-for-rotation")
	if err := store.RotateKey(ctx, old, security.KeyRecord{Digest: replacement, Principal: security.Principal{ID: "principal-b", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionWrite}}}}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LookupKey(ctx, old); err != nil || found {
		t.Fatalf("rotated old lookup = %v, %v", found, err)
	}
	if got, found, err := store.LookupKey(ctx, replacement); err != nil || !found || !security.Authorized(got, "games", security.ActionWrite) {
		t.Fatalf("replacement lookup = %#v, %v, %v", got, found, err)
	}
}

func TestPostgresAPIKeyConcurrentCreate(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE api_key_audit, api_key_scopes, api_keys"); err != nil {
		t.Fatal(err)
	}
	digest, _ := security.HashAPIKey("same-key-for-concurrent-test")
	record := security.KeyRecord{Digest: digest, Principal: security.Principal{ID: "principal-concurrent", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionRead}}}}
	results := make(chan error, 2)
	go func() { results <- store.CreateKey(ctx, record) }()
	go func() { results <- store.CreateKey(ctx, record) }()
	var success, duplicate int
	for range 2 {
		if err := <-results; err == nil {
			success++
		} else if errors.Is(err, ErrKeyAlreadyExists) {
			duplicate++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || duplicate != 1 {
		t.Fatalf("success=%d duplicate=%d", success, duplicate)
	}
}

func TestPostgresMaterializationAndKeysetPagination(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE materialization_replay_attempts, materialization_ledger, raw_events, normalized_records"); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, input := range []struct {
		eventID, recordID string
		at                time.Time
	}{
		{"event-a", "a", base}, {"event-b", "b", base}, {"event-c", "c", base.Add(time.Second)},
	} {
		e := event.Event{SpecVersion: event.SpecVersion, ID: input.eventID, Source: "test", Type: "test.v1", Time: input.at, Data: json.RawMessage(`{"raw": true}`)}
		r := recordstore.Record{Dataset: "games", DatasetVersion: "1.0.0", ID: input.recordID, Data: json.RawMessage(`{"ok": true}`), UpdatedAt: input.at}
		inserted, err := store.Admit(ctx, e)
		if err != nil || !inserted {
			t.Fatalf("Admit(%s) = %v, %v", input.eventID, inserted, err)
		}
		ready, err := store.PrepareMaterialization(ctx, e, "games", "1.0.0")
		if err != nil || !ready {
			t.Fatalf("PrepareMaterialization(%s) = %v, %v", input.eventID, ready, err)
		}
		if err := store.CompleteMaterialization(ctx, e, r, "games", "1.0.0"); err != nil {
			t.Fatalf("CompleteMaterialization(%s) = %v", input.eventID, err)
		}
	}
	duplicate := event.Event{SpecVersion: event.SpecVersion, ID: "event-a", Source: "test", Type: "test.v1", Time: base, Data: json.RawMessage(`{"raw": true}`)}
	inserted, err := store.Admit(ctx, duplicate)
	if err != nil || inserted {
		t.Fatalf("duplicate Admit() = %v, %v", inserted, err)
	}
	ready, err := store.PrepareMaterialization(ctx, duplicate, "games", "1.0.0")
	if err != nil || ready {
		t.Fatalf("duplicate PrepareMaterialization() = %v, %v", ready, err)
	}
	first, err := store.List(ctx, "games", "1.0.0", nil, 2)
	if err != nil || len(first.Records) != 2 || first.Next == nil {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	if first.Records[0].ID != "a" || first.Records[1].ID != "b" {
		t.Fatalf("first page IDs = %q, %q", first.Records[0].ID, first.Records[1].ID)
	}
	second, err := store.List(ctx, "games", "1.0.0", first.Next, 2)
	if err != nil || len(second.Records) != 1 || second.Records[0].ID != "c" || second.Next != nil {
		t.Fatalf("second page = %#v, %v", second, err)
	}
}

func TestPostgresExactScalarFilterUsesGINContainmentIndex(t *testing.T) {
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
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, `EXPLAIN (COSTS false) SELECT id FROM normalized_records WHERE dataset=$1 AND dataset_version=$2 AND data @> $3::jsonb`, "games", "v1", `{"status":"live"}`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "normalized_records_data_gin") {
		t.Fatalf("GIN index was not selected:\n%s", plan.String())
	}
}

func TestPostgresFailedMaterializationRequiresExplicitReplay(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE materialization_replay_attempts, materialization_ledger, raw_events, normalized_records"); err != nil {
		t.Fatal(err)
	}
	e := event.Event{SpecVersion: event.SpecVersion, ID: "retry-1", Source: "test", Type: "test.v1", Time: time.Now().UTC(), Data: json.RawMessage(`{"raw":true}`)}
	if admitted, err := store.Admit(ctx, e); err != nil || !admitted {
		t.Fatalf("Admit()=%v,%v", admitted, err)
	}
	if ready, err := store.PrepareMaterialization(ctx, e, "games", "v1"); err != nil || !ready {
		t.Fatalf("Prepare()=%v,%v", ready, err)
	}
	if err := store.FailMaterialization(ctx, e, "games", "v1", errors.New("invalid payload")); err != nil {
		t.Fatal(err)
	}
	if admitted, err := store.Admit(ctx, e); err != nil || admitted {
		t.Fatalf("duplicate Admit()=%v,%v", admitted, err)
	}
	if ready, err := store.PrepareMaterialization(ctx, e, "games", "v1"); err != nil || ready {
		t.Fatalf("duplicate failed Prepare()=%v,%v", ready, err)
	}
	var state string
	var attempts int
	if err := store.pool.QueryRow(ctx, "SELECT state, attempts FROM materialization_ledger WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4", e.Source, e.ID, "games", "v1").Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || attempts != 1 {
		t.Fatalf("ledger=%s attempts=%d", state, attempts)
	}
}

func TestPostgresExplicitReplayIsIdempotentAndCanSucceed(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE materialization_replay_attempts, materialization_ledger, raw_events, normalized_records"); err != nil {
		t.Fatal(err)
	}
	e := event.Event{SpecVersion: event.SpecVersion, ID: "replay-1", Source: "feed", Type: "game.updated", Time: time.Now().UTC(), Data: json.RawMessage(`{"raw":true}`)}
	if admitted, err := store.Admit(ctx, e); err != nil || !admitted {
		t.Fatalf("Admit()=%v,%v", admitted, err)
	}
	if ready, err := store.PrepareMaterialization(ctx, e, "games", "v1"); err != nil || !ready {
		t.Fatalf("Prepare()=%v,%v", ready, err)
	}
	if err := store.FailMaterialization(ctx, e, "games", "v1", errors.New("bad source payload")); err != nil {
		t.Fatal(err)
	}
	request := replay.Request{Source: e.Source, EventID: e.ID, Dataset: "games", DatasetVersion: "v1", RequestID: "operator-1", Actor: "admin@example", Reason: "normalizer fixed"}
	first, err := store.RequestMaterializationReplay(ctx, request)
	if err != nil || !first.Accepted || !reflect.DeepEqual(first.Event, e) {
		t.Fatalf("first replay = %#v, %v", first, err)
	}
	duplicate, err := store.RequestMaterializationReplay(ctx, request)
	if err != nil || duplicate.Accepted || !reflect.DeepEqual(duplicate.Event, e) {
		t.Fatalf("duplicate replay = %#v, %v", duplicate, err)
	}
	failed, err := store.ListFailedMaterializations(ctx, replay.ListRequest{Limit: 10})
	if err != nil || len(failed.Items) != 0 {
		t.Fatalf("failed after request = %#v, %v", failed, err)
	}
	if ready, err := store.PrepareMaterialization(ctx, first.Event, "games", "v1"); err != nil || !ready {
		t.Fatalf("replay Prepare()=%v,%v", ready, err)
	}
	r := recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "replay-record", Data: json.RawMessage(`{"ok":true}`), UpdatedAt: e.Time}
	if err := store.CompleteMaterialization(ctx, first.Event, r, "games", "v1"); err != nil {
		t.Fatal(err)
	}
	var ledgerState, attemptState, originalDiagnostic, actor, reason string
	var attempts int
	if err := store.pool.QueryRow(ctx, "SELECT state, attempts FROM materialization_ledger WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4", e.Source, e.ID, "games", "v1").Scan(&ledgerState, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT state, original_diagnostic_error, actor, reason FROM materialization_replay_attempts WHERE source=$1 AND event_id=$2 AND dataset=$3 AND dataset_version=$4 AND request_id=$5", e.Source, e.ID, "games", "v1", request.RequestID).Scan(&attemptState, &originalDiagnostic, &actor, &reason); err != nil {
		t.Fatal(err)
	}
	if ledgerState != "succeeded" || attemptState != "succeeded" || attempts != 2 || originalDiagnostic != "bad source payload" || actor != request.Actor || reason != request.Reason {
		t.Fatalf("ledger=%s attempts=%d replay=%s diagnostic=%q actor=%q reason=%q", ledgerState, attempts, attemptState, originalDiagnostic, actor, reason)
	}
}

func TestPostgresRecoverableEvents(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE materialization_replay_attempts, materialization_ledger, raw_events, normalized_records"); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	newEvent := func(id string, at time.Time) event.Event {
		return event.Event{SpecVersion: event.SpecVersion, ID: id, Source: "feed", Type: "game.updated", Time: at, Data: json.RawMessage(`{"game":"one"}`)}
	}
	admitAt := func(e event.Event, receivedAt time.Time) {
		t.Helper()
		if admitted, err := store.Admit(ctx, e); err != nil || !admitted {
			t.Fatalf("Admit(%s) = %v, %v", e.ID, admitted, err)
		}
		if _, err := store.pool.Exec(ctx, "UPDATE raw_events SET received_at=$3 WHERE source=$1 AND id=$2", e.Source, e.ID, receivedAt); err != nil {
			t.Fatalf("set received_at for %s: %v", e.ID, err)
		}
	}

	// The second admission from the same source proves recovery progresses past
	// an already admitted event rather than treating source identity as a cursor.
	missing := newEvent("01-missing", base)
	pending := newEvent("02-pending", base.Add(time.Minute))
	succeeded := newEvent("03-succeeded", base.Add(2*time.Minute))
	failed := newEvent("04-failed", base.Add(3*time.Minute))
	foreign := event.Event{SpecVersion: event.SpecVersion, ID: "00-foreign", Source: "other-feed", Type: "game.updated", Time: base, Data: json.RawMessage(`{"game":"other"}`)}
	admitAt(foreign, base.Add(-time.Second))
	admitAt(missing, base)
	admitAt(pending, base.Add(time.Second))
	admitAt(succeeded, base.Add(2*time.Second))
	admitAt(failed, base.Add(3*time.Second))

	if ready, err := store.PrepareMaterialization(ctx, pending, "games", "v1"); err != nil || !ready {
		t.Fatalf("Prepare pending = %v, %v", ready, err)
	}
	if ready, err := store.PrepareMaterialization(ctx, succeeded, "games", "v1"); err != nil || !ready {
		t.Fatalf("Prepare succeeded = %v, %v", ready, err)
	}
	if err := store.CompleteMaterialization(ctx, succeeded, recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "one", Data: json.RawMessage(`{"ok":true}`), UpdatedAt: succeeded.Time}, "games", "v1"); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.PrepareMaterialization(ctx, failed, "games", "v1"); err != nil || !ready {
		t.Fatalf("Prepare failed = %v, %v", ready, err)
	}
	if err := store.FailMaterialization(ctx, failed, "games", "v1", errors.New("deterministic invalid payload")); err != nil {
		t.Fatal(err)
	}

	got, err := store.RecoverableEvents(ctx, "games", "v1", "feed", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != missing.ID || got[1].ID != pending.ID {
		t.Fatalf("recoverable IDs = %#v", eventIDs(got))
	}
	if !reflect.DeepEqual(got[0], missing) {
		t.Fatalf("missing event was not reconstructed: got %#v, want %#v", got[0], missing)
	}
	if !reflect.DeepEqual(got[1], pending) {
		t.Fatalf("pending event was not reconstructed: got %#v, want %#v", got[1], pending)
	}

	limited, err := store.RecoverableEvents(ctx, "games", "v1", "feed", 1)
	if err != nil || len(limited) != 1 || limited[0].ID != missing.ID {
		t.Fatalf("limited recovery = %#v, %v", eventIDs(limited), err)
	}
	// A restart reopens pending ownership through the existing single-owner
	// prepare operation, while failed entries remain absent from recovery.
	if ready, err := store.PrepareMaterialization(ctx, pending, "games", "v1"); err != nil || !ready {
		t.Fatalf("reopen pending = %v, %v", ready, err)
	}
}

func TestPostgresDatasetRevisionsCoexistAndAreReadExactly(t *testing.T) {
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
	if _, err := store.pool.Exec(ctx, "TRUNCATE materialization_replay_attempts, materialization_ledger, raw_events, normalized_records"); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	e := event.Event{SpecVersion: event.SpecVersion, ID: "raw-1", Source: "feed", Type: "game.updated", Time: at, Data: json.RawMessage(`{"game":"one"}`)}
	if admitted, err := store.Admit(ctx, e); err != nil || !admitted {
		t.Fatalf("Admit() = %v, %v", admitted, err)
	}
	for _, revision := range []struct {
		version string
		data    json.RawMessage
	}{
		{version: "v1", data: json.RawMessage(`{"status":"old"}`)},
		{version: "v2", data: json.RawMessage(`{"status":"new"}`)},
	} {
		if ready, err := store.PrepareMaterialization(ctx, e, "games", revision.version); err != nil || !ready {
			t.Fatalf("PrepareMaterialization(%s) = %v, %v", revision.version, ready, err)
		}
		r := recordstore.Record{Dataset: "games", DatasetVersion: revision.version, ID: "game-1", Data: revision.data, UpdatedAt: at}
		if err := store.CompleteMaterialization(ctx, e, r, "games", revision.version); err != nil {
			t.Fatalf("CompleteMaterialization(%s) = %v", revision.version, err)
		}
	}

	for _, want := range []struct {
		version string
		data    string
	}{
		{version: "v1", data: `{"status":"old"}`},
		{version: "v2", data: `{"status":"new"}`},
	} {
		page, err := store.List(ctx, "games", want.version, nil, 10)
		if err != nil {
			t.Fatalf("List(%s): %v", want.version, err)
		}
		if len(page.Records) != 1 || page.Records[0].DatasetVersion != want.version || string(page.Records[0].Data) != want.data {
			t.Fatalf("List(%s) records = %#v; want one %s record with data %s", want.version, page.Records, want.version, want.data)
		}
	}
}

func eventIDs(events []event.Event) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}
