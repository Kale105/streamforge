package postgres

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/coordination"
	"github.com/Kale105/streamforge/internal/recordstore"
	"github.com/Kale105/streamforge/internal/replay"
)

func TestValidateCoordinationLease(t *testing.T) {
	assignment := coordination.Assignment{Dataset: "games", Source: "feed", Partition: "0"}
	if err := validateCoordinationLease(coordination.Lease{Assignment: assignment, Holder: "worker", Token: 1}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(validateCoordinationLease(coordination.Lease{}), coordination.ErrInvalidAssignment) {
		t.Fatal("empty lease accepted")
	}
}

func TestMigrationFilesAreOrderedAndUsable(t *testing.T) {
	migrations, err := migrationFiles()
	if err != nil {
		t.Fatalf("migrationFiles() error = %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("migrationFiles() returned no migrations")
	}
	for i, migration := range migrations {
		if migration.version <= 0 || migration.sql == "" || len(migration.checksum) != 64 {
			t.Fatalf("invalid migration: %#v", migration)
		}
		if i > 0 && migrations[i-1].version >= migration.version {
			t.Fatalf("migrations are not strictly ordered: %#v", migrations)
		}
	}
}

func TestValidateMigrationHistory(t *testing.T) {
	embedded := []migration{
		{version: 1, name: "0001_initial.sql", checksum: "one"},
		{version: 2, name: "0002_next.sql", checksum: "two"},
	}
	checksum := "one"
	backfill, err := validateMigrationHistory(embedded, []appliedMigration{{version: 1, name: "0001_initial.sql", checksum: &checksum}})
	if err != nil || len(backfill) != 0 {
		t.Fatalf("matching history = %#v, %v", backfill, err)
	}
	backfill, err = validateMigrationHistory(embedded, []appliedMigration{{version: 1, name: "0001_initial.sql"}})
	if err != nil || len(backfill) != 1 || backfill[0].version != 1 {
		t.Fatalf("legacy checksum backfill = %#v, %v", backfill, err)
	}
	for name, applied := range map[string][]appliedMigration{
		"gap":      {{version: 2, name: "0002_next.sql", checksum: ptr("two")}},
		"unknown":  {{version: 3, name: "0003_future.sql", checksum: ptr("three")}},
		"renamed":  {{version: 1, name: "renamed.sql", checksum: ptr("one")}},
		"changed":  {{version: 1, name: "0001_initial.sql", checksum: ptr("changed")}},
		"too many": {{version: 1, name: "0001_initial.sql", checksum: ptr("one")}, {version: 2, name: "0002_next.sql", checksum: ptr("two")}, {version: 3, name: "0003_future.sql", checksum: ptr("three")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateMigrationHistory(embedded, applied); err == nil {
				t.Fatal("validateMigrationHistory() error = nil")
			}
		})
	}
}

func ptr(value string) *string { return &value }

func TestValidateReplayRequests(t *testing.T) {
	validRequest := replay.Request{Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1", RequestID: "admin-1", Actor: "operator@example", Reason: "schema fix"}
	if err := validateReplayRequest(validRequest); err != nil {
		t.Fatalf("validateReplayRequest(valid) error = %v", err)
	}
	for name, request := range map[string]replay.Request{
		"source":          {EventID: "one", Dataset: "games", DatasetVersion: "v1", RequestID: "x", Actor: "a", Reason: "r"},
		"event":           {Source: "feed", Dataset: "games", DatasetVersion: "v1", RequestID: "x", Actor: "a", Reason: "r"},
		"dataset":         {Source: "feed", EventID: "one", DatasetVersion: "v1", RequestID: "x", Actor: "a", Reason: "r"},
		"version":         {Source: "feed", EventID: "one", Dataset: "games", RequestID: "x", Actor: "a", Reason: "r"},
		"request id":      {Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1", Actor: "a", Reason: "r"},
		"actor":           {Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1", RequestID: "x", Reason: "r"},
		"reason":          {Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1", RequestID: "x", Actor: "a"},
		"long request id": {Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1", RequestID: string(make([]byte, 257)), Actor: "a", Reason: "r"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateReplayRequest(request); err == nil {
				t.Fatal("validateReplayRequest() error = nil")
			}
		})
	}
	validCursor := &replay.Cursor{UpdatedAt: time.Now(), Source: "feed", EventID: "one", Dataset: "games", DatasetVersion: "v1"}
	if err := validateReplayListRequest(replay.ListRequest{Limit: 1, After: validCursor}); err != nil {
		t.Fatalf("validateReplayListRequest(valid) error = %v", err)
	}
	for name, request := range map[string]replay.ListRequest{
		"limit":  {},
		"cursor": {Limit: 1, After: &replay.Cursor{}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateReplayListRequest(request); err == nil {
				t.Fatal("validateReplayListRequest() error = nil")
			}
		})
	}
}

func TestValidateRecord(t *testing.T) {
	valid := recordstore.Record{Dataset: "games", DatasetVersion: "v1", ID: "one", Data: json.RawMessage(`{"score": 1}`), UpdatedAt: time.Now()}
	if err := validateRecord(valid); err != nil {
		t.Fatalf("validateRecord(valid) error = %v", err)
	}
	for name, record := range map[string]recordstore.Record{
		"dataset": {DatasetVersion: "v1", ID: "one", Data: valid.Data, UpdatedAt: valid.UpdatedAt},
		"version": {Dataset: "games", ID: "one", Data: valid.Data, UpdatedAt: valid.UpdatedAt},
		"id":      {Dataset: "games", DatasetVersion: "v1", Data: valid.Data, UpdatedAt: valid.UpdatedAt},
		"data":    {Dataset: "games", DatasetVersion: "v1", ID: "one", Data: json.RawMessage(`{`), UpdatedAt: valid.UpdatedAt},
		"time":    {Dataset: "games", DatasetVersion: "v1", ID: "one", Data: valid.Data},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRecord(record); err == nil {
				t.Fatal("validateRecord() error = nil")
			}
		})
	}
}

func TestValidateMaterializationRevision(t *testing.T) {
	for name, input := range map[string]struct {
		dataset string
		version string
		valid   bool
	}{
		"valid":         {dataset: "games", version: "v1", valid: true},
		"empty dataset": {version: "v1"},
		"empty version": {dataset: "games"},
		"both empty":    {},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateMaterializationRevision(input.dataset, input.version)
			if (err == nil) != input.valid {
				t.Fatalf("validateMaterializationRevision(%q, %q) error = %v, want valid=%v", input.dataset, input.version, err, input.valid)
			}
		})
	}
}

func TestValidateRecoveryRequest(t *testing.T) {
	for name, input := range map[string]struct {
		dataset string
		version string
		source  string
		limit   int
		valid   bool
	}{
		"valid":          {dataset: "games", version: "v1", source: "feed", limit: 1, valid: true},
		"empty dataset":  {version: "v1", source: "feed", limit: 1},
		"empty version":  {dataset: "games", source: "feed", limit: 1},
		"empty source":   {dataset: "games", version: "v1", limit: 1},
		"zero limit":     {dataset: "games", version: "v1", source: "feed"},
		"negative limit": {dataset: "games", version: "v1", source: "feed", limit: -1},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateRecoveryRequest(input.dataset, input.version, input.source, input.limit)
			if (err == nil) != input.valid {
				t.Fatalf("validateRecoveryRequest(%q, %q, %q, %d) error = %v, want valid=%v", input.dataset, input.version, input.source, input.limit, err, input.valid)
			}
		})
	}
}
