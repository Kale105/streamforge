package postgres

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/Kale105/streamforge/internal/apigateway/security"
)

func TestValidateKeyRecord(t *testing.T) {
	valid := security.KeyRecord{
		Digest:    sha256.Sum256([]byte("not a plaintext key in storage")),
		Principal: security.Principal{ID: "principal-1", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionRead}}},
	}
	if err := validateKeyRecord(valid); err != nil {
		t.Fatalf("valid record: %v", err)
	}
	for _, record := range []security.KeyRecord{
		{},
		{Digest: valid.Digest},
		{Digest: valid.Digest, Principal: security.Principal{ID: " ", Scopes: valid.Principal.Scopes}},
		{Digest: valid.Digest, Principal: security.Principal{ID: "p"}},
		{Digest: valid.Digest, Principal: security.Principal{ID: "p", Scopes: []security.Scope{{Dataset: "", Action: security.ActionRead}}}},
		{Digest: valid.Digest, Principal: security.Principal{ID: "p", Scopes: []security.Scope{{Dataset: "games", Action: "delete"}}}},
		{Digest: valid.Digest, Principal: security.Principal{ID: "p", Scopes: []security.Scope{{Dataset: "games", Action: security.ActionRead}, {Dataset: "games", Action: security.ActionRead}}}},
	} {
		if err := validateKeyRecord(record); !errors.Is(err, ErrInvalidKeyRecord) {
			t.Errorf("validateKeyRecord(%#v) = %v", record.Principal, err)
		}
	}
}

func TestValidateKeyRecordAllowsExactDistinctScopes(t *testing.T) {
	record := security.KeyRecord{Digest: sha256.Sum256([]byte("digest-only")), Principal: security.Principal{ID: "tenant-a", Scopes: []security.Scope{
		{Dataset: "games", Action: security.ActionRead}, {Dataset: "games", Action: security.ActionWrite}, {Dataset: "teams", Action: security.ActionRead},
	}}}
	if err := validateKeyRecord(record); err != nil {
		t.Fatal(err)
	}
}
