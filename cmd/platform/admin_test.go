package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Kale105/streamforge/internal/apigateway/security"
)

func TestParseScopes(t *testing.T) {
	scopes, err := parseScopes("games:read, teams:write")
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) != 2 || scopes[0] != (security.Scope{Dataset: "games", Action: security.ActionRead}) {
		t.Fatalf("scopes = %#v", scopes)
	}
	for _, raw := range []string{"", "games", "games:delete", "games:read,games:read"} {
		if _, err := parseScopes(raw); err == nil {
			t.Fatalf("parseScopes(%q) succeeded", raw)
		}
	}
}

func TestDatasetReadScopeProtectsOnlyConfiguredReadRoute(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/games?limit=1", nil)
	dataset, action, protected := datasetReadScope(request, "/v1/games", "games")
	if !protected || dataset != "games" || action != security.ActionRead {
		t.Fatalf("scope = %q %q %v", dataset, action, protected)
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
		httptest.NewRequest(http.MethodPost, "/v1/games", nil),
	} {
		if _, _, protected := datasetReadScope(request, "/v1/games", "games"); protected {
			t.Fatalf("unexpected protected route %s %s", request.Method, request.URL.Path)
		}
	}
	head := httptest.NewRequest(http.MethodHead, "/v1/games", nil)
	if _, _, protected := datasetReadScope(head, "/v1/games", "games"); !protected {
		t.Fatal("HEAD dataset read was not protected")
	}
}

func TestGeneratedAPIKeyAndDigest(t *testing.T) {
	plaintext, digest, err := generateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(plaintext, "sf_") || len(plaintext) < 40 {
		t.Fatalf("generated key has unexpected shape")
	}
	want, err := security.HashAPIKey(plaintext)
	if err != nil || digest != want {
		t.Fatal("generated digest does not match key")
	}
	parsed, err := parseDigest(strings.ToUpper(fmtDigest(digest)))
	if err != nil || parsed != digest {
		t.Fatal("digest did not round trip")
	}
}

func TestParseDigestRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{"", "abc", strings.Repeat("z", 64), strings.Repeat("00", 33)} {
		if _, err := parseDigest(raw); err == nil {
			t.Fatalf("parseDigest(%q) succeeded", raw)
		}
	}
}

func fmtDigest(digest [32]byte) string {
	const alphabet = "0123456789abcdef"
	result := make([]byte, 64)
	for i, value := range digest {
		result[i*2] = alphabet[value>>4]
		result[i*2+1] = alphabet[value&15]
	}
	return string(result)
}
