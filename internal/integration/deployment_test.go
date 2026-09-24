package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Kale105/streamforge/internal/config"
	"gopkg.in/yaml.v3"
)

// Structural tests are a deliberate fallback where Docker/Helm are unavailable.
// scripts/verify-provider.ps1 runs their native render/config gates when present.
func TestProviderComposeStructure(t *testing.T) {
	body, err := os.ReadFile("../../compose.provider.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Services map[string]map[string]any `yaml:"services"`
		Volumes  map[string]any            `yaml:"volumes"`
	}
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatalf("parse provider Compose: %v", err)
	}
	for _, service := range []string{"postgres", "redpanda", "topic-init", "fake-source", "collector", "processor", "sink", "dlq-indexer", "replay", "query", "provider-admin"} {
		value := document.Services[service]
		if value == nil {
			t.Errorf("missing provider service %q", service)
			continue
		}
		if service != "topic-init" && service != "provider-admin" && value["healthcheck"] == nil {
			t.Errorf("service %q lacks a supported healthcheck", service)
		}
		if service != "topic-init" && value["stop_grace_period"] == nil {
			t.Errorf("service %q lacks graceful termination", service)
		}
	}
	for _, volume := range []string{"provider-postgres", "provider-redpanda"} {
		if _, ok := document.Volumes[volume]; !ok {
			t.Errorf("missing provider volume %q", volume)
		}
	}
	if !strings.Contains(string(body), "single-broker") || !strings.Contains(string(body), "development") {
		t.Error("Compose must explicitly identify its single-broker development scope")
	}
	for _, role := range []string{"collector", "processor", "sink", "dlq-indexer", "replay"} {
		service := document.Services[role]
		if !strings.Contains(strings.Join(anyStrings(service["command"]), " "), "-admin-addr") {
			t.Errorf("worker %s does not enable its admin listener", role)
		}
	}
}

func TestProviderExampleIsPinnedAndLoadable(t *testing.T) {
	root := filepath.Join("..", "..", "deploy", "provider-example")
	if _, err := config.LoadDataset(filepath.Join(root, "dataset.yaml")); err != nil {
		t.Fatalf("provider example must be a valid pinned package: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(root, "dataset.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ file, digest string }{
		{"raw.schema.json", "84f660ec1b9fdebb846d32e4616e300a35046f907d67a159f7518a455b202949"},
		{"normalized.schema.json", "3f2a59ccfbee8476edc44b6f95a28a1f07af014f7702eae4776a11e1d96e439b"},
	} {
		contents, err := os.ReadFile(filepath.Join(root, item.file))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(contents)
		if actual := hex.EncodeToString(sum[:]); actual != item.digest || !strings.Contains(string(body), item.digest) {
			t.Errorf("%s digest is not pinned to its actual bytes", item.file)
		}
	}
}

func TestHelmProviderStaticPolicy(t *testing.T) {
	base := filepath.Join("..", "..", "deploy", "helm", "streamforge")
	deployment := mustRead(t, filepath.Join(base, "templates", "deployments.yaml"))
	values := mustRead(t, filepath.Join(base, "values.yaml"))
	schema := mustRead(t, filepath.Join(base, "values.schema.json"))
	for _, role := range []string{"collector", "processor", "sink", "dlqIndexer", "replay", "query", "providerAdmin"} {
		if !strings.Contains(deployment, `"`+role+`"`) {
			t.Errorf("Helm deployment template omits role %q", role)
		}
	}
	for _, token := range []string{"livenessProbe", "readinessProbe", "terminationGracePeriodSeconds", "automountServiceAccountToken: false", "kafka-tls", "checksum/dataset-config", "secretKeyRef"} {
		if !strings.Contains(deployment, token) {
			t.Errorf("Helm deployment template missing %q", token)
		}
	}
	if strings.Contains(values, "tag: \"\"") || strings.Contains(values, "tag: latest") || !strings.Contains(schema, `"const": "latest"`) {
		t.Error("Helm values must require a non-latest immutable image tag")
	}
	for _, secret := range []string{"hardcoded-demo-password", "postgres://", "password: change-me"} {
		if strings.Contains(values, secret) {
			t.Errorf("Helm defaults contain plaintext secret material %q", secret)
		}
	}
}

func TestProviderVerifierUsesWritableTempCache(t *testing.T) {
	script := mustRead(t, filepath.Join("..", "..", "scripts", "verify-provider.ps1"))
	for _, token := range []string{"streamforge-go-build", "New-Item -ItemType Directory -Force -Path $env:GOCACHE", "[System.IO.File]::WriteAllText", "SKIP docker compose config", "SKIP helm lint/template"} {
		if !strings.Contains(script, token) {
			t.Errorf("provider verifier must contain %q", token)
		}
	}
}

func anyStrings(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, item.(string))
	}
	return result
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
