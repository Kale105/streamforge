package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Kale105/streamforge/internal/apigateway/security"
	"github.com/Kale105/streamforge/internal/config"
	"github.com/Kale105/streamforge/internal/materializer"
	"github.com/Kale105/streamforge/internal/normalizer"
	"github.com/Kale105/streamforge/internal/provideradmin"
	replaymodel "github.com/Kale105/streamforge/internal/replay"
	"github.com/Kale105/streamforge/internal/storage/postgres"
)

func runKey(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: platform key <create|revoke> [options]")
	}
	switch args[0] {
	case "create":
		return runKeyCreate(args[1:])
	case "revoke":
		return runKeyRevoke(args[1:])
	default:
		return fmt.Errorf("unknown key command %q; expected create or revoke", args[0])
	}
}

func runKeyCreate(args []string) error {
	flags := flag.NewFlagSet("key create", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	principal := flags.String("principal", "", "stable API consumer identity")
	scopeList := flags.String("scopes", "", "comma-separated dataset:read or dataset:write scopes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("key create does not accept positional arguments")
	}
	if *principal == "" {
		return errors.New("key create requires -principal")
	}
	scopes, err := parseScopes(*scopeList)
	if err != nil {
		return err
	}
	plaintext, digest, err := generateAPIKey()
	if err != nil {
		return err
	}
	ctx := context.Background()
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.CreateKey(ctx, security.KeyRecord{Digest: digest, Principal: security.Principal{ID: *principal, Scopes: scopes}}); err != nil {
		return err
	}
	// The plaintext is intentionally shown exactly once. Only its digest was
	// passed to persistence; operators must transfer it over a secure channel.
	fmt.Fprintf(os.Stdout, "api_key=%s\nsha256=%x\n", plaintext, digest)
	return nil
}

func runKeyRevoke(args []string) error {
	flags := flag.NewFlagSet("key revoke", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	digestText := flags.String("digest", "", "hex SHA-256 digest printed when the key was created")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("key revoke does not accept positional arguments")
	}
	digest, err := parseDigest(*digestText)
	if err != nil {
		return err
	}
	ctx := context.Background()
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	revoked, err := store.RevokeKey(ctx, digest)
	if err != nil {
		return err
	}
	if !revoked {
		return errors.New("API key was not active")
	}
	fmt.Fprintln(os.Stdout, "API key revoked")
	return nil
}

func runFailures(args []string) error {
	flags := flag.NewFlagSet("failures", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	limit := flags.Int("limit", 100, "maximum failures to return")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("failures does not accept positional arguments")
	}
	ctx := context.Background()
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	page, err := store.ListFailedMaterializations(ctx, replaymodel.ListRequest{Limit: *limit})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(page)
}

func runReplay(args []string) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	configPath := flags.String("config", "dataset.yaml", "dataset YAML file")
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	eventID := flags.String("event-id", "", "failed source event ID")
	requestID := flags.String("request-id", "", "idempotency ID; generated when omitted")
	actor := flags.String("actor", "", "operator identity recorded in the audit trail")
	reason := flags.String("reason", "", "reason for replay")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("replay does not accept positional arguments")
	}
	if *eventID == "" || *actor == "" || *reason == "" {
		return errors.New("replay requires -event-id, -actor, and -reason")
	}
	if *requestID == "" {
		generated, err := randomID(16)
		if err != nil {
			return err
		}
		*requestID = generated
	}
	spec, err := config.LoadDataset(*configPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	normalize, err := normalizer.New(spec)
	if err != nil {
		return fmt.Errorf("construct normalizer: %w", err)
	}
	destination, err := materializer.New(normalize, store, spec.Metadata.Name, spec.Metadata.Version)
	if err != nil {
		return fmt.Errorf("construct materializer: %w", err)
	}
	result, err := store.RequestMaterializationReplay(ctx, replaymodel.Request{
		Source:         "urn:streamforge:dataset:" + spec.Metadata.Name,
		EventID:        *eventID,
		Dataset:        spec.Metadata.Name,
		DatasetVersion: spec.Metadata.Version,
		RequestID:      *requestID,
		Actor:          *actor,
		Reason:         *reason,
	})
	if err != nil {
		return err
	}
	if err := destination.Handle(ctx, result.Event); err != nil {
		return fmt.Errorf("execute replay request %s: %w", *requestID, err)
	}
	fmt.Fprintf(os.Stdout, "replay %s completed\n", *requestID)
	return nil
}

func parseScopes(raw string) ([]security.Scope, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("key create requires at least one -scopes entry")
	}
	parts := strings.Split(raw, ",")
	scopes := make([]security.Scope, 0, len(parts))
	seen := make(map[security.Scope]struct{}, len(parts))
	for _, part := range parts {
		datasetName, actionText, ok := strings.Cut(strings.TrimSpace(part), ":")
		action := security.Action(actionText)
		if !ok || datasetName == "" || (action != security.ActionRead && action != security.ActionWrite && action != security.ActionProviderAdmin) {
			return nil, fmt.Errorf("invalid scope %q; expected dataset:read, dataset:write, or *:provider-admin", part)
		}
		scope := security.Scope{Dataset: datasetName, Action: action}
		if _, duplicate := seen[scope]; duplicate {
			return nil, fmt.Errorf("duplicate scope %q", part)
		}
		seen[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

func generateAPIKey() (string, [32]byte, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", [32]byte{}, fmt.Errorf("generate API key: %w", err)
	}
	plaintext := "sf_" + base64.RawURLEncoding.EncodeToString(secret)
	digest, err := security.HashAPIKey(plaintext)
	return plaintext, digest, err
}

func randomID(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate request ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func parseDigest(raw string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(digest) {
		return digest, errors.New("digest must be exactly 64 hexadecimal characters")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func openMigratedStore(ctx context.Context, databaseURL string) (*postgres.Store, error) {
	if databaseURL == "" {
		return nil, errors.New("database URL is required through -database-url or STREAMFORGE_DATABASE_URL")
	}
	store, err := postgres.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// runProviderAdmin is deliberately a separate server process. It does not
// mount personal-mode ledger routes and accepts only provider-admin scoped API
// keys, keeping provider replay authority isolated from ordinary data reads.
func runProviderAdmin(args []string) error {
	flags := flag.NewFlagSet("provider-admin", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("STREAMFORGE_DATABASE_URL"), "PostgreSQL connection URL")
	listen := flags.String("listen", ":8081", "provider admin HTTP listen address")
	clusterID := flags.String("cluster-id", "local", "provider Kafka cluster identity")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("provider-admin does not accept positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := openMigratedStore(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	service, err := provideradmin.New(store.ProviderDLQ(), providerAdminAuthorizer{keys: store}, provideradmin.Config{ClusterID: *clusterID})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *listen, Handler: service, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

type providerAdminAuthorizer struct{ keys security.KeyStore }

func (a providerAdminAuthorizer) Authorize(ctx context.Context, _ string) (string, error) {
	// The provideradmin package owns response shaping; this adapter only maps
	// the existing API-key store to its minimal authorization contract.
	request, ok := provideradmin.RequestFromContext(ctx)
	if !ok || request == nil {
		return "", provideradmin.ErrUnauthorized
	}
	values := request.Header.Values(security.APIKeyHeader)
	if len(values) != 1 {
		return "", provideradmin.ErrUnauthorized
	}
	digest, err := security.HashAPIKey(values[0])
	if err != nil {
		return "", provideradmin.ErrUnauthorized
	}
	principal, found, err := a.keys.LookupKey(ctx, digest)
	if err != nil || !found {
		return "", provideradmin.ErrUnauthorized
	}
	if !security.Authorized(principal, "*", security.ActionProviderAdmin) {
		return "", provideradmin.ErrForbidden
	}
	return principal.ID, nil
}
