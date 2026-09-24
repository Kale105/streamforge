package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShutdownHTTPForcesAllServersAfterSharedDeadline(t *testing.T) {
	release := make(chan struct{})
	publicStarted := make(chan struct{})
	metricsStarted := make(chan struct{})
	public, publicDone := lifecycleServer(t, publicStarted, release)
	metrics, metricsDone := lifecycleServer(t, metricsStarted, release)

	go func() { _, _ = http.Get("http://" + public.Addr) }()
	go func() { _, _ = http.Get("http://" + metrics.Addr) }()
	<-publicStarted
	<-metricsStarted

	err := shutdownHTTP(50*time.Millisecond, public, metrics)
	if err == nil {
		t.Fatal("shutdown succeeded despite handlers that refuse to return")
	}
	close(release)
	for _, done := range []<-chan error{publicDone, metricsDone} {
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve error = %v", err)
		}
	}
}

func TestPersonalServeRejectsProviderCollectorBeforeDatabaseOrListeners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.yaml")
	contents := `apiVersion: streamforge.dev/v1alpha1
kind: Dataset
metadata:
  name: games
  version: v1
collector:
  type: sse
  sse:
    url: https://example.test/events
    requestTimeout: 1s
    maxMessageBytes: 1024
    maxLineBytes: 512
normalization:
  key: id
  fields:
    - source: id
      target: id
storage:
  engine: postgres
api:
  enabled: true
  path: /v1/games
  fields: [id]
  filters: []
  sorts: []
  pagination:
    type: cursor
    default: 1
    maximum: 1
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	err := serve(context.Background(), serverOptions{configPath: path, databaseURL: "postgres://unreachable", listenAddress: "127.0.0.1:0", drainTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "sse collectors require provider mode") {
		t.Fatalf("personal serve error=%v", err)
	}
}

func lifecycleServer(t *testing.T, started chan<- struct{}, release <-chan struct{}) (*http.Server, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Addr: listener.Addr().String(), Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		started <- struct{}{}
		<-release
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	return server, done
}
