package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/recordstore"
)

// TestBenchmarkServer is an opt-in local benchmark fixture. It exercises the
// real API handler while keeping database performance out of the measurement.
func TestBenchmarkServer(t *testing.T) {
	if os.Getenv("STREAMFORGE_BENCH_SERVER") != "1" {
		t.Skip("set STREAMFORGE_BENCH_SERVER=1 to run the local fixture")
	}
	reader := benchmarkReader{records: makeBenchmarkRecords(100)}
	handler, err := New(reader, readinessStub{}, Config{
		Datasets: map[string]DatasetPolicy{"games": {
			Path: "/v1/games", Version: "v1", Fields: []string{"score", "state"},
		}},
		DefaultPageSize: 100, MaximumPageSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Addr: "127.0.0.1:18080", Handler: handler, ReadHeaderTimeout: 2 * time.Second}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}

func BenchmarkListAPI100Records(b *testing.B) {
	reader := benchmarkReader{records: makeBenchmarkRecords(100)}
	handler, err := New(reader, readinessStub{}, Config{
		Datasets: map[string]DatasetPolicy{"games": {
			Path: "/v1/games", Version: "v1", Fields: []string{"score", "state"},
		}},
		DefaultPageSize: 100, MaximumPageSize: 100,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request, err := http.NewRequest(http.MethodGet, "/v1/games?limit=100", nil)
		if err != nil {
			b.Fatal(err)
		}
		response := &benchmarkResponseWriter{header: make(http.Header)}
		handler.ServeHTTP(response, request)
		if response.status != http.StatusOK {
			b.Fatalf("status = %d", response.status)
		}
	}
}

type benchmarkReader struct{ records []recordstore.Record }

func (r benchmarkReader) List(context.Context, string, string, *recordstore.Cursor, int) (recordstore.Page, error) {
	return recordstore.Page{Records: r.records}, nil
}

func makeBenchmarkRecords(count int) []recordstore.Record {
	records := make([]recordstore.Record, count)
	for i := range records {
		records[i] = recordstore.Record{
			Dataset: "games", DatasetVersion: "v1", ID: "game",
			Data:      json.RawMessage(`{"score":118,"state":"final","internal":"hidden"}`),
			UpdatedAt: time.Unix(1_700_000_000+int64(i), 0).UTC(),
		}
	}
	return records
}

type benchmarkResponseWriter struct {
	header http.Header
	status int
}

func (w *benchmarkResponseWriter) Header() http.Header    { return w.header }
func (w *benchmarkResponseWriter) WriteHeader(status int) { w.status = status }
func (w *benchmarkResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(p), nil
}
