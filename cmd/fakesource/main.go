// Command fakesource provides deterministic local data for the Compose quickstart.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	var sequence atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /events/latest", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":       "sample-1",
			"category": "demo",
			"value":    sequence.Add(1),
		})
	})
	server := &http.Server{
		Addr:              ":8090",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
