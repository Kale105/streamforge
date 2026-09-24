package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

func TestWebhookAuthJSONAndBackpressure(t *testing.T) {
	c, err := New(Config{Address: "127.0.0.1:0", Source: "urn:test", Type: "test.event", MaxBodyBytes: 32, MaxHeaderBytes: 4096, RequestTimeout: 80 * time.Millisecond, Secret: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Collect(ctx, func(context.Context, event.Event) error { return errors.New("full") }) }()
	deadline := time.Now().Add(time.Second)
	for c.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	body := `{"ok":true}`
	req, _ := http.NewRequest(http.MethodPost, "http://"+c.Addr(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	bad, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", bad.StatusCode)
	}
	bad.Body.Close()
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write([]byte(body))
	req, _ = http.NewRequest(http.MethodPost, "http://"+c.Addr(), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Signature", hex.EncodeToString(mac.Sum(nil)))
	good, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if good.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("backpressure status %d", good.StatusCode)
	}
	good.Body.Close()
	big, _ := http.NewRequest(http.MethodPost, "http://"+c.Addr(), strings.NewReader(`{"long":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`))
	big.Header.Set("Content-Type", "application/json")
	over, err := http.DefaultClient.Do(big)
	if err != nil {
		t.Fatal(err)
	}
	if over.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("size status %d", over.StatusCode)
	}
	over.Body.Close()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestWebhookDeterministicID(t *testing.T) {
	if stableID("s", []byte(`{"a":1,"b":2}`)) != stableID("s", []byte(` { "b" : 2, "a":1 } `)) {
		t.Fatal("IDs differ")
	}
}

func TestWebhookParsesContentTypeAndCopiesSecret(t *testing.T) {
	secret := []byte("secret")
	c, err := New(Config{Address: "127.0.0.1:0", Source: "urn:test", Type: "test.event", MaxBodyBytes: 32, MaxHeaderBytes: 4096, RequestTimeout: time.Second, Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	secret[0] = 'X'
	body := []byte(`{"ok":true}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	request := func(contentType string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "http://example.test", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", contentType)
		r.Header.Set("X-Webhook-Signature", hex.EncodeToString(mac.Sum(nil)))
		return r
	}
	for _, contentType := range []string{"application/json; charset=utf-8", "application/json; charset=\"utf-8\""} {
		w := httptest.NewRecorder()
		c.handler(func(context.Context, event.Event) error { return nil }).ServeHTTP(w, request(contentType))
		if w.Code != http.StatusAccepted {
			t.Fatalf("content type %q: status %d", contentType, w.Code)
		}
	}
	w := httptest.NewRecorder()
	c.handler(func(context.Context, event.Event) error { return nil }).ServeHTTP(w, request("application/json; charset"))
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("malformed content type status %d", w.Code)
	}
}
