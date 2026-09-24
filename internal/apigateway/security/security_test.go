package security

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"
)

func TestMemoryKeyStoreUsesDigestAndCopiesPrincipal(t *testing.T) {
	store := NewMemoryKeyStore()
	digest, err := HashAPIKey("opaque-secret")
	if err != nil {
		t.Fatal(err)
	}
	p := Principal{ID: "client-a", Scopes: []Scope{{Dataset: "games", Action: ActionRead}}}
	if err := store.PutDigest(digest, p); err != nil {
		t.Fatal(err)
	}
	p.Scopes[0].Dataset = "changed"
	got, ok, err := store.LookupKey(context.Background(), digest)
	if err != nil || !ok || got.Scopes[0].Dataset != "games" {
		t.Fatalf("lookup = %#v, %v, %v", got, ok, err)
	}
	if _, ok, _ := store.LookupKey(context.Background(), sha256.Sum256([]byte("wrong"))); ok {
		t.Fatal("unexpected key match")
	}
}

func TestAuthorized(t *testing.T) {
	p := Principal{Scopes: []Scope{{Dataset: "games", Action: ActionRead}, {Dataset: "*", Action: ActionWrite}}}
	for _, tc := range []struct {
		dataset string
		action  Action
		want    bool
	}{
		{"games", ActionRead, true}, {"teams", ActionRead, false}, {"teams", ActionWrite, true}, {"", ActionRead, false},
	} {
		if got := Authorized(p, tc.dataset, tc.action); got != tc.want {
			t.Errorf("Authorized(%q, %q) = %v", tc.dataset, tc.action, got)
		}
	}
}

func TestLimiterRefillsAndRoundsRetryAfter(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	limiter, err := NewLimiter(RateLimit{Rate: 2, Burst: 2}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("first request rejected")
	}
	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("second request rejected")
	}
	if ok, retry := limiter.Allow("a"); ok || retry != time.Second {
		t.Fatalf("third = %v, %v", ok, retry)
	}
	clock.Advance(500 * time.Millisecond)
	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("request after refill rejected")
	}
}

func TestLimiterConcurrent(t *testing.T) {
	limiter, err := NewLimiter(RateLimit{Rate: 1, Burst: 100}, systemClock{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	accepted := 0
	var acceptedMu sync.Mutex
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := limiter.Allow("same-client"); ok {
				acceptedMu.Lock()
				accepted++
				acceptedMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 100 {
		t.Fatalf("accepted = %d; want 100", accepted)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }
