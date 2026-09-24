// Package security supplies transport-agnostic API-key authentication,
// authorization, and per-principal request limiting for an API gateway.
//
// It deliberately stores and passes only SHA-256 API-key digests. Callers must
// provision a key over a secure channel and store its digest, not its plaintext.
package security

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

// Action is an operation that can be granted on a dataset.
type Action string

const (
	ActionRead  Action = "read"
	ActionWrite Action = "write"
	// ActionProviderAdmin is intentionally separate from dataset read/write.
	// It gates provider DLQ inspection and replay orchestration.
	ActionProviderAdmin Action = "provider-admin"
)

// Scope grants Action on Dataset. A Dataset of "*" grants that action on all
// datasets. Dataset names are intentionally matched exactly otherwise.
type Scope struct {
	Dataset string
	Action  Action
}

// Principal is the non-secret identity associated with an API key. ID must be
// stable and non-empty; it is used only as an internal limiter key.
type Principal struct {
	ID     string
	Scopes []Scope
}

// KeyStore permits an eventual durable key repository. Implementations receive
// only a SHA-256 digest, so plaintext API keys cannot be persisted by this API.
type KeyStore interface {
	LookupKey(context.Context, [sha256.Size]byte) (Principal, bool, error)
}

// KeyRecord is the non-secret material required to provision an API key. Digest
// must be calculated before this value reaches a repository; plaintext keys are
// deliberately not accepted by this API.
type KeyRecord struct {
	Digest    [sha256.Size]byte
	Principal Principal
}

// KeyLifecycleStore is the administrative counterpart to KeyStore. Rotation is
// an atomic create-and-revoke operation: callers generate the replacement key
// and supply only its digest.
type KeyLifecycleStore interface {
	KeyStore
	CreateKey(context.Context, KeyRecord) error
	RevokeKey(context.Context, [sha256.Size]byte) (bool, error)
	RotateKey(context.Context, [sha256.Size]byte, KeyRecord) error
}

// HashAPIKey returns the fixed-size SHA-256 digest used for key lookup. It does
// not retain the supplied key. An empty key is invalid and returns an error.
func HashAPIKey(key string) ([sha256.Size]byte, error) {
	if key == "" {
		return [sha256.Size]byte{}, errors.New("API key is empty")
	}
	return sha256.Sum256([]byte(key)), nil
}

// Authorized reports whether principal can perform action on dataset.
func Authorized(principal Principal, dataset string, action Action) bool {
	if dataset == "" || action == "" {
		return false
	}
	for _, scope := range principal.Scopes {
		if scope.Action == action && (scope.Dataset == dataset || scope.Dataset == "*") {
			return true
		}
	}
	return false
}

// MemoryKeyStore is a concurrency-safe in-memory KeyStore for local use and
// tests. Its map is indexed by the complete fixed-size digest; it never stores
// plaintext key material. Production persistence can implement KeyStore.
type MemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[[sha256.Size]byte]Principal
}

func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{keys: make(map[[sha256.Size]byte]Principal)}
}

// PutDigest installs or replaces a principal by its already-computed digest.
func (s *MemoryKeyStore) PutDigest(digest [sha256.Size]byte, principal Principal) error {
	if s == nil {
		return errors.New("key store is nil")
	}
	if principal.ID == "" {
		return errors.New("principal ID is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[digest] = clonePrincipal(principal)
	return nil
}

func (s *MemoryKeyStore) LookupKey(_ context.Context, digest [sha256.Size]byte) (Principal, bool, error) {
	if s == nil {
		return Principal{}, false, errors.New("key store is nil")
	}
	s.mu.RLock()
	principal, ok := s.keys[digest]
	s.mu.RUnlock()
	return clonePrincipal(principal), ok, nil
}

func clonePrincipal(p Principal) Principal {
	p.Scopes = append([]Scope(nil), p.Scopes...)
	return p
}

// Clock makes rate-limiting behavior deterministic in tests.
type Clock interface{ Now() time.Time }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// RateLimit configures a token bucket. Rate is replenished tokens per second;
// Burst is its maximum capacity. Both values must be positive.
type RateLimit struct {
	Rate  float64
	Burst int
}

// QuotaRequest contains the minimum non-secret identity needed to make a
// quota decision. It is deliberately suitable for a remote/shared quota
// service: callers never supply an API key or its digest.
type QuotaRequest struct {
	PrincipalID string
	Dataset     string
}

// QuotaDecision is the result of one quota check. RetryAfter is meaningful
// only when Allowed is false.
type QuotaDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

// QuotaChecker is the replaceable quota boundary. Implementations may use a
// local token bucket in personal mode or a shared service for API replicas.
// An error means the decision is unknown and callers must fail closed.
type QuotaChecker interface {
	CheckQuota(context.Context, QuotaRequest) (QuotaDecision, error)
}

// Usage is a deliberately non-billing request observation. It contains no
// credentials, key digest, request body, path, or query data.
type Usage struct {
	PrincipalID string
	Dataset     string
	Status      int
}

// UsageRecorder is an optional, replaceable non-billing usage sink. Recording
// failures must not change a successful API response.
type UsageRecorder interface {
	RecordUsage(context.Context, Usage) error
}

func (r RateLimit) valid() bool { return r.Rate > 0 && r.Burst > 0 }

// Limiter maintains independent token buckets keyed by principal ID.
type Limiter struct {
	mu           sync.Mutex
	clock        Clock
	defaultLimit RateLimit
	buckets      map[string]bucket
}

type bucket struct {
	tokens  float64
	updated time.Time
}

func NewLimiter(limit RateLimit, clock Clock) (*Limiter, error) {
	if !limit.valid() {
		return nil, errors.New("rate and burst must be positive")
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Limiter{clock: clock, defaultLimit: limit, buckets: make(map[string]bucket)}, nil
}

// Allow consumes one token for principalID. When rejected, retryAfter is rounded
// up to a whole second as required by the HTTP Retry-After header.
func (l *Limiter) Allow(principalID string) (allowed bool, retryAfter time.Duration) {
	if l == nil || principalID == "" {
		return false, time.Second
	}
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, exists := l.buckets[principalID]
	if !exists {
		b = bucket{tokens: float64(l.defaultLimit.Burst), updated: now}
	}
	if now.After(b.updated) {
		b.tokens += now.Sub(b.updated).Seconds() * l.defaultLimit.Rate
		if b.tokens > float64(l.defaultLimit.Burst) {
			b.tokens = float64(l.defaultLimit.Burst)
		}
		b.updated = now
	}
	if b.tokens >= 1 {
		b.tokens--
		l.buckets[principalID] = b
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.defaultLimit.Rate * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	l.buckets[principalID] = b
	return false, wait
}

// CheckQuota adapts the local token bucket to QuotaChecker. The context is
// checked before consuming a token so canceled requests never mutate state.
func (l *Limiter) CheckQuota(ctx context.Context, request QuotaRequest) (QuotaDecision, error) {
	if err := ctx.Err(); err != nil {
		return QuotaDecision{}, err
	}
	if request.PrincipalID == "" || request.Dataset == "" {
		return QuotaDecision{}, errors.New("quota principal and dataset are required")
	}
	allowed, retryAfter := l.Allow(request.PrincipalID)
	return QuotaDecision{Allowed: allowed, RetryAfter: retryAfter}, nil
}
