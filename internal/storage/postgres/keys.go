package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/Kale105/streamforge/internal/apigateway/security"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidKeyRecord = errors.New("invalid API key record")
	ErrKeyAlreadyExists = errors.New("API key already exists")
	ErrKeyNotFound      = errors.New("API key not found")
)

var _ security.KeyLifecycleStore = (*Store)(nil)

// CreateKey persists an already-computed SHA-256 digest and exact scopes. It
// never accepts or writes plaintext API key material.
func (s *Store) CreateKey(ctx context.Context, record security.KeyRecord) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := validateKeyRecord(record); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.New("begin API key creation")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := createKey(ctx, tx, record); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit API key creation")
	}
	return nil
}

// LookupKey returns enabled keys only. Revoked keys intentionally look absent.
func (s *Store) LookupKey(ctx context.Context, digest [sha256.Size]byte) (security.Principal, bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return security.Principal{}, false, err
	}
	var principal security.Principal
	err := s.pool.QueryRow(ctx, `SELECT principal_id FROM api_keys WHERE digest = $1 AND enabled`, digest[:]).Scan(&principal.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return security.Principal{}, false, nil
	}
	if err != nil {
		return security.Principal{}, false, errors.New("lookup API key")
	}
	rows, err := s.pool.Query(ctx, `SELECT dataset, action FROM api_key_scopes WHERE digest = $1 ORDER BY dataset, action`, digest[:])
	if err != nil {
		return security.Principal{}, false, errors.New("lookup API key scopes")
	}
	defer rows.Close()
	for rows.Next() {
		var scope security.Scope
		if err := rows.Scan(&scope.Dataset, &scope.Action); err != nil {
			return security.Principal{}, false, errors.New("scan API key scope")
		}
		principal.Scopes = append(principal.Scopes, scope)
	}
	if rows.Err() != nil {
		return security.Principal{}, false, errors.New("iterate API key scopes")
	}
	return principal, true, nil
}

// RevokeKey disables a key once and appends a revocation audit event.
func (s *Store) RevokeKey(ctx context.Context, digest [sha256.Size]byte) (bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, errors.New("begin API key revocation")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE api_keys SET enabled = FALSE, revoked_at = now() WHERE digest = $1 AND enabled`, digest[:])
	if err != nil {
		return false, errors.New("revoke API key")
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, errors.New("commit API key revocation")
		}
		return false, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO api_key_audit (digest, event_type) VALUES ($1, 'revoked')`, digest[:]); err != nil {
		return false, errors.New("audit API key revocation")
	}
	if err := tx.Commit(ctx); err != nil {
		return false, errors.New("commit API key revocation")
	}
	return true, nil
}

// RotateKey atomically provisions replacement and revokes the old digest.
func (s *Store) RotateKey(ctx context.Context, previous [sha256.Size]byte, replacement security.KeyRecord) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return err
	}
	if err := validateKeyRecord(replacement); err != nil {
		return err
	}
	if previous == replacement.Digest {
		return ErrInvalidKeyRecord
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.New("begin API key rotation")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT enabled FROM api_keys WHERE digest = $1 FOR UPDATE`, previous[:]).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrKeyNotFound
	}
	if err != nil {
		return errors.New("load API key for rotation")
	}
	if !enabled {
		return ErrKeyNotFound
	}
	if err := createKey(ctx, tx, replacement); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE api_keys SET enabled = FALSE, revoked_at = now() WHERE digest = $1`, previous[:]); err != nil {
		return errors.New("revoke rotated API key")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO api_key_audit (digest, event_type) VALUES ($1, 'revoked')`, previous[:]); err != nil {
		return errors.New("audit API key rotation")
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit API key rotation")
	}
	return nil
}

func createKey(ctx context.Context, tx pgx.Tx, record security.KeyRecord) error {
	_, err := tx.Exec(ctx, `INSERT INTO api_keys (digest, principal_id) VALUES ($1, $2)`, record.Digest[:], record.Principal.ID)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrKeyAlreadyExists
		}
		return errors.New("create API key")
	}
	for _, scope := range record.Principal.Scopes {
		if _, err := tx.Exec(ctx, `INSERT INTO api_key_scopes (digest, dataset, action) VALUES ($1, $2, $3)`, record.Digest[:], scope.Dataset, scope.Action); err != nil {
			return errors.New("create API key scope")
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO api_key_audit (digest, event_type) VALUES ($1, 'created')`, record.Digest[:]); err != nil {
		return errors.New("audit API key creation")
	}
	return nil
}

func validateKeyRecord(record security.KeyRecord) error {
	if record.Digest == ([sha256.Size]byte{}) || strings.TrimSpace(record.Principal.ID) == "" || len(record.Principal.ID) > 256 || len(record.Principal.Scopes) == 0 {
		return ErrInvalidKeyRecord
	}
	seen := make(map[security.Scope]struct{}, len(record.Principal.Scopes))
	for _, scope := range record.Principal.Scopes {
		if scope.Dataset == "" || len(scope.Dataset) > 256 || (scope.Action != security.ActionRead && scope.Action != security.ActionWrite) {
			return ErrInvalidKeyRecord
		}
		if _, ok := seen[scope]; ok {
			return ErrInvalidKeyRecord
		}
		seen[scope] = struct{}{}
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
