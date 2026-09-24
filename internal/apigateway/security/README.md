# API gateway security

`security` is an isolated HTTP middleware package for future API-gateway wiring.
It hashes the `X-API-Key` header with SHA-256 before lookup, grants exact
dataset/action scopes (with an explicit `*` dataset wildcard), and limits each
authenticated principal with a mutex-protected token bucket.

Configure a `KeyStore`, `RequestScope`, and optional `Limiter`, then wrap the
gateway handler with `Middleware.Wrap`. `MemoryKeyStore` is intended for local
development and tests; durable systems should implement `KeyStore` and persist
only the `[32]byte` SHA-256 digest. Error bodies are generic JSON and never
include credentials or hashes. The limiter accepts a `Clock` for deterministic
tests and returns a rounded-up duration used for `Retry-After`.
