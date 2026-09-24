# Load-generator benchmark evidence

`cmd/loadgen` is a bounded, concurrent HTTP `GET` harness for the read API. It is intentionally a measurement tool, not a capacity claim or a replacement for a distributed load-testing system.

Each run emits a versioned JSON report. It records a redacted exact command, request configuration, start/end/duration, request/error/status counts, nearest-rank p50/p95/p99 definitions, Go/runtime metadata, executable/build information, Git commit when available, and the optional raw-report path. API keys are excluded from output; sensitive URL query parameters and metadata keys are rejected.

## Local reproducible run

Start a local API target. Put a disposable key in the environment instead of command history, then capture an immutable raw result for each measured run:

```powershell
$env:STREAMFORGE_LOADGEN_API_KEY = "replace-with-a-disposable-test-key"
New-Item -ItemType Directory -Force .\evidence\baseline | Out-Null
go run ./cmd/loadgen -url http://127.0.0.1:8080/v1/sample-events -duration 60s -concurrency 8 -request-timeout 2s -metadata profile=baseline -metadata host=local -metadata dataset=sample-events -output .\evidence\baseline\run-1.json
go run ./cmd/loadgen -url http://127.0.0.1:8080/v1/sample-events -duration 60s -concurrency 8 -request-timeout 2s -metadata profile=baseline -metadata host=local -metadata dataset=sample-events -output .\evidence\baseline\run-2.json
go run ./cmd/loadgen -url http://127.0.0.1:8080/v1/sample-events -duration 60s -concurrency 8 -request-timeout 2s -metadata profile=baseline -metadata host=local -metadata dataset=sample-events -output .\evidence\baseline\run-3.json
go run ./cmd/loadgen -aggregate-run .\evidence\baseline\run-1.json -aggregate-run .\evidence\baseline\run-2.json -aggregate-run .\evidence\baseline\run-3.json > .\evidence\baseline\summary.json
```

The generator still writes the same run report to stdout, so redirecting output is also supported. `-output` writes the raw file with owner-only permissions where the OS supports them and records that path in the report.

## External target

Use the same command from the actual generator host, replace only the URL and deliberately chosen metadata, and save the server-side logs/metrics for the same interval:

```bash
STREAMFORGE_LOADGEN_API_KEY='disposable-test-key' \
  go run ./cmd/loadgen -url 'https://api.example.test/v1/sample-events' \
  -duration 60s -concurrency 8 -request-timeout 2s \
  -metadata profile=baseline -metadata host=runner-a -metadata dataset=sample-events \
  -output ./evidence/baseline/run-1.json
```

Do not put real keys, bearer tokens, passwords, or customer data into URLs, metadata, shell history, or committed evidence.

## Evidence policy

An aggregate summary is refused unless it receives at least three valid raw reports with one `profile` and an identical target, duration, concurrency, timeout, and non-profile metadata. It sums request/error/status counts. It deliberately leaves aggregate percentiles as `0` because individual reports do not contain raw histograms; reconstructing a combined p95/p99 from three percentile values would be false precision. Compare the p50/p95/p99 values in each raw run instead.

Latency is bucketed to whole milliseconds. p50/p95/p99 use the nearest-rank definition over all requests in that one run; a request at or over the configured timeout occupies the final bucket. HTTP responses, including non-2xx responses, are listed by status. Transport and body-limit failures are listed as errors.

Warm up separately and do not retain it as evidence. Keep dataset revision, endpoint parameters, auth mode, rate limits, worker/API replica counts, database/broker configuration, and network placement unchanged across the three measured runs. Preserve API metrics, server logs, dependency versions, and any fault/recovery notes alongside raw JSON. Do not infer availability, durability, throughput capacity, or cross-host scalability from this harness alone.

## Profile names

| Profile | Purpose | Starting settings |
| --- | --- | --- |
| `smoke` | Verify target/auth/report shape | 10s, concurrency 1, timeout 2s |
| `baseline` | Repeatable single-host comparison | 60s, concurrency 8, timeout 2s |
| `saturation` | Controlled contention experiment | 60s; increase concurrency stepwise |
| `recovery` | Observe a documented dependency interruption | 60s, concurrency 8; record fault and catch-up |
