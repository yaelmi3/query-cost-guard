# Changelog

## v0.1.0 (unreleased)

Initial release.

### Features

- `QueryCostGuard` class — wraps a `google.cloud.bigquery.Client` and enforces per-query cost limits via `maximum_bytes_billed`
- Dollar-based limits (`max_cost_usd`) automatically converted to bytes using live pricing from the Google Cloud Billing Catalog API
- Byte-based limits (`max_bytes`) passed through directly
- 24-hour TTL cache for live pricing; graceful fallback to static `$6.25/TiB` on API unavailability (`OnPricingFailure.WARN`) or hard failure (`OnPricingFailure.RAISE`)
- `QueryCostGuard.estimate()` — dry-run cost estimation without executing the query
- `QueryResult` and `EstimateResult` — immutable pydantic models with bytes billed, actual cost, duration, and price used
- `QueryCostExceededError` — raised with full context (estimated cost, limit, query tag) when BigQuery rejects a job for exceeding `maximum_bytes_billed`
- CLI (`query-cost-guard estimate`) — dry-run estimation from `--query`, `--file`, or stdin; human-readable and `--json` output; optional `--max-cost` threshold with pass/fail and non-zero exit code on failure
