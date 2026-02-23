# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install with all extras
uv sync --extra cli --extra dev

# Run all tests
uv run pytest tests/ -v

# Run a single test
uv run pytest tests/test_bigquery.py::test_query_returns_query_result_with_metrics -v

# Lint
uv run ruff check src/ tests/
uv run pylint src/query_cost_guard tests
```

## Architecture

**query-cost-guard** is a standalone PyPI package that adds per-query dollar-based cost limits to BigQuery. It wraps `google.cloud.bigquery.Client` via dependency injection (no subclassing) and converts USD limits into `maximum_bytes_billed` using live pricing from the Google Cloud Billing Catalog API.

### Core execution flow

```
guard.query(sql, params=QueryParams(max_cost_usd=0.50))
  → resolve pricing (TTLCache → Billing API → fallback $6.25/TiB)
  → convert USD → bytes: max_bytes = max_cost_usd / price_per_byte
  → merge into QueryJobConfig.maximum_bytes_billed (preserves caller config)
  → execute query (BigQuery enforces server-side — atomic, no dry-run race)
  → extract bytes_billed, duration from QueryJob
  → compute actual_cost_usd = bytes_billed * price_per_byte
  → log structured metrics via structlog
  → return QueryResult
```

### Key design decisions

- **Server-side enforcement**: `maximum_bytes_billed` is set on the job config, not a dry-run check, so there's no race condition between estimation and execution. BigQuery raises a `BillingTierLimitExceeded` 400 error which is caught and re-raised as `QueryCostExceededError`.
- **Pricing resolution**: Three-tier — `TTLCache` (24h) → `CloudCatalogClient` Billing API → static fallback. `OnPricingFailure.WARN` (default) silently logs and falls back; `OnPricingFailure.RAISE` raises `PricingUnavailableError`.
- **Pydantic frozen models**: `QueryResult`, `EstimateResult`, `QueryCostExceededContext` are all frozen pydantic v2 models for immutability and validation.
- **Keyword-only API**: `QueryParams` uses `*` to prevent positional argument errors.
- **`None` vs `0`**: `None` means unmeasured (e.g., bytes not available from job), `0` means measured zero.

### Module responsibilities

| Module | Responsibility |
|---|---|
| `bigquery.py` | `QueryCostGuard` class — query/estimate entry points, config merging, cost conversion |
| `pricing.py` | Billing Catalog API, SKU parsing, unit normalization (TiBy → bytes), TTL cache |
| `result.py` | `QueryResult`, `EstimateResult` pydantic models |
| `exceptions.py` | `QueryCostExceededError`, `PricingUnavailableError`, `QueryCostExceededContext` |
| `constants.py` | `OnPricingFailure` enum, `DEFAULT_PRICE_PER_TIB_USD`, cache TTL |
| `cli.py` | typer CLI — `estimate` subcommand (dry-run, JSON/human output, stdin/file/--query input) |

## Code conventions

- 120-char line length enforced by pylint (soft limit in ruff)
- No docstrings (pylint `missing-docstring` disabled)
- structlog for all logging — no f-strings in log calls
- Type hints on all functions and pydantic fields
