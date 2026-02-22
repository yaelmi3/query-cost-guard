<p align="center">
  <img src="logo.png" alt="query-cost-guard" width="200">
</p>

# query-cost-guard

Per-query cost guard for BigQuery. Set dollar or byte limits before any query runs.

## Why

BigQuery charges per byte scanned. A single bad query can burn through your daily quota.
The native `maximum_bytes_billed` only speaks in bytes — this package lets you think in dollars.

## Install

```bash
pip install query-cost-guard

# With CLI
pip install query-cost-guard[cli]
```

## Usage

### Dollar-based guard

```python
from google.cloud.bigquery import Client
from query_cost_guard import QueryCostGuard, QueryParams

guard = QueryCostGuard(client=Client())

result = guard.query(
    sql="SELECT * FROM `project.dataset.table` WHERE date > '2026-01-01'",
    params=QueryParams(max_cost_usd=0.50),
)

result.rows              # list[dict]
result.actual_cost_usd   # 0.03
result.bytes_billed      # 4_831_838_208
result.duration_seconds  # 2.41
```

### Byte-based guard

```python
result = guard.query(
    sql="SELECT * FROM `project.dataset.table`",
    params=QueryParams(max_bytes=5 * 1024**4),  # 5 TiB
)
```

`max_cost_usd` and `max_bytes` are mutually exclusive.

### When cost exceeds the limit

BigQuery rejects the query **before execution** — no bytes are billed.

```python
from query_cost_guard import QueryCostExceededError, QueryParams

try:
    result = guard.query(sql=huge_query, params=QueryParams(max_cost_usd=0.10))
except QueryCostExceededError as e:
    e.context.estimated_cost_usd  # 2.34
    e.context.max_cost_usd        # 0.10
    e.context.bytes_estimated     # 374_000_000_000
```

### With existing job config

```python
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

result = guard.query(
    sql=sql,
    params=QueryParams(max_cost_usd=1.00, query_tag="observation_troubleshooting"),
    job_config=QueryJobConfig(
        query_parameters=[ScalarQueryParameter("account_id", "INT64", 115)]
    ),
    location="US",
)
```

### Pricing

Live pricing is fetched from the [Cloud Billing Catalog API](https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list) and cached for 24 hours.
If the API is unavailable, falls back to the static rate of **$6.25/TiB**.

```python
from query_cost_guard import QueryCostGuard, OnPricingFailure

# Fail hard if pricing can't be resolved
guard = QueryCostGuard(client=client, on_pricing_failure=OnPricingFailure.RAISE)

# Or pin a known price
guard = QueryCostGuard(client=client, price_per_tib_override=6.25)
```

### Dry-run estimation

```python
estimate = guard.estimate(sql="SELECT * FROM `project.dataset.big_table`")
estimate.estimated_bytes       # 4_831_838_208
estimate.estimated_cost_usd    # 0.0275
estimate.price_per_tib_usd     # 6.25
```

## CLI

Estimate query cost without executing:

```bash
query-cost-guard --project my-gcp-project estimate \
  --query "SELECT * FROM \`project.dataset.table\`"

# Estimated bytes:  4,831,838,208
# Estimated cost:   $0.0275 (at $6.25/TiB)
```

With a cost threshold:

```bash
query-cost-guard --project my-gcp-project estimate \
  --query "SELECT * FROM big_table" \
  --max-cost 0.50

# Guard:  ✓ PASS (limit $0.50)
```

JSON output, file input, stdin:

```bash
query-cost-guard --project my-proj estimate --query "SELECT 1" --json
query-cost-guard --project my-proj estimate --file query.sql
cat query.sql | query-cost-guard --project my-proj estimate
```

Credentials default to Application Default Credentials. Override with `--credentials /path/to/sa.json`.

## How it works

Uses BigQuery's `maximum_bytes_billed` on the `QueryJobConfig`. Single API call, no race condition, enforced server-side.
Both `estimate()` and the CLI use `dry_run=True` for zero-cost estimation.

## License

MIT
