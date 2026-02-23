import json
import math
import sys
from pathlib import Path
from typing import Annotated

import structlog
import typer
from capacity import TiB, byte
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery import Client, QueryJobConfig
from google.oauth2 import service_account

from query_cost_guard.bigquery import _guard_project_errors
from query_cost_guard.pricing import fetch_price_per_byte, get_fallback_price_per_byte

logger = structlog.get_logger()

app = typer.Typer(name="query-cost-guard", help="Per-query cost guard for cloud databases.")

TIB_IN_BYTES = int(TiB // byte)


@app.callback()
def main(
    ctx: typer.Context,
    project: Annotated[str | None, typer.Option(help="GCP project ID")] = None,
    credentials: Annotated[Path | None, typer.Option(help="Path to service account JSON")] = None,
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["project"] = project
    ctx.obj["credentials"] = credentials


@app.command()
def estimate(
    ctx: typer.Context,
    query: Annotated[str | None, typer.Option(help="SQL query to estimate")] = None,
    file: Annotated[Path | None, typer.Option(help="Path to .sql file")] = None,
    max_cost: Annotated[float | None, typer.Option(help="Cost threshold in USD to check against")] = None,
    output_json: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
) -> None:
    sql = _resolve_query(query=query, file=file)
    bq_client = _build_client(project=ctx.obj["project"], credentials=ctx.obj["credentials"])
    price_per_byte = _resolve_pricing()

    job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
    with _guard_project_errors():
        dry_run_job = bq_client.query(sql, job_config=job_config)
    estimated_bytes = dry_run_job.total_bytes_processed
    estimated_cost = estimated_bytes * price_per_byte
    price_per_tib = price_per_byte * TIB_IN_BYTES

    if output_json:
        _print_json(
            estimated_bytes=estimated_bytes,
            estimated_cost_usd=estimated_cost,
            price_per_tib_usd=price_per_tib,
            max_cost_usd=max_cost,
        )
        return

    _print_human(
        estimated_bytes=estimated_bytes,
        estimated_cost=estimated_cost,
        price_per_tib=price_per_tib,
        max_cost=max_cost,
    )


def _resolve_query(*, query: str | None, file: Path | None) -> str:
    if query and file:
        raise typer.BadParameter("Provide --query or --file, not both")

    if query:
        return query

    if file:
        return file.read_text()

    if not sys.stdin.isatty():
        return sys.stdin.read()

    raise typer.BadParameter("Provide --query, --file, or pipe SQL via stdin")


def _build_client(*, project: str | None, credentials: Path | None) -> Client:
    kwargs: dict = {}
    if project:
        kwargs["project"] = project
    if credentials:
        kwargs["credentials"] = service_account.Credentials.from_service_account_file(
            str(credentials),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    return Client(**kwargs)


def _resolve_pricing() -> float:
    try:
        return fetch_price_per_byte()
    except (OSError, ValueError, RuntimeError, GoogleAPICallError) as exc:
        logger.warning("Live pricing unavailable, using fallback", exc_type=type(exc).__name__, reason=str(exc).split("\n")[0])
        return get_fallback_price_per_byte()


def _print_json(
    *,
    estimated_bytes: int,
    estimated_cost_usd: float,
    price_per_tib_usd: float,
    max_cost_usd: float | None,
) -> None:
    result = {
        "estimated_bytes": estimated_bytes,
        "estimated_cost_usd": round(estimated_cost_usd, 6),
        "price_per_tib_usd": round(price_per_tib_usd, 2),
    }
    if max_cost_usd is not None:
        result["max_cost_usd"] = max_cost_usd
        result["pass"] = estimated_cost_usd <= max_cost_usd
    typer.echo(json.dumps(result, indent=2))


def _print_human(
    *,
    estimated_bytes: int,
    estimated_cost: float,
    price_per_tib: float,
    max_cost: float | None,
) -> None:
    typer.echo(f"Estimated bytes:  {estimated_bytes:,}")
    typer.echo(f"Estimated cost:   {_format_cost(estimated_cost)} (at ${price_per_tib:.2f}/TiB)")

    if max_cost is not None:
        passed = estimated_cost <= max_cost
        symbol = "✓ PASS" if passed else "✗ FAIL"
        typer.echo(f"Guard:            {symbol} (limit ${max_cost:.2f})")
        if not passed:
            raise typer.Exit(code=1)


def _format_cost(cost: float) -> str:
    if cost == 0:
        return "$0.00"
    if cost >= 0.01:
        return f"${cost:.4f}"
    decimals = max(4, -math.floor(math.log10(cost)) + 1)
    return f"${cost:.{decimals}f}"
