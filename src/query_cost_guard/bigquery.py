from contextlib import contextmanager

import structlog
from cachetools import TTLCache
from capacity import TiB, byte
from google.api_core.exceptions import BadRequest, Forbidden, GoogleAPICallError
from google.cloud.bigquery import Client, QueryJobConfig
from pydantic import BaseModel, Field, model_validator

from query_cost_guard.constants import PRICING_CACHE_TTL_SECONDS, OnPricingFailure
from query_cost_guard.exceptions import PricingUnavailableError, QueryCostExceededContext, QueryCostExceededError
from query_cost_guard.pricing import fetch_price_per_byte, get_fallback_price_per_byte
from query_cost_guard.result import EstimateResult, QueryResult

logger = structlog.get_logger()

TIB_IN_BYTES = int(TiB // byte)


class QueryParams(BaseModel, frozen=True):
    max_cost_usd: float | None = Field(default=None, gt=0)
    max_bytes: int | None = Field(default=None, gt=0)
    query_tag: str | None = None

    @model_validator(mode="after")
    def exactly_one_threshold(self):
        if (self.max_cost_usd is None) == (self.max_bytes is None):
            raise ValueError("Exactly one of max_cost_usd or max_bytes must be provided")
        return self


class QueryCostGuard:
    def __init__(
        self,
        *,
        client: Client,
        on_pricing_failure: OnPricingFailure = OnPricingFailure.WARN,
        price_per_tib_override: float | None = None,
        pricing_cache_ttl_seconds: int = PRICING_CACHE_TTL_SECONDS,
    ) -> None:
        self._client = client
        self._on_pricing_failure = on_pricing_failure
        self._price_per_tib_override = price_per_tib_override
        self._pricing_cache: TTLCache[str, float] = TTLCache(maxsize=1, ttl=pricing_cache_ttl_seconds)

    def query(
        self,
        *,
        sql: str,
        params: QueryParams,
        job_config: QueryJobConfig | None = None,
        location: str | None = None,
    ) -> QueryResult:
        price_per_byte = self._resolve_price_per_byte()

        resolved_max_bytes = (
            int(params.max_cost_usd / price_per_byte)
            if params.max_cost_usd is not None
            else params.max_bytes
        )

        merged_config = _merge_job_config(job_config=job_config, maximum_bytes_billed=resolved_max_bytes)

        try:
            with _guard_project_errors():
                query_job = self._client.query(sql, job_config=merged_config, location=location)
            rows = [dict(row) for row in query_job.result()]
        except Forbidden as exc:
            if not _is_bytes_billed_exceeded(exc):
                raise
            self._handle_cost_exceeded(
                params=params,
                resolved_max_bytes=resolved_max_bytes,
                price_per_byte=price_per_byte,
                exc=exc,
            )

        return self._build_result(query_job=query_job, rows=rows, params=params, price_per_byte=price_per_byte)

    def estimate(
        self,
        *,
        sql: str,
        job_config: QueryJobConfig | None = None,
        location: str | None = None,
    ) -> EstimateResult:
        price_per_byte = self._resolve_price_per_byte()
        dry_run_config = job_config or QueryJobConfig()
        dry_run_config.dry_run = True
        dry_run_config.use_query_cache = False

        with _guard_project_errors():
            dry_run_job = self._client.query(sql, job_config=dry_run_config, location=location)
        estimated_bytes = dry_run_job.total_bytes_processed

        return EstimateResult(
            estimated_bytes=estimated_bytes,
            estimated_cost_usd=estimated_bytes * price_per_byte,
            price_per_tib_usd=price_per_byte * TIB_IN_BYTES,
        )

    def _handle_cost_exceeded(
        self,
        *,
        params: QueryParams,
        resolved_max_bytes: int,
        price_per_byte: float,
        exc: Forbidden,
    ) -> None:
        logger.warning(
            "Query rejected by BigQuery cost guard",
            query_tag=params.query_tag,
            max_bytes_allowed=resolved_max_bytes,
            max_cost_usd=params.max_cost_usd,
            error=str(exc),
        )
        raise QueryCostExceededError(
            context=QueryCostExceededContext(
                estimated_cost_usd=resolved_max_bytes * price_per_byte,
                max_cost_usd=params.max_cost_usd,
                max_bytes=resolved_max_bytes,
                bytes_estimated=resolved_max_bytes,
                query_tag=params.query_tag,
            ),
        ) from exc

    def _build_result(self, *, query_job, rows: list[dict], params: QueryParams, price_per_byte: float) -> QueryResult:
        bytes_billed = query_job.total_bytes_billed or 0
        bytes_processed = query_job.total_bytes_processed or 0
        duration_seconds = (query_job.ended - query_job.started).total_seconds()
        actual_cost_usd = bytes_billed * price_per_byte
        price_per_tib_used = price_per_byte * TIB_IN_BYTES

        logger.info(
            "Query executed",
            query_tag=params.query_tag,
            bytes_billed=bytes_billed,
            bytes_processed=bytes_processed,
            actual_cost_usd=actual_cost_usd,
            max_cost_usd=params.max_cost_usd,
            max_bytes=params.max_bytes,
            duration_seconds=duration_seconds,
            price_per_tib_used=price_per_tib_used,
        )

        return QueryResult(
            rows=rows,
            actual_cost_usd=actual_cost_usd,
            bytes_billed=bytes_billed,
            bytes_processed=bytes_processed,
            duration_seconds=duration_seconds,
            price_per_tib_used=price_per_tib_used,
            query_tag=params.query_tag,
        )

    def _resolve_price_per_byte(self) -> float:
        if self._price_per_tib_override is not None:
            return self._price_per_tib_override / TIB_IN_BYTES

        if (cached := self._pricing_cache.get("price_per_byte")) is not None:
            return cached

        try:
            price_per_byte = fetch_price_per_byte()
        except (OSError, ValueError, RuntimeError, GoogleAPICallError) as exc:
            if self._on_pricing_failure == OnPricingFailure.RAISE:
                raise PricingUnavailableError(reason=str(exc)) from exc
            logger.warning(
                "Live pricing unavailable, using static fallback",
                exc_type=type(exc).__name__,
                reason=str(exc).split("\n")[0],
            )
            price_per_byte = get_fallback_price_per_byte()

        self._pricing_cache["price_per_byte"] = price_per_byte
        return price_per_byte


def _merge_job_config(*, job_config: QueryJobConfig | None, maximum_bytes_billed: int) -> QueryJobConfig:
    config = job_config or QueryJobConfig()
    config.maximum_bytes_billed = maximum_bytes_billed
    return config


def _is_bytes_billed_exceeded(exc: Forbidden) -> bool:
    return any(error.get("reason") == "billingTierLimitExceeded" for error in (exc.errors or []))


@contextmanager
def _guard_project_errors():
    try:
        yield
    except BadRequest as exc:
        if "ProjectId must be non-empty" in str(exc):
            raise ValueError(
                "GCP project not found or inaccessible. Verify the project ID is correct."
            ) from exc
        raise
