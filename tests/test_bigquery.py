from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from capacity import TiB, byte
from google.api_core.exceptions import Forbidden
from google.cloud.bigquery import QueryJobConfig
from pydantic import ValidationError

from query_cost_guard.bigquery import QueryCostGuard, QueryParams, _merge_job_config
from query_cost_guard.constants import OnPricingFailure
from query_cost_guard.exceptions import PricingUnavailableError, QueryCostExceededError

TIB_IN_BYTES = int(TiB // byte)
FALLBACK_PRICE_PER_BYTE = 6.25 / TIB_IN_BYTES


def test_query_params_requires_exactly_one_threshold():
    with pytest.raises(ValidationError, match="Exactly one"):
        QueryParams(max_cost_usd=1.0, max_bytes=1000)


def test_query_params_requires_at_least_one_threshold():
    with pytest.raises(ValidationError, match="Exactly one"):
        QueryParams()


def test_query_params_accepts_max_cost_usd():
    params = QueryParams(max_cost_usd=5.0)
    assert params.max_cost_usd == 5.0
    assert params.max_bytes is None


def test_query_params_accepts_max_bytes():
    params = QueryParams(max_bytes=1_000_000)
    assert params.max_bytes == 1_000_000
    assert params.max_cost_usd is None


def test_query_params_rejects_zero_max_cost():
    with pytest.raises(ValidationError):
        QueryParams(max_cost_usd=0)


def test_query_params_rejects_negative_max_bytes():
    with pytest.raises(ValidationError):
        QueryParams(max_bytes=-1)


def test_query_params_is_frozen():
    params = QueryParams(max_cost_usd=1.0)
    with pytest.raises(ValidationError):
        params.max_cost_usd = 2.0


def test_merge_job_config_creates_new_when_none():
    config = _merge_job_config(job_config=None, maximum_bytes_billed=5000)
    assert config.maximum_bytes_billed == 5000


def test_merge_job_config_preserves_existing_settings():
    original = QueryJobConfig()
    original.use_legacy_sql = True
    merged = _merge_job_config(job_config=original, maximum_bytes_billed=9999)
    assert merged.maximum_bytes_billed == 9999
    assert merged.use_legacy_sql is True
    assert merged is original


def _make_mock_client(*, total_bytes_billed=1_000_000, total_bytes_processed=2_000_000, duration_seconds=1.5):
    client = MagicMock()
    started = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    ended = started + timedelta(seconds=duration_seconds)
    job = MagicMock()
    job.result.return_value = [{"col": "val"}]
    job.total_bytes_billed = total_bytes_billed
    job.total_bytes_processed = total_bytes_processed
    job.started = started
    job.ended = ended
    client.query.return_value = job
    return client


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_with_max_cost_usd_sets_maximum_bytes_billed(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client)
    guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0))

    called_config = client.query.call_args.kwargs.get("job_config") or client.query.call_args[1].get("job_config")
    expected_max_bytes = int(1.0 / FALLBACK_PRICE_PER_BYTE)
    assert called_config.maximum_bytes_billed == expected_max_bytes


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_with_max_bytes_passes_directly(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client)
    guard.query(sql="SELECT 1", params=QueryParams(max_bytes=500_000_000))

    called_config = client.query.call_args.kwargs.get("job_config") or client.query.call_args[1].get("job_config")
    assert called_config.maximum_bytes_billed == 500_000_000


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_returns_query_result_with_metrics(_mock_pricing):
    client = _make_mock_client(total_bytes_billed=5_000_000, total_bytes_processed=6_000_000, duration_seconds=2.0)
    guard = QueryCostGuard(client=client)
    result = guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=10.0, query_tag="my_tag"))

    assert result.rows == [{"col": "val"}]
    assert result.bytes_billed == 5_000_000
    assert result.bytes_processed == 6_000_000
    assert result.duration_seconds == 2.0
    assert result.query_tag == "my_tag"
    assert result.actual_cost_usd == 5_000_000 * FALLBACK_PRICE_PER_BYTE
    assert result.price_per_tib_used == FALLBACK_PRICE_PER_BYTE * TIB_IN_BYTES


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_raises_query_cost_exceeded_on_forbidden(_mock_pricing):
    client = MagicMock()
    client.query.side_effect = Forbidden("Query exceeded limit")
    guard = QueryCostGuard(client=client)

    with pytest.raises(QueryCostExceededError) as exc_info:
        guard.query(sql="SELECT *", params=QueryParams(max_cost_usd=0.01, query_tag="expensive"))

    assert exc_info.value.context.query_tag == "expensive"
    assert exc_info.value.context.max_cost_usd == 0.01


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_passes_location_to_client(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client)
    guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0), location="US")

    assert client.query.call_args.kwargs["location"] == "US"


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_preserves_caller_job_config(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client)
    caller_config = QueryJobConfig(use_legacy_sql=True)
    guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0), job_config=caller_config)

    called_config = client.query.call_args.kwargs.get("job_config") or client.query.call_args[1].get("job_config")
    assert called_config.use_legacy_sql is True
    assert called_config.maximum_bytes_billed is not None


def test_price_per_tib_override_skips_api():
    client = _make_mock_client()
    guard = QueryCostGuard(client=client, price_per_tib_override=10.0)
    result = guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0))

    expected_price_per_byte = 10.0 / TIB_IN_BYTES
    assert result.price_per_tib_used == 10.0
    assert result.actual_cost_usd == 1_000_000 * expected_price_per_byte


@patch("query_cost_guard.bigquery.fetch_price_per_byte", side_effect=RuntimeError("API down"))
def test_on_pricing_failure_warn_uses_fallback(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client, on_pricing_failure=OnPricingFailure.WARN)
    result = guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0))
    assert result.price_per_tib_used == pytest.approx(6.25, abs=0.01)


@patch("query_cost_guard.bigquery.fetch_price_per_byte", side_effect=RuntimeError("API down"))
def test_on_pricing_failure_raise_propagates(_mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client, on_pricing_failure=OnPricingFailure.RAISE)
    with pytest.raises(PricingUnavailableError):
        guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0))


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_pricing_cache_avoids_repeated_api_calls(mock_pricing):
    client = _make_mock_client()
    guard = QueryCostGuard(client=client)
    guard.query(sql="SELECT 1", params=QueryParams(max_cost_usd=1.0))
    guard.query(sql="SELECT 2", params=QueryParams(max_cost_usd=1.0))
    mock_pricing.assert_called_once()


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_query_handles_none_bytes_fields(_mock_pricing):
    client = MagicMock()
    job = MagicMock()
    job.result.return_value = []
    job.total_bytes_billed = None
    job.total_bytes_processed = None
    started = datetime(2026, 1, 1, tzinfo=UTC)
    job.started = started
    job.ended = started + timedelta(seconds=0.5)
    client.query.return_value = job

    guard = QueryCostGuard(client=client)
    result = guard.query(sql="SELECT 1", params=QueryParams(max_bytes=1000))
    assert result.bytes_billed == 0
    assert result.bytes_processed == 0


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_estimate_returns_dry_run_result(_mock_pricing):
    client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 5_000_000_000
    client.query.return_value = dry_job

    guard = QueryCostGuard(client=client)
    result = guard.estimate(sql="SELECT * FROM big_table")

    assert result.estimated_bytes == 5_000_000_000
    assert result.estimated_cost_usd == 5_000_000_000 * FALLBACK_PRICE_PER_BYTE
    assert result.price_per_tib_usd == pytest.approx(6.25, abs=0.01)

    called_config = client.query.call_args.kwargs.get("job_config") or client.query.call_args[1].get("job_config")
    assert called_config.dry_run is True
    assert called_config.use_query_cache is False


@patch("query_cost_guard.bigquery.fetch_price_per_byte", return_value=FALLBACK_PRICE_PER_BYTE)
def test_estimate_passes_location(_mock_pricing):
    client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = 1000
    client.query.return_value = dry_job

    guard = QueryCostGuard(client=client)
    guard.estimate(sql="SELECT 1", location="EU")

    assert client.query.call_args.kwargs["location"] == "EU"


def test_estimate_with_price_override():
    client = MagicMock()
    dry_job = MagicMock()
    dry_job.total_bytes_processed = TIB_IN_BYTES
    client.query.return_value = dry_job

    guard = QueryCostGuard(client=client, price_per_tib_override=10.0)
    result = guard.estimate(sql="SELECT 1")

    assert result.price_per_tib_usd == 10.0
    assert result.estimated_cost_usd == pytest.approx(10.0, abs=0.01)
