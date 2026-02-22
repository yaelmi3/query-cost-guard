from unittest.mock import MagicMock, patch

import pytest
from capacity import GiB, TiB, byte

from query_cost_guard.constants import DEFAULT_PRICE_PER_TIB_USD
from query_cost_guard.exceptions import PricingUnavailableError
from query_cost_guard.pricing import (
    _discover_bigquery_service_name,
    _extract_price_per_byte,
    _is_on_demand_analysis_sku,
    fetch_price_per_byte,
    get_fallback_price_per_byte,
)

TIB_IN_BYTES = int(TiB // byte)


def test_fallback_price_per_byte():
    price = get_fallback_price_per_byte()
    expected = DEFAULT_PRICE_PER_TIB_USD / TIB_IN_BYTES
    assert price == expected


def test_fallback_price_roundtrip():
    price = get_fallback_price_per_byte()
    reconstructed_tib_price = price * TIB_IN_BYTES
    assert abs(reconstructed_tib_price - DEFAULT_PRICE_PER_TIB_USD) < 1e-6


def _make_sku(
    *,
    description="Analysis Bytes - On Demand",
    resource_family="ApplicationServices",
    usage_type="OnDemand",
):
    sku = MagicMock()
    sku.description = description
    sku.category.resource_family = resource_family
    sku.category.usage_type = usage_type
    return sku


def test_is_on_demand_analysis_sku_matches():
    sku = _make_sku()
    assert _is_on_demand_analysis_sku(sku) is True


def test_is_on_demand_analysis_sku_rejects_wrong_description():
    sku = _make_sku(description="Storage")
    assert _is_on_demand_analysis_sku(sku) is False


def test_is_on_demand_analysis_sku_rejects_wrong_family():
    sku = _make_sku(resource_family="Storage")
    assert _is_on_demand_analysis_sku(sku) is False


def test_is_on_demand_analysis_sku_rejects_wrong_usage():
    sku = _make_sku(usage_type="Committed")
    assert _is_on_demand_analysis_sku(sku) is False


def _make_sku_with_pricing(*, usage_unit="TiBy", units=6, nanos=250_000_000):
    sku = _make_sku()
    sku.sku_id = "test-sku-123"
    tiered_rate = MagicMock()
    tiered_rate.unit_price.units = units
    tiered_rate.unit_price.nanos = nanos
    pricing_expression = MagicMock()
    pricing_expression.usage_unit = usage_unit
    pricing_expression.tiered_rates = [tiered_rate]
    pricing_info = MagicMock()
    pricing_info.pricing_expression = pricing_expression
    sku.pricing_info = [pricing_info]
    return sku


def test_extract_price_per_byte_tib():
    sku = _make_sku_with_pricing(usage_unit="TiBy", units=6, nanos=250_000_000)
    price_per_byte = _extract_price_per_byte(sku=sku)
    expected = 6.25 / TIB_IN_BYTES
    assert abs(price_per_byte - expected) < 1e-20


def test_extract_price_per_byte_gib():
    gib_in_bytes = int(GiB // byte)
    price_per_gib = 0.006103515625
    sku = _make_sku_with_pricing(usage_unit="GiBy", units=0, nanos=int(price_per_gib * 1e9))
    price_per_byte = _extract_price_per_byte(sku=sku)
    expected = price_per_gib / gib_in_bytes
    assert abs(price_per_byte - expected) < 1e-15


def test_extract_price_per_byte_unknown_unit():
    sku = _make_sku_with_pricing(usage_unit="ZiBy")
    with pytest.raises(PricingUnavailableError, match="Unknown usage unit"):
        _extract_price_per_byte(sku=sku)


def test_discover_bigquery_service_name_found():
    client = MagicMock()
    bq_service = MagicMock()
    bq_service.display_name = "BigQuery"
    bq_service.name = "services/BIGQUERY_ID"
    other_service = MagicMock()
    other_service.display_name = "Cloud Storage"
    client.list_services.return_value = [other_service, bq_service]
    assert _discover_bigquery_service_name(client=client) == "services/BIGQUERY_ID"


def test_discover_bigquery_service_name_not_found():
    client = MagicMock()
    client.list_services.return_value = []
    with pytest.raises(PricingUnavailableError, match="BigQuery service not found"):
        _discover_bigquery_service_name(client=client)


@patch("query_cost_guard.pricing._fetch_on_demand_price_per_byte")
@patch("query_cost_guard.pricing._discover_bigquery_service_name")
@patch("query_cost_guard.pricing.CloudCatalogClient")
def test_fetch_price_per_byte_end_to_end(_mock_client_cls, mock_discover, mock_fetch):
    mock_discover.return_value = "services/BQ"
    mock_fetch.return_value = 5.68e-12
    result = fetch_price_per_byte()
    assert result == 5.68e-12
    mock_discover.assert_called_once()
    mock_fetch.assert_called_once()
