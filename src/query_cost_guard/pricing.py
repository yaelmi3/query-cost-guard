import structlog
from capacity import GiB, KiB, MiB, TiB, byte
from google.cloud.billing_v1 import CloudCatalogClient
from google.cloud.billing_v1.types import ListServicesRequest, ListSkusRequest

from query_cost_guard.constants import DEFAULT_PRICE_PER_TIB_USD
from query_cost_guard.exceptions import PricingUnavailableError

logger = structlog.get_logger()

USAGE_UNIT_TO_CAPACITY = {
    "By": byte,
    "KiBy": KiB,
    "MiBy": MiB,
    "GiBy": GiB,
    "TiBy": TiB,
}


def fetch_price_per_byte() -> float:
    client = CloudCatalogClient()
    service_name = _discover_bigquery_service_name(client=client)
    return _fetch_on_demand_price_per_byte(client=client, service_name=service_name)


def get_fallback_price_per_byte() -> float:
    tib_in_bytes = int(TiB // byte)
    return DEFAULT_PRICE_PER_TIB_USD / tib_in_bytes


def _discover_bigquery_service_name(*, client: CloudCatalogClient) -> str:
    for service in client.list_services(request=ListServicesRequest()):
        if service.display_name == "BigQuery":
            return service.name
    raise PricingUnavailableError(reason="BigQuery service not found in Cloud Billing Catalog")


def _fetch_on_demand_price_per_byte(*, client: CloudCatalogClient, service_name: str) -> float:
    all_skus = client.list_skus(request=ListSkusRequest(parent=service_name))
    if (sku := next((sku for sku in all_skus if _is_on_demand_analysis_sku(sku)), None)) is None:
        raise PricingUnavailableError(reason="BigQuery on-demand analysis SKU not found in Billing Catalog")
    return _extract_price_per_byte(sku=sku)


def _extract_price_per_byte(sku) -> float:
    pricing_expression = sku.pricing_info[0].pricing_expression
    usage_unit = pricing_expression.usage_unit
    unit_price = pricing_expression.tiered_rates[0].unit_price
    price_per_unit = unit_price.units + unit_price.nanos / 1e9

    if (unit_capacity := USAGE_UNIT_TO_CAPACITY.get(usage_unit)) is None:
        raise PricingUnavailableError(reason=f"Unknown usage unit: {usage_unit}")

    bytes_per_unit = int(unit_capacity // byte)
    price_per_byte = price_per_unit / bytes_per_unit
    logger.info(
        "Resolved BigQuery on-demand pricing",
        price_per_unit=price_per_unit,
        usage_unit=usage_unit,
        price_per_byte=price_per_byte,
        sku_id=sku.sku_id,
    )
    return price_per_byte


def _is_on_demand_analysis_sku(sku) -> bool:
    description = sku.description.lower()
    category = sku.category
    return (
        "analysis" in description
        and "on demand" in description
        and category.resource_family == "ApplicationServices"
        and category.usage_type == "OnDemand"
    )
