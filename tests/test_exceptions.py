import pytest
from pydantic import ValidationError

from query_cost_guard.exceptions import PricingUnavailableError, QueryCostExceededContext, QueryCostExceededError


def test_query_cost_exceeded_context_with_max_cost_usd():
    context = QueryCostExceededContext(
        estimated_cost_usd=2.34,
        max_cost_usd=0.10,
        bytes_estimated=374_000_000_000,
        query_tag="troubleshooting",
    )
    assert context.estimated_cost_usd == 2.34
    assert context.max_cost_usd == 0.10
    assert context.max_bytes is None
    assert context.bytes_estimated == 374_000_000_000
    assert context.query_tag == "troubleshooting"


def test_query_cost_exceeded_context_with_max_bytes():
    context = QueryCostExceededContext(
        estimated_cost_usd=2.34,
        max_bytes=500_000_000_000,
        bytes_estimated=374_000_000_000,
    )
    assert context.max_bytes == 500_000_000_000
    assert context.max_cost_usd is None
    assert context.query_tag is None


def test_query_cost_exceeded_context_rejects_negative_cost():
    with pytest.raises(ValidationError):
        QueryCostExceededContext(estimated_cost_usd=-1, bytes_estimated=100)


def test_query_cost_exceeded_context_rejects_negative_bytes():
    with pytest.raises(ValidationError):
        QueryCostExceededContext(estimated_cost_usd=1.0, bytes_estimated=-1)


def test_query_cost_exceeded_context_rejects_zero_max_cost():
    with pytest.raises(ValidationError):
        QueryCostExceededContext(estimated_cost_usd=1.0, max_cost_usd=0, bytes_estimated=100)


def test_query_cost_exceeded_context_rejects_zero_max_bytes():
    with pytest.raises(ValidationError):
        QueryCostExceededContext(estimated_cost_usd=1.0, max_bytes=0, bytes_estimated=100)


def test_query_cost_exceeded_context_is_frozen():
    context = QueryCostExceededContext(estimated_cost_usd=1.0, bytes_estimated=100)
    with pytest.raises(ValidationError):
        context.estimated_cost_usd = 5.0


def test_query_cost_exceeded_str_with_dollar_limit():
    context = QueryCostExceededContext(
        estimated_cost_usd=2.34,
        max_cost_usd=0.10,
        bytes_estimated=374_000_000_000,
    )
    exc = QueryCostExceededError(context=context)
    assert "$2.34" in str(exc)
    assert "$0.10" in str(exc)
    assert "374,000,000,000 bytes" in str(exc)


def test_query_cost_exceeded_str_with_byte_limit():
    context = QueryCostExceededContext(
        estimated_cost_usd=2.34,
        max_bytes=500_000_000_000,
        bytes_estimated=374_000_000_000,
    )
    exc = QueryCostExceededError(context=context)
    assert "500,000,000,000 bytes" in str(exc)


def test_query_cost_exceeded_carries_context():
    context = QueryCostExceededContext(
        estimated_cost_usd=2.34,
        max_cost_usd=0.10,
        bytes_estimated=374_000_000_000,
        query_tag="my_tag",
    )
    exc = QueryCostExceededError(context=context)
    assert exc.context is context
    assert exc.context.query_tag == "my_tag"


def test_pricing_unavailable_carries_reason():
    exc = PricingUnavailableError(reason="API disabled")
    assert exc.reason == "API disabled"
    assert "API disabled" in str(exc)
