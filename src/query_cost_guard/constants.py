from enum import StrEnum

DEFAULT_PRICE_PER_TIB_USD = 6.25

PRICING_CACHE_TTL_SECONDS = 86_400


class OnPricingFailure(StrEnum):
    WARN = "warn"
    RAISE = "raise"
