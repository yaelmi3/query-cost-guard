from pydantic import BaseModel, Field


class QueryCostExceededContext(BaseModel, frozen=True):
    estimated_cost_usd: float = Field(ge=0)
    max_cost_usd: float | None = Field(default=None, gt=0)
    max_bytes: int | None = Field(default=None, gt=0)
    bytes_estimated: int = Field(ge=0)
    query_tag: str | None = None


class QueryCostExceededError(Exception):
    def __init__(self, context: QueryCostExceededContext) -> None:
        self.context = context
        super().__init__(str(self))

    def __str__(self) -> str:
        limit = (
            f"${self.context.max_cost_usd:.2f}"
            if self.context.max_cost_usd is not None
            else f"{self.context.max_bytes:,} bytes"
        )
        return (
            f"Query estimated at ${self.context.estimated_cost_usd:.2f} "
            f"({self.context.bytes_estimated:,} bytes) exceeds limit of {limit}"
        )


class PricingUnavailableError(Exception):
    def __init__(self, *, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Live pricing unavailable: {reason}")
