from pydantic import BaseModel, Field


class QueryResult(BaseModel, frozen=True):
    rows: list[dict]
    actual_cost_usd: float = Field(ge=0)
    bytes_billed: int = Field(ge=0)
    bytes_processed: int = Field(ge=0)
    duration_seconds: float = Field(ge=0)
    price_per_tib_used: float = Field(gt=0)
    query_tag: str | None = None


class EstimateResult(BaseModel, frozen=True):
    estimated_bytes: int = Field(ge=0)
    estimated_cost_usd: float = Field(ge=0)
    price_per_tib_usd: float = Field(gt=0)
