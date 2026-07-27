"""Runtime configuration, read from the environment (see ``.env.example``)."""

from __future__ import annotations

import os
from datetime import timedelta

from pydantic import BaseModel

from flowforge.core.budget import Budget
from flowforge.llm.limits import RateLimit


class Settings(BaseModel):
    database_url: str = "postgresql://flowforge:flowforge@localhost:5432/flowforge"
    redis_url: str = "redis://localhost:6379/0"
    openai_api_key: str | None = None
    otel_endpoint: str | None = None
    otel_service_name: str = "flowforge"

    tenant_budget_usd_per_day: float | None = None
    """Default per-tenant spend cap. ``None`` means accounting without enforcement."""

    llm_rate_limit_per_second: float | None = None
    """Sustained per-provider call rate. ``None`` means unlimited."""

    def default_budget(self) -> Budget | None:
        if self.tenant_budget_usd_per_day is None:
            return None
        return Budget(limit_usd=self.tenant_budget_usd_per_day, window=timedelta(days=1))

    def rate_limit(self) -> RateLimit | None:
        if self.llm_rate_limit_per_second is None:
            return None
        return RateLimit(per_second=self.llm_rate_limit_per_second)

    @classmethod
    def from_env(cls) -> Settings:
        overrides = {
            "database_url": os.environ.get("DATABASE_URL"),
            "redis_url": os.environ.get("REDIS_URL"),
            "openai_api_key": os.environ.get("OPENAI_API_KEY"),
            "otel_endpoint": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
            "otel_service_name": os.environ.get("OTEL_SERVICE_NAME"),
            "tenant_budget_usd_per_day": os.environ.get("TENANT_BUDGET_USD_PER_DAY"),
            "llm_rate_limit_per_second": os.environ.get("LLM_RATE_LIMIT_PER_SECOND"),
        }
        return cls(**{k: v for k, v in overrides.items() if v})
