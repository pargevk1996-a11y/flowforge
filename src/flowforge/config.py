"""Runtime configuration, read from the environment (see ``.env.example``)."""

from __future__ import annotations

import os

from pydantic import BaseModel


class Settings(BaseModel):
    database_url: str = "postgresql://flowforge:flowforge@localhost:5432/flowforge"
    redis_url: str = "redis://localhost:6379/0"
    openai_api_key: str | None = None
    otel_endpoint: str | None = None
    otel_service_name: str = "flowforge"

    @classmethod
    def from_env(cls) -> Settings:
        overrides = {
            "database_url": os.environ.get("DATABASE_URL"),
            "redis_url": os.environ.get("REDIS_URL"),
            "openai_api_key": os.environ.get("OPENAI_API_KEY"),
            "otel_endpoint": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
            "otel_service_name": os.environ.get("OTEL_SERVICE_NAME"),
        }
        return cls(**{k: v for k, v in overrides.items() if v})
