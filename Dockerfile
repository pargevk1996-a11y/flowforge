# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependency layer
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install ".[serve,llm,otel]"

# Runs either the control plane or a worker depending on the command.
# api:    flowforge api
# worker: flowforge worker
ENTRYPOINT ["flowforge"]
CMD ["--help"]
