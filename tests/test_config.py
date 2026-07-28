"""Tests for configuration — that the knobs `.env.example` advertises are real.

A documented setting nothing reads is worse than no setting: it tells an operator
they have a lever when they do not.
"""

from __future__ import annotations

import pytest

from flowforge.config import Settings
from flowforge.llm import RateLimit
from workflows.demo import build_demo_control_plane


def test_unset_values_fall_back_to_the_defaults() -> None:
    settings = Settings.from_env()  # no env vars needed: every field has a default
    assert settings.database_url.startswith("postgresql://")
    assert settings.default_budget() is None  # accounting, not enforcement
    assert settings.rate_limit() is None  # unlimited


def test_env_values_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TENANT_BUDGET_USD_PER_DAY", "12.5")
    monkeypatch.setenv("LLM_RATE_LIMIT_PER_SECOND", "3")

    settings = Settings.from_env()
    budget = settings.default_budget()
    limit = settings.rate_limit()

    assert budget is not None and budget.limit_usd == 12.5
    assert budget.window.total_seconds() == 86400
    assert limit == RateLimit(per_second=3.0)


def test_empty_env_values_are_ignored_rather_than_parsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`.env` files carry empty keys; those mean "unset", not "zero"."""
    monkeypatch.setenv("TENANT_BUDGET_USD_PER_DAY", "")
    monkeypatch.setenv("LLM_RATE_LIMIT_PER_SECOND", "")

    settings = Settings.from_env()

    assert settings.default_budget() is None
    assert settings.rate_limit() is None


def test_a_zero_rate_limit_is_refused_at_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loud at boot rather than dividing by zero on the first model call."""
    monkeypatch.setenv("LLM_RATE_LIMIT_PER_SECOND", "0")

    with pytest.raises(ValueError, match="positive"):
        Settings.from_env().rate_limit()


def test_the_demo_assembly_honours_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TENANT_BUDGET_USD_PER_DAY", "7")

    cp = build_demo_control_plane()

    assert cp.budget is not None
    budget = cp.budget.budget_for("anyone")
    assert budget is not None and budget.limit_usd == 7.0


def test_the_demo_assembly_registers_both_reference_workflows() -> None:
    cp = build_demo_control_plane(Settings())

    assert cp.registry.get("invoice_to_payment") is not None
    assert cp.registry.get("contract_review") is not None
    assert {t.name for t in cp.triggers.all()} == {
        "invoice_email",
        "invoice_webhook",
        "invoice_sweep",
    }
