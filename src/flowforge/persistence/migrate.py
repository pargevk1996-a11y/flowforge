"""Minimal forward-only migration runner.

Migrations are idempotent SQL (``CREATE ... IF NOT EXISTS``) applied in filename
order. ``asyncpg`` is imported lazily.
"""

from __future__ import annotations

from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


async def apply_migrations(dsn: str) -> list[str]:
    """Apply all migrations; return the filenames applied, in order."""
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        applied: list[str] = []
        for path in migration_files():
            await conn.execute(path.read_text())
            applied.append(path.name)
        return applied
    finally:
        await conn.close()
