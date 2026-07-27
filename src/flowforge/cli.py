"""flowforge command-line entrypoint.

``version`` and ``migrate`` are wired up; ``api`` and ``worker`` are added as those
subsystems land (see the README roadmap) so the surface stays honest.
"""

from __future__ import annotations

import argparse
import asyncio

from flowforge import __version__

_PENDING = {
    "api": "control plane (FastAPI) — not implemented yet",
    "worker": "durable worker loop entrypoint — not implemented yet",
}


def _migrate() -> int:
    from flowforge.config import Settings
    from flowforge.persistence import apply_migrations

    applied = asyncio.run(apply_migrations(Settings.from_env().database_url))
    print("applied:", ", ".join(applied) if applied else "(none)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowforge")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the flowforge version")
    sub.add_parser("migrate", help="apply Postgres migrations")
    for name, help_text in _PENDING.items():
        sub.add_parser(name, help=help_text)

    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "migrate":
        return _migrate()
    if args.command in _PENDING:
        parser.exit(status=2, message=f"{args.command}: {_PENDING[args.command]}\n")
    parser.print_help()
    return 0
