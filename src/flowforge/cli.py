"""flowforge command-line entrypoint.

Only ``version`` is wired up today; ``api``, ``worker`` and ``migrate`` are added
as those subsystems land (see the README roadmap) so the surface stays honest.
"""

from __future__ import annotations

import argparse

from flowforge import __version__

_PENDING = {
    "api": "control plane (FastAPI) — not implemented yet",
    "worker": "durable worker loop — not implemented yet",
    "migrate": "apply Postgres migrations — not implemented yet",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowforge")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the flowforge version")
    for name, help_text in _PENDING.items():
        sub.add_parser(name, help=help_text)

    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0
    if args.command in _PENDING:
        parser.exit(status=2, message=f"{args.command}: {_PENDING[args.command]}\n")
    parser.print_help()
    return 0
