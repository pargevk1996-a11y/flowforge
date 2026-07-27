"""flowforge command-line entrypoint.

``version``, ``migrate`` and ``api`` are wired up; ``worker`` is added when a
standalone worker process earns its keep (``api`` already runs one in-process,
which is what a single-node deployment wants).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from flowforge import __version__

REPO_ROOT = Path(__file__).resolve().parents[2]
UI_DIST = REPO_ROOT / "ui" / "dist"
"""Where ``npm run build`` puts the debugger, when it has been built."""

_PENDING = {
    "worker": "standalone worker process — not implemented yet (`api` runs one)",
}


def _migrate() -> int:
    from flowforge.config import Settings
    from flowforge.persistence import apply_migrations

    applied = asyncio.run(apply_migrations(Settings.from_env().database_url))
    print("applied:", ", ".join(applied) if applied else "(none)")
    return 0


def _api(host: str, port: int, *, demo: bool) -> int:
    """Serve the control plane, its background loops, and the debugger UI."""
    import uvicorn

    from flowforge.api import create_app

    if not demo:
        print(
            "flowforge api currently serves the bundled demo assembly; pass --demo "
            "to acknowledge that, or call create_app() from your own module with "
            "your workflows registered."
        )
        return 2

    # The reference workflows live beside the package rather than inside it — a
    # library has no business installing a top-level `workflows` module — so the
    # demo only assembles from a source checkout.
    if not (REPO_ROOT / "workflows").is_dir():
        print(f"--demo needs the reference workflows, which are not at {REPO_ROOT}")
        return 2
    sys.path.insert(0, str(REPO_ROOT))
    from workflows.demo import build_demo_control_plane

    app = create_app(
        build_demo_control_plane(),
        run_background=True,
        ui_dir=UI_DIST if UI_DIST.exists() else None,
    )
    if not UI_DIST.exists():
        print(
            f"note: no UI build at {UI_DIST} — run "
            "`npm --prefix ui install && npm --prefix ui run build`"
        )
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowforge")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the flowforge version")
    sub.add_parser("migrate", help="apply Postgres migrations")

    api = sub.add_parser("api", help="serve the control plane and the debugger UI")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument(
        "--demo",
        action="store_true",
        help="serve the reference workflows with a canned LLM client",
    )

    for name, help_text in _PENDING.items():
        sub.add_parser(name, help=help_text)

    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "migrate":
        return _migrate()
    if args.command == "api":
        return _api(args.host, args.port, demo=args.demo)
    if args.command in _PENDING:
        parser.exit(status=2, message=f"{args.command}: {_PENDING[args.command]}\n")
    parser.print_help()
    return 0
