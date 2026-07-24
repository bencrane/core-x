"""spawn_deployed — the fleet's durable launch helper (2026-07-23 directive §1.3).

The measured doctrine (03_modal_compute.md §6.1): a ``local_entrypoint`` calling
``.remote()`` issues a SYNC input that Modal's server cancels ~74–131 s after the
launching client stops heartbeating — detached or not. The durable form spawns on
the DEPLOYED app (ASYNC input, no client tether) and records the ``fc-…`` id.

This helper is the one place that shape lives: deploy-freshness gate (a spawn runs
the DEPLOYED snapshot; nothing else warns when it is stale), spawn, print the fc-id
and the follow commands. Thin by mandate — no framework, no polling, no ledger.

Usage from a ``local_entrypoint``::

    from pipelines._shared.launch import spawn_deployed

    @app.local_entrypoint()
    def build(since: str = ""):
        spawn_deployed("my-app", "build_fn", deploy_file=__file__,
                       since=since or None, trigger_callback_url=None)

The worker owns its own terminal ledger row; follow with ``modal app logs <app>``
and collect the result later via ``modal.FunctionCall.from_id(fc).get(timeout=0)``
(raises ``TimeoutError`` while running, returns the result when done).
"""

from __future__ import annotations

import json
import os
import subprocess


def _git_head_short() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 — freshness gate degrades to always-deploy
        return ""


def _deployed_commit(app_name: str) -> str:
    """Short commit of the app's newest deployed version ('' if unknown)."""
    try:
        out = subprocess.run(
            ["modal", "app", "history", app_name, "--json"],
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout
        rows = json.loads(out)
        if not rows:
            return ""
        newest = max(rows, key=lambda r: int(str(r.get("Version", "v0")).lstrip("v") or 0))
        return (newest.get("Commit") or "").strip()
    except Exception:  # noqa: BLE001 — unknown app / no history -> deploy
        return ""


def spawn_deployed(app_name: str, fn_name: str, *, deploy_file: str | None = None,
                   **kwargs) -> str:
    """Deploy-if-stale, spawn ``fn_name`` on the deployed ``app_name``, print the fc-id.

    Args:
        app_name: Deployed Modal app name.
        fn_name: Function name on that app.
        deploy_file: Path to the worker file. When given, the deployed snapshot's
            commit is compared against ``git rev-parse --short HEAD``; on mismatch
            (or unknown) the file is ``modal deploy``-ed first, so the spawn can
            never run a stale snapshot. Pass ``__file__`` from the entrypoint.
        **kwargs: Passed verbatim to ``Function.spawn``.

    Returns:
        The ``fc-…`` function-call id (also printed — record it; it is the only
        durable handle to the run).
    """
    import modal

    if deploy_file:
        head, deployed = _git_head_short(), _deployed_commit(app_name)
        if not head or not deployed or head != deployed:
            print(f"[launch] deploying {deploy_file} "
                  f"(deployed commit {deployed or '?'} != HEAD {head or '?'})")
            subprocess.run(["modal", "deploy", deploy_file], check=True)
        else:
            print(f"[launch] deployed snapshot fresh @ {deployed}")
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(deploy_file)),
        ).stdout.strip()
        if dirty:
            print("[launch] WARNING: worktree dirty — the deployed snapshot may "
                  "not match committed main. Launch doctrine: deploy from "
                  "freshly-pulled main.")

    fc = modal.Function.from_name(app_name, fn_name).spawn(**kwargs)
    print(f"FUNCTION_CALL_ID: {fc.object_id}")
    print(f"Follow:  modal app logs {app_name}")
    print("Result:  python3 -c \"import modal; "
          f"print(modal.FunctionCall.from_id('{fc.object_id}').get(timeout=0))\"")
    return fc.object_id
