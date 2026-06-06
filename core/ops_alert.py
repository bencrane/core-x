"""Operational alerts via webhook — best-effort human signal on terminal failure.

This module provides a single `alert()` function for fleet-wide failure notification.
It is pure Python with no dependencies on modal/duckdb/lance, so it is safe to import
at module-load time and cheap to ship into each Modal image:

    image = modal.Image.debian_slim(...).pip_install(...).add_local_python_source("core.ops_alert")

The alert function reads `OPS_ALERT_WEBHOOK` from the environment (Slack, Discord, or
Telegram incoming webhook URL) and POSTs a JSON payload on failure. If the webhook
is unset or the POST fails, it silently no-ops — alerts must never block or raise
exceptions that mask the actual error.

Usage in a worker:

    from core.ops_alert import alert
    ...
    @app.function(secrets=[modal.Secret.from_name("ops-alerts")])
    def my_ingest(**kwargs):
        try:
            # do work
        except Exception as exc:
            alert(f"my_ingest failed: {str(exc)[:300]}")
            raise

The webhook URL is stored in Doppler (`core-x/prd`) and mirrored to Modal secret
`ops-alerts` on worker deploy.
"""

from __future__ import annotations

import os


def alert(msg: str) -> None:
    """Best-effort human alert on terminal failure/rollback.

    No-op if OPS_ALERT_WEBHOOK unset; never raises (must not mask or block
    the build). Sends a JSON payload compatible with Slack/Discord incoming
    webhooks.

    Args:
        msg: Alert message (will be included in webhook payload).
    """
    url = os.environ.get("OPS_ALERT_WEBHOOK")
    if not url:
        return
    try:
        import requests

        requests.post(url, json={"text": msg}, timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: alert POST failed: {exc}")
