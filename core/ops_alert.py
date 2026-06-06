"""Shared ops alerting — best-effort Telegram notification on terminal failure.

Feed workers call ``alert(feed, message)`` from their terminal/finally handler so an
unattended rollback or error reaches a human. No-op (and never raises) when the
Telegram env is absent, so it can neither mask nor block a build.

Wiring (per worker): attach ``modal.Secret.from_name("ops-alerts")`` and
``.add_local_python_source("core.ops_alert")`` to the image, then
``from core.ops_alert import alert``.

Secret (Modal ``ops-alerts``; canonical in Doppler ``core-x/prd``):
  OPS_ALERT_TELEGRAM_TOKEN    bot token (corex_ops_alerts_bot)
  OPS_ALERT_TELEGRAM_CHAT_ID  destination chat id
"""
from __future__ import annotations

import os


def alert(feed: str, message: str) -> bool:
    """POST a failure alert to the ops Telegram chat. Returns True if sent, False if
    skipped (env unset) or on a swallowed delivery error. Never raises."""
    token = os.environ.get("OPS_ALERT_TELEGRAM_TOKEN")
    chat_id = os.environ.get("OPS_ALERT_TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        print("ops_alert: OPS_ALERT_TELEGRAM_* unset; skipping alert.")
        return False
    try:
        import requests

        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": f"🚨 [{feed}] {message}",
                  "disable_web_page_preview": True},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"ops_alert: Telegram returned {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:  # noqa: BLE001 — alerting must never break the caller
        print(f"WARN: ops alert POST failed: {exc}")
        return False
