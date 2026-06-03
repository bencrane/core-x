"""Contact-hydration launch tool — the GTM agent's typed trigger for the Waterfall
ICP contact-hydration pipeline (Directive 20-Final).

This is the interactive kickoff surface: the Anthropic GTM agent calls
``launch_contact_hydration`` with a saved campaign/audience + a NATIVE Waterfall
cascade (its dynamically-generated persona), and the tool (1) confirms the audience
exists in ``ops.campaign_audiences``, then (2) fires the ``blitz-contact-hydration-
waterfall`` Trigger task via Trigger.dev's REST trigger endpoint. The Trigger task
dispatches the Modal worker, which routes every Blitz call through the rate-governed
``blitz-gateway`` (Directive 23) — the worker holds no key and the gateway owns the
global ≤5-RPS budget.

WHY HERE. This mirrors the gateway's "structured, explicit, never-raw" tool
philosophy (the same surface that owns ``save_campaign_audience``): one purpose-built,
typed entry point the managed agent populates, not a generic task-runner. It is a
control-plane SIGNAL (fire a Trigger run) — it writes no dataset and makes no Blitz
call itself.

CONFIG. ``TRIGGER_SECRET_KEY`` (a Trigger.dev prod ``tr_…`` key) must be set on the
gtm-mcp service env — the only new credential this tool needs. The Trigger trigger is
issued over plain HTTPS with the stdlib (no new dependency)."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from .. import database

TASK_ID = "blitz-contact-hydration-waterfall"
TRIGGER_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")
_VALID_PRIORITIES = {"high", "normal", "low"}


def _audience_exists(campaign_id: str, audience_name: str | None) -> tuple[bool, str | None]:
    """(exists, resolved_audience_name). When audience_name is omitted, resolve the
    campaign's most-recently-updated audience. Read-only psycopg on the hq-x DSN —
    independent of the DuckDB attach (same split as tools/ops.py)."""
    dsn = database.hqx_dsn()
    if not dsn:
        raise RuntimeError(
            "HQX_DB_URL_POOLED is not set — cannot reach hq-x to validate the audience."
        )
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        if audience_name:
            cur.execute(
                "SELECT audience_name FROM ops.campaign_audiences "
                "WHERE campaign_id = %s AND audience_name = %s",
                (campaign_id, audience_name),
            )
        else:
            cur.execute(
                "SELECT audience_name FROM ops.campaign_audiences "
                "WHERE campaign_id = %s ORDER BY updated_at DESC LIMIT 1",
                (campaign_id,),
            )
        row = cur.fetchone()
    return (bool(row), row[0] if row else None)


def _trigger_run(payload: dict) -> dict[str, Any]:
    """POST the Trigger.dev REST trigger endpoint. Returns the parsed run handle
    (``{"id": "run_…"}``). Raises with the upstream body on a non-2xx."""
    key = os.environ.get("TRIGGER_SECRET_KEY")
    if not key:
        raise RuntimeError(
            "TRIGGER_SECRET_KEY is not set on the gtm-mcp service — cannot trigger the "
            "hydration task. Provision the Trigger.dev prod key in the service env."
        )
    url = f"{TRIGGER_BASE}/api/v1/tasks/{TASK_ID}/trigger"
    body = json.dumps({"payload": payload}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:  # surface the upstream error body
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Trigger.dev trigger failed ({exc.code}): {detail}") from None


def launch_contact_hydration(
    campaign_id: str,
    cascade: list[dict],
    audience_name: str | None = None,
    max_results_per_company: int = 5,
    enrich_email: bool = True,
    enrich_phone: bool = False,
    priority: str = "high",
) -> dict[str, Any]:
    """Kick off Waterfall ICP contact hydration for a saved campaign audience.

    Resolves the audience's companies from the Lance plane (via its stored
    ``source_query``) and finds the best decision-maker(s) at each via BlitzAPI's
    Waterfall ICP cascade, optionally enriching verified work emails (and US phones).
    All Blitz egress is routed through the rate-governed gateway; this returns
    immediately with a Trigger run handle — the run executes asynchronously.

    Args:
      campaign_id: the campaign whose audience to hydrate (matches a row in
        ``ops.campaign_audiences``; save it first with ``save_campaign_audience``).
      cascade: the NATIVE Waterfall cascade — an ordered list (≤8) of preference
        tiers, each ``{"include_title": [...], "exclude_title": [...]?,
        "location": [...]?, "include_headline_search": bool?}``. The engine tries
        tiers in order and stops once ``max_results_per_company`` is filled, so put
        the most senior / most-wanted titles first (e.g. tier 1 ``["CMO","Chief
        Marketing Officer"]`` → tier 2 ``["VP Marketing","Head of Marketing"]``).
        ``include_title`` is full-text; wrap a value in ``[...]`` for exact match.
        ``location`` is ISO-3166 alpha-2 (or ``["WORLD"]``).
      audience_name: which saved audience for the campaign. Omit to use the
        campaign's most-recently-updated audience.
      max_results_per_company: contacts per company, 1–25 (default 5).
      enrich_email: also resolve a verified work email per matched contact (default
        true). Degrades to off if the Blitz plan doesn't unlock email enrichment.
      enrich_phone: also resolve a direct phone for US matches (default false).
      priority: gateway egress lane — ``"high"`` (default; interactive, preempts
        background bulk sweeps), ``"normal"``, or ``"low"``.

    Returns ``{"status": "launched", "task_id", "campaign_id", "audience_name",
    "run_id", "priority"}``. Raises if the campaign/audience is unknown or the
    Trigger trigger fails.
    """
    cid = (campaign_id or "").strip()
    if not cid:
        raise ValueError("campaign_id is required")
    if not isinstance(cascade, list) or not cascade:
        raise ValueError("cascade must be a non-empty list of Waterfall tiers")
    for i, tier in enumerate(cascade):
        if not isinstance(tier, dict) or not tier.get("include_title"):
            raise ValueError(f"cascade[{i}] must be an object with a non-empty 'include_title' list")
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"priority must be one of {sorted(_VALID_PRIORITIES)}")
    try:
        mrpc = max(1, min(int(max_results_per_company), 25))
    except (TypeError, ValueError):
        raise ValueError("max_results_per_company must be an integer 1–25")

    aname = (audience_name or "").strip() or None
    exists, resolved = _audience_exists(cid, aname)
    if not exists:
        raise ValueError(
            f"no saved audience for campaign_id={cid!r}"
            + (f" audience_name={aname!r}" if aname else "")
            + " — save it first with save_campaign_audience."
        )

    payload = {
        "campaignId": cid,
        "audienceName": resolved,
        "cascade": cascade,
        "maxResultsPerCompany": mrpc,
        "enrichEmail": bool(enrich_email),
        "enrichPhone": bool(enrich_phone),
        "priority": priority,
    }
    handle = _trigger_run(payload)

    return {
        "status": "launched",
        "task_id": TASK_ID,
        "campaign_id": cid,
        "audience_name": resolved,
        "run_id": handle.get("id"),
        "priority": priority,
        "max_results_per_company": mrpc,
        "enrich_email": bool(enrich_email),
        "enrich_phone": bool(enrich_phone),
    }


def register(mcp) -> None:
    """Mount the hydration launch tool onto the FastMCP server."""
    mcp.add_tool(launch_contact_hydration)
