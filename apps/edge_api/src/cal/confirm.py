"""Booking confirmation email (Resend) — sent from the CREATE path.

Since edge_api creates the cal.com booking (from a Close activity), it already holds the attendee,
company, time, and event type, and gets the join URL back in the create-booking RESPONSE (``location``).
So the confirmation is sent right here in ``/internal/cal/book`` — no dependency on the inbound
``/webhooks/cal`` capture (that path is for bookings cal.com tells us about; these we make ourselves).

cal.com is configured silent to attendees (org "Disable all booking emails to guests"), so THIS is the
booker's only confirmation. Best-effort: a send failure never fails the booking (already durably
recorded). Idempotent via Resend's ``Idempotency-Key`` (the booking uid) so a retry can't double-send.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from .. import config

logger = logging.getLogger(__name__)

_RESEND_URL = "https://api.resend.com/emails"
_TIMEOUT_SEC = 15.0

# slug → human meeting label for the subject/body. Unknown slugs degrade to a generic "meeting".
_DURATION_LABEL = {"15min": "15-minute meeting", "30min": "30-minute meeting", "secret": "meeting"}


def _human_when(start_iso: str, tz_name: str) -> str:
    """Format the UTC start in the attendee's timezone, e.g. 'Tuesday, June 16, 2026 at 1:30 PM EDT'.
    Falls back to the raw ISO string if parsing fails. Built manually (no %-d/%-I) for portability."""
    try:
        dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(ZoneInfo(tz_name or "UTC"))
    except Exception:  # noqa: BLE001 — unparseable input → show the raw value
        return start_iso
    hour12 = dt.hour % 12 or 12
    ampm = "AM" if dt.hour < 12 else "PM"
    tzabbr = dt.strftime("%Z")
    return f"{dt.strftime('%A, %B')} {dt.day}, {dt.year} at {hour12}:{dt.minute:02d} {ampm} {tzabbr}".strip()


def _join_url(value: Any) -> str | None:
    """Only accept an http(s) URL as the join link (v2 ``location`` may carry a non-URL for some
    location types); anything else → omit the join line."""
    if isinstance(value, str) and value.startswith("http"):
        return value
    return None


async def send_booking_confirmation(
    *,
    to_email: str | None,
    to_name: str | None,
    event_slug: str | None,
    start_iso: str,
    tz_name: str | None,
    join_url: Any,
    idempotency_key: str | None,
) -> dict[str, Any]:
    """Send the attendee's booking confirmation via Resend. Best-effort — returns a status dict, never
    raises for a send failure (the caller has already recorded the booking)."""
    api_key = config.resend_api_key()
    if not api_key:
        logger.warning("RESEND_API_KEY unset — skipping booking confirmation to %s", to_email)
        return {"sent": False, "reason": "RESEND_API_KEY unset"}
    if not to_email:
        return {"sent": False, "reason": "no attendee email"}

    first = (to_name or "").strip().split(" ")[0] if to_name else "there"
    when = _human_when(start_iso, tz_name or "UTC")
    label = _DURATION_LABEL.get(event_slug or "", "meeting")
    url = _join_url(join_url)

    subject = f"Your {label} is confirmed — {when}"
    join_html = (
        f'<p style="margin:0 0 12px"><strong>Where:</strong> '
        f'<a href="{url}">Join the video call</a></p>'
        if url else ""
    )
    join_text = f"Where: Join the video call — {url}\n" if url else ""
    html = (
        '<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;'
        'font-size:15px;color:#111;line-height:1.55">'
        f"<p style=\"margin:0 0 12px\">Hi {first},</p>"
        f"<p style=\"margin:0 0 12px\">Your {label} is confirmed.</p>"
        f"<p style=\"margin:0 0 12px\"><strong>When:</strong> {when}</p>"
        f"{join_html}"
        '<p style="margin:0 0 12px">We look forward to speaking with you.</p>'
        "</div>"
    )
    text = (
        f"Hi {first},\n\nYour {label} is confirmed.\n\n"
        f"When: {when}\n{join_text}\nWe look forward to speaking with you.\n"
    )

    body: dict[str, Any] = {
        "from": config.booking_email_from(),
        "to": [to_email],
        "subject": subject,
        "html": html,
        "text": text,
    }
    reply_to = config.booking_email_reply_to()
    if reply_to:
        body["reply_to"] = reply_to

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if idempotency_key:
        headers["Idempotency-Key"] = f"cal-confirm:{idempotency_key}"

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
            r = await client.post(_RESEND_URL, headers=headers, json=body)
    except Exception as exc:  # noqa: BLE001 — network error is best-effort
        logger.warning("Resend confirmation errored for %s: %s", to_email, exc)
        return {"sent": False, "reason": f"resend error: {exc}"}
    if r.status_code >= 400:
        logger.warning(
            "Resend confirmation failed %s for %s: %s", r.status_code, to_email, r.text[:300]
        )
        return {"sent": False, "reason": f"resend {r.status_code}"}
    logger.info("booking confirmation sent to %s (%s)", to_email, when)
    return {"sent": True}
