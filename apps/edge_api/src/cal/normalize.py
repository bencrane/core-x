"""Project a cal.com booking envelope onto the ``corex.bookings`` row shape.

Mapped against the REAL captured payload (not guessed). The system identifiers
(``uid`` / ``bookingId`` / ``eventTypeId``) are present on every booking regardless
of how the event-type form is configured; the person/company fields come from the
attendee object + the form's custom ``responses``.

Name handling is robust to the form sending EITHER split ``firstName``/``lastName``
OR a single full ``name`` (the form changed between bookings) — it prefers the split
fields and falls back to splitting the full name.
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any

# triggerEvent → lifecycle kind. cal.com's enum is BOOKING_CANCELLED (two L's); the
# single-L spelling is accepted defensively.
_CREATED = {"BOOKING_CREATED"}
_CANCELLED = {"BOOKING_CANCELLED", "BOOKING_CANCELED"}
_RESCHEDULED = {"BOOKING_RESCHEDULED"}


def event_kind(trigger_event: str | None) -> str | None:
    """'created' | 'cancelled' | 'rescheduled' for the three lifecycle events; None otherwise."""
    te = (trigger_event or "").upper()
    if te in _CREATED:
        return "created"
    if te in _CANCELLED:
        return "cancelled"
    if te in _RESCHEDULED:
        return "rescheduled"
    return None


def _val(answer: Any) -> str | None:
    """A cal.com response answer is either a bare scalar or ``{label, value}`` — return value text."""
    if isinstance(answer, dict):
        answer = answer.get("value")
    if isinstance(answer, str) and answer.strip():
        return answer.strip()
    return None


def _find_response(responses: Any, key_candidates: list[str], label_substrings: list[str]) -> str | None:
    """Resolve a custom-form answer by exact key (case-insensitive) first, then by label
    substring. Grounded in the real payload (key ``Company-Website`` / label ``Company Website``),
    so a renamed key still resolves while its label stays sensible."""
    if not isinstance(responses, dict):
        return None
    lower = {k.lower(): v for k, v in responses.items() if isinstance(k, str)}
    for key in key_candidates:
        if key.lower() in lower:
            val = _val(lower[key.lower()])
            if val:
                return val
    for answer in responses.values():
        if isinstance(answer, dict):
            label = answer.get("label")
            if isinstance(label, str) and any(s in label.lower() for s in label_substrings):
                val = _val(answer)
                if val:
                    return val
    return None


def _names(attendee: dict[str, Any], responses: Any) -> tuple[str | None, str | None]:
    """Prefer the attendee's split ``firstName``/``lastName``; fall back to splitting the full
    ``name`` (so a fullname-only form still yields first/last). Any explicit field present wins."""
    first = (attendee.get("firstName") or "").strip() or None
    last = (attendee.get("lastName") or "").strip() or None
    if first and last:
        return first, last
    full = (attendee.get("name") or _find_response(responses, ["name"], ["name"]) or "").strip()
    if full:
        parts = full.split()
        return (first or parts[0]), (last or (" ".join(parts[1:]) or None))
    return first, last


def _apex(url: str | None) -> str | None:
    """Bare registrable host from a URL/host: strip scheme, path/query, leading ``www``, lowercase.
    ``https://ersgovcon.com`` → ``ersgovcon.com``."""
    if not isinstance(url, str) or not url.strip():
        return None
    host = re.sub(r"^https?://", "", url.strip(), flags=re.IGNORECASE)
    host = host.split("/")[0].split("?")[0].strip()
    if host.lower().startswith("www."):
        host = host[4:]
    return host.lower() or None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _parse_ts(value: Any) -> _dt.datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def extract(trigger_event: str | None, envelope: dict[str, Any]) -> dict[str, Any]:
    """The ``corex.bookings`` field projection for a booking event."""
    inner = envelope.get("payload") or {}
    attendees = inner.get("attendees") or []
    att0 = attendees[0] if attendees and isinstance(attendees[0], dict) else {}
    responses = inner.get("responses") or {}
    first, last = _names(att0, responses)
    return {
        # system identifiers — present regardless of form config
        "cal_event_uid": inner.get("uid"),
        "cal_booking_id": _as_int(inner.get("bookingId")),
        "event_type_id": _as_int(inner.get("eventTypeId")),
        # person (attendee + form)
        "first_name": first,
        "last_name": last,
        "email": att0.get("email") or _find_response(responses, ["email"], ["email"]),
        # company (form custom fields)
        "company_name": _find_response(responses, ["Company-Name"], ["company name"]),
        "domain": _apex(_find_response(responses, ["Company-Website"], ["company website", "website", "domain"])),
        "title": None,  # no job-title field on the event-type form (stays null for now)
        # timing
        "start_time": _parse_ts(inner.get("startTime")),
        "end_time": _parse_ts(inner.get("endTime")),
        "booked_at": _parse_ts(envelope.get("createdAt")),
        "status": "cancelled" if event_kind(trigger_event) == "cancelled" else "booked",
    }
