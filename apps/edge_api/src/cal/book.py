"""cal.com booking — OUTBOUND create (POST /v2/bookings) + the cal-book Trigger.dev kickoff.

The mirror of the INBOUND cal path (``webhooks_cal.py`` + ``queries.py``): there cal.com pushes
booking events to us; here we CREATE a booking ON cal.com from a completed Close custom activity. The
created booking then fires cal.com's own ``BOOKING_CREATED`` webhook back into ``/webhooks/cal``, so
the existing normalize → ``corex.bookings`` → research/enrich/materialize pipeline runs unchanged.
One closed loop, no double-write.

  • ``trigger_book(...)``        — best-effort Trigger.dev kickoff (fired from the Close webhook)
  • ``build_booking_request(...)`` — pure map: Close activity fields + contact → cal.com request parts
  • ``create_booking(...)``      — POST ``{CAL_API_BASE}/bookings`` (Bearer CAL_API_KEY + cal-api-version)

Idempotency lives in two layers above the HTTP call: the Trigger-run idempotency key
(``cal-book:{close_activity_id}``) collapses Close's duplicate deliveries to ONE run, and the
``ops.cal_booking_runs`` ledger claim guards the actual create. The TS task runs ``maxAttempts=1`` (a
booking is a real side effect — no blind retry).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from .. import config
from ..services.trigger_dev_client import trigger_task

logger = logging.getLogger(__name__)

BOOK_TASK = "cal-book"
_TIMEOUT_SEC = 30.0


class CalBookingError(Exception):
    """A cal.com create-booking call returned a non-2xx, or the request could not be built."""

    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        self.status_code = status_code
        self.body = body
        super().__init__(message)


async def trigger_book(
    *,
    close_activity_id: str,
    close_lead_id: str | None = None,
    close_contact_id: str | None = None,
    custom_activity_type_id: str | None = None,
) -> str | None:
    """Fire the ``cal-book`` task for a completed Close custom activity; return the Trigger run id.

    Best-effort: logs + returns None on any failure (the raw Close event is already durable). The
    Trigger-run idempotency key is derived from ``close_activity_id`` so Close's duplicate deliveries
    (and a re-fire) resolve to ONE run."""
    payload = {
        "closeActivityId": close_activity_id,
        "closeLeadId": close_lead_id,
        "closeContactId": close_contact_id,
        "customActivityTypeId": custom_activity_type_id,
    }
    try:
        run = await trigger_task(
            BOOK_TASK, payload, idempotency_key=f"cal-book:{close_activity_id}"
        )
    except Exception as exc:  # noqa: BLE001 — best-effort kickoff; never fail the webhook
        logger.warning("cal-book trigger failed for activity %s: %s", close_activity_id, exc)
        return None
    run_id = run.get("id") if isinstance(run, dict) else None
    logger.info("cal-book triggered for activity %s -> %s", close_activity_id, run_id)
    return run_id


def _read_mapped(source: dict[str, Any], key: str | None) -> Any:
    """Read a value from a Close object by a mapped key, tolerant of flattened (``custom.cf_x``) and
    nested (``{'custom': {'cf_x': ...}}``) custom-field shapes."""
    if not key or not isinstance(source, dict):
        return None
    if source.get(key) is not None:
        return source[key]
    if "." in key:  # nested fallback — split on the FIRST dot (cf ids carry no dots)
        head, tail = key.split(".", 1)
        nested = source.get(head)
        if isinstance(nested, dict) and nested.get(tail) is not None:
            return nested[tail]
    return None


def _to_utc_iso(value: Any, default_tz: str) -> str | None:
    """Normalize a Close datetime value to cal.com's UTC ISO-8601 (``…Z``). A naive value is
    interpreted in ``default_tz``. Unparseable input is passed through for cal.com to validate."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s
    if dt.tzinfo is None:
        try:
            dt = dt.replace(tzinfo=ZoneInfo(default_tz))
        except Exception:  # noqa: BLE001 — bad tz name → treat as UTC
            dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_booking_request(
    *,
    activity: dict[str, Any],
    attendee_name: str,
    attendee_email: str,
    field_map: dict[str, Any],
    event_type_id_map: dict[str, int],
    default_tz: str,
) -> dict[str, Any]:
    """Map a Close custom activity + resolved contact identity into the cal.com create-booking parts.

    Returns ``{event_type_id, event_type_slug, start, attendee, booking_fields_responses}``. Raises
    ``CalBookingError`` when a required piece (field map, event-type slug, start) is missing — the
    caller marks the ledger error and surfaces it."""
    if not field_map:
        raise CalBookingError("CLOSE_BOOKING_FIELD_MAP not configured")

    raw_slug = _read_mapped(activity, field_map.get("event_type_slug"))
    slug = str(raw_slug).strip() if raw_slug is not None else ""
    if not slug:
        raise CalBookingError("event-type slug not present on the activity (check field map)")
    event_type_id = event_type_id_map.get(slug)
    if event_type_id is None:
        raise CalBookingError(f"unknown event-type slug {slug!r} (not in event-type id map)")

    start = _to_utc_iso(_read_mapped(activity, field_map.get("start")), default_tz)
    if not start:
        raise CalBookingError("start datetime not present on the activity (check field map)")

    tz = _read_mapped(activity, field_map.get("timezone")) or default_tz

    # cal.com's name/email booking fields mirror the attendee; custom booking fields (e.g. the 30min
    # event's required Company-Name / Company-Website) come off the activity via the field map.
    responses: dict[str, Any] = {"name": attendee_name, "email": attendee_email}
    for cal_slug, src_key in (field_map.get("booking_fields") or {}).items():
        v = _read_mapped(activity, src_key)
        if v is not None:
            responses[cal_slug] = v

    return {
        "event_type_id": event_type_id,
        "event_type_slug": slug,
        "start": start,
        "attendee": {"name": attendee_name, "email": attendee_email, "timeZone": str(tz)},
        "booking_fields_responses": responses,
    }


async def create_booking(
    *,
    event_type_id: int,
    start: str,
    attendee: dict[str, str],
    booking_fields_responses: dict[str, Any],
    length_minutes: int | None = None,
) -> dict[str, Any]:
    """POST ``{CAL_API_BASE}/bookings`` — create a cal.com booking. Returns the parsed ``data`` object
    (booking id/uid/iCalUID). Raises ``CalBookingError`` on a missing key or a non-2xx response."""
    api_key = config.cal_api_key()
    if not api_key:
        raise CalBookingError("CAL_API_KEY not configured")

    body: dict[str, Any] = {
        "start": start,
        "eventTypeId": event_type_id,
        "attendee": attendee,
        "bookingFieldsResponses": booking_fields_responses,
    }
    if length_minutes is not None:
        body["lengthInMinutes"] = length_minutes

    headers = {
        "Authorization": f"Bearer {api_key}",
        "cal-api-version": config.cal_api_version(),
        "Content-Type": "application/json",
    }
    url = f"{config.cal_api_base()}/bookings"
    async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code >= 400:
        try:
            err: Any = r.json()
        except Exception:  # noqa: BLE001
            err = r.text
        raise CalBookingError(
            f"cal.com create-booking {r.status_code}", status_code=r.status_code, body=err
        )
    try:
        parsed = r.json()
    except Exception as exc:  # noqa: BLE001
        raise CalBookingError("cal.com returned a non-JSON booking response") from exc
    # v2 wraps the booking under {status, data: {...}}; tolerate a bare object too.
    data = parsed.get("data") if isinstance(parsed, dict) else None
    if not isinstance(data, dict):
        data = parsed if isinstance(parsed, dict) else {}
    # The booking ``id`` is the authoritative "created" signal — its absence means a 2xx of an
    # unexpected shape. Fail rather than stamp a PHANTOM success (a ledger row that claims a booking
    # exists with no identifiers, which would also wrongly short-circuit a later retry as already-booked).
    if data.get("id") is None:
        raise CalBookingError(
            "cal.com booking response missing an id (unexpected 2xx shape)", body=parsed
        )
    return data
