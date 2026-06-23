"""gtm.contacts — curated GTM contact intake (append-only).

Endpoints (mounted at ``/api/v1/contacts``, service-token gated):
  POST /land   → land ONE contact (one row per request), idempotent on byte-identical resends
  POST /check  → has this work_email (optionally at this company) been landed?
  GET  /stats  → row / distinct-person / distinct-company counts + main-contact rollup

WIRE CONTRACT. ONE contact per request, FLAT singular fields (no nested raw_payload on the wire)::

    {
      "full_name":            "Jane A. Smith",                         // required (middle name/initial optional)
      "work_email":           "jane.smith@auxcap.com",                 // required — person identity key
      "job_title":            "VP, Asset Based Lending",               // optional
      "is_main_contact":      "true",                                  // optional — "true"/"false" (also 1/0, yes/no)
      "city":                 "Wayne",                                 // optional
      "state":                "PA",                                    // optional
      "country":              "United States",                         // optional
      "company_name":         "Auxilior Capital Partners",             // required
      "company_domain":       "auxcap.com",                            // optional*
      "company_linkedin_url": "https://www.linkedin.com/company/auxilior-capital-partners"  // optional*
    }

  * At least one of `company_domain` or `company_linkedin_url` must normalize to a usable bridge key.

STORAGE. Stored TWO ways, both faithful to source:
  1. raw_payload (jsonb) — the flat body EXACTLY as sent. Immutable source of truth; drift-proof.
  2. flat typed columns — verbatim values + canonical bridge keys (domain_norm, company_linkedin_url_norm)
     computed server-side. full_name is split into first/middle/last; the verbatim full_name is kept.
     job_title is verbatim — semantic normalization (→ job_level enum) is a SEPARATE downstream stage.

IDENTITY / GRAIN. (person × company), APPEND-ONLY HISTORY.
    person_id   = sha256(work_email_norm)                  — stable cross-company person key (email-rail)
    contact_key = sha256(work_email_norm | domain_norm)    — stable (person × company) key; reader takes latest by landed_at
    record_id   = sha256(identity | every mutable field)   — PK; identical resend = no-op (ON CONFLICT DO NOTHING),
                                                              any change lands a NEW historical row. No in-place mutation.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb

from ..db import get_db_connection
from ..service_token import require_service_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/contacts", tags=["contacts"])


# ── domain normalization (mirrors firmographics_blitz._normalized_domain) ──
_SCHEME_RE = re.compile(r"^https?://", flags=re.I)
_WWW_RE = re.compile(r"^www\.", flags=re.I)
_PATH_RE = re.compile(r"/.*$")
_TRAIL_DOTS_RE = re.compile(r"\.+$")


def _normalize_domain(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    s = _SCHEME_RE.sub("", s)
    s = _WWW_RE.sub("", s)
    s = _PATH_RE.sub("", s)
    s = _TRAIL_DOTS_RE.sub("", s)
    return s or None


# ── linkedin url normalization — lower, strip scheme + www, collapse locale subdomain, strip trailing / ──
_LI_LOCALE_RE = re.compile(r"^[a-z]{2}\.linkedin\.com/", flags=re.I)


def _normalize_linkedin(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if not s:
        return None
    s = _SCHEME_RE.sub("", s)
    s = _WWW_RE.sub("", s)
    s = _LI_LOCALE_RE.sub("linkedin.com/", s)
    s = s.rstrip("/")
    if not s.startswith("linkedin.com/"):
        return None
    return s or None


# ── email normalization — lower + trim; accepted only if it is shaped like an address ──
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _normalize_email(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    return s if _EMAIL_RE.match(s) else None


# ── is_main_contact coercion — "true"/"false" (also 1/0, yes/no, bool). NULL if absent/unparseable ──
_TRUE = {"true", "t", "1", "yes", "y"}
_FALSE = {"false", "f", "0", "no", "n"}


def _to_bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return None


def _s(v: Any) -> str | None:
    """Verbatim text projection — trims whitespace only, never interprets."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _split_name(full: str) -> tuple[str | None, str | None, str | None]:
    """first / middle / last from a verbatim full name. Single token → first only;
    two → first+last; three+ → middle captures everything between (incl. a middle initial)."""
    parts = full.split()
    if not parts:
        return None, None, None
    first = parts[0]
    last = parts[-1] if len(parts) >= 2 else None
    middle = " ".join(parts[1:-1]) if len(parts) >= 3 else None
    return first, middle, last


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# Column order is the single source of truth for the INSERT placeholders below.
_COLS = (
    "record_id", "contact_key", "person_id",
    "full_name", "first_name", "middle_name", "last_name",
    "work_email", "work_email_norm", "job_title", "is_main_contact",
    "city", "state", "country",
    "company_name", "company_domain", "domain_norm",
    "company_linkedin_url", "company_linkedin_url_norm",
    "source", "raw_payload",
)
_INSERT_SQL = (
    f"INSERT INTO gtm.contacts ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (record_id) DO NOTHING"
)

_STATS_SQL = """
    SELECT count(*)                                          AS rows,
           count(DISTINCT person_id)                         AS distinct_persons,
           count(DISTINCT contact_key)                       AS distinct_contacts,
           count(DISTINCT domain_norm)                       AS distinct_domains,
           count(*) FILTER (WHERE is_main_contact IS TRUE)   AS main_contact_rows,
           count(*) FILTER (WHERE domain_norm IS NOT NULL)   AS with_domain,
           count(*) FILTER (WHERE company_linkedin_url_norm IS NOT NULL) AS with_linkedin
    FROM gtm.contacts
"""

_SOURCE = "contacts"


def _to_row(rec: dict[str, Any]) -> tuple | None:
    """Project one flat contact body → the full column tuple. None if a hard requirement fails:
    work_email must be a valid address, full_name + company_name must be present, and at least one
    of company_domain / company_linkedin_url must normalize to a usable bridge key."""
    work_email_norm = _normalize_email(rec.get("work_email"))
    full_name = _s(rec.get("full_name"))
    company_name = _s(rec.get("company_name"))
    if not work_email_norm or not full_name or not company_name:
        return None

    domain_norm = _normalize_domain(rec.get("company_domain"))
    linkedin_url_norm = _normalize_linkedin(rec.get("company_linkedin_url"))
    if not domain_norm and not linkedin_url_norm:
        return None

    first_name, middle_name, last_name = _split_name(full_name)
    job_title = _s(rec.get("job_title"))
    is_main_contact = _to_bool(rec.get("is_main_contact"))
    city, state, country = _s(rec.get("city")), _s(rec.get("state")), _s(rec.get("country"))

    person_id = _sha(work_email_norm)
    contact_key = _sha(f"{work_email_norm}|{domain_norm or ''}")
    # Append-only history key: identity + every mutable field. Identical resend → no-op; any change → new row.
    record_id = _sha("|".join([
        work_email_norm,
        domain_norm or "",
        linkedin_url_norm or "",
        full_name,
        job_title or "",
        "" if is_main_contact is None else ("true" if is_main_contact else "false"),
        city or "", state or "", country or "",
        company_name,
    ]))

    return (
        record_id, contact_key, person_id,
        full_name, first_name, middle_name, last_name,
        _s(rec.get("work_email")), work_email_norm, job_title, is_main_contact,
        city, state, country,
        company_name, _s(rec.get("company_domain")), domain_norm,
        _s(rec.get("company_linkedin_url")), linkedin_url_norm,
        _SOURCE, Jsonb(rec),
    )


@router.post("/land", dependencies=[Depends(require_service_token)])
async def land(body: dict[str, Any]) -> dict[str, Any]:
    """Land ONE contact. Required: full_name, work_email (valid), company_name, and at least one of
    company_domain / company_linkedin_url. Optional: job_title, is_main_contact, city, state, country."""
    row = _to_row(body)
    if row is None:
        # Precise 422 so a bulk loader can reconcile drops (a 4xx is not retried by the caller).
        if not _normalize_email(body.get("work_email")):
            raise HTTPException(status_code=422, detail="work_email is required and must be a valid email address")
        if not _s(body.get("full_name")):
            raise HTTPException(status_code=422, detail="full_name is required (non-empty string)")
        if not _s(body.get("company_name")):
            raise HTTPException(status_code=422, detail="company_name is required (non-empty string)")
        raise HTTPException(
            status_code=422,
            detail="need at least one of company_domain or company_linkedin_url that normalizes to a usable bridge key",
        )

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_SQL, row)
            landed = cur.rowcount == 1
        await conn.commit()

    return {
        "landed": landed,                 # False ⇒ this exact (person, company, field-state) was already present
        "already_present": not landed,
        "record_id": row[0],
        "contact_key": row[1],
        "person_id": row[2],
        "work_email_norm": row[8],
        "domain_norm": row[16],
        "is_main_contact": row[10],
    }


@router.post("/check", dependencies=[Depends(require_service_token)])
async def check(body: dict[str, Any]) -> dict[str, Any]:
    """Has this contact been landed? Send `work_email` (required); optionally scope by `company_domain`.
    Returns the count of historical rows and the LATEST observed state for the contact."""
    work_email_norm = _normalize_email(body.get("work_email"))
    if not work_email_norm:
        raise HTTPException(status_code=422, detail="work_email is required and must be a valid email address")
    domain_norm = _normalize_domain(body.get("company_domain"))

    # contact_key when a domain is supplied (person×company), else person-wide (across companies).
    if domain_norm:
        key_col, key_val = "contact_key", _sha(f"{work_email_norm}|{domain_norm}")
    else:
        key_col, key_val = "person_id", _sha(work_email_norm)

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT count(*)                AS n,
                       max(landed_at)          AS most_recent_at,
                       (SELECT job_title       FROM gtm.contacts WHERE {key_col} = %s ORDER BY landed_at DESC LIMIT 1),
                       (SELECT is_main_contact FROM gtm.contacts WHERE {key_col} = %s ORDER BY landed_at DESC LIMIT 1),
                       (SELECT full_name       FROM gtm.contacts WHERE {key_col} = %s ORDER BY landed_at DESC LIMIT 1),
                       (SELECT company_name    FROM gtm.contacts WHERE {key_col} = %s ORDER BY landed_at DESC LIMIT 1)
                FROM gtm.contacts WHERE {key_col} = %s
                """,
                (key_val,) * 5,
            )
            r = await cur.fetchone()
    return {
        "work_email_norm": work_email_norm,
        "domain_norm": domain_norm,
        "scope": "contact_key" if domain_norm else "person_id",
        "found": r[0] > 0,
        "record_count": r[0],
        "most_recent_at": r[1].isoformat() if r[1] else None,
        "latest_job_title": r[2],
        "latest_is_main_contact": r[3],
        "latest_full_name": r[4],
        "latest_company_name": r[5],
    }


@router.get("/stats", dependencies=[Depends(require_service_token)])
async def stats() -> dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_STATS_SQL)
            r = await cur.fetchone()
    return {
        "rows": r[0],
        "distinct_persons": r[1],
        "distinct_contacts": r[2],
        "distinct_domains": r[3],
        "main_contact_rows": r[4],
        "with_domain": r[5],
        "with_linkedin": r[6],
    }
