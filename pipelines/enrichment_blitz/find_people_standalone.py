"""Standalone Blitz Find People — gateway-routed company→all-people discovery.

Take a company identity (``company_linkedin_url``; or a ``domain`` resolved to one via the
gateway's ``resolve`` hop) and pull EVERY person Blitz returns for that company via
``POST /v2/search/people`` with NO person-level filters (exclude none). Cursor-paginated.
Endpoint-less Modal worker, structural twin of ``enrich_phone_standalone.py`` — same gateway
egress, same ops/ledger shape — but a *discovery* (search) primitive, not an enrichment one.

DECOUPLED EGRESS (Directive 23/28 §1). This worker holds NO Blitz key and makes NO direct
``api.blitz-api.ai`` calls. Every Blitz call routes through ``core/blitz_gateway.py``
(``blitz-gateway`` app, fn ``blitz_call``) — the authoritative global ≤5-RPS priority egress.
Find People is FREE on the Unlimited plan; the only governor is the 5-RPS bucket. Bulk lane
LOW/NORMAL so interactive GTM (HIGH) is never starved.

SINK. Upsert per (person_linkedin_norm × company_domain) into ``ops.blitz_find_people`` — the
find-people system-of-record owned by this pipeline. Per-run terminal state into
``ops.blitz_find_people_runs``. The Blitz person object is stored VERBATIM in ``raw_payload``;
the projected columns (names, headline, location) are a convenience ON TOP, never a replacement.

    modal deploy pipelines/enrichment_blitz/find_people_standalone.py
    modal run    pipelines/enrichment_blitz/find_people_standalone.py::init_ops
    modal run    pipelines/enrichment_blitz/find_people_standalone.py::run_manual \\
                 --companies-json '[{"domain":"x.com","company_linkedin_url":"https://www.linkedin.com/company/x"}]'
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
from typing import Any

import modal

FEED = "blitz_find_people"
GATEWAY_APP, GATEWAY_FN = "blitz-gateway", "blitz_call"
SEARCH_PATH = "/v2/search/people"          # POST: {company:{linkedin_url:[...]}, people:{}, max_results, cursor}
RESOLVE_NAME = "resolve"                    # gateway logical → /v2/enrichment/domain-to-linkedin
PAGE = 50                                   # Blitz max per request
MAX_PAGES = 200                             # safety cap (≤10k people/company; cohort is small co's)
VALID_PRIORITIES = {"low", "normal"}

# Empty person filter block = return ALL people (exclude none).
EMPTY_PEOPLE = {
    "job_title": {"include_linkedin_headline": False, "include": [], "exclude": []},
    "job_function": [], "job_level": [], "min_connections": 0,
    "location": {"city": [], "country_code": [], "continent": [], "sales_region": []},
    "education": {"include": [], "exclude": []},
}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",   # ops.blitz_find_people + ops.blitz_find_people_runs
    "requests>=2.32",         # Trigger callback
)
app = modal.App("blitz-find-people", image=image)
SECRETS = [modal.Secret.from_name("hqx-postgres")]   # NO blitz-api secret — gateway owns the key.

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.blitz_find_people (
    record_id            text        PRIMARY KEY,   -- md5(person_linkedin_norm | company_domain)
    person_linkedin_url  text        NOT NULL,
    person_linkedin_norm text        NOT NULL,
    company_domain       text,
    company_linkedin_url text,
    first_name           text,
    last_name            text,
    full_name            text,
    headline             text,
    loc_city             text,
    loc_state            text,
    loc_country_iso      text,
    loc_continent        text,
    raw_payload          jsonb       NOT NULL,
    source               text        NOT NULL,
    batch_label          text,
    landed_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS blitz_find_people_domain_idx   ON ops.blitz_find_people (company_domain);
CREATE INDEX IF NOT EXISTS blitz_find_people_linkedin_idx ON ops.blitz_find_people (person_linkedin_norm);
CREATE INDEX IF NOT EXISTS blitz_find_people_country_idx  ON ops.blitz_find_people (loc_country_iso);

CREATE TABLE IF NOT EXISTS ops.blitz_find_people_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    batch_label     text,
    run_root        text,
    priority        text        NOT NULL,
    companies       bigint      NOT NULL DEFAULT 0,
    resolved        bigint      NOT NULL DEFAULT 0,
    no_linkedin     bigint      NOT NULL DEFAULT 0,
    people_found    bigint      NOT NULL DEFAULT 0,
    people_upserted bigint      NOT NULL DEFAULT 0,
    gateway_calls   bigint      NOT NULL DEFAULT 0,
    failed          bigint      NOT NULL DEFAULT 0,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_find_people_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS blitz_find_people_runs_feed_idx        ON ops.blitz_find_people_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_find_people_runs_recorded_at_idx ON ops.blitz_find_people_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _norm_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _WWW.sub("", _SCHEME.sub("", d)).split("/", 1)[0].rstrip(".")
    return d or None


def _norm_li(raw: str | None) -> str | None:
    s = (raw or "").strip().lower()
    if not s:
        return None
    s = _WWW.sub("", _SCHEME.sub("", s)).split("?", 1)[0].rstrip("/")
    return s or None


def _hqx_dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError("Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in hqx-postgres.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


def _resolve_company_li(gw, company: dict, priority: str, counts: dict) -> str | None:
    """company_linkedin_url passthrough, else resolve domain→linkedin via the gateway (free on Unlimited)."""
    li = (company.get("company_linkedin_url") or "").strip() or None
    if li:
        return li
    dom = _norm_domain(company.get("domain"))
    if not dom:
        return None
    counts["gateway_calls"] += 1
    rb = gw.remote(endpoint=RESOLVE_NAME, payload={"domain": dom}, priority=priority)
    data = rb.get("data") if isinstance(rb, dict) else None
    if isinstance(data, dict) and data.get("found"):
        counts["resolved"] += 1
        return (data.get("company_linkedin_url") or "").strip() or None
    return None


def _find_people_for_company(gw, company_li: str, priority: str, counts: dict) -> list[dict]:
    """Paginate POST /v2/search/people for one company, NO person filters → every person."""
    people: list[dict] = []
    cursor = None
    for _ in range(MAX_PAGES):
        payload = {"company": {"linkedin_url": [company_li]}, "people": EMPTY_PEOPLE,
                   "max_results": PAGE, "cursor": cursor}
        counts["gateway_calls"] += 1
        rb = gw.remote(endpoint=SEARCH_PATH, payload=payload, priority=priority)
        if not (isinstance(rb, dict) and rb.get("ok")):
            counts["failed"] += 1
            break
        data = rb.get("data") or {}
        results = data.get("results") or []
        people.extend(results)
        cursor = data.get("cursor")
        if not cursor or not results:
            break
    return people


def _upsert_person(cur, p: dict, company_li: str, company_domain: str | None, batch_label: str | None) -> bool:
    from psycopg.types.json import Jsonb
    purl = (p.get("linkedin_url") or "").strip() or None
    pnorm = _norm_li(purl)
    if not pnorm:
        return False
    rid = hashlib.md5(f"{pnorm}|{company_domain or ''}".encode()).hexdigest()
    loc = p.get("location") or {}
    cur.execute(
        """
        INSERT INTO ops.blitz_find_people
            (record_id, person_linkedin_url, person_linkedin_norm, company_domain, company_linkedin_url,
             first_name, last_name, full_name, headline, loc_city, loc_state, loc_country_iso, loc_continent,
             raw_payload, source, batch_label, landed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (record_id) DO UPDATE SET
            person_linkedin_url = EXCLUDED.person_linkedin_url,
            first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, full_name=EXCLUDED.full_name,
            headline=EXCLUDED.headline, loc_city=EXCLUDED.loc_city, loc_state=EXCLUDED.loc_state,
            loc_country_iso=EXCLUDED.loc_country_iso, loc_continent=EXCLUDED.loc_continent,
            raw_payload=EXCLUDED.raw_payload, batch_label=EXCLUDED.batch_label, landed_at=now()
        """,
        (rid, purl, pnorm, company_domain, company_li,
         p.get("first_name"), p.get("last_name"), p.get("full_name"), p.get("headline"),
         loc.get("city"), loc.get("state_code"), loc.get("country_code"), loc.get("continent"),
         Jsonb(p), FEED, batch_label),
    )
    return True


def _record_run(cur, batch_label, run_root, priority, counts, status, error, started_at, completed_at):
    cur.execute(OPS_DDL)
    cur.execute(
        """INSERT INTO ops.blitz_find_people_runs
           (feed, batch_label, run_root, priority, companies, resolved, no_linkedin,
            people_found, people_upserted, gateway_calls, failed, status, error, started_at, completed_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (FEED, batch_label, run_root, priority, counts["companies"], counts["resolved"],
         counts["no_linkedin"], counts["people_found"], counts["people_upserted"],
         counts["gateway_calls"], counts["failed"], status, error, started_at, completed_at),
    )


def _post_callback(url, body):
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    try:
        import requests
        requests.post(url, json=body, timeout=30)
    except Exception as exc:  # noqa: BLE001
        print(f"callback POST failed: {exc}")


def _run(companies: list[dict], batch_label, run_id, priority, trigger_callback_url) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"companies": 0, "resolved": 0, "no_linkedin": 0, "people_found": 0,
              "people_upserted": 0, "gateway_calls": 0, "failed": 0}
    status, error = "error", None
    gw = _gateway()
    try:
        companies = [c for c in (companies or []) if isinstance(c, dict)]
        counts["companies"] = len(companies)
        with _open_conn(_hqx_dsn()) as conn:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            for c in companies:
                company_li = _resolve_company_li(gw, c, priority, counts)
                if not company_li:
                    counts["no_linkedin"] += 1
                    continue
                dom = _norm_domain(c.get("domain"))
                ppl = _find_people_for_company(gw, company_li, priority, counts)
                counts["people_found"] += len(ppl)
                for p in ppl:
                    if _upsert_person(cur, p, company_li, dom, batch_label):
                        counts["people_upserted"] += 1
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc); status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        try:
            with _open_conn(_hqx_dsn()) as conn2:
                _record_run(conn2.cursor(), batch_label, run_id, priority, counts, status, error, started_at, completed_at)
        except Exception as exc:  # noqa: BLE001
            print(f"run-ledger write failed: {exc}")
        _post_callback(trigger_callback_url, {"status": status, "feed": FEED, "batch_label": batch_label,
                                              "error": error, **counts})
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_find_people(companies: list[dict], batch_label: str | None = None, run_id: str | None = None,
                    priority: str = "low", trigger_callback_url: str | None = None) -> dict:
    """Find every person at each company (no person filters), upsert ops.blitz_find_people.
    companies: [{"domain": str|None, "company_linkedin_url": str|None}, ...]."""
    return _run(companies, batch_label, run_id, priority, trigger_callback_url)


# ── Reverse enrich: email/phone → FULL person profile (same grain as find-people) ──
REVERSE_EMAIL_PATH = "/v2/enrichment/email-to-person"   # {email}  → {found, person{...}}
REVERSE_PHONE_PATH = "/v2/enrichment/phone-to-person"   # {phone}  → {found, person{...}}


def _reverse_run(contacts, batch_label, run_id, priority, trigger_callback_url) -> dict:
    """Pull the full Blitz profile for a known person via reverse lookup — email-to-person
    (preferred) or phone-to-person — and upsert into ops.blitz_find_people (same person grain,
    keyed record_id=md5(linkedin_norm|company_domain)). Gateway-routed, free on Unlimited."""
    started_at = dt.datetime.now(dt.timezone.utc)
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"companies": 0, "resolved": 0, "no_linkedin": 0, "people_found": 0,
              "people_upserted": 0, "gateway_calls": 0, "failed": 0}
    status, error = "error", None
    gw = _gateway()
    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict)]
        counts["companies"] = len(contacts)   # 'companies' slot reused = contacts processed
        with _open_conn(_hqx_dsn()) as conn:
            cur = conn.cursor()
            cur.execute(OPS_DDL)
            for c in contacts:
                dom = _norm_domain(c.get("company_domain"))
                email = (c.get("email") or "").strip() or None
                phone = (c.get("phone") or "").strip() or None
                if email:
                    ep, pl = REVERSE_EMAIL_PATH, {"email": email}
                elif phone:
                    ep, pl = REVERSE_PHONE_PATH, {"phone": phone}
                else:
                    counts["no_linkedin"] += 1
                    continue
                counts["gateway_calls"] += 1
                rb = gw.remote(endpoint=ep, payload=pl, priority=priority)
                if not (isinstance(rb, dict) and rb.get("ok")):
                    counts["failed"] += 1
                    continue
                data = rb.get("data") or {}
                person = data.get("person") if isinstance(data, dict) else None
                if not (data.get("found") and isinstance(person, dict) and person.get("linkedin_url")):
                    continue   # reverse miss
                counts["people_found"] += 1
                if _upsert_person(cur, person, None, dom, batch_label):
                    counts["people_upserted"] += 1
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc); status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        try:
            with _open_conn(_hqx_dsn()) as conn2:
                _record_run(conn2.cursor(), batch_label, run_id, priority, counts, status, error, started_at, completed_at)
        except Exception as exc:  # noqa: BLE001
            print(f"run-ledger write failed: {exc}")
        _post_callback(trigger_callback_url, {"status": status, "feed": FEED, "batch_label": batch_label,
                                              "mode": "reverse", "error": error, **counts})
    return {"feed": FEED, "mode": "reverse", "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_reverse_enrich(contacts: list[dict], batch_label: str | None = None, run_id: str | None = None,
                       priority: str = "low", trigger_callback_url: str | None = None) -> dict:
    """Reverse-enrich known people → full Blitz profile from email (email-to-person, preferred)
    or phone (phone-to-person). contacts: [{"company_domain","email","phone"}]."""
    return _reverse_run(contacts, batch_label, run_id, priority, trigger_callback_url)


@app.function(secrets=SECRETS, timeout=60 * 5)
def apply_ops_ddl() -> dict:
    with _open_conn(_hqx_dsn()) as conn:
        conn.cursor().execute(OPS_DDL)
    return {"ok": True, "tables": ["ops.blitz_find_people", "ops.blitz_find_people_runs"]}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run_manual(companies_json: str, priority: str = "low") -> None:
    import json
    companies = json.loads(companies_json)
    print(json.dumps(run_find_people.remote(companies, batch_label="manual", priority=priority), indent=2, default=str))
