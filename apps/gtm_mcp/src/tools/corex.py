"""corex — the GTM control surface: the gtm-agent's conversational write + read path
over a managed go-to-market motion (operational state in the hq-x `corex` schema).

The agent carries a motion as data so the flow is REPLICABLE, not hand-done in chat:
an `initiative` (the strategic frame, forked demand/supply) → `campaign` (the
EmailBison/Lob/Vapi send unit; channel+provider are columns) → `lead`
(contact-in-campaign; per-lead research/copy/mailer derivations) → `send` (the
provider's atomic record, the inbound↔outbound reconciliation key). The thesis is a
`gtm_audience_pair` — two DISTINCT stamped Lance selections (`audience`) bound on the
initiative: the demand audience selects leads; the supply audience materializes into
the supply-side direct-mail campaign on call-book.

Conventions mirror `tools/ops.py` EXACTLY (the directive's structured write path):
own psycopg connection on `database.hqx_dsn()`, an idempotent ensure-DDL preamble
(`COREX_DDL`, self-bootstrapping like `save_campaign_audience`), the mutation inside
one `conn.transaction()`, every value parameterized, `jsonb` bound with
`psycopg.types.json.Jsonb`. The audience-running tools call the existing
`audience.execute_audience_query` (the read-only-gated Lance engine) internally — they
never re-implement SQL execution. corex is ADDITIVE: a clean schema for outbound GTM
state; it never reaches `business.*`, and it READS `ops.email_resolutions` (the
verified-email store, same hq-x DB) at enroll time — it does not rewire ops.

Ids arrive from the MCP client as JSON strings; every tool coerces id params to
`uuid.UUID` (`_uid`) so psycopg binds them as the `uuid` type — a bare `str` binds as
`text` and `uuid = text` has no operator in Postgres.

Blueprint: docs/reference/COREX_GTM_CONTROL_SURFACE.md.
"""

from __future__ import annotations

import re
import uuid as _uuid
from contextlib import contextmanager
from typing import Any

from .. import database
from . import audience

# ── Schema DDL — the canonical, self-bootstrapping copy (mirror of the sibling
# migrations/0001_corex.sql). Applied idempotently at the top of every write so the
# tables exist on a fresh DB with no out-of-band step — exactly the ops.py pattern.
# Additive only: zero DROP/TRUNCATE (the PreToolUse hook is satisfied by construction).
COREX_DDL = """
CREATE SCHEMA IF NOT EXISTS corex;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS corex.contact (
    contact_id        uuid PRIMARY KEY,
    company_id        uuid NOT NULL,
    normalized_domain text,
    full_name         text,
    title             text,
    identity          jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS contact_company_id_idx        ON corex.contact (company_id);
CREATE INDEX IF NOT EXISTS contact_normalized_domain_idx ON corex.contact (normalized_domain);

CREATE TABLE IF NOT EXISTS corex.audience (
    audience_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name           text NOT NULL,
    gtm_side       text CHECK (gtm_side IN ('demand','supply')),
    source_sql     text NOT NULL,
    result_key     text NOT NULL DEFAULT 'company_id',
    datasets       jsonb NOT NULL DEFAULT '[]'::jsonb,
    row_count      bigint,
    headline_stats jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_run_at    timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audience_gtm_side_idx    ON corex.audience (gtm_side);
CREATE INDEX IF NOT EXISTS audience_last_run_at_idx ON corex.audience (last_run_at DESC);

CREATE TABLE IF NOT EXISTS corex.initiative (
    initiative_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gtm_side      text NOT NULL CHECK (gtm_side IN ('demand','supply')),
    brand         text NOT NULL,
    name          text NOT NULL,
    thesis        text,
    client_domain text,
    status        text NOT NULL DEFAULT 'active'
                  CHECK (status IN ('draft','active','paused','archived')),
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (brand, name)
);
CREATE INDEX IF NOT EXISTS initiative_gtm_side_idx ON corex.initiative (gtm_side);
CREATE INDEX IF NOT EXISTS initiative_status_idx   ON corex.initiative (status);

CREATE TABLE IF NOT EXISTS corex.gtm_audience_pair (
    pair_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id      uuid NOT NULL UNIQUE REFERENCES corex.initiative (initiative_id),
    demand_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    supply_audience_id uuid NOT NULL REFERENCES corex.audience (audience_id),
    thesis             text NOT NULL,
    metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (demand_audience_id <> supply_audience_id)
);
CREATE INDEX IF NOT EXISTS pair_demand_audience_idx ON corex.gtm_audience_pair (demand_audience_id);
CREATE INDEX IF NOT EXISTS pair_supply_audience_idx ON corex.gtm_audience_pair (supply_audience_id);

CREATE TABLE IF NOT EXISTS corex.campaign_group (
    group_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid REFERENCES corex.initiative (initiative_id),
    name          text NOT NULL,
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (initiative_id, name)
);

CREATE TABLE IF NOT EXISTS corex.campaign (
    campaign_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    initiative_id uuid NOT NULL REFERENCES corex.initiative (initiative_id),
    audience_id   uuid REFERENCES corex.audience (audience_id),
    group_id      uuid REFERENCES corex.campaign_group (group_id),
    campaign_key  text NOT NULL,
    channel       text NOT NULL CHECK (channel  IN ('email','direct_mail','voice')),
    provider      text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    provider_campaign_id text,
    status        text NOT NULL DEFAULT 'draft'
                  CHECK (status IN ('draft','enrolling','live','paused','complete')),
    config        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CHECK ( (provider='emailbison' AND channel='email')
         OR (provider='lob'        AND channel='direct_mail')
         OR (provider='vapi'       AND channel='voice') ),
    UNIQUE (initiative_id, campaign_key)
);
CREATE INDEX IF NOT EXISTS campaign_initiative_idx ON corex.campaign (initiative_id);
CREATE INDEX IF NOT EXISTS campaign_audience_idx   ON corex.campaign (audience_id);
CREATE INDEX IF NOT EXISTS campaign_group_idx      ON corex.campaign (group_id);
CREATE INDEX IF NOT EXISTS campaign_key_idx        ON corex.campaign (campaign_key);

CREATE TABLE IF NOT EXISTS corex.lead (
    lead_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id    uuid NOT NULL REFERENCES corex.campaign (campaign_id),
    contact_id     uuid NOT NULL REFERENCES corex.contact (contact_id),
    provider_lead_id text,
    enrolled_from_audience_id uuid REFERENCES corex.audience (audience_id),
    contactable    jsonb NOT NULL DEFAULT '{}'::jsonb,
    research_status  text NOT NULL DEFAULT 'pending'
                     CHECK (research_status IN ('pending','running','ready','failed','skipped')),
    research         jsonb NOT NULL DEFAULT '{}'::jsonb,
    research_at      timestamptz,
    copy_status      text NOT NULL DEFAULT 'pending'
                     CHECK (copy_status IN ('pending','drafting','ready','approved','failed')),
    copy             jsonb NOT NULL DEFAULT '{}'::jsonb,
    copy_at          timestamptz,
    mailer_status    text NOT NULL DEFAULT 'pending'
                     CHECK (mailer_status IN ('pending','materializing','ready','sent','failed')),
    mailer           jsonb NOT NULL DEFAULT '{}'::jsonb,
    mailer_at        timestamptz,
    status         text NOT NULL DEFAULT 'enrolled'
                   CHECK (status IN ('enrolled','researched','drafted','materialized','sent','engaged','disqualified')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, contact_id)
);
CREATE INDEX IF NOT EXISTS lead_campaign_idx      ON corex.lead (campaign_id);
CREATE INDEX IF NOT EXISTS lead_contact_idx       ON corex.lead (contact_id);
CREATE INDEX IF NOT EXISTS lead_status_idx        ON corex.lead (status);
CREATE INDEX IF NOT EXISTS lead_provider_lead_idx ON corex.lead (provider_lead_id);

CREATE TABLE IF NOT EXISTS corex.send (
    send_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id          uuid NOT NULL REFERENCES corex.lead (lead_id),
    campaign_id      uuid NOT NULL REFERENCES corex.campaign (campaign_id),
    provider         text NOT NULL CHECK (provider IN ('emailbison','lob','vapi')),
    provider_send_id text,
    touch_no         smallint NOT NULL DEFAULT 1,
    direction        text NOT NULL DEFAULT 'outbound'
                     CHECK (direction IN ('outbound','inbound')),
    status           text NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued','submitted','in_transit','delivered',
                                       'returned','responded','failed','canceled')),
    inbound_ref      jsonb NOT NULL DEFAULT '{}'::jsonb,
    events           jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_event_at    timestamptz,
    submitted_at     timestamptz,
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_send_id)
);
CREATE INDEX IF NOT EXISTS send_lead_idx         ON corex.send (lead_id);
CREATE INDEX IF NOT EXISTS send_campaign_idx     ON corex.send (campaign_id);
CREATE INDEX IF NOT EXISTS send_provider_sid_idx ON corex.send (provider, provider_send_id);
CREATE INDEX IF NOT EXISTS send_status_idx       ON corex.send (status);
CREATE INDEX IF NOT EXISTS send_inbound_ref_gin  ON corex.send USING gin (inbound_ref jsonb_path_ops);

ALTER TABLE corex.initiative
    ADD COLUMN IF NOT EXISTS audience_pair_id uuid REFERENCES corex.gtm_audience_pair (pair_id);
CREATE INDEX IF NOT EXISTS initiative_pair_idx ON corex.initiative (audience_pair_id);
"""

_SLUG_RE = re.compile(r"[^A-Z0-9]+")
_TITLE_RANKS = (
    ("CEO", 95), ("PRESIDENT", 92), ("CHIEF", 90), ("FOUNDER", 88), ("OWNER", 85),
    ("PARTNER", 80), ("PRINCIPAL", 78), ("VICE PRESIDENT", 70), ("VP", 70),
    ("DIRECTOR", 60), ("HEAD", 55), ("MANAGER", 40),
)


# ── helpers ──────────────────────────────────────────────────────────────────
def _uid(value: Any):
    """Coerce a uuid string to uuid.UUID so psycopg binds it as the `uuid` type
    (a bare str binds as text, and `uuid = text` has no operator). None passes through."""
    if value is None or isinstance(value, _uuid.UUID):
        return value
    return _uuid.UUID(str(value))


def _jsonify(v: Any) -> Any:
    """Recursively make a psycopg row JSON-safe for the MCP channel: uuid → str,
    datetime → ISO string. Passes everything else through."""
    if isinstance(v, dict):
        return {k: _jsonify(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonify(x) for x in v]
    if isinstance(v, _uuid.UUID):
        return str(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _slug(text: str, maxlen: int = 16) -> str:
    """A compact UPPER_ALNUM token for campaign keys, e.g. 'FL surety carriers' → 'FLSURETYCARRIER'."""
    s = _SLUG_RE.sub("", (text or "").upper())
    return s[:maxlen] or "AUD"


def _title_rank(title: str | None) -> int:
    """Rank a job title for 'best contact at a company' selection (higher = more senior)."""
    t = (title or "").upper()
    return max((rank for kw, rank in _TITLE_RANKS if kw in t), default=0)


def _J(obj: Any, default: Any = None):
    """Bind a Python value as jsonb (lazy import — psycopg is optional at module load)."""
    from psycopg.types.json import Jsonb
    return Jsonb(obj if obj is not None else (default if default is not None else {}))


@contextmanager
def _cursor():
    """Own psycopg connection on the hq-x DSN, inside one transaction, with the corex
    DDL ensured (self-bootstrap) and a dict_row cursor — the ops.py write idiom."""
    dsn = database.hqx_dsn()
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED is not set — cannot reach hq-x to write corex state.")
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(COREX_DDL)
                yield cur


def _stamp_count(sql: str) -> int:
    """True row count of an audience SQL (not the 1000-row execute_audience_query cap):
    wrap it in count(*) and run it through the read-only Lance engine."""
    wrapped = f"SELECT count(*) AS n FROM (\n{sql}\n) _corex_cnt"
    res = database.query(wrapped, datasets=database.referenced_datasets(sql))
    rows = res.get("rows") or []
    return int(rows[0]["n"]) if rows and rows[0].get("n") is not None else 0


def _insert_audience(cur, name: str, source_sql: str, gtm_side: str | None,
                     result_key: str, run: bool, headline_stats: dict | None) -> dict:
    """Insert one corex.audience row, stamping {row_count, last_run_at} when run=True."""
    row_count = _stamp_count(source_sql) if run else None
    cur.execute(
        """
        INSERT INTO corex.audience
            (name, gtm_side, source_sql, result_key, datasets, row_count, headline_stats, last_run_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CASE WHEN %s THEN now() ELSE NULL END)
        RETURNING audience_id, name, gtm_side, result_key, row_count, last_run_at, created_at
        """,
        (name.strip(), gtm_side, source_sql.strip(), (result_key or "company_id").strip(),
         _J(sorted(database.referenced_datasets(source_sql)), default=[]),
         row_count, _J(headline_stats, default={}), run),
    )
    return _jsonify(cur.fetchone())


# ════════════════════════ WRITE / ADVANCE TOOLS ═══════════════════════════════
def create_initiative(
    gtm_side: str,
    brand: str,
    name: str,
    thesis: str | None = None,
    client_domain: str | None = None,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """Create (or update) a GTM initiative — the strategic frame and root of the motion.

    `gtm_side` forks the same shape into two trees: **'demand'** = outreach to win a
    client (brand is your own, e.g. OutboundSolutions.com); **'supply'** = lead-gen FOR
    a client (brand is the client's; set `client_domain`). `name` is unique within a
    brand — re-creating the same (brand, name) updates it in place. Returns the
    initiative row (`initiative_id` is the handle you pass to every downstream tool).
    """
    if gtm_side not in ("demand", "supply"):
        raise ValueError("gtm_side must be 'demand' or 'supply'")
    if not (brand or "").strip() or not (name or "").strip():
        raise ValueError("brand and name are required")
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO corex.initiative (gtm_side, brand, name, thesis, client_domain, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (brand, name) DO UPDATE
               SET gtm_side=EXCLUDED.gtm_side, thesis=EXCLUDED.thesis,
                   client_domain=EXCLUDED.client_domain, metadata=EXCLUDED.metadata, updated_at=now()
            RETURNING initiative_id, gtm_side, brand, name, thesis, client_domain, status,
                      created_at, updated_at, (xmax = 0) AS inserted
            """,
            (gtm_side, brand.strip(), name.strip(), thesis, client_domain, _J(metadata)),
        )
        row = cur.fetchone()
    return {"status": "ok", "operation": "inserted" if row.pop("inserted") else "updated",
            "initiative": _jsonify(row)}


def define_audience(
    name: str,
    source_sql: str,
    gtm_side: str | None = None,
    result_key: str = "company_id",
    run: bool = True,
    headline_stats: dict | None = None,
) -> dict[str, Any]:
    """Define and STAMP a reusable audience — a DuckDB SQL selection over the committed
    Lance lake plus its stamp {row_count, last_run_at}. The SQL is stored as DATA (never
    a write-time mutation); `run=True` executes it read-only via `execute_audience_query`
    to capture the true row count. `result_key` is the id column the rows resolve to
    ('company_id' | 'contact_id' | 'recipient_uei'). Use `define_audience_pair` instead
    when binding the two sides of a thesis. Returns the audience row with its stamp.
    """
    if not (name or "").strip() or not (source_sql or "").strip():
        raise ValueError("name and source_sql are required")
    if gtm_side is not None and gtm_side not in ("demand", "supply"):
        raise ValueError("gtm_side, when given, must be 'demand' or 'supply'")
    with _cursor() as cur:
        aud = _insert_audience(cur, name, source_sql, gtm_side, result_key, run, headline_stats)
    return {"status": "ok", "audience": aud}


def define_audience_pair(
    initiative_id: str,
    demand_sql: str,
    supply_sql: str,
    thesis: str,
    demand_name: str,
    supply_name: str,
    demand_result_key: str = "company_id",
    supply_result_key: str = "recipient_uei",
    run: bool = True,
) -> dict[str, Any]:
    """Set the initiative's thesis-as-data: bind TWO DISTINCT stamped audiences as a
    `gtm_audience_pair` (1:1 on the initiative). The **demand** audience selects the
    leads you'll outreach; the **supply** audience is what materializes into the
    direct-mail campaign on call-book. Both SQLs are stamped (true row counts via the
    read-only Lance engine) when `run=True`. Re-running updates the pair in place.
    Returns `{pair_id, demand_audience:{…stamp…}, supply_audience:{…stamp…}}`.
    """
    initiative_id = _uid(initiative_id)
    if not (thesis or "").strip():
        raise ValueError("thesis is required")
    for label, sql, nm in (("demand", demand_sql, demand_name), ("supply", supply_sql, supply_name)):
        if not (sql or "").strip() or not (nm or "").strip():
            raise ValueError(f"{label}_sql and {label}_name are required")
    with _cursor() as cur:
        demand = _insert_audience(cur, demand_name, demand_sql, "demand", demand_result_key, run, None)
        supply = _insert_audience(cur, supply_name, supply_sql, "supply", supply_result_key, run, None)
        cur.execute(
            """
            INSERT INTO corex.gtm_audience_pair (initiative_id, demand_audience_id, supply_audience_id, thesis)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (initiative_id) DO UPDATE
               SET demand_audience_id=EXCLUDED.demand_audience_id,
                   supply_audience_id=EXCLUDED.supply_audience_id,
                   thesis=EXCLUDED.thesis, updated_at=now()
            RETURNING pair_id, (xmax = 0) AS inserted
            """,
            (initiative_id, _uid(demand["audience_id"]), _uid(supply["audience_id"]), thesis.strip()),
        )
        pair = cur.fetchone()
        cur.execute(
            "UPDATE corex.initiative SET audience_pair_id=%s, updated_at=now() WHERE initiative_id=%s",
            (pair["pair_id"], initiative_id),
        )
    return {"status": "ok", "operation": "inserted" if pair["inserted"] else "updated",
            "pair_id": str(pair["pair_id"]), "demand_audience": demand, "supply_audience": supply}


def create_campaign(
    initiative_id: str,
    channel: str,
    provider: str,
    audience_id: str | None = None,
    group_id: str | None = None,
    campaign_key: str | None = None,
    quality: str | None = None,
    segment: str | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    """Open a campaign — the EmailBison/Lob/Vapi send unit — under an initiative.
    `channel`+`provider` must agree ('email'↔'emailbison', 'direct_mail'↔'lob',
    'voice'↔'vapi'). Points at one `audience_id` (the selection it enrolls) and may be
    tagged into a `group_id` bag. `campaign_key` is the deterministic human handle; if
    omitted it is minted `{AUDIENCE_SLUG}_{QUALITY}_{SEGMENT}` (quality/segment default
    'ALL'). Re-creating the same (initiative, campaign_key) updates it. Returns the row.
    """
    if channel not in ("email", "direct_mail", "voice"):
        raise ValueError("channel must be 'email' | 'direct_mail' | 'voice'")
    if provider not in ("emailbison", "lob", "vapi"):
        raise ValueError("provider must be 'emailbison' | 'lob' | 'vapi'")
    initiative_id, audience_id, group_id = _uid(initiative_id), _uid(audience_id), _uid(group_id)
    if not campaign_key:
        slug = "AUD"
        if audience_id:
            with _cursor() as cur:
                cur.execute("SELECT name FROM corex.audience WHERE audience_id=%s", (audience_id,))
                r = cur.fetchone()
                if r:
                    slug = _slug(r["name"])
        campaign_key = f"{slug}_{_slug(quality or 'ALL', 10)}_{_slug(segment or 'ALL', 10)}"
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO corex.campaign (initiative_id, audience_id, group_id, campaign_key, channel, provider, config)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (initiative_id, campaign_key) DO UPDATE
               SET audience_id=EXCLUDED.audience_id, group_id=EXCLUDED.group_id,
                   channel=EXCLUDED.channel, provider=EXCLUDED.provider,
                   config=EXCLUDED.config, updated_at=now()
            RETURNING campaign_id, initiative_id, audience_id, group_id, campaign_key,
                      channel, provider, status, created_at, (xmax = 0) AS inserted
            """,
            (initiative_id, audience_id, group_id, campaign_key, channel, provider, _J(config)),
        )
        row = cur.fetchone()
    return {"status": "ok", "operation": "inserted" if row.pop("inserted") else "updated",
            "campaign": _jsonify(row)}


def _resolve_contacts(rows: list[dict], result_key: str) -> list[dict]:
    """Turn audience rows into contact identities. result_key='contact_id' → the rows
    already ARE contacts. result_key='company_id' → resolve the most-senior contact at
    each company from the Lance `people` graph (one batched query)."""
    if result_key == "contact_id":
        out = []
        for r in rows:
            cid = r.get("contact_id")
            if cid:
                out.append({"contact_id": str(cid), "company_id": str(r.get("company_id") or ""),
                            "normalized_domain": r.get("normalized_domain"),
                            "full_name": r.get("full_name"), "title": r.get("title")})
        return out

    company_ids = [str(r["company_id"]) for r in rows if r.get("company_id")]
    if not company_ids:
        return []
    in_list = ",".join("'" + c.replace("'", "''") + "'" for c in dict.fromkeys(company_ids))
    pres = database.query(
        f"""SELECT contact_id, company_id, normalized_domain, full_name, title
            FROM people WHERE company_id IN ({in_list})""",
        datasets={"people"}, max_rows=1000,
    )
    best: dict[str, dict] = {}
    for p in pres["rows"]:
        comp = str(p["company_id"])
        if comp not in best or _title_rank(p.get("title")) > _title_rank(best[comp].get("title")):
            best[comp] = p
    return [{"contact_id": str(p["contact_id"]), "company_id": str(p["company_id"]),
             "normalized_domain": p.get("normalized_domain"), "full_name": p.get("full_name"),
             "title": p.get("title")} for p in best.values()]


def enroll_leads_from_audience(
    campaign_id: str,
    audience_id: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    """Enroll an audience as leads in a campaign. Re-runs the audience's stored SQL over
    the live Lance lake (freshness over a frozen snapshot), resolves each row to a
    contact (companies resolve to their most-senior person via the `people` graph), and
    upserts a passive `corex.contact` + a `corex.lead` per contact. The verified email is
    sourced from `ops.email_resolutions` (same hq-x DB, keyed by contact_id) and stamped
    on the lead's `contactable`. Idempotent: one human appears once per campaign. Returns
    `{enrolled, leads:[{lead_id, contact_id, email}]}`.
    """
    campaign_id, audience_id = _uid(campaign_id), _uid(audience_id)
    with _cursor() as cur:
        if audience_id is None:
            cur.execute("SELECT audience_id FROM corex.campaign WHERE campaign_id=%s", (campaign_id,))
            r = cur.fetchone()
            if not r:
                raise ValueError(f"unknown campaign {campaign_id}")
            if not r["audience_id"]:
                raise ValueError("campaign has no audience_id; pass audience_id explicitly")
            audience_id = r["audience_id"]
        cur.execute("SELECT source_sql, result_key FROM corex.audience WHERE audience_id=%s", (audience_id,))
        a = cur.fetchone()
        if not a:
            raise ValueError(f"unknown audience {audience_id}")
        source_sql, result_key = a["source_sql"], a["result_key"]

    # Read path (DuckDB/Lance), separate connection from the psycopg write.
    rows = audience.execute_audience_query(source_sql)["rows"][: max(1, limit)]
    contacts = _resolve_contacts(rows, result_key)

    enrolled: list[dict] = []
    with _cursor() as cur:
        for c in contacts:
            cur.execute(
                "SELECT email, verification_status FROM ops.email_resolutions "
                "WHERE contact_id::text = %s ORDER BY resolved_at DESC NULLS LAST LIMIT 1",
                (c["contact_id"],),
            )
            er = cur.fetchone()
            contactable = {"email": er["email"], "email_status": er["verification_status"]} if er else {}
            cur.execute(
                """
                INSERT INTO corex.contact (contact_id, company_id, normalized_domain, full_name, title)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (contact_id) DO UPDATE
                   SET company_id=EXCLUDED.company_id, normalized_domain=EXCLUDED.normalized_domain,
                       full_name=EXCLUDED.full_name, title=EXCLUDED.title, updated_at=now()
                """,
                (_uid(c["contact_id"]), _uid(c["company_id"] or c["contact_id"]),
                 c.get("normalized_domain"), c.get("full_name"), c.get("title")),
            )
            cur.execute(
                """
                INSERT INTO corex.lead (campaign_id, contact_id, enrolled_from_audience_id, contactable)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (campaign_id, contact_id) DO UPDATE
                   SET enrolled_from_audience_id=EXCLUDED.enrolled_from_audience_id,
                       contactable=EXCLUDED.contactable, updated_at=now()
                RETURNING lead_id, contact_id
                """,
                (campaign_id, _uid(c["contact_id"]), audience_id, _J(contactable)),
            )
            led = cur.fetchone()
            enrolled.append({"lead_id": str(led["lead_id"]), "contact_id": str(led["contact_id"]),
                             "email": contactable.get("email")})
    return {"status": "ok", "enrolled": len(enrolled), "leads": enrolled}


def attach_research(lead_id: str, research: dict, status: str = "ready") -> dict[str, Any]:
    """Record the per-lead parallel.ai research bundle ("how does THIS company serve the
    supply audience") on a lead. Advances the lead to 'researched'. `status` ∈
    pending|running|ready|failed|skipped. Returns the lead's research/status."""
    lead_id = _uid(lead_id)
    if not isinstance(research, dict):
        raise ValueError("research must be a JSON object (dict)")
    with _cursor() as cur:
        cur.execute(
            """
            UPDATE corex.lead
               SET research=%s, research_status=%s, research_at=now(),
                   status=CASE WHEN status='enrolled' THEN 'researched' ELSE status END, updated_at=now()
            WHERE lead_id=%s
            RETURNING lead_id, research_status, status
            """,
            (_J(research), status, lead_id),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"unknown lead {lead_id}")
    return {"status": "ok", "lead": _jsonify(row)}


def draft_copy(
    lead_id: str,
    merge_vars: dict,
    variant: str | None = None,
    template_id: str | None = None,
    status: str = "ready",
) -> dict[str, Any]:
    """Stage the per-lead, Clay-compiled copy merge-vars (the template interpolates them
    per record). Advances the lead to 'drafted'. `status` ∈
    pending|drafting|ready|approved|failed. Returns the lead's copy/status."""
    lead_id = _uid(lead_id)
    if not isinstance(merge_vars, dict):
        raise ValueError("merge_vars must be a JSON object (dict)")
    payload = {"merge_vars": merge_vars, "variant": variant, "template_id": template_id}
    with _cursor() as cur:
        cur.execute(
            """
            UPDATE corex.lead
               SET copy=%s, copy_status=%s, copy_at=now(),
                   status=CASE WHEN status IN ('enrolled','researched') THEN 'drafted' ELSE status END,
                   updated_at=now()
            WHERE lead_id=%s
            RETURNING lead_id, copy_status, status
            """,
            (_J(payload), status, lead_id),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"unknown lead {lead_id}")
    return {"status": "ok", "lead": _jsonify(row)}


def materialize_supply_campaign(lead_id: str, mailer_spec: dict | None = None) -> dict[str, Any]:
    """**Call-book pivot.** Materialize the direct-mail campaign a lead's company would
    run to its market: read the initiative's pair → supply audience, stamp its size, mint
    (once) a supply-side `corex.campaign` (direct_mail/lob), set the lead's `mailer`
    bundle with the stamped inbound artifacts (dub QR token, landing slug), and create the
    `corex.send` the mail piece becomes — pre-stamped with `inbound_ref` so a later QR
    scan / call / visit reconciles back to it. Advances the lead to 'materialized'.
    Returns `{supply_campaign_key, supply_count, lead:{mailer, send_id}}`.
    """
    lead_id = _uid(lead_id)
    with _cursor() as cur:
        cur.execute(
            """
            SELECT l.lead_id, l.campaign_id, c.initiative_id, co.normalized_domain
            FROM corex.lead l
            JOIN corex.campaign c ON c.campaign_id = l.campaign_id
            JOIN corex.contact  co ON co.contact_id = l.contact_id
            WHERE l.lead_id=%s
            """,
            (lead_id,),
        )
        lr = cur.fetchone()
        if not lr:
            raise ValueError(f"unknown lead {lead_id}")
        initiative_id = lr["initiative_id"]
        domain = lr["normalized_domain"] or "prospect"
        cur.execute(
            """
            SELECT p.supply_audience_id, a.source_sql, a.name
            FROM corex.gtm_audience_pair p
            JOIN corex.audience a ON a.audience_id = p.supply_audience_id
            WHERE p.initiative_id=%s
            """,
            (initiative_id,),
        )
        pr = cur.fetchone()
        if not pr:
            raise ValueError("initiative has no audience pair — call define_audience_pair first")
        supply_audience_id = pr["supply_audience_id"]
        supply_sql, supply_name = pr["source_sql"], pr["name"]

    supply_count = _stamp_count(supply_sql)
    supply_key = f"{_slug(domain.split('.')[0])}_SUPPLY_{_slug(supply_name, 10)}"
    slug = re.sub(r"[^a-z0-9]+", "-", domain.split(".")[0].lower()).strip("-") or "prospect"
    qr_token = _uuid.uuid5(_uuid.NAMESPACE_URL, f"corex/qr/{lead_id}").hex[:12]
    mailer = {
        "supply_campaign_key": supply_key, "supply_audience_id": str(supply_audience_id),
        "supply_count": supply_count, "landing_slug": slug, "qr_token": qr_token,
        "dub_link": None, "vapi_number": None, **(mailer_spec or {}),
    }
    inbound_ref = {"landing_slug": slug, "qr_token": qr_token}

    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO corex.campaign (initiative_id, audience_id, campaign_key, channel, provider, config)
            VALUES (%s, %s, %s, 'direct_mail', 'lob', %s)
            ON CONFLICT (initiative_id, campaign_key) DO UPDATE
               SET audience_id=EXCLUDED.audience_id, config=EXCLUDED.config, updated_at=now()
            RETURNING campaign_id
            """,
            (initiative_id, supply_audience_id, supply_key, _J({"materialized_for_lead": str(lead_id)})),
        )
        supply_campaign_id = cur.fetchone()["campaign_id"]
        cur.execute(
            """
            UPDATE corex.lead
               SET mailer=%s, mailer_status='ready', mailer_at=now(),
                   status=CASE WHEN status IN ('enrolled','researched','drafted')
                               THEN 'materialized' ELSE status END, updated_at=now()
            WHERE lead_id=%s
            """,
            (_J(mailer), lead_id),
        )
        cur.execute(
            """
            INSERT INTO corex.send (lead_id, campaign_id, provider, touch_no, status, inbound_ref)
            VALUES (%s, %s, 'lob', 1, 'queued', %s)
            RETURNING send_id
            """,
            (lead_id, supply_campaign_id, _J(inbound_ref)),
        )
        send_id = str(cur.fetchone()["send_id"])
    return {"status": "ok", "supply_campaign_key": supply_key,
            "supply_campaign_id": str(supply_campaign_id), "supply_count": supply_count,
            "lead": {"lead_id": str(lead_id), "mailer": mailer, "send_id": send_id}}


def record_send(
    lead_id: str,
    provider: str,
    provider_send_id: str,
    touch_no: int = 1,
    status: str = "submitted",
    inbound_ref: dict | None = None,
) -> dict[str, Any]:
    """Record the provider's atomic send once dispatched — the row a later webhook
    reconciler updates. Upserts on (provider, provider_send_id) (Lob psc_… / EmailBison
    id / Vapi call id), merging any `inbound_ref` (dub short-id, vapi number, landing
    slug). Returns the send row."""
    lead_id = _uid(lead_id)
    if provider not in ("emailbison", "lob", "vapi"):
        raise ValueError("provider must be 'emailbison' | 'lob' | 'vapi'")
    if not (provider_send_id or "").strip():
        raise ValueError("provider_send_id is required (the provider's atomic id)")
    with _cursor() as cur:
        cur.execute("SELECT campaign_id FROM corex.lead WHERE lead_id=%s", (lead_id,))
        lr = cur.fetchone()
        if not lr:
            raise ValueError(f"unknown lead {lead_id}")
        cur.execute(
            """
            INSERT INTO corex.send (lead_id, campaign_id, provider, provider_send_id, touch_no, status, inbound_ref, submitted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (provider, provider_send_id) DO UPDATE
               SET status=EXCLUDED.status,
                   inbound_ref = corex.send.inbound_ref || EXCLUDED.inbound_ref,
                   last_event_at=now(), updated_at=now()
            RETURNING send_id, lead_id, provider, provider_send_id, status, touch_no, (xmax = 0) AS inserted
            """,
            (lead_id, lr["campaign_id"], provider, provider_send_id.strip(), touch_no, status,
             _J(inbound_ref)),
        )
        row = cur.fetchone()
    return {"status": "ok", "operation": "inserted" if row.pop("inserted") else "updated",
            "send": _jsonify(row)}


_STATUS_TABLES = {
    "initiative": ("corex.initiative", "initiative_id"),
    "campaign": ("corex.campaign", "campaign_id"),
    "lead": ("corex.lead", "lead_id"),
    "send": ("corex.send", "send_id"),
}


def update_status(object_type: str, object_id: str, status: str) -> dict[str, Any]:
    """Generic lifecycle nudge: set the `status` of an initiative | campaign | lead |
    send. The new status is validated by that table's CHECK constraint (e.g. a campaign
    is draft|enrolling|live|paused|complete). Returns the updated id + status."""
    target = _STATUS_TABLES.get(object_type)
    if not target:
        raise ValueError(f"object_type must be one of {sorted(_STATUS_TABLES)}")
    table, pk = target
    with _cursor() as cur:
        cur.execute(
            f"UPDATE {table} SET status=%s, updated_at=now() WHERE {pk}=%s RETURNING {pk}, status",
            (status, _uid(object_id)),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"unknown {object_type} {object_id}")
    return {"status": "ok", "object_type": object_type, "updated": _jsonify(row)}


# ════════════════════════════ READ TOOLS ══════════════════════════════════════
def get_initiative(initiative_id: str) -> dict[str, Any]:
    """Read an initiative with its bound audience pair (both stamps) and its campaigns —
    the tree from the root, one hop down. Returns `{initiative, pair:{demand_audience,
    supply_audience}, campaigns:[…]}`."""
    initiative_id = _uid(initiative_id)
    with _cursor() as cur:
        cur.execute("SELECT * FROM corex.initiative WHERE initiative_id=%s", (initiative_id,))
        init = cur.fetchone()
        if not init:
            raise ValueError(f"unknown initiative {initiative_id}")
        cur.execute(
            """
            SELECT p.pair_id, p.thesis,
                   d.audience_id AS demand_audience_id, d.name AS demand_name, d.row_count AS demand_count,
                   s.audience_id AS supply_audience_id, s.name AS supply_name, s.row_count AS supply_count
            FROM corex.gtm_audience_pair p
            JOIN corex.audience d ON d.audience_id = p.demand_audience_id
            JOIN corex.audience s ON s.audience_id = p.supply_audience_id
            WHERE p.initiative_id=%s
            """,
            (initiative_id,),
        )
        pair = cur.fetchone()
        cur.execute(
            "SELECT campaign_id, campaign_key, channel, provider, audience_id, status "
            "FROM corex.campaign WHERE initiative_id=%s ORDER BY created_at",
            (initiative_id,),
        )
        campaigns = cur.fetchall()
    return {"status": "ok", "initiative": _jsonify(init), "pair": _jsonify(pair),
            "campaigns": _jsonify(campaigns)}


def list_campaigns(
    initiative_id: str | None = None,
    group_id: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """List campaigns (optionally scoped by initiative, group, or status) with channel /
    provider / key / audience and a live lead count each. Returns `{campaigns:[…]}`."""
    initiative_id, group_id = _uid(initiative_id), _uid(group_id)
    clauses, params = [], []
    if initiative_id:
        clauses.append("c.initiative_id=%s"); params.append(initiative_id)
    if group_id:
        clauses.append("c.group_id=%s"); params.append(group_id)
    if status:
        clauses.append("c.status=%s"); params.append(status)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _cursor() as cur:
        cur.execute(
            f"""
            SELECT c.campaign_id, c.campaign_key, c.channel, c.provider, c.audience_id,
                   c.status, c.initiative_id, count(l.lead_id) AS lead_count
            FROM corex.campaign c
            LEFT JOIN corex.lead l ON l.campaign_id = c.campaign_id
            {where}
            GROUP BY c.campaign_id
            ORDER BY c.created_at
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return {"status": "ok", "count": len(rows), "campaigns": _jsonify(rows)}


def get_campaign_funnel(campaign_id: str) -> dict[str, Any]:
    """The conversational dashboard for a campaign: lead counts by status
    (enrolled→researched→drafted→materialized→sent→engaged), send counts by status, and
    the backing audience stamp. Returns `{campaign, audience_stamp, leads_by_status,
    sends_by_status}`."""
    campaign_id = _uid(campaign_id)
    with _cursor() as cur:
        cur.execute(
            """
            SELECT c.campaign_id, c.campaign_key, c.channel, c.provider, c.status,
                   c.initiative_id, a.audience_id, a.name AS audience_name,
                   a.row_count AS audience_row_count, a.last_run_at AS audience_last_run_at
            FROM corex.campaign c
            LEFT JOIN corex.audience a ON a.audience_id = c.audience_id
            WHERE c.campaign_id=%s
            """,
            (campaign_id,),
        )
        camp = cur.fetchone()
        if not camp:
            raise ValueError(f"unknown campaign {campaign_id}")
        cur.execute(
            "SELECT status, count(*) AS n FROM corex.lead WHERE campaign_id=%s GROUP BY status",
            (campaign_id,),
        )
        leads_by_status = {r["status"]: int(r["n"]) for r in cur.fetchall()}
        cur.execute(
            "SELECT status, count(*) AS n FROM corex.send WHERE campaign_id=%s GROUP BY status",
            (campaign_id,),
        )
        sends_by_status = {r["status"]: int(r["n"]) for r in cur.fetchall()}
    stamp = {"audience_id": _jsonify(camp.pop("audience_id")),
             "name": camp.pop("audience_name"),
             "row_count": camp.pop("audience_row_count"),
             "last_run_at": _jsonify(camp.pop("audience_last_run_at"))}
    return {"status": "ok", "campaign": _jsonify(camp), "audience_stamp": stamp,
            "leads_by_status": leads_by_status, "sends_by_status": sends_by_status}


def get_lead(lead_id: str) -> dict[str, Any]:
    """The full per-lead bundle: the lead + its passive contact + research / copy / mailer
    derivations + its sends. Returns `{lead, contact, sends:[…]}`."""
    lead_id = _uid(lead_id)
    with _cursor() as cur:
        cur.execute("SELECT * FROM corex.lead WHERE lead_id=%s", (lead_id,))
        lead = cur.fetchone()
        if not lead:
            raise ValueError(f"unknown lead {lead_id}")
        cur.execute("SELECT * FROM corex.contact WHERE contact_id=%s", (lead["contact_id"],))
        contact = cur.fetchone()
        cur.execute(
            "SELECT send_id, provider, provider_send_id, touch_no, direction, status, inbound_ref "
            "FROM corex.send WHERE lead_id=%s ORDER BY touch_no, created_at",
            (lead_id,),
        )
        sends = cur.fetchall()
    return {"status": "ok", "lead": _jsonify(lead), "contact": _jsonify(contact),
            "sends": _jsonify(sends)}


def register(mcp) -> None:
    """Mount the corex GTM control-surface tools onto the FastMCP server. Each function's
    signature + docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (
        # write / advance
        create_initiative, define_audience, define_audience_pair, create_campaign,
        enroll_leads_from_audience, attach_research, draft_copy, materialize_supply_campaign,
        record_send, update_status,
        # read
        get_initiative, list_campaigns, get_campaign_funnel, get_lead,
    ):
        mcp.add_tool(fn)
