#!/usr/bin/env python3
"""push_sam_to_close — SAM audience mart → Close.com loader (cohort-driven, idempotent).

Supersedes the SFNet prototype (`push_sfnet_to_close.py`) as the canonical Close loader.
The engine is deliberately dumb and stable: WHO gets pushed lives in a cohort SQL file,
never in this code.

ARCHITECTURE
    cohort SQL  (--cohort path.sql; DuckDB over Lance; must return sam_person_id)
        │  extra columns whose names match Close custom fields ride along automatically
        ▼
    engine      resolve enrichment chain → map to Close Lead/Contact → push → ledger
        ▼
    ledger      active/close_sam_leads  (sam_person_id ↔ close_lead_id/close_contact_id)

COHORT CONTRACT
    * MUST return a `sam_person_id` column (person grain; engine dedupes).
    * MAY return extra columns. Any column whose name equals a Close **custom field
      name** (lead- or contact-level, discovered live at runtime) is attached to the
      pushed object — e.g. returning `nearest_base`, `sub_amt_24m`, `size_band`
      populates those lead fields with zero engine changes. Unknown columns are
      reported and ignored. Lead-level extras are taken from any row of the UEI group.
    * The SQL runs in DuckDB with every needed Lance registered as a view:
      gtm_sam_people, gtm_sam_entities, gtm_sam_person_identity, gtm_sam_person_titles,
      gtm_sam_person_firm_emails, phone_resolutions, work_emails.

FIXED ENRICHMENT (engine-owned; changing it is a code change, deliberately)
    person   gtm_sam_people (display_name, first/last)  + gtm_sam_person_titles
             (title_verbatim, dm_class; fallback best_title)
    identity gtm_sam_person_identity → person_linkedin_url_norm
             (slug canon: 'linkedin.com/in/' || lower(slug) — house convention)
    phone    phone_resolutions via LI slug; mobile preferred            [dialer fuel]
    email    work_emails via LI slug (MV verdict carried), fallback
             gtm_sam_person_firm_emails (match_tier carried)
    lead     gtm_sam_entities (legal_business_name, normalized_domain,
             physical_state, primary_naics)

CLOSE MAPPING (anchors — operator-decided 2026-07-06)
    Lead.custom.uei                 ← uei            ★ entity anchor (uei IS the entity PK)
    Contact.custom.sam_person_id    ← sam_person_id  ★ person anchor
    normalized_domain rides as Lead.custom.company_domain (secondary, non-identity).

IDEMPOTENCY
    Ledger grain: 1 row / pushed contact (+ lead_anchor rows for leads without SAM
    contacts). A sam_person_id in the ledger is never re-pushed; a uei in the ledger
    reuses its close_lead_id. `--seed-from-close` backfills the ledger from Close
    itself (the ad-hoc pushes stamped uei + sam_person_id custom fields) so history
    this engine didn't write is still honored.

RUN
    doppler run -p core-x -c prd -- python3 pipelines/gtm/push_sam_to_close.py --seed-from-close
    doppler run -p core-x -c prd -- python3 pipelines/gtm/push_sam_to_close.py --cohort pipelines/gtm/close_cohorts/x.sql            # DRY RUN (default)
    doppler run -p core-x -c prd -- python3 pipelines/gtm/push_sam_to_close.py --cohort pipelines/gtm/close_cohorts/x.sql --live
    doppler run -p core-x -c prd -- python3 pipelines/gtm/push_sam_to_close.py --verify
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ACTIVE = "s3://data-sink/active"
LEDGER_URI = os.environ.get("CLOSE_SAM_LEDGER_URI", f"{ACTIVE}/close_sam_leads/")
DATA_STORAGE_VERSION = "2.1"
CLOSE_API = "https://api.close.com/api/v1"

# Lances registered as DuckDB views for cohort SQL + engine enrichment.
MART_VIEWS = {
    "gtm_sam_people":             f"{ACTIVE}/gtm_sam_people/",
    "gtm_sam_entities":           f"{ACTIVE}/gtm_sam_entities/",
    "gtm_sam_person_identity":    f"{ACTIVE}/gtm_sam_person_identity/",
    "gtm_sam_person_titles":      f"{ACTIVE}/gtm_sam_person_titles/",
    "gtm_sam_person_firm_emails": f"{ACTIVE}/gtm_sam_person_firm_emails/",
    "phone_resolutions":          f"{ACTIVE}/phone_resolutions/",
    "work_emails":                f"{ACTIVE}/work_emails/",
    # cohort-side inputs (audience definition, not engine enrichment)
    "usaspending_subaward_canonical": f"{ACTIVE}/usaspending_subaward_canonical/",
    "firmographics_blitz":            f"{ACTIVE}/firmographics_blitz/",
    "gtm_sam_person_contactability":  f"{ACTIVE}/gtm_sam_person_contactability/",
    # audience marts (scripts/build_gtm_audience_marts.py) — prefer these in cohort
    # SQL over the raw sources above; windowed aggregates, seconds not minutes
    "gtm_sub_combo_lanes":            f"{ACTIVE}/gtm_sub_combo_lanes/",
    "gtm_prime_combo_lanes":          f"{ACTIVE}/gtm_prime_combo_lanes/",
    "gtm_audience_entities":          f"{ACTIVE}/gtm_audience_entities/",
    "gtm_audience_people":            f"{ACTIVE}/gtm_audience_people/",
}

# House slug canon (match_sam_person_identity.py): both sides normalize to
# 'linkedin.com/in/<lower slug>' before joining.
LI_SLUG_SQL = ("CASE WHEN {c} IS NOT NULL AND regexp_extract(lower({c}), "
               "'linkedin\\.com/in/([^/?#]+)', 1) != '' "
               "THEN 'linkedin.com/in/' || regexp_extract(lower({c}), "
               "'linkedin\\.com/in/([^/?#]+)', 1) END")

LEDGER_COLUMNS = [
    ("sam_person_id", "string"), ("uei", "string"),
    ("close_lead_id", "string"), ("close_contact_id", "string"),
    ("normalized_domain", "string"), ("row_type", "string"),  # contact | lead_anchor
    ("cohort_name", "string"), ("batch_label", "string"),
    ("email", "string"), ("phone", "string"),
    ("pushed_at", "timestamp"),
]


# ── infra ──────────────────────────────────────────────────────────────────────

def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _ledger_schema():
    import pyarrow as pa
    t = {"string": pa.string(), "timestamp": pa.timestamp("us", tz="UTC")}
    return pa.schema([pa.field(n, t[k]) for n, k in LEDGER_COLUMNS])


def _open_ledger(so):
    import lance
    try:
        return lance.dataset(LEDGER_URI, storage_options=so)
    except (ValueError, OSError):
        return None


def _append_ledger(rows: list[dict], so) -> None:
    import lance
    import pyarrow as pa
    if not rows:
        return
    schema = _ledger_schema()
    tbl = pa.table({n: [r.get(n) for r in rows] for n, _ in LEDGER_COLUMNS}, schema=schema)
    ds = _open_ledger(so)
    if ds is None:
        lance.write_dataset(tbl, LEDGER_URI, mode="create",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        ds = _open_ledger(so)
        for c in ("sam_person_id", "uei", "close_lead_id", "close_contact_id"):
            try:
                ds.create_scalar_index(c, index_type="BTREE")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN BTREE {c}: {exc}")
    else:
        lance.write_dataset(tbl, LEDGER_URI, mode="append",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    print(f"ledger: +{len(rows)} rows → {LEDGER_URI}")


def _ledger_state(so) -> tuple[set[str], dict[str, str]]:
    """(pushed sam_person_ids, uei → close_lead_id)."""
    ds = _open_ledger(so)
    if ds is None:
        return set(), {}
    t = ds.to_table(columns=["sam_person_id", "uei", "close_lead_id"])
    pushed = {v for v in t.column("sam_person_id").to_pylist() if v}
    lead_by_uei: dict[str, str] = {}
    for uei, lid in zip(t.column("uei").to_pylist(), t.column("close_lead_id").to_pylist()):
        if uei and lid and uei not in lead_by_uei:
            lead_by_uei[uei] = lid
    return pushed, lead_by_uei


# ── Close API ──────────────────────────────────────────────────────────────────

def _close_call(method: str, path: str, payload: dict | None = None) -> dict:
    auth = base64.b64encode(f"{os.environ['CLOSE_API_KEY']}:".encode()).decode()
    body = json.dumps(payload).encode() if payload is not None else None
    for attempt in range(5):
        req = urllib.request.Request(
            f"{CLOSE_API}{path}", data=body, method=method,
            headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = float(e.headers.get("Retry-After", "2") or 2)
                time.sleep(wait + 0.5)
                continue
            detail = e.read().decode()[:500]
            raise RuntimeError(f"Close {method} {path} → {e.code}: {detail}") from e
        except (urllib.error.URLError, TimeoutError):
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Close {method} {path}: retries exhausted")


def _custom_field_maps() -> tuple[dict[str, str], dict[str, str]]:
    """name → cf_ id, for lead and contact custom fields (live discovery)."""
    lead = {f["name"]: f["id"] for f in _close_call("GET", "/custom_field/lead/")["data"]}
    contact = {f["name"]: f["id"] for f in _close_call("GET", "/custom_field/contact/")["data"]}
    return lead, contact


# ── cohort resolution ─────────────────────────────────────────────────────────

def _duck(so):
    import duckdb
    import lance
    con = duckdb.connect(":memory:")
    for name, uri in MART_VIEWS.items():
        ds = lance.dataset(uri, storage_options=so)
        con.register(f"_l_{name}", ds)
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM _l_{name}")
    return con


def resolve_cohort(cohort_sql: str, so, limit: int | None):
    """Run the cohort SQL, then bolt on the fixed enrichment chain. Returns a DataFrame."""
    con = _duck(so)
    con.execute(f"CREATE TEMP TABLE cohort AS {cohort_sql}")
    cols = [d[0] for d in con.execute("SELECT * FROM cohort LIMIT 0").description]
    if "sam_person_id" not in cols:
        raise RuntimeError(f"cohort SQL must return sam_person_id; got {cols}")
    extra_cols = [c for c in cols if c != "sam_person_id"]
    extra_sel = "".join(f", any_value(c.{c}) AS x_{c}" for c in extra_cols)
    lim = f"LIMIT {int(limit)}" if limit else ""

    li_ph = LI_SLUG_SQL.format(c="person_linkedin_url")
    df = con.execute(f"""
        WITH people AS (
            SELECT c.sam_person_id{extra_sel}
            FROM cohort c GROUP BY 1
        ),
        base AS (
            SELECT pe.*, p.uei, p.display_name, p.first_name, p.last_name, p.best_title
            FROM people pe JOIN gtm_sam_people p USING (sam_person_id)
        ),
        ident AS (
            SELECT sam_person_id, any_value(person_linkedin_url_norm) AS li_norm
            FROM gtm_sam_person_identity GROUP BY 1
        ),
        titles AS (
            SELECT sam_person_id, any_value(title_verbatim) AS title_verbatim,
                   any_value(dm_class) AS dm_class
            FROM gtm_sam_person_titles GROUP BY 1
        ),
        phones AS (
            SELECT {li_ph} AS li_norm,
                   arg_min(phone, CASE WHEN phone_type='mobile' THEN 0 ELSE 1 END) AS phone,
                   arg_min(phone_type, CASE WHEN phone_type='mobile' THEN 0 ELSE 1 END) AS phone_type,
                   arg_min(source_vendor, CASE WHEN phone_type='mobile' THEN 0 ELSE 1 END) AS phone_vendor
            FROM phone_resolutions
            WHERE phone IS NOT NULL AND {li_ph} IS NOT NULL
            GROUP BY 1
        ),
        emails AS (
            SELECT {li_ph} AS li_norm,
                   arg_min(email, CASE mv_result WHEN 'ok' THEN 0 ELSE 1 END) AS email,
                   arg_min(mv_result,  CASE mv_result WHEN 'ok' THEN 0 ELSE 1 END) AS mv_result,
                   arg_min(mv_quality, CASE mv_result WHEN 'ok' THEN 0 ELSE 1 END) AS mv_quality
            FROM work_emails
            WHERE email IS NOT NULL AND {li_ph} IS NOT NULL
            GROUP BY 1
        ),
        firm_emails AS (
            SELECT sam_person_id, arg_min(email, match_score * -1) AS firm_email,
                   arg_min(match_tier, match_score * -1) AS firm_email_tier
            FROM gtm_sam_person_firm_emails GROUP BY 1
        )
        SELECT b.*, i.li_norm,
               coalesce(t.title_verbatim, b.best_title) AS title,
               t.dm_class,
               ph.phone, ph.phone_type, ph.phone_vendor,
               coalesce(em.email, fe.firm_email) AS email,
               CASE WHEN em.email IS NOT NULL THEN 'work_emails'
                    WHEN fe.firm_email IS NOT NULL THEN 'firm_email:' || fe.firm_email_tier END AS email_source,
               em.mv_result, em.mv_quality,
               e.legal_business_name, e.normalized_domain, e.physical_state, e.primary_naics
        FROM base b
        JOIN gtm_sam_entities e USING (uei)
        LEFT JOIN ident i USING (sam_person_id)
        LEFT JOIN titles t USING (sam_person_id)
        LEFT JOIN phones ph ON ph.li_norm = i.li_norm
        LEFT JOIN emails em ON em.li_norm = i.li_norm
        LEFT JOIN firm_emails fe USING (sam_person_id)
        ORDER BY b.uei, b.sam_person_id
        {lim}
    """).df()
    return df, extra_cols


# ── push ──────────────────────────────────────────────────────────────────────

def run_push(cohort_path: str, live: bool, limit: int | None) -> None:
    so = _r2_storage_options()
    cohort_name = Path(cohort_path).stem
    batch_label = f"{cohort_name}_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    sql = Path(cohort_path).read_text()

    df, extra_cols = resolve_cohort(sql, so, limit)
    pushed, lead_by_uei = _ledger_state(so)
    df["already_pushed"] = df["sam_person_id"].isin(pushed)
    todo = df[~df["already_pushed"]]

    n_lead_new = todo[~todo["uei"].isin(lead_by_uei)]["uei"].nunique()
    print(f"cohort={cohort_name}  resolved={len(df):,}  already_in_ledger={int(df['already_pushed'].sum()):,}")
    print(f"to_push: contacts={len(todo):,}  ueis={todo['uei'].nunique():,} "
          f"(new leads={n_lead_new:,}, existing leads={todo['uei'].nunique() - n_lead_new:,})")
    print(f"coverage: phone={todo['phone'].notna().sum():,}  email={todo['email'].notna().sum():,}  "
          f"li={todo['li_norm'].notna().sum():,}  dm={int((todo['dm_class'] == 'dm').sum()):,}")
    if extra_cols:
        print(f"cohort extra columns: {extra_cols}")

    if todo.empty:
        print("nothing to push."); return

    lead_cf, contact_cf = _custom_field_maps()
    known_lead_x = [c for c in extra_cols if c in lead_cf]
    known_contact_x = [c for c in extra_cols if c in contact_cf]
    unknown_x = [c for c in extra_cols if c not in lead_cf and c not in contact_cf]
    if known_lead_x:
        print(f"  → lead custom fields from cohort: {known_lead_x}")
    if known_contact_x:
        print(f"  → contact custom fields from cohort: {known_contact_x}")
    if unknown_x:
        print(f"  → IGNORED (no matching Close custom field): {unknown_x}")

    if not live:
        print("\n=== DRY RUN (no writes) — sample roster ===")
        cols = ["sam_person_id", "uei", "legal_business_name", "display_name", "title",
                "dm_class", "phone", "email", "email_source"]
        print(todo[cols].head(12).to_string(index=False))
        print(f"\nre-run with --live to push {len(todo):,} contacts under batch {batch_label}")
        return

    now = dt.datetime.now(dt.timezone.utc)
    ledger_rows: list[dict] = []
    n_leads_created = n_contacts = 0
    try:
        for uei, grp in todo.groupby("uei", sort=False):
            first = grp.iloc[0]
            lead_id = lead_by_uei.get(uei)
            if lead_id is None:
                custom = {f"custom.{lead_cf['uei']}": uei}
                if first["normalized_domain"] and "company_domain" in lead_cf:
                    custom[f"custom.{lead_cf['company_domain']}"] = first["normalized_domain"]
                for name, col in (("physical_state", "physical_state"), ("primary_naics", "primary_naics")):
                    v = first[col]
                    if name in lead_cf and v is not None and v == v:
                        custom[f"custom.{lead_cf[name]}"] = v
                for c in known_lead_x:
                    v = first[f"x_{c}"]
                    if v is not None and v == v:
                        custom[f"custom.{lead_cf[c]}"] = v
                payload = {"name": first["legal_business_name"] or uei, **custom}
                if first["normalized_domain"] and isinstance(first["normalized_domain"], str):
                    payload["url"] = f"https://{first['normalized_domain']}"
                lead = _close_call("POST", "/lead/", payload)
                lead_id = lead["id"]
                lead_by_uei[uei] = lead_id
                n_leads_created += 1
            for _, r in grp.iterrows():
                cc = {f"custom.{contact_cf['sam_person_id']}": r["sam_person_id"]}
                for name, col in (("first_name", "first_name"), ("last_name", "last_name"),
                                  ("dm_status", "dm_class"), ("linkedin_url", "li_norm"),
                                  ("email_mv_result", "mv_result"), ("email_mv_quality", "mv_quality"),
                                  ("phone_vendor", "phone_vendor")):
                    v = r[col]
                    if name in contact_cf and v is not None and v == v:
                        cc[f"custom.{contact_cf[name]}"] = v
                for c in known_contact_x:
                    v = r[f"x_{c}"]
                    if v is not None and v == v:
                        cc[f"custom.{contact_cf[c]}"] = v
                payload = {"lead_id": lead_id, "name": r["display_name"], **cc}
                if r["title"] and r["title"] == r["title"]:
                    payload["title"] = r["title"]
                if r["phone"] and r["phone"] == r["phone"]:
                    payload["phones"] = [{"phone": r["phone"], "type": "mobile" if r["phone_type"] == "mobile" else "direct"}]
                if r["email"] and r["email"] == r["email"]:
                    payload["emails"] = [{"email": r["email"], "type": "office"}]
                contact = _close_call("POST", "/contact/", payload)
                n_contacts += 1
                ledger_rows.append({
                    "sam_person_id": r["sam_person_id"], "uei": uei,
                    "close_lead_id": lead_id, "close_contact_id": contact["id"],
                    "normalized_domain": r["normalized_domain"], "row_type": "contact",
                    "cohort_name": cohort_name, "batch_label": batch_label,
                    "email": r["email"] if r["email"] == r["email"] else None,
                    "phone": r["phone"] if r["phone"] == r["phone"] else None,
                    "pushed_at": now,
                })
    finally:
        # Partial-failure safety: whatever reached Close is ledgered even if a later call died.
        _append_ledger(ledger_rows, so)
    print(f"PUSHED leads_created={n_leads_created} contacts={n_contacts} batch={batch_label}")


# ── seed from Close (backfill history the engine didn't write) ────────────────

def seed_from_close() -> None:
    so = _r2_storage_options()
    pushed, lead_by_uei = _ledger_state(so)
    # Contact objects return custom fields as FLAT `custom.cf_<id>` keys (leads use a
    # nested name-keyed dict) — resolve the sam_person_id cf id up front.
    _, contact_cf = _custom_field_maps()
    spid_key = f"custom.{contact_cf['sam_person_id']}"
    now = dt.datetime.now(dt.timezone.utc)
    rows: list[dict] = []
    skip = 0
    n_leads = n_no_uei = 0
    while True:
        d = _close_call("GET", f"/lead/?_limit=100&_skip={skip}"
                               "&_fields=id,name,custom,contacts")
        for lead in d["data"]:
            n_leads += 1
            custom = lead.get("custom") or {}
            uei = custom.get("uei")
            if not uei:
                n_no_uei += 1
                continue
            domain = custom.get("company_domain")
            sam_contacts = 0
            for c in lead.get("contacts") or []:
                spid = c.get(spid_key)
                if not spid or spid in pushed:
                    continue
                sam_contacts += 1
                emails = c.get("emails") or []
                phones = c.get("phones") or []
                rows.append({
                    "sam_person_id": spid, "uei": uei,
                    "close_lead_id": lead["id"], "close_contact_id": c["id"],
                    "normalized_domain": domain, "row_type": "contact",
                    "cohort_name": "seed_from_close", "batch_label": f"seed_{now:%Y%m%dT%H%M%SZ}",
                    "email": emails[0]["email"] if emails else None,
                    "phone": phones[0]["phone"] if phones else None,
                    "pushed_at": now,
                })
                pushed.add(spid)
            if sam_contacts == 0 and uei not in lead_by_uei:
                rows.append({
                    "sam_person_id": None, "uei": uei,
                    "close_lead_id": lead["id"], "close_contact_id": None,
                    "normalized_domain": domain, "row_type": "lead_anchor",
                    "cohort_name": "seed_from_close", "batch_label": f"seed_{now:%Y%m%dT%H%M%SZ}",
                    "email": None, "phone": None, "pushed_at": now,
                })
                lead_by_uei[uei] = lead["id"]
        if not d.get("has_more"):
            break
        skip += 100
        print(f"  scanned {skip} leads …")
    print(f"scanned {n_leads:,} Close leads ({n_no_uei:,} without uei → skipped, e.g. sfnet prototype)")
    _append_ledger(rows, so)
    print(f"seeded: {sum(1 for r in rows if r['row_type']=='contact'):,} contacts, "
          f"{sum(1 for r in rows if r['row_type']=='lead_anchor'):,} lead anchors")


# ── verify ────────────────────────────────────────────────────────────────────

def verify() -> None:
    import duckdb
    so = _r2_storage_options()
    ds = _open_ledger(so)
    if ds is None:
        print("ledger absent — nothing pushed yet."); return
    print(f"{LEDGER_URI}  rows={ds.count_rows():,}  v={ds.version}")
    con = duckdb.connect(":memory:"); con.register("l", ds)
    print(con.execute("""
        SELECT cohort_name, row_type, count(*) n,
               count(DISTINCT uei) ueis, count(email) with_email, count(phone) with_phone
        FROM l GROUP BY 1,2 ORDER BY 1,2""").df().to_string(index=False))
    dup = con.execute("SELECT count(*) FROM (SELECT sam_person_id FROM l "
                      "WHERE sam_person_id IS NOT NULL GROUP BY 1 HAVING count(*)>1)").fetchone()[0]
    print(f"duplicate sam_person_id rows: {dup} (must be 0)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", help="path to cohort SQL (must return sam_person_id)")
    ap.add_argument("--live", action="store_true", help="actually push (default: dry run)")
    ap.add_argument("--limit", type=int, help="cap resolved cohort size")
    ap.add_argument("--seed-from-close", action="store_true",
                    help="backfill ledger from Close custom fields (uei / sam_person_id)")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if args.verify:
        verify()
    elif args.seed_from_close:
        seed_from_close()
    elif args.cohort:
        run_push(args.cohort, live=args.live, limit=args.limit)
    else:
        ap.error("one of --cohort / --seed-from-close / --verify required")
