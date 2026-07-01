#!/usr/bin/env python3
"""Sync the Close lead/contact crosswalk: core-x Lance ledger → hq-x public.close_crosswalk.

The Close→Insights webhook resolves an incoming call (close_lead_id / close_contact_id) to a
briefing anchor (normalized_domain) + the active/people person_id. That resolution table lives
in hq-x Postgres (read by the BFF on the service-role key). This job mirrors the SoR mapping
(active/close_sfnet_leads, written by push_sfnet_to_close.py) into it.

SOURCE : s3://data-sink/active/close_sfnet_leads/   (Lance ledger; + sfnet_main_contacts for company_name)
TARGET : hq-x public.close_crosswalk                (HQX_DB_URL_DIRECT)
Idempotent UPSERT on close_contact_id. Append-safe: re-run after any push to pick up new leads.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with 'psycopg[binary]' \
        python3 pipelines/gtm/sync_close_crosswalk_to_hqx.py
"""
from __future__ import annotations

import os

import duckdb
import lance
import psycopg

ACTIVE = "s3://data-sink/active"
LEDGER_URI = os.environ.get("CLOSE_LEDGER_URI", f"{ACTIVE}/close_sfnet_leads/")


def _so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}


# normalized linkedin (mirror the push fallback) — lower, strip scheme/www/query/trailing slash.
_NL = ("nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
       "lower(trim({c})),'\\?.*$',''),'^https?://',''),'^www\\.',''),'/+$',''),'')")


def main() -> int:
    so = _so()
    con = duckdb.connect()
    con.register("led_r", lance.dataset(LEDGER_URI, storage_options=so).scanner().to_reader())
    con.register("mc_r", lance.dataset(f"{ACTIVE}/sfnet_main_contacts/", storage_options=so).scanner(
        columns=["sfnet_person_id", "company_name", "linkedin_url"]).to_reader())
    con.register("ph_r", lance.dataset(f"{ACTIVE}/phone_resolutions/", storage_options=so).scanner(
        columns=["person_id", "person_linkedin_url", "phone", "phone_type", "resolved_at"]).to_reader())
    con.execute("CREATE TABLE led AS SELECT * FROM led_r")
    con.execute("CREATE TABLE mc AS SELECT * FROM mc_r")
    # contact_phone = the SAME best phone the push sent to Close (so it equals the number Close
    # dials, i.e. the webhook's remote_phone). Best = prefer mobile, then most recent; digits only.
    con.execute(f"""
        CREATE TABLE ph_best AS
        SELECT person_id, {_NL.format(c='person_linkedin_url')} li,
               regexp_replace(phone,'[^0-9]','','g') AS digits
        FROM (
          SELECT *, row_number() OVER (PARTITION BY person_id
            ORDER BY (lower(coalesce(phone_type,''))='mobile') DESC, resolved_at DESC) rn
          FROM ph_r WHERE phone IS NOT NULL) WHERE rn=1
    """)
    con.execute("CREATE TABLE ph_cid AS SELECT person_id, any_value(digits) digits FROM ph_best WHERE person_id IS NOT NULL GROUP BY 1")
    con.execute("CREATE TABLE ph_li AS SELECT li, any_value(digits) digits FROM ph_best WHERE li IS NOT NULL GROUP BY 1")
    rows = con.execute(f"""
        SELECT l.close_contact_id, l.close_lead_id, l.normalized_domain, l.resolved_contact_id,
               l.sfnet_company_id, l.sfnet_person_id, mc.company_name, l.pushed_at,
               nullif(coalesce(pc.digits, pl.digits), '') AS contact_phone
        FROM led l
        LEFT JOIN mc ON mc.sfnet_person_id = l.sfnet_person_id
        LEFT JOIN ph_cid pc ON pc.person_id = l.resolved_contact_id
        LEFT JOIN ph_li  pl ON pl.li = {_NL.format(c='mc.linkedin_url')} AND mc.linkedin_url IS NOT NULL
        WHERE l.close_contact_id IS NOT NULL
    """).fetchall()
    con.close()
    print(f"ledger rows to upsert: {len(rows):,}", flush=True)

    dsn = os.environ["HQX_DB_URL_DIRECT"]
    with psycopg.connect(dsn, autocommit=False) as pg:
        with pg.cursor() as cur:
            cur.execute("ALTER TABLE public.close_crosswalk ADD COLUMN IF NOT EXISTS contact_phone text")
            cur.execute("CREATE INDEX IF NOT EXISTS close_crosswalk_phone_idx ON public.close_crosswalk (contact_phone)")
            cur.executemany("""
                INSERT INTO public.close_crosswalk
                  (close_contact_id, close_lead_id, normalized_domain, resolved_contact_id,
                   sfnet_company_id, sfnet_person_id, company_name, pushed_at, contact_phone)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (close_contact_id) DO UPDATE SET
                  close_lead_id=EXCLUDED.close_lead_id,
                  normalized_domain=EXCLUDED.normalized_domain,
                  resolved_contact_id=EXCLUDED.resolved_contact_id,
                  sfnet_company_id=EXCLUDED.sfnet_company_id,
                  sfnet_person_id=EXCLUDED.sfnet_person_id,
                  company_name=EXCLUDED.company_name,
                  pushed_at=EXCLUDED.pushed_at,
                  contact_phone=EXCLUDED.contact_phone
            """, rows)
        pg.commit()
        n = pg.execute("SELECT count(*) FROM public.close_crosswalk").fetchone()[0]
        dom = pg.execute("SELECT count(*) FROM public.close_crosswalk WHERE normalized_domain IS NOT NULL").fetchone()[0]
        ph = pg.execute("SELECT count(*) FROM public.close_crosswalk WHERE contact_phone IS NOT NULL").fetchone()[0]
    print(f"close_crosswalk: {n:,} rows · {dom:,} with domain · {ph:,} with contact_phone", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
