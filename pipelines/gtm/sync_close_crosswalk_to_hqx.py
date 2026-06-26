#!/usr/bin/env python3
"""Sync the Close lead/contact crosswalk: core-x Lance ledger → hq-x public.close_crosswalk.

The Close→Insights webhook resolves an incoming call (close_lead_id / close_contact_id) to a
briefing anchor (normalized_domain) + the active/people contact_id. That resolution table lives
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


def main() -> int:
    so = _so()
    con = duckdb.connect()
    con.register("led_r", lance.dataset(LEDGER_URI, storage_options=so).scanner().to_reader())
    con.register("mc_r", lance.dataset(f"{ACTIVE}/sfnet_main_contacts/", storage_options=so).scanner(
        columns=["sfnet_person_id", "company_name"]).to_reader())
    rows = con.execute("""
        SELECT l.close_contact_id, l.close_lead_id, l.normalized_domain, l.resolved_contact_id,
               l.sfnet_company_id, l.sfnet_person_id, mc.company_name, l.pushed_at
        FROM led_r l
        LEFT JOIN mc_r mc ON mc.sfnet_person_id = l.sfnet_person_id
        WHERE l.close_contact_id IS NOT NULL
    """).fetchall()
    con.close()
    print(f"ledger rows to upsert: {len(rows):,}", flush=True)

    dsn = os.environ["HQX_DB_URL_DIRECT"]
    with psycopg.connect(dsn, autocommit=False) as pg:
        with pg.cursor() as cur:
            cur.executemany("""
                INSERT INTO public.close_crosswalk
                  (close_contact_id, close_lead_id, normalized_domain, resolved_contact_id,
                   sfnet_company_id, sfnet_person_id, company_name, pushed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (close_contact_id) DO UPDATE SET
                  close_lead_id=EXCLUDED.close_lead_id,
                  normalized_domain=EXCLUDED.normalized_domain,
                  resolved_contact_id=EXCLUDED.resolved_contact_id,
                  sfnet_company_id=EXCLUDED.sfnet_company_id,
                  sfnet_person_id=EXCLUDED.sfnet_person_id,
                  company_name=EXCLUDED.company_name,
                  pushed_at=EXCLUDED.pushed_at
            """, rows)
        pg.commit()
        n = pg.execute("SELECT count(*) FROM public.close_crosswalk").fetchone()[0]
        nd = pg.execute("SELECT count(DISTINCT close_lead_id) FROM public.close_crosswalk").fetchone()[0]
        dom = pg.execute("SELECT count(*) FROM public.close_crosswalk WHERE normalized_domain IS NOT NULL").fetchone()[0]
    print(f"close_crosswalk: {n:,} rows · {nd:,} distinct leads · {dom:,} with normalized_domain", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
