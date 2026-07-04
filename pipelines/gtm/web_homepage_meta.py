"""web_homepage_meta — homepage meta-description snapshots (domain-keyed Lance).

Validated by the 12-entity live test (2026-07-04): meta tags are not a
substitute for firmographic payloads, but they fill ~1/3 of the
no-description gap at zero vendor cost and liveness-classify the rest
(parked/default/dead domains = personalization hygiene).

Dataset: s3://data-sink/active/web_homepage_meta/
Grain:   1 row per (normalized_domain, run_id) — append-only event log;
         consumers take latest per domain. Raw strings, never normalized.
Columns: normalized_domain · fetch_url · final_url · http_status ·
         meta_description · og_description · page_title · liveness_class
         (ok | parked_or_default | no_meta | unreachable) · error ·
         fetched_at · run_id
Indexes: BTREE normalized_domain · BITMAP liveness_class (built/refreshed
         after append).

Worklist (v1 cohort, recomputed per run): DSBS entities with award activity
since the window date, domain-bearing, with NO description in
firmographics_blitz.about or clay_find_companies — the measured 4,935-entity
gap queue. Domains fetched within RECRAWL_DAYS are skipped (idempotent).

Fetch: one homepage GET per distinct domain (https, www., http fallbacks),
8s timeout, ~48 threads, single page, meta/og/title extraction only — no
full-HTML storage.

Run:
    doppler run -- python3 pipelines/gtm/web_homepage_meta.py --dry-run   # worklist size only
    doppler run -- python3 pipelines/gtm/web_homepage_meta.py            # crawl + append
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import ssl
import sys
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

_PROD_URI = "s3://data-sink/active/web_homepage_meta/"
DATASET_URI = os.environ.get("WEB_HOMEPAGE_META_LANCE_URI", _PROD_URI)

SRC = {
    "gtm_sam_entities": "s3://data-sink/active/gtm_sam_entities/",
    "usaspending_subaward_canonical": "s3://data-sink/active/usaspending_subaward_canonical/",
    "usaspending_fpds_prime_award_state": "s3://data-sink/active/usaspending_fpds_prime_award_state/",
    "firmographics_blitz": "s3://data-sink/active/firmographics_blitz/",
    "clay_find_companies": "s3://data-sink/active/clay_find_companies/",
}

WINDOW_DATE = os.environ.get("GTM_AWARD_WINDOW", "2024-07-04")
RECRAWL_DAYS = 30
THREADS = int(os.environ.get("GTM_CRAWL_THREADS", "48"))
TIMEOUT_S = int(os.environ.get("GTM_CRAWL_TIMEOUT_S", "8"))
MAX_BYTES = 400_000

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["normalized_domain"]
BITMAP_INDEXES = ["liveness_class"]

_PARKED_RE = re.compile(
    r"coming soon|under construction|plesk|cpanel|default web ?site|apache2 "
    r"(debian|ubuntu)|iis windows|domain (is )?for sale|godaddy|sedo|parked|"
    r"account suspended|index of /|website is currently unavailable|"
    r"future home of|hugedomains|namecheap", re.I)

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.web_homepage_meta_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    dataset_uri       text        NOT NULL,
    run_id            text,
    worklist          bigint,
    skipped_recent    bigint,
    fetched           bigint,
    n_ok              bigint,
    n_no_meta         bigint,
    n_parked          bigint,
    n_unreachable     bigint,
    inputs            jsonb,
    status            text        NOT NULL,
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS web_homepage_meta_runs_recorded_at_idx
    ON ops.web_homepage_meta_runs (recorded_at DESC);
"""


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record(run_id, uri, counts, lineage, status, error, started_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """INSERT INTO ops.web_homepage_meta_runs
                    (feed, dataset_uri, run_id, worklist, skipped_recent, fetched,
                     n_ok, n_no_meta, n_parked, n_unreachable, inputs, status,
                     error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                ("web_homepage_meta", uri, run_id, counts.get("worklist"),
                 counts.get("skipped_recent"), counts.get("fetched"),
                 counts.get("ok"), counts.get("no_meta"), counts.get("parked_or_default"),
                 counts.get("unreachable"), json.dumps(lineage), status, error,
                 started_at, dt.datetime.now(dt.timezone.utc)))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


def _worklist(so, lineage) -> list[str]:
    import duckdb
    import lance

    con = duckdb.connect(":memory:")

    def opends(name):
        ds = lance.dataset(SRC[name], storage_options=so)
        lineage.append({"name": name, "uri": SRC[name], "version": ds.version,
                        "rows_at_read": ds.count_rows()})
        return ds

    ent = opends("gtm_sam_entities")
    con.register("ent", ent.scanner(
        columns=["uei", "normalized_domain"],
        filter="in_dsbs AND (is_subawardee OR is_prime_recipient) "
               "AND normalized_domain IS NOT NULL").to_reader())
    con.execute("CREATE TEMP TABLE e AS SELECT * FROM ent")
    sub = opends("usaspending_subaward_canonical")
    con.register("sub", sub.scanner(
        columns=["subawardee_uei", "subaward_action_date"]).to_reader())
    con.execute(f"""CREATE TEMP TABLE s AS SELECT subawardee_uei AS uei FROM sub
        GROUP BY 1 HAVING max(subaward_action_date) >= DATE '{WINDOW_DATE}'""")
    pa = opends("usaspending_fpds_prime_award_state")
    con.register("pa", pa.scanner(
        columns=["recipient_uei", "last_action_date"]).to_reader())
    con.execute(f"""CREATE TEMP TABLE pr AS SELECT recipient_uei AS uei FROM pa
        WHERE recipient_uei IS NOT NULL GROUP BY 1
        HAVING max(last_action_date) >= DATE '{WINDOW_DATE}'""")
    fb = opends("firmographics_blitz")
    con.register("fb", fb.scanner(columns=["uei", "domain_norm", "about"]).to_reader())
    con.execute("""CREATE TEMP TABLE bl_u AS SELECT DISTINCT uei FROM fb
        WHERE uei IS NOT NULL AND about IS NOT NULL AND len(trim(about)) > 0""")
    con.execute("""CREATE TEMP TABLE bl_d AS SELECT DISTINCT lower(domain_norm) AS dom
        FROM fb WHERE domain_norm IS NOT NULL AND about IS NOT NULL
        AND len(trim(about)) > 0""")
    cc = opends("clay_find_companies")
    con.register("cc", cc.scanner(
        columns=["domain_norm", "description", "derived_description"]).to_reader())
    con.execute("""CREATE TEMP TABLE cl AS SELECT DISTINCT lower(domain_norm) AS dom
        FROM cc WHERE domain_norm IS NOT NULL AND (
            (description IS NOT NULL AND len(trim(description)) > 0) OR
            (derived_description IS NOT NULL AND len(trim(derived_description)) > 0))""")
    doms = [r[0] for r in con.execute("""
        SELECT DISTINCT e.normalized_domain FROM e
        WHERE (e.uei IN (SELECT uei FROM s) OR e.uei IN (SELECT uei FROM pr))
          AND e.uei NOT IN (SELECT uei FROM bl_u)
          AND e.normalized_domain NOT IN (SELECT dom FROM bl_d)
          AND e.normalized_domain NOT IN (SELECT dom FROM cl)
        ORDER BY 1""").fetchall()]
    con.close()
    return doms


def _fetch_one(dom: str):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"}
    last_err = None
    for url in (f"https://{dom}", f"https://www.{dom}", f"http://{dom}"):
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=TIMEOUT_S, context=ctx) as resp:
                html = resp.read(MAX_BYTES).decode("utf-8", "ignore")
                status = resp.status
                final_url = resp.geturl()
            meta = (re.search(r'<meta[^>]+name=["\']description["\'][^>]*content=["\']([^"\']*)', html, re.I)
                    or re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]*name=["\']description["\']', html, re.I))
            og = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]*content=["\']([^"\']*)', html, re.I)
            title = re.search(r"<title[^>]*>([^<]*)</title>", html, re.I | re.S)
            meta_s = meta.group(1).strip()[:1000] if meta else None
            og_s = og.group(1).strip()[:1000] if og else None
            title_s = re.sub(r"\s+", " ", title.group(1)).strip()[:300] if title else None
            probe = " ".join(x for x in (title_s, meta_s, og_s, html[:2000]) if x)
            if _PARKED_RE.search(probe):
                klass = "parked_or_default"
            elif (meta_s and len(meta_s) > 0) or (og_s and len(og_s) > 0):
                klass = "ok"
            else:
                klass = "no_meta"
            return (dom, url, final_url, int(status), meta_s, og_s, title_s, klass, None)
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)[:200]
            continue
    return (dom, None, None, None, None, None, None, "unreachable", last_err)


def run(dry_run: bool = False) -> dict:
    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    run_id = f"crawl-{started:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    lineage: list[dict] = []
    counts: dict = {}
    status, error = "error", None
    try:
        doms = _worklist(so, lineage)
        counts["worklist"] = len(doms)
        print(f"run_id={run_id} worklist={len(doms):,} window={WINDOW_DATE}")
        for e in lineage:
            print(f"  input {e['name']} v{e['version']} rows={e['rows_at_read']:,}")

        # idempotency: skip domains fetched within RECRAWL_DAYS
        recent: set[str] = set()
        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            cutoff = started - dt.timedelta(days=RECRAWL_DAYS)
            t = ds.scanner(columns=["normalized_domain", "fetched_at"]).to_table()
            recent = {d for d, ts in zip(t["normalized_domain"].to_pylist(),
                                         t["fetched_at"].to_pylist())
                      if ts is not None and ts >= cutoff.replace(tzinfo=None)}
        except Exception:  # noqa: BLE001 — net-new dataset
            pass
        todo = [d for d in doms if d not in recent]
        counts["skipped_recent"] = len(doms) - len(todo)
        print(f"skipped_recent={counts['skipped_recent']:,} to_fetch={len(todo):,}")
        if dry_run:
            status = "success"
            return {"run_id": run_id, **counts, "dry_run": True}

        results = []
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for i, r in enumerate(ex.map(_fetch_one, todo), 1):
                results.append(r)
                if i % 500 == 0:
                    print(f"  fetched {i:,}/{len(todo):,}")
        counts["fetched"] = len(results)
        for k in ("ok", "no_meta", "parked_or_default", "unreachable"):
            counts[k] = sum(1 for r in results if r[7] == k)
        print(f"classes: ok={counts['ok']:,} no_meta={counts['no_meta']:,} "
              f"parked={counts['parked_or_default']:,} "
              f"unreachable={counts['unreachable']:,}")

        fetched_at = started.replace(tzinfo=None)
        table = pa.table({
            "normalized_domain": pa.array([r[0] for r in results], pa.string()),
            "fetch_url": pa.array([r[1] for r in results], pa.string()),
            "final_url": pa.array([r[2] for r in results], pa.string()),
            "http_status": pa.array([r[3] for r in results], pa.int32()),
            "meta_description": pa.array([r[4] for r in results], pa.string()),
            "og_description": pa.array([r[5] for r in results], pa.string()),
            "page_title": pa.array([r[6] for r in results], pa.string()),
            "liveness_class": pa.array([r[7] for r in results], pa.string()),
            "error": pa.array([r[8] for r in results], pa.string()),
            "fetched_at": pa.array([fetched_at] * len(results), pa.timestamp("us")),
            "run_id": pa.array([run_id] * len(results), pa.string()),
        })
        mode = "append"
        try:
            lance.dataset(DATASET_URI, storage_options=so)
        except Exception:  # noqa: BLE001
            mode = "overwrite"  # net-new create
        lance.write_dataset(table, DATASET_URI, mode=mode,
                            data_storage_version=DATA_STORAGE_VERSION,
                            storage_options=so)
        ds = lance.dataset(DATASET_URI, storage_options=so)
        have = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                for i in ds.list_indices()}
        for col in BTREE_INDEXES:
            if f"{col}_idx" not in have:
                ds.create_scalar_index(col, index_type="BTREE")
        for col in BITMAP_INDEXES:
            if f"{col}_idx" not in have:
                ds.create_scalar_index(col, index_type="BITMAP")
        print(f"appended {len(results):,} → total {ds.count_rows():,} (v{ds.version})")
        status = "success"
        return {"run_id": run_id, **counts, "total": ds.count_rows(),
                "version": ds.version}
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record(run_id, DATASET_URI, counts, lineage, status, error, started)


if __name__ == "__main__":
    print(run(dry_run="--dry-run" in sys.argv))
