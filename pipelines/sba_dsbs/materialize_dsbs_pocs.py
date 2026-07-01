"""Compute worker — explode DSBS firm-level people into a person-grain POC Lance.

Part of the ``dsbs-pocs`` Modal app. Endpoint-less; dispatcher-resolvable or run manually.
Clean-room data plane: DuckDB/Python read + parse, Lance written straight to R2. No catalog.

WHY THIS EXISTS
    sba_dsbs_certified_firms carries the DSBS people ONLY as two unexploded firm-level strings:
    ``contact_person`` (a single primary-contact name) and ``current_principals`` (the SBA-declared
    owners/officers as a verbatim "Name - Title; Name - Title; ..." string). Neither is queryable at
    the person level. This lands the explosion — 1 row per (uei × named person) — the DSBS parallel
    to active/sam_pocs.

SOURCE : s3://data-sink/active/sba_dsbs_certified_firms/  (uei, legal_business_name, contact_person,
                                                           current_principals)
TARGET : s3://data-sink/active/dsbs_pocs/                 (native Lance v2.1; overwrite)

GRAIN / SCHEMA. 1 row per (uei × POC entry):
    uei, legal_business_name, poc_type ('contact_person'|'current_principal'), slot_no,
    full_name, first_name, last_name, title (principals only), name_key (order-independent
    normalized name for dedup/name-join), raw_entry (verbatim segment), materialized_at.
    contact_person and current_principals are BOTH kept (source-tagged via poc_type); the primary
    contact is frequently also a principal — dedup downstream on (uei, name_key) when distinct
    people are wanted (mirrors sam_pocs, which is slot-grain not person-deduped).

PARSE. current_principals: split on ';'; each segment split on the first " - " into name / title.
    Names handle "First [M.] Last" and "Last, First". Suffixes (Jr/Sr/II…) dropped from first/last
    derivation and name_key. Nothing normalized destructively — raw_entry preserves the source.

INDEXES: BTREE uei, name_key ; BITMAP poc_type.

    modal run    pipelines/sba_dsbs/materialize_dsbs_pocs.py::run
    modal run    pipelines/sba_dsbs/materialize_dsbs_pocs.py::verify_only
    modal deploy pipelines/sba_dsbs/materialize_dsbs_pocs.py
    # local materialize: doppler run -p core-x -c prd -- uv run \
    #   --with pylance --with duckdb --with pyarrow --with 'psycopg[binary]' --with modal \
    #   python pipelines/sba_dsbs/materialize_dsbs_pocs.py
"""
from __future__ import annotations

import os

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "dsbs_pocs"
DATASET_URI = os.environ.get("DSBS_POCS_URI", f"{_ACTIVE}/{DATASET}/")
DSBS_URI = f"{_ACTIVE}/sba_dsbs_certified_firms/"
FEED = "dsbs_pocs"
SOURCE_DB = "r2:sba_dsbs_certified_firms(contact_person+current_principals)"
DATA_STORAGE_VERSION = "2.1"

INDEXES = {"BTREE": ["uei", "name_key"], "BITMAP": ["poc_type"]}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dsbs_pocs_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed text NOT NULL, source_db text NOT NULL, datasets jsonb NOT NULL,
    mode text NOT NULL DEFAULT 'overwrite', rows_total bigint NOT NULL DEFAULT 0,
    firms bigint NOT NULL DEFAULT 0, status text NOT NULL, error text,
    started_at timestamptz, completed_at timestamptz, recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dsbs_pocs_runs_feed_idx        ON ops.dsbs_pocs_runs (feed);
CREATE INDEX IF NOT EXISTS dsbs_pocs_runs_recorded_at_idx ON ops.dsbs_pocs_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("dsbs-pocs", image=image)


def _r2_storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


# ── parse helpers ──────────────────────────────────────────────────────────────
import re  # noqa: E402
import unicodedata  # noqa: E402

_DASH = re.compile(r"\s[-–—]\s")
_SUFFIX = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "cpa", "esq", "pe", "mba", "jd", "dds"}


def _clean(s):
    if s is None:
        return None
    s = " ".join(str(s).split()).strip(" .,-")
    return s or None


def _split_principal(seg: str):
    parts = _DASH.split(seg.strip(), maxsplit=1)
    return _clean(parts[0]), (_clean(parts[1]) if len(parts) > 1 else None)


def _name_parts(name):
    if not name:
        return (None, None)
    n = name
    if "," in n:  # "Last, First [Middle]"
        a, b = n.split(",", 1)
        n = f"{_clean(b) or ''} {_clean(a) or ''}".strip()
    toks = [t for t in re.split(r"[^A-Za-z]+", n) if t]
    keep = [t for t in toks if t.lower().strip(".") not in _SUFFIX] or toks
    if not keep:
        return (None, None)
    first = keep[0]
    last = keep[-1] if len(keep) >= 2 else keep[0]
    return (first, last)


def _name_key(name):
    if not name:
        return None
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    toks = sorted(t for t in re.split(r"[^a-z]+", s) if len(t) >= 2 and t not in _SUFFIX)
    return " ".join(toks) or None


def build_dsbs_pocs() -> dict:
    import datetime as dt

    import lance
    import pyarrow as pa

    so = _r2_storage_options()
    tbl = lance.dataset(DSBS_URI, storage_options=so).to_table(
        columns=["uei", "legal_business_name", "contact_person", "current_principals"])
    uei = tbl.column("uei").to_pylist()
    name = tbl.column("legal_business_name").to_pylist()
    cp = tbl.column("contact_person").to_pylist()
    prn = tbl.column("current_principals").to_pylist()

    C = {k: [] for k in ["uei", "legal_business_name", "poc_type", "slot_no", "full_name",
                         "first_name", "last_name", "title", "name_key", "raw_entry"]}

    def emit(u, ln, ptype, slot, full, title, raw):
        full = _clean(full)
        if not full:
            return
        f, l = _name_parts(full)
        C["uei"].append(u); C["legal_business_name"].append(ln)
        C["poc_type"].append(ptype); C["slot_no"].append(slot)
        C["full_name"].append(full); C["first_name"].append(f); C["last_name"].append(l)
        C["title"].append(_clean(title)); C["name_key"].append(_name_key(full))
        C["raw_entry"].append(raw)

    firms_with = set()
    for u, ln, c, p in zip(uei, name, cp, prn):
        if u is None:
            continue
        if c and str(c).strip():
            emit(u, ln, "contact_person", 0, c, None, c)
            firms_with.add(u)
        if p and str(p).strip():
            for i, seg in enumerate(str(p).split(";")):
                if not seg.strip():
                    continue
                nm, ti = _split_principal(seg)
                emit(u, ln, "current_principal", i, nm, ti, seg.strip())
                firms_with.add(u)

    n = len(C["uei"])
    mat = dt.datetime.now(dt.UTC)
    schema = pa.schema([
        ("uei", pa.string()), ("legal_business_name", pa.string()), ("poc_type", pa.string()),
        ("slot_no", pa.int32()), ("full_name", pa.string()), ("first_name", pa.string()),
        ("last_name", pa.string()), ("title", pa.string()), ("name_key", pa.string()),
        ("raw_entry", pa.string()), ("materialized_at", pa.timestamp("us", tz="UTC")),
    ])
    out = pa.table({**{k: pa.array(v) for k, v in C.items()},
                    "materialized_at": pa.array([mat] * n, pa.timestamp("us", tz="UTC"))}, schema=schema)

    lance.write_dataset(out, DATASET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(DATASET_URI, storage_options=so)
    assert ds.count_rows() == n
    for typ, cols in INDEXES.items():
        for col in cols:
            try:
                ds.create_scalar_index(col, index_type=typ)
            except Exception as exc:  # noqa: BLE001
                print(f"  idx skip {typ} {col}: {exc}", flush=True)
    import duckdb
    con = duckdb.connect(":memory:"); con.register("t", ds.to_table(columns=["poc_type", "name_key", "uei"]))
    by_type = con.execute("SELECT poc_type, count(*) FROM t GROUP BY 1 ORDER BY 2 DESC").fetchall()
    distinct_people = con.execute("SELECT count(DISTINCT uei||'|'||coalesce(name_key,'')) FROM t").fetchone()[0]
    print(f"WROTE {DATASET_URI}  rows={n:,}  firms_with_a_poc={len(firms_with):,}  "
          f"distinct(uei,name_key)={distinct_people:,}  by_type={by_type}", flush=True)
    return {"uri": DATASET_URI, "rows": n, "firms": len(firms_with)}


def _record_run(rows_total, firms, status, error, started, completed):
    import psycopg
    from psycopg.types.json import Jsonb
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED unset; skipping ops ledger.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("INSERT INTO ops.dsbs_pocs_runs (feed,source_db,datasets,mode,rows_total,firms,"
                        "status,error,started_at,completed_at) VALUES (%s,%s,%s,'overwrite',%s,%s,%s,%s,%s,%s)",
                        (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), rows_total, firms, status,
                         error, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops ledger write failed: {exc}")


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


@app.function(secrets=_SECRETS, timeout=60 * 20, memory=16384, cpu=4.0)
def ingest() -> dict:
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_dsbs_pocs()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run(res.get("rows", 0), res.get("firms", 0), status, error, started,
                    dt.datetime.now(dt.timezone.utc))
    return res


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices())
    out = {"uri": DATASET_URI, "rows": ds.count_rows(), "cols": len(ds.schema),
           "schema": [f.name for f in ds.schema], "indices": idx}
    print(out)
    return out


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))


if __name__ == "__main__":
    import datetime as dt
    started = dt.datetime.now(dt.timezone.utc)
    status, error, res = "error", None, {}
    try:
        res = build_dsbs_pocs()
        status = "success"
    finally:
        _record_run(res.get("rows", 0), res.get("firms", 0), status, error, started,
                    dt.datetime.now(dt.timezone.utc))
    print(res)
