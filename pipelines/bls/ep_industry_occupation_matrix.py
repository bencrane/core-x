"""BLS EP National Employment Matrix — full industry×occupation staffing pattern → Lance.

The operator's `occupation.xlsx` (BLS EP summary workbook, landed under
landing/bls/employment-projections/) does NOT carry the matrix cells — its Table 1.9 is a
423-row *directory* of industries, each hyperlinking to an online per-industry matrix at
    https://data.bls.gov/projections/nationalMatrix?queryParams=<code>&ioType=i
Those pages are server-rendered (the occupation×employment table is inline HTML — the
"download" button just serializes it client-side), so this worker fetches all 423, parses
the embedded table, and unions them into one Lance dataset: the complete 2024→2034
industry-occupation projection matrix that OEWS (current-year) and the TE1000-only EP feed
each lacked.

    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ep_industry_occupation_matrix --run
    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ep_industry_occupation_matrix --run --limit 5   # smoke

GRAIN & FIDELITY (no data left behind)
    One row per (industry_code, occupation_code) cell across ALL 423 industry levels
    (TE1000 total → sectors → subsectors → detailed industries) — the published matrix at
    every hierarchy level; nothing is aggregated or deduped (industry_code is part of the
    key). The 13 source columns match the IND_TE1000 download already ingested; the eight
    measures are typed DOUBLE (em-dash / blank → NULL via coerce), occupation codes arrive
    clean (no Excel formula-escaping). Every industry is tagged with title / industry_type /
    2022 NAICS from the Table 1.9 index. The scrape is FAIL-CLOSED: any industry that cannot
    be fetched+parsed after retries aborts the run — a partial matrix is never written.

    Self-check: the TE1000 slice must reproduce the standalone bls_employment_projections
    feed (1,113 rows) — asserted before write.

CONTROL PLANE
    Terminal state → ops.bls_runs (dataset='ep_industry_occupation_matrix'), best-effort.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import random
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipelines.bls.ingest import (
    BUCKET,
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _record_run,
    _s3_client,
    _storage_options,
)

SCRATCH_DIR = tempfile.gettempdir()
LANDING_KEY = "landing/bls/employment-projections/occupation.xlsx"
INDEX_SHEET = "Table 1.9"
MATRIX_URL = "https://data.bls.gov/projections/nationalMatrix?queryParams={code}&ioType=i"
URI = os.environ.get(
    "BLS_EP_MATRIX_LANCE_URI",
    f"s3://{BUCKET}/active/bls_ep_industry_occupation_matrix_2024_2034/",
)
RELEASE = "2024-2034"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
MAX_WORKERS = 6
RETRIES = 4

# source header → canonical typed column (the 8 numeric measures)
MEASURES = {
    "2024 Employment": "emp_2024",
    "2024 Percent of Industry": "pct_industry_2024",
    "2024 Percent of Occupation": "pct_occupation_2024",
    "Projected 2034 Employment": "emp_2034",
    "Projected 2034 Percent of Industry": "pct_industry_2034",
    "Projected 2034 Percent of Occupation": "pct_occupation_2034",
    "Employment Change, 2024-2034": "emp_change_2024_2034",
    "Employment Percent Change, 2024-2034": "emp_pct_change_2024_2034",
}


# ── industry index (from the landed Table 1.9) ─────────────────────────────────────
def _read_industry_index(xlsx_path: str) -> list[dict]:
    import openpyxl

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    try:
        ws = wb[INDEX_SHEET]
        out: list[dict] = []
        for r in ws.iter_rows(min_row=3, values_only=True):  # rows 1-2 = title + header
            code = r[1]
            if not code:
                continue
            naics = str(r[3]).strip() if len(r) > 3 and r[3] not in (None, "—") else None
            out.append({
                "industry_code": str(code).strip(),
                "industry_title": str(r[0]).strip() if r[0] else None,
                "industry_type": str(r[2]).strip() if len(r) > 2 and r[2] else None,
                "naics_code": naics,
            })
        return out
    finally:
        wb.close()


# ── fetch one industry's matrix page (retry, fail-closed) ──────────────────────────
def _fetch(code: str) -> str:
    url = MATRIX_URL.format(code=urllib.parse.quote(code))
    last = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=45) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                html = resp.read().decode("utf-8", "replace")
            if "Occupation Code" not in html:
                raise RuntimeError("matrix table absent in response")
            return html
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep((2 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"[{code}] fetch failed after {RETRIES}: {last}")


# ── parse an industry page → cleaned row dicts ─────────────────────────────────────
def _parse(html: str, ind: dict) -> list[dict]:
    import pandas as pd

    df = pd.read_html(io.StringIO(html))[0]
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]  # flatten
    for src in MEASURES:  # coerce measures: "—"/blank → NaN
        df[src] = pd.to_numeric(df[src].astype(str).str.replace(",", "", regex=False),
                                errors="coerce")
    for c in ("Occupation Sort", "Display Level"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    def num(v):
        return None if v is None or (isinstance(v, float) and v != v) else float(v)

    def int_(v):
        return None if v is None or (isinstance(v, float) and v != v) else int(v)

    rows: list[dict] = []
    for _, r in df.iterrows():
        rec = {
            "industry_code": ind["industry_code"],
            "industry_title": ind["industry_title"],
            "industry_type": ind["industry_type"],
            "naics_code": ind["naics_code"],
            "occupation_title": (str(r["Occupation Title"]).strip() or None),
            "occupation_code": (str(r["Occupation Code"]).strip() or None),
            "occupation_type": (str(r["Occupation Type"]).strip() or None),
            "occupation_sort": int_(r["Occupation Sort"]),
            "display_level": int_(r["Display Level"]),
        }
        for src, dst in MEASURES.items():
            rec[dst] = num(r[src])
        rows.append(rec)
    return rows


_SCHEMA_COLS = [
    "industry_code", "industry_title", "industry_type", "naics_code",
    "occupation_title", "occupation_code", "occupation_type",
    "emp_2024", "pct_industry_2024", "pct_occupation_2024",
    "emp_2034", "pct_industry_2034", "pct_occupation_2034",
    "emp_change_2024_2034", "emp_pct_change_2024_2034",
    "occupation_sort", "display_level",
]


def _arrow_table(rows: list[dict], now):
    import pyarrow as pa

    str_cols = {"industry_code", "industry_title", "industry_type", "naics_code",
                "occupation_title", "occupation_code", "occupation_type"}
    int_cols = {"occupation_sort", "display_level"}
    fields = []
    for c in _SCHEMA_COLS:
        t = pa.string() if c in str_cols else (pa.int32() if c in int_cols else pa.float64())
        fields.append((c, t))
    fields += [("projections_vintage", pa.string()), ("source", pa.string()),
               ("ingested_at", pa.timestamp("us", tz="UTC"))]
    schema = pa.schema(fields)
    data = {c: [r.get(c) for r in rows] for c in _SCHEMA_COLS}
    n = len(rows)
    data["projections_vintage"] = [RELEASE] * n
    data["source"] = ["BLS EP National Employment Matrix, by industry (data.bls.gov/projections)"] * n
    data["ingested_at"] = [now] * n
    return pa.table(data, schema=schema)


# ── orchestration ──────────────────────────────────────────────────────────────────
def run(limit: int | None = None, keep_temp: bool = False) -> dict:
    import lance

    so = _storage_options()
    s3 = _s3_client()
    now = dt.datetime.now(dt.timezone.utc)
    started = dt.datetime.now(dt.timezone.utc)
    status, error, built, rows_total = "error", None, [], 0

    xlsx = os.path.join(SCRATCH_DIR, "bls_occupation_workbook.xlsx")
    try:
        s3.download_file(BUCKET, LANDING_KEY, xlsx)
        industries = _read_industry_index(xlsx)
        if limit:
            industries = industries[:limit]
        print(f"industries to fetch: {len(industries)}")

        all_rows: list[dict] = []
        per_industry: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_fetch, ind["industry_code"]): ind for ind in industries}
            done = 0
            for fut in as_completed(futs):
                ind = futs[fut]
                html = fut.result()  # raises → aborts the run (fail-closed)
                recs = _parse(html, ind)
                if not recs:
                    raise RuntimeError(f"[{ind['industry_code']}] parsed 0 rows")
                all_rows.extend(recs)
                per_industry[ind["industry_code"]] = len(recs)
                done += 1
                if done % 50 == 0 or done == len(industries):
                    print(f"  fetched {done}/{len(industries)}  rows so far={len(all_rows):,}")

        rows_total = len(all_rows)
        # self-check: TE1000 slice must reproduce the standalone EP feed (1,113 rows)
        te = per_industry.get("TE1000")
        if not limit and te not in (None, 1113):
            raise RuntimeError(f"TE1000 slice={te}, expected 1113 — matrix drift, aborting")
        print(f"parsed {rows_total:,} rows across {len(per_industry)} industries (TE1000={te})")

        table = _arrow_table(all_rows, now)
        lance.write_dataset(table, URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        del table, all_rows
        print(f"wrote Lance -> {URI}")
        built = _build_indexes(URI, ["industry_code", "occupation_code", "naics_code"], [], so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        raise
    finally:
        _record_run("ep_industry_occupation_matrix", URI,
                    f"{LANDING_KEY} + {MATRIX_URL.format(code='<423 industries>')}",
                    RELEASE, int(rows_total), 0, built, status, error, started,
                    dt.datetime.now(dt.timezone.utc))
        if not keep_temp:
            try:
                os.remove(xlsx)
            except OSError:
                pass
    return {"rows": rows_total, "indexes": built, "uri": URI}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="BLS EP industry-occupation matrix scrape → Lance.")
    ap.add_argument("--run", action="store_true", help="fetch all industries and materialize")
    ap.add_argument("--limit", type=int, help="fetch only first N industries (smoke)")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args(argv)
    if not args.run:
        ap.error("specify --run")
    r = run(limit=args.limit, keep_temp=args.keep_temp)
    print(f"\n=== EP matrix ingest ===\n  rows={r['rows']:,}  idx={len(r['indexes'])}  -> {r['uri']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
