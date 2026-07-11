"""Reference loader — bea_naics_concordance: the official BEA Industry Economic Accounts ↔ 2017
NAICS crosswalk. Resolves the NULL `bea_industry_code` in bea_industry_value_added and bridges
BEA GDP-by-Industry lines onto the NAICS grain (the one open item from the labor-share stack).

Pattern A direct hydration (keyless static xlsx → PyArrow → Lance SoR). Source:
`www.bea.gov/sites/default/files/2023-10/BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx`
sheet `NAICS Codes`, header row 5. Each data row maps one 2017 NAICS code up through BEA's five
levels of detail — Sector (21) → Summary (71) → Underlying Summary (138) → Detail (402) → GO
Detail (414). The **Summary** level (bea_summary_code/desc) is the grain of the landed
bea_industry_value_added tables, so a name/desc join there yields the BEA industry code; the
naics_code column is the NAICS-grain bridge (a 6-digit award NAICS rolls up by prefix).

Raw stays lossless: BEA codes/descriptions land verbatim; `naics_code_clean` (digits-only) and
`naics_level` are ADDITIONAL derived columns. The `*` multi-I-O-industry marker is preserved
verbatim in naics_code and flagged in `naics_multi_io`.

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with requests \\
      --with openpyxl --with 'psycopg[binary]' \\
      python -m pipelines.reference.materialize_bea_naics_concordance --smoke   # throwaway URI
    ... python -m pipelines.reference.materialize_bea_naics_concordance                   # full

SoR:  s3://data-sink/active/bea_naics_concordance/  (Lance v2.1, mode=overwrite; ~499 rows)
Ledger: ops.labor_share_runs (dataset='bea_naics_concordance').
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re

from pipelines.bls.ingest import _build_indexes, _storage_options  # noqa: E402
from pipelines.reference.labor_share_ingest import (  # noqa: E402
    BUCKET,
    SOURCE_TAG,
    _delete_prefix,
    _download,
    _record_run,
    _s,
    _write_lance,
)

OUT_URI = os.environ.get("BEA_NAICS_CONCORDANCE_URI", f"s3://{BUCKET}/active/bea_naics_concordance/")
SOURCE_URL = ("https://www.bea.gov/sites/default/files/2023-10/"
              "BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx")
SHEET = "NAICS Codes"
HEADER_ROW = 5  # data begins row 6


def _code(v) -> str | None:
    """BEA/NAICS code cell → verbatim string (ints rendered without the trailing .0)."""
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, int):
        return str(v)
    return str(v).strip() or None


def _schema():
    import pyarrow as pa
    return pa.schema([
        ("naics_code", pa.string()), ("naics_code_clean", pa.string()), ("naics_level", pa.int32()),
        ("naics_multi_io", pa.bool_()), ("naics_description", pa.string()),
        ("bea_sector_code", pa.string()), ("bea_sector_desc", pa.string()),
        ("bea_summary_code", pa.string()), ("bea_summary_desc", pa.string()),
        ("bea_u_summary_code", pa.string()), ("bea_u_summary_desc", pa.string()),
        ("bea_detail_code", pa.string()), ("bea_detail_desc", pa.string()),
        ("bea_go_detail_code", pa.string()), ("bea_go_detail_desc", pa.string()),
        ("notes", pa.string()), ("source", pa.string()), ("source_url", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def run(*, storage_options: dict, uri: str = OUT_URI) -> dict:
    import openpyxl
    import pyarrow as pa

    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at
    so = storage_options
    status, error_text, built = "error", None, []
    rows: list[dict] = []
    cov: dict = {}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(_download(SOURCE_URL)), read_only=True, data_only=True)
        ws = wb[SHEET]
        for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
            summary = _code(r[2]) if len(r) > 2 else None
            naics = _code(r[11]) if len(r) > 11 else None
            # keep only real concordance rows (footnote/legend rows have neither)
            if summary is None or naics is None:
                continue
            clean = re.sub(r"\D", "", naics)
            rows.append({
                "naics_code": naics, "naics_code_clean": clean or None,
                "naics_level": len(clean) if clean else None,
                "naics_multi_io": "*" in naics, "naics_description": _s(r[12]) if len(r) > 12 else None,
                "bea_sector_code": _code(r[0]), "bea_sector_desc": _s(r[1]),
                "bea_summary_code": summary, "bea_summary_desc": _s(r[3]),
                "bea_u_summary_code": _code(r[4]), "bea_u_summary_desc": _s(r[5]),
                "bea_detail_code": _code(r[6]), "bea_detail_desc": _s(r[7]),
                "bea_go_detail_code": _code(r[8]), "bea_go_detail_desc": _s(r[9]),
                "notes": _s(r[10]), "source": f"{SOURCE_TAG}:bea_naics_concordance",
                "source_url": SOURCE_URL, "ingested_at": ingested_at,
            })
        wb.close()

        _write_lance(pa.Table.from_pylist(rows, schema=_schema()), uri, so)
        built = _build_indexes(uri, btree=["naics_code_clean", "bea_summary_code"],
                               bitmap=["bea_sector_code", "naics_level"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        print(f"FATAL bea_naics_concordance: {exc}", flush=True)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        cov = {
            "rows": len(rows),
            "distinct_naics": len({r["naics_code_clean"] for r in rows if r["naics_code_clean"]}),
            "distinct_summary": len({r["bea_summary_code"] for r in rows}),
            "distinct_detail": len({r["bea_detail_code"] for r in rows if r["bea_detail_code"]}),
            "distinct_sectors": len({r["bea_sector_code"] for r in rows if r["bea_sector_code"]}),
            "naics_level_counts": {lvl: sum(1 for r in rows if r["naics_level"] == lvl)
                                   for lvl in (2, 3, 4, 5, 6)},
            "multi_io_rows": sum(1 for r in rows if r["naics_multi_io"]),
        }
        _record_run("bea_naics_concordance", uri, SOURCE_URL, len(rows), built,
                    cov, status, error_text, started_at, completed_at)
        print(f"BEA_NAICS_CONCORDANCE SUMMARY: {cov} status={status}", flush=True)
    return {"dataset": "bea_naics_concordance", "rows": len(rows), "indexes": built,
            "coverage": cov, "status": status}


def _cli() -> None:
    p = argparse.ArgumentParser(description="Materialize the BEA Industry ↔ 2017 NAICS concordance.")
    p.add_argument("--smoke", action="store_true", help="write to a throwaway _smoke_ URI and delete after.")
    a = p.parse_args()
    so = _storage_options()
    uri = f"s3://{BUCKET}/active/_smoke_bea_naics_concordance/" if a.smoke else OUT_URI
    out = run(storage_options=so, uri=uri)
    if a.smoke:
        _delete_prefix(uri)
    print("RESULT:", out, flush=True)


if __name__ == "__main__":
    _cli()
