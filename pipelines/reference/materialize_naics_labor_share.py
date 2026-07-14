"""naics_labor_share — the composed labor-share dim (SUSB share × ECEC burden, BEA cross-check).

The declared follow-on to the labor-share-of-revenue stack (PR #1118–#1120,
docs/reference/LABOR_SHARE_OF_REVENUE_STACK.md). Composes the landed calibration
ingredients into ONE row per 6-digit NAICS so any award's (naics, psc) closes the identity
with a single join:

    expected labor $ by category = award_$ × loaded_labor_share × category_mix
                                            └── this dim ──┘      └ naics_psc_labor_profile_categories ┘

Inputs (all Lance, s3://data-sink/active/):
  census_susb_naics_payroll_receipts — payroll_share = annual_payroll / receipts, '01: Total'
    size class, resolved at the most specific NAICS level available (6→5→4→3→sector; sector
    ranges 31-33 / 44-45 / 48-49 handled). Sector 92 (public administration) is structurally
    absent from SUSB → economy share, flagged payroll_share_level=0.
  bls_ecec_burden — burden_multiplier = total_comp / wages_salaries; the private-industry ×
    all-occupations × all-workers cell matched to the NAICS via a deterministic sector→ECEC
    (CES supersector) map, most-specific-first (e.g. 336411 exact, 6231xx → nursing care,
    54 → prof/sci/tech). Unmapped sectors (11 ag, 21 mining) fall to goods/service-providing,
    then economy-private (1.4294 @ 2026 Q01). ECEC industry codes come from bls_ecec_costs
    (the burden table carries only the group text).
  bea_industry_value_added + bea_naics_concordance — comp_share_of_output at the latest
    derived year, bound by NAICS-prefix → BEA summary line. Carried as the calibration
    CROSS-CHECK column (value-added-basis share), never composed into the scalar.

Universe: distinct 6-digit NAICS in naics_psc_labor_dim (the combo layer, 853) ∪ SUSB
6-digit codes (970). Composed scalar:

    loaded_labor_share = payroll_share × burden_multiplier

payroll_share is receipts-basis and can exceed 1 for pass-through-heavy industries (e.g.
holding companies; SUSB 6-digit max ≈ 2.51) — kept verbatim, flagged via share_gt_1 in the
run coverage; discounting for award-level pass-through (sub-out) is downstream query-time
work against award_subout_rollup, not baked here.

Ledger: ops.labor_share_runs (shared with the ingest module).

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \\
      --with boto3 --with 'psycopg[binary]' \\
      python -m pipelines.reference.materialize_naics_labor_share --smoke   # throwaway URI
    ...                                                                      # full (no flag)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

from pipelines.bls.ingest import (  # noqa: E402 — fleet plumbing, do not reimplement
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)
from pipelines.reference.labor_share_ingest import _record_run  # shared ops ledger

BUCKET = "data-sink"
A = f"s3://{BUCKET}/active"
SOURCE_TAG = "materialize_naics_labor_share"
OUT_URI = os.environ.get("NAICS_LABOR_SHARE_URI", f"{A}/naics_labor_share/")

# ── NAICS prefix → ECEC industry code (CES-supersector taxonomy), most-specific-first ──
# Codes verified against the landed bls_ecec_costs private × datatype D × national universe.
_ECEC_PREFIX_MAP: list[tuple[str, str]] = [
    ("336411", "336411"),  # aircraft manufacturing (exact detail cell)
    ("6231", "623100"),    # nursing care facilities
    ("623", "623000"),     # nursing and residential care
    ("622", "622000"),     # hospitals
    ("62", "620000"),      # health care and social assistance
    ("6112", "612000"),    # junior colleges
    ("6113", "612000"),    # colleges, universities, professional schools
    ("61", "610000"),      # educational services
    ("522", "522000"),     # credit intermediation
    ("524", "524000"),     # insurance carriers
    ("52", "520000"),      # finance and insurance
    ("22", "220000"),
    ("23", "230000"),
    ("31", "300000"), ("32", "300000"), ("33", "300000"),  # manufacturing
    ("42", "420000"),                                       # wholesale trade
    ("44", "412000"), ("45", "412000"),                     # retail trade
    ("48", "430000"), ("49", "430000"),                     # transportation & warehousing
    ("51", "510000"),
    ("53", "530000"),
    ("54", "540000"),
    ("55", "540A00"),      # management of companies → professional & business services
    ("56", "560000"),
    ("71", "700000"),      # leisure and hospitality
    ("72", "720000"),      # accommodation and food services
    ("81", "810000"),
]
# Supersector fallback for sectors with no published ECEC cell.
_ECEC_GOODS_SECTORS = {"11", "21"}          # → G00000 (goods-producing)
_ECEC_ECON_CODE = "000000"
_SECTOR_RANGE = {"31": "31-33", "32": "31-33", "33": "31-33",
                 "44": "44-45", "45": "44-45", "48": "48-49", "49": "48-49"}

# ── validation gate bounds ──────────────────────────────────────────────────────────
GATE_MIN_ROWS = 900
GATE_SHARE_COVERAGE = 1.0         # payroll_share non-null fraction (sector 92 → economy fallback, level 0)
GATE_BEA_COVERAGE = 0.90          # bea comp-share non-null fraction
GATE_BURDEN_BAND = (1.05, 1.90)
GATE_ECON_SHARE = (0.176307, 5e-4)          # SUSB economy anchor
GATE_ECON_BURDEN = (1.429448, 1e-3)         # ECEC economy-private anchor
GATE_SECTOR54_MEDIAN = (0.30, 0.55)         # 6-digit sector-54 payroll_share median band


def _gate(name: str, ok: bool, detail: str) -> None:
    print(f"GATE {name}: {'PASS' if ok else 'FAIL'} — {detail}", flush=True)
    if not ok:
        raise RuntimeError(f"gate failed: {name} — {detail}")


def _ecec_cell_for(naics: str) -> tuple[str, str]:
    """→ (ecec_industry_code, match_level)."""
    for prefix, code in _ECEC_PREFIX_MAP:
        if naics.startswith(prefix):
            return code, ("detail" if len(prefix) > 2 else "sector")
    if naics[:2] in _ECEC_GOODS_SECTORS:
        return "G00000", "supersector"
    return _ECEC_ECON_CODE, "economy"


def _schema():
    import pyarrow as pa
    return pa.schema([
        ("naics_code", pa.string()), ("naics_description", pa.string()),
        ("in_combo_layer", pa.bool_()),
        ("payroll_share", pa.float64()), ("payroll_share_naics", pa.string()),
        ("payroll_share_level", pa.int32()),
        ("burden_multiplier", pa.float64()), ("burden_industry_code", pa.string()),
        ("burden_industry_group", pa.string()), ("burden_quarter", pa.string()),
        ("burden_match_level", pa.string()),
        ("loaded_labor_share", pa.float64()),
        ("bea_summary_code", pa.string()), ("bea_summary_desc", pa.string()),
        ("bea_comp_share_of_output", pa.float64()), ("bea_share_year", pa.int32()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def _load_inputs(so: dict):
    import lance

    def tbl(name, columns=None, filter=None):  # noqa: A002
        return lance.dataset(f"{A}/{name}/", storage_options=so) \
            .scanner(columns=columns, filter=filter).to_table().to_pylist()

    susb = tbl("census_susb_naics_payroll_receipts",
               columns=["naics", "naics_level", "naics_description", "size_class",
                        "payroll_share"],
               filter="size_class = '01: Total'")
    burden = tbl("bls_ecec_burden",
                 filter=("ownership = 'Private industry workers' AND "
                         "occupation_group = 'All occupations' AND subcell = 'All workers'"))
    # burden carries only the group TEXT; recover code↔text from the costs catalog
    cost_pairs = tbl("bls_ecec_costs", columns=["industry_code", "industry_group"],
                     filter="owner_code = '2' AND datatype_code = 'D' AND area_code = '99999'")
    combos = tbl("naics_psc_labor_dim", columns=["naics_code"])
    conc = tbl("bea_naics_concordance",
               columns=["naics_code_clean", "bea_summary_code", "bea_summary_desc"])
    bea = tbl("bea_industry_value_added",
              columns=["industry_name", "year", "comp_share_of_output"],
              filter="component = 'derived_comp_share'")
    return susb, burden, cost_pairs, combos, conc, bea


def run(*, storage_options: dict | None = None, uri: str = OUT_URI) -> dict:
    import pyarrow as pa

    so = storage_options or _storage_options()
    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at
    status, error_text, built = "error", None, []
    rows: list[dict] = []
    cov: dict = {}
    try:
        susb, burden, cost_pairs, combos, conc, bea = _load_inputs(so)

        # SUSB total-row share by naics (all levels present, incl. economy '--')
        share_by_naics = {r["naics"]: r for r in susb}
        econ_share = share_by_naics["--"]["payroll_share"]
        _gate("econ-susb-anchor", abs(econ_share - GATE_ECON_SHARE[0]) <= GATE_ECON_SHARE[1],
              f"economy payroll_share={econ_share:.6f}")

        # ECEC burden by industry code (text → code via the costs catalog)
        code_by_group = {p["industry_group"]: p["industry_code"] for p in cost_pairs}
        burden_by_code = {}
        for b in burden:
            code = code_by_group.get(b["industry_group"])
            if code:
                burden_by_code[code] = b
        econ_burden = burden_by_code[_ECEC_ECON_CODE]["burden_multiplier"]
        _gate("econ-burden-anchor",
              abs(econ_burden - GATE_ECON_BURDEN[0]) <= GATE_ECON_BURDEN[1],
              f"economy-private burden={econ_burden:.6f}")
        _gate("burden-band",
              all(GATE_BURDEN_BAND[0] <= b["burden_multiplier"] <= GATE_BURDEN_BAND[1]
                  for b in burden),
              f"{len(burden_by_code)} matched cells within {GATE_BURDEN_BAND}")

        # BEA: latest derived comp-share per summary desc (case/space-normalized)
        bea_latest: dict[str, dict] = {}
        for r in bea:
            key = (r["industry_name"] or "").strip().lower()
            cur = bea_latest.get(key)
            if cur is None or r["year"] > cur["year"]:
                bea_latest[key] = r
        # concordance: naics prefix → summary line (longest prefix wins at query below)
        conc_by_naics: dict[str, dict] = {}
        for c in conc:
            k = c["naics_code_clean"]
            if k:
                conc_by_naics.setdefault(k, c)

        def resolve_share(naics: str):
            """Walk 6→5→4→3→sector(range-mapped) for the most specific SUSB total row."""
            for n in range(6, 2, -1):
                r = share_by_naics.get(naics[:n])
                if r and r["payroll_share"] is not None:
                    return r["payroll_share"], r["naics"], n
            sector = _SECTOR_RANGE.get(naics[:2], naics[:2])
            r = share_by_naics.get(sector)
            if r and r["payroll_share"] is not None:
                return r["payroll_share"], r["naics"], 2
            if naics[:2] == "92":  # public administration — structurally absent from SUSB
                return econ_share, "--", 0
            return None, None, None

        def resolve_bea(naics: str):
            for n in range(6, 1, -1):
                c = conc_by_naics.get(naics[:n])
                if not c:
                    continue
                hit = bea_latest.get((c["bea_summary_desc"] or "").strip().lower())
                if hit:
                    return (c["bea_summary_code"], c["bea_summary_desc"],
                            hit["comp_share_of_output"], hit["year"])
            return None, None, None, None

        combo_naics = {c["naics_code"] for c in combos if c["naics_code"]}
        susb6 = {r["naics"] for r in susb if r["naics_level"] == 6}
        universe = sorted(combo_naics | susb6)

        for naics in universe:
            share, share_naics, share_level = resolve_share(naics)
            ecec_code, match_level = _ecec_cell_for(naics)
            bcell = burden_by_code.get(ecec_code) or burden_by_code[_ECEC_ECON_CODE]
            if ecec_code not in burden_by_code:
                ecec_code, match_level = _ECEC_ECON_CODE, "economy"
            bea_code, bea_desc, bea_share, bea_year = resolve_bea(naics)
            desc_row = share_by_naics.get(naics)
            rows.append({
                "naics_code": naics,
                "naics_description": desc_row["naics_description"] if desc_row else None,
                "in_combo_layer": naics in combo_naics,
                "payroll_share": share, "payroll_share_naics": share_naics,
                "payroll_share_level": share_level,
                "burden_multiplier": bcell["burden_multiplier"],
                "burden_industry_code": ecec_code,
                "burden_industry_group": bcell["industry_group"],
                "burden_quarter": bcell["quarter"],
                "burden_match_level": match_level,
                "loaded_labor_share": (share * bcell["burden_multiplier"]
                                       if share is not None else None),
                "bea_summary_code": bea_code, "bea_summary_desc": bea_desc,
                "bea_comp_share_of_output": bea_share, "bea_share_year": bea_year,
                "source": SOURCE_TAG, "ingested_at": ingested_at,
            })

        # ── gates over the composed output ──
        n = len(rows)
        with_share = sum(1 for r in rows if r["payroll_share"] is not None)
        with_bea = sum(1 for r in rows if r["bea_comp_share_of_output"] is not None)
        _gate("row-count", n >= GATE_MIN_ROWS and n == len(universe),
              f"rows={n} (universe={len(universe)}, combo={len(combo_naics)}, susb6={len(susb6)})")
        _gate("share-coverage", with_share / n >= GATE_SHARE_COVERAGE,
              f"payroll_share coverage {with_share}/{n} = {with_share / n:.4f}")
        _gate("bea-coverage", with_bea / n >= GATE_BEA_COVERAGE,
              f"bea comp-share coverage {with_bea}/{n} = {with_bea / n:.4f}")
        s54 = sorted(r["payroll_share"] for r in rows
                     if r["naics_code"].startswith("54") and r["payroll_share"] is not None)
        s54_med = s54[len(s54) // 2]
        _gate("sector54-median",
              GATE_SECTOR54_MEDIAN[0] <= s54_med <= GATE_SECTOR54_MEDIAN[1],
              f"sector-54 median payroll_share={s54_med:.4f} over {len(s54)} rows")
        combo_resolved = sum(1 for r in rows if r["in_combo_layer"] and r["payroll_share"] is not None)
        _gate("combo-coverage", combo_resolved / len(combo_naics) >= GATE_SHARE_COVERAGE,
              f"combo-layer NAICS with share {combo_resolved}/{len(combo_naics)}")

        tbl = pa.Table.from_pylist(rows, schema=_schema())
        import lance
        lance.write_dataset(tbl, uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
        built = _build_indexes(uri, btree=["naics_code"],
                               bitmap=["payroll_share_level", "burden_match_level",
                                       "in_combo_layer"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        print(f"FATAL naics_labor_share: {exc}", flush=True)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        loaded = [r["loaded_labor_share"] for r in rows if r["loaded_labor_share"] is not None]
        cov = {
            "rows": len(rows),
            "combo_layer_rows": sum(1 for r in rows if r["in_combo_layer"]),
            "share_level_dist": {str(k): sum(1 for r in rows if r["payroll_share_level"] == k)
                                 for k in (6, 5, 4, 3, 2, 0, None)},
            "burden_match_dist": {k: sum(1 for r in rows if r["burden_match_level"] == k)
                                  for k in ("detail", "sector", "supersector", "economy")},
            "share_gt_1": sum(1 for r in rows
                              if r["payroll_share"] is not None and r["payroll_share"] > 1),
            "loaded_min": round(min(loaded), 6) if loaded else None,
            "loaded_max": round(max(loaded), 6) if loaded else None,
            "loaded_median": round(sorted(loaded)[len(loaded) // 2], 6) if loaded else None,
        }
        _record_run("naics_labor_share", uri, "composed:susb×ecec_burden(+bea xcheck)",
                    len(rows), built, cov, status, error_text, started_at, completed_at)
        print(f"NAICS_LABOR_SHARE SUMMARY: {cov} status={status}", flush=True)
    return {"dataset": "naics_labor_share", "rows": len(rows), "indexes": built,
            "coverage": cov, "status": status}


def _cli() -> None:
    p = argparse.ArgumentParser(description="Compose naics_labor_share (SUSB × ECEC burden, BEA cross-check).")
    p.add_argument("--smoke", action="store_true",
                   help="write to a throwaway _smoke_ URI and delete it after.")
    a = p.parse_args()
    uri = f"{A}/_smoke_naics_labor_share/" if a.smoke else OUT_URI
    result = run(uri=uri)
    if a.smoke:
        s3 = _s3_client()
        prefix = uri.split(f"{BUCKET}/", 1)[1]
        keys = [o["Key"] for page in s3.get_paginator("list_objects_v2")
                .paginate(Bucket=BUCKET, Prefix=prefix) for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET,
                              Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
        print(f"smoke cleanup: deleted {len(keys)} objects under {prefix}", flush=True)
    print(f"\n=== naics_labor_share summary ===\n  {result}", flush=True)


if __name__ == "__main__":
    _cli()
