"""BLS ECEC full universe — CM flat-file snapshot (R2 landing) → bls_ecec_costs + bls_ecec_burden.

Supersedes the 48-series/2020+ API slice landed by ``labor_share_ingest --stream ecec``
(that module's SUSB/BEA streams remain authoritative; its ECEC stream is deprecated).
Rebuilds the FULL published ECEC universe — all ~7,998 series, complete history — from
the authoritative BLS CM time-series flat files already landed at
``s3://data-sink/landing/bls/time-series/cm/`` (operator-verified byte-exact snapshot,
2026-07-11). Zero web fetching, zero LLM: deterministic in-memory parse + overwrite.

  cm.series.txt          catalog: 7,998 series, 15 tab-delimited cols (CRLF, space-padded
                         series_id — every field is strip()ed)
  cm.data.1.AllData.txt  all observations: series_id, year, period, value, footnote_codes
  cm.{owner,estimate,industry,occupation,subcell,datatype,area,seasonal,footnote}.txt
                         dim-code → text mapping files (tab-delimited w/ header)

  Dataset 1 — s3://data-sink/active/bls_ecec_costs/   (OVERWRITE)
    Grain: series_id × year × period. Catalog joined to AllData; every dim code lands
    verbatim AND decoded. Prior-slice columns (series_id, series_title, ownership,
    component, industry_group, occupation_group, year, period, value, source,
    ingested_at) keep their names/types; new columns are additive.

  Dataset 2 — s3://data-sink/active/bls_ecec_burden/  (OVERWRITE)
    burden_multiplier = total_comp / wages_salaries (estimate 01 / 02, datatype D cost
    level, area 99999 national, not seasonally adjusted) at the latest common quarter
    per (ownership × industry_group × occupation_group × subcell). Prior schema kept;
    occupation_group / subcell / area are additive.

Validation gates are hard-fail (see _gate calls). Ledger: ops.labor_share_runs (reused
from labor_share_ingest; audit-write failure warns, never raises).

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with boto3 \\
      --with 'psycopg[binary]' python -m pipelines.reference.ecec_full_universe --smoke
    ...                        python -m pipelines.reference.ecec_full_universe
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

# Fleet R2/index plumbing + ledger — reuse verbatim (do not reimplement).
from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)
from pipelines.reference.labor_share_ingest import _record_run  # noqa: E402

BUCKET = "data-sink"
LANDING_PREFIX = "landing/bls/time-series/cm/"
SOURCE_TAG = "ecec_full_universe"
SOURCE_URL = "r2:landing/bls/time-series/cm/"

COSTS_URI = os.environ.get("ECEC_COSTS_LANCE_URI", f"s3://{BUCKET}/active/bls_ecec_costs/")
BURDEN_URI = os.environ.get("ECEC_BURDEN_LANCE_URI", f"s3://{BUCKET}/active/bls_ecec_burden/")

EXPECTED_SERIES = 7_998
SERIES_COLS = 15
MIN_COST_ROWS = 400_000

# Anchor: private total-comp cost/hour, all-ind, all-occ, all-workers, national.
ANCHOR_SID = "CMU2010000000000D"
ANCHOR_VALUES = {(2025, "Q04"): 46.15, (2026, "Q01"): 46.60}
ECON_MULT_PRIOR = 1.4294  # private × all industries × all occ × all workers @ 2026 Q01
ECON_MULT_TOL = 0.01
BURDEN_BAND = (1.05, 1.90)
ECON_BAND = (1.35, 1.50)

_MAPPING_FILES = {
    "owner": ("cm.owner.txt", "owner_code", "owner_text"),
    "estimate": ("cm.estimate.txt", "estimate_code", "estimate_text"),
    "industry": ("cm.industry.txt", "industry_code", "industry_text"),
    "occupation": ("cm.occupation.txt", "occupation_code", "occupation_text"),
    "subcell": ("cm.subcell.txt", "subcell_code", "subcell_text"),
    "datatype": ("cm.datatype.txt", "datatype_code", "datatype_text"),
    "area": ("cm.area.txt", "area_code", "area_text"),
    "seasonal": ("cm.seasonal.txt", "seasonal_code", "seasonal_text"),
}


def _gate(name: str, ok: bool, detail: str) -> None:
    line = f"GATE {name}: {'PASS' if ok else 'FAIL'} — {detail}"
    print(line, flush=True)
    if not ok:
        raise RuntimeError(line)


# ── landing-file parse ──────────────────────────────────────────────────────────────
def _fetch_rows(s3, filename: str) -> list[list[str]]:
    """Landing file → list of stripped field lists (header included as row 0)."""
    body = s3.get_object(Bucket=BUCKET, Key=LANDING_PREFIX + filename)["Body"].read()
    text = body.decode("ascii")
    rows = []
    for line in text.split("\r\n"):
        if not line.strip():
            continue
        rows.append([f.strip() for f in line.split("\t")])
    return rows


def _fetch_mapping(s3, filename: str, code_col: str, text_col: str) -> dict[str, str]:
    rows = _fetch_rows(s3, filename)
    header = rows[0]
    ci, ti = header.index(code_col), header.index(text_col)
    return {r[ci]: r[ti] for r in rows[1:]}


def _float(v: str) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _costs_schema(with_value_raw: bool):
    import pyarrow as pa
    fields = [
        ("series_id", pa.string()), ("seasonal", pa.string()),
        ("owner_code", pa.string()), ("ownership", pa.string()),
        ("estimate_code", pa.string()), ("component", pa.string()),
        ("industry_code", pa.string()), ("industry_group", pa.string()),
        ("occupation_code", pa.string()), ("occupation_group", pa.string()),
        ("subcell_code", pa.string()), ("subcell", pa.string()),
        ("area_code", pa.string()), ("area", pa.string()),
        ("datatype_code", pa.string()), ("datatype", pa.string()),
        ("series_title", pa.string()), ("footnote_codes", pa.string()),
        ("year", pa.int32()), ("period", pa.string()), ("value", pa.float64()),
    ]
    if with_value_raw:
        fields.append(("value_raw", pa.string()))
    fields += [
        ("begin_year", pa.int32()), ("end_year", pa.int32()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
    return pa.schema(fields)


def _burden_schema():
    import pyarrow as pa
    return pa.schema([
        ("ownership", pa.string()), ("industry_group", pa.string()), ("quarter", pa.string()),
        ("total_comp", pa.float64()), ("wages_salaries", pa.float64()),
        ("burden_multiplier", pa.float64()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
        # additive over the prior 24-row schema:
        ("occupation_group", pa.string()), ("subcell", pa.string()), ("area", pa.string()),
    ])


def _write_lance(table, uri: str, so: dict) -> None:
    import lance
    lance.write_dataset(table, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)


# ── run ─────────────────────────────────────────────────────────────────────────────
def run(*, costs_uri: str = COSTS_URI, burden_uri: str = BURDEN_URI) -> dict:
    import pyarrow as pa

    so = _storage_options()
    s3 = _s3_client()
    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at

    maps = {k: _fetch_mapping(s3, f, c, t) for k, (f, c, t) in _MAPPING_FILES.items()}

    # ── catalog ──
    srows = _fetch_rows(s3, "cm.series.txt")
    sheader = srows[0]
    idx = {c: sheader.index(c) for c in sheader}
    catalog: dict[str, dict] = {}
    unresolved: list[str] = []
    for r in srows[1:]:
        sid = r[idx["series_id"]]
        rec = {c: r[idx[c]] for c in sheader}
        for dim, code_col in (("owner", "owner_code"), ("estimate", "estimate_code"),
                              ("industry", "industry_code"), ("occupation", "occupation_code"),
                              ("subcell", "subcell_code"), ("datatype", "datatype_code"),
                              ("area", "area_code"), ("seasonal", "seasonal")):
            code = rec[code_col]
            if code not in maps[dim]:
                unresolved.append(f"{sid}:{dim}={code}")
            rec[f"_{dim}_text"] = maps[dim].get(code)
        catalog[sid] = rec

    _gate("1-series-parse",
          len(catalog) == EXPECTED_SERIES and len(srows) - 1 == EXPECTED_SERIES
          and all(len(r) == SERIES_COLS for r in srows[1:]),
          f"series={len(catalog)} (expected {EXPECTED_SERIES}), data_rows={len(srows) - 1}, "
          f"all_15_cols={all(len(r) == SERIES_COLS for r in srows[1:])}")
    _gate("3-dim-resolution", not unresolved,
          f"unresolved={len(unresolved)}{' e.g. ' + ', '.join(unresolved[:5]) if unresolved else ''}")

    # ── observations ──
    drows = _fetch_rows(s3, "cm.data.1.AllData.txt")
    dheader = drows[0]
    di = {c: dheader.index(c) for c in dheader}
    alldata_rows = len(drows) - 1
    orphans = [r[di["series_id"]] for r in drows[1:] if r[di["series_id"]] not in catalog]
    _gate("2-alldata-join", not orphans,
          f"alldata_rows={alldata_rows}, orphan_series_ids={len(set(orphans))}")

    non_numeric = sum(1 for r in drows[1:] if _float(r[di["value"]]) is None)
    with_value_raw = non_numeric > 0
    print(f"non-numeric values: {non_numeric} → value_raw column {'INCLUDED' if with_value_raw else 'omitted'}",
          flush=True)

    cost_rows: list[dict] = []
    for r in drows[1:]:
        sid = r[di["series_id"]]
        c = catalog[sid]
        raw_val = r[di["value"]]
        row = {
            "series_id": sid, "seasonal": c["seasonal"],
            "owner_code": c["owner_code"], "ownership": c["_owner_text"],
            "estimate_code": c["estimate_code"], "component": c["_estimate_text"],
            "industry_code": c["industry_code"], "industry_group": c["_industry_text"],
            "occupation_code": c["occupation_code"], "occupation_group": c["_occupation_text"],
            "subcell_code": c["subcell_code"], "subcell": c["_subcell_text"],
            "area_code": c["area_code"], "area": c["_area_text"],
            "datatype_code": c["datatype_code"], "datatype": c["_datatype_text"],
            "series_title": c["series_title"], "footnote_codes": r[di["footnote_codes"]],
            "year": int(r[di["year"]]), "period": r[di["period"]],
            "value": _float(raw_val),
            "begin_year": int(c["begin_year"]), "end_year": int(c["end_year"]),
            "source": f"{SOURCE_TAG}:cm_flat_files", "ingested_at": ingested_at,
        }
        if with_value_raw:
            row["value_raw"] = raw_val
        cost_rows.append(row)

    _gate("2b-joined-count", len(cost_rows) == alldata_rows,
          f"joined={len(cost_rows)} == alldata={alldata_rows}")

    by_sid_q = {(r["series_id"], r["year"], r["period"]): r["value"] for r in cost_rows}
    anchor_ok = all(by_sid_q.get((ANCHOR_SID, y, p)) == v for (y, p), v in ANCHOR_VALUES.items())
    _gate("4-anchor", anchor_ok,
          f"{ANCHOR_SID}: " + ", ".join(
              f"{y} {p} = {by_sid_q.get((ANCHOR_SID, y, p))} (expect {v})"
              for (y, p), v in ANCHOR_VALUES.items()))
    _gate("6-row-count", len(cost_rows) >= MIN_COST_ROWS,
          f"cost_rows={len(cost_rows)} (≥ {MIN_COST_ROWS})")

    # ── burden: est 01/02, datatype D, area 99999, NSA; latest common quarter per grain ──
    lvl: dict[tuple, dict[tuple, float]] = {}
    for r in cost_rows:
        if (r["datatype_code"] != "D" or r["area_code"] != "99999"
                or r["seasonal"] != "U" or r["value"] is None
                or r["estimate_code"] not in ("01", "02")):
            continue
        grain = (r["ownership"], r["industry_group"], r["occupation_group"], r["subcell"])
        lvl.setdefault((grain, r["estimate_code"]), {})[(r["year"], r["period"])] = r["value"]

    burden_rows: list[dict] = []
    for (grain, est) in list(lvl):
        if est != "01":
            continue
        tc_q, ws_q = lvl[(grain, "01")], lvl.get((grain, "02"))
        if not ws_q:
            continue
        common = sorted(set(tc_q) & set(ws_q))
        if not common:
            continue
        y, p = common[-1]
        if not ws_q[(y, p)]:
            continue
        ownership, industry_group, occupation_group, subcell = grain
        burden_rows.append({
            "ownership": ownership, "industry_group": industry_group,
            "quarter": f"{y} {p}", "total_comp": tc_q[(y, p)],
            "wages_salaries": ws_q[(y, p)],
            "burden_multiplier": tc_q[(y, p)] / ws_q[(y, p)],
            "source": f"{SOURCE_TAG}:cm_flat_files", "ingested_at": ingested_at,
            "occupation_group": occupation_group, "subcell": subcell,
            "area": maps["area"].get("99999"),
        })

    econ = next((b for b in burden_rows
                 if b["ownership"] == "Private industry workers"
                 and b["industry_group"] == "All industries"
                 and b["occupation_group"] == "All occupations"
                 and b["subcell"] == "All workers"), None)
    mults = [b["burden_multiplier"] for b in burden_rows]
    _gate("5-burden",
          econ is not None
          and ECON_BAND[0] <= econ["burden_multiplier"] <= ECON_BAND[1]
          and abs(econ["burden_multiplier"] - ECON_MULT_PRIOR) <= ECON_MULT_TOL
          and econ["quarter"] == "2026 Q01"
          and min(mults) >= BURDEN_BAND[0] and max(mults) <= BURDEN_BAND[1],
          (f"econ_private={econ['burden_multiplier']:.4f} @ {econ['quarter']}" if econ
           else "econ row MISSING")
          + f"; burden_rows={len(burden_rows)}"
          + (f", min={min(mults):.4f}, max={max(mults):.4f}" if mults else ""))
    print(f"burden: rows={len(burden_rows)}, min={min(mults):.4f}, max={max(mults):.4f}, "
          f"econ={econ['burden_multiplier']:.6f} @ {econ['quarter']}", flush=True)

    # ── write + index + ledger ──
    status, error_text, built = "error", None, []
    cov_costs = {
        "series": len(catalog), "alldata_rows": alldata_rows, "cost_rows": len(cost_rows),
        "non_numeric_values": non_numeric, "value_raw_col": with_value_raw,
        "anchor": {f"{y} {p}": by_sid_q.get((ANCHOR_SID, y, p)) for (y, p) in ANCHOR_VALUES},
        "owners": {k: sum(1 for c in catalog.values() if c["owner_code"] == k) for k in maps["owner"]},
    }
    cov_burden = {
        "burden_rows": len(burden_rows),
        "economy_private_multiplier": round(econ["burden_multiplier"], 4),
        "economy_private_quarter": econ["quarter"],
        "burden_min": round(min(mults), 4), "burden_max": round(max(mults), 4),
    }
    try:
        tbl = pa.Table.from_pylist(cost_rows, schema=_costs_schema(with_value_raw))
        _write_lance(tbl, costs_uri, so)
        built = _build_indexes(costs_uri, btree=["series_id"],
                               bitmap=["ownership", "component", "industry_group",
                                       "occupation_group", "subcell", "datatype", "period"],
                               so=so)
        btbl = pa.Table.from_pylist(burden_rows, schema=_burden_schema())
        _write_lance(btbl, burden_uri, so)
        built += ["burden:" + b for b in _build_indexes(
            burden_uri, btree=["industry_group"],
            bitmap=["ownership", "occupation_group", "subcell"], so=so)]
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("bls_ecec_costs", costs_uri, SOURCE_URL, len(cost_rows), built,
                    cov_costs, status, error_text, started_at, completed_at)
        _record_run("bls_ecec_burden", burden_uri, SOURCE_URL, len(burden_rows), built,
                    cov_burden, status, error_text, started_at, completed_at)
        print(f"ECEC FULL-UNIVERSE SUMMARY: costs={cov_costs} burden={cov_burden} status={status}",
              flush=True)
    return {"cost_rows": len(cost_rows), "burden_rows": len(burden_rows),
            "indexes": built, "cov_costs": cov_costs, "cov_burden": cov_burden, "status": status}


# ── CLI ─────────────────────────────────────────────────────────────────────────────
def _smoke_uri(uri: str) -> str:
    base = uri.rstrip("/").rsplit("/", 1)[-1]
    return f"s3://{BUCKET}/active/_smoke_{base}/"


def _delete_prefix(uri: str) -> None:
    s3 = _s3_client()
    prefix = uri.split(f"{BUCKET}/", 1)[1]
    paginator = s3.get_paginator("list_objects_v2")
    keys = [o["Key"] for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix)
            for o in page.get("Contents", [])]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    print(f"  smoke cleanup: deleted {len(keys)} objects under {prefix}", flush=True)


def _cli() -> None:
    p = argparse.ArgumentParser(description="BLS ECEC full-universe rebuild (CM flat files → Lance).")
    p.add_argument("--smoke", action="store_true",
                   help="write to throwaway _smoke_ URIs and delete them after.")
    a = p.parse_args()
    costs_uri, burden_uri = COSTS_URI, BURDEN_URI
    if a.smoke:
        costs_uri, burden_uri = _smoke_uri(COSTS_URI), _smoke_uri(BURDEN_URI)
    try:
        r = run(costs_uri=costs_uri, burden_uri=burden_uri)
    finally:
        if a.smoke:
            _delete_prefix(costs_uri)
            _delete_prefix(burden_uri)
    print(f"\n=== ecec full-universe summary ===\n  {r}", flush=True)


if __name__ == "__main__":
    _cli()
