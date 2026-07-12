"""VA veteran demand-side cluster — data.va.gov Socrata → county-grain Lance reference datasets.

The demand denominator for GTM market pages that rank where clinician-staffing demand
(VA C&P exam work, NAICS 621111 × PSC Q403) outruns local medical-labor supply. The exam
contracts book to a few national primes (QTC/OptumServe/Loyal Source/VES) at their ops hubs
and have no meaningful place-of-performance geography — actual demand is wherever veterans
live. This lands veteran population + disability-compensation recipients at county grain,
keyed on 5-char county FIPS + state, joinable to txn_events_combo_by_geo.pop_county_fips and
SAM entity physical_state.

  Dataset 1 — s3://data-sink/active/va_vetpop_county/         (OVERWRITE)
    VetPop2023 county projection (jrjd-qghv). Grain: fips × snapshot_year × age_group × sex.
    Spans 31 projection years (FY2023 base → FY2053).
  Dataset 1b — s3://data-sink/active/va_vetpop_county_total/  (OVERWRITE)
    Rollup: fips × snapshot_year → veterans_total (sum over age/sex) — the plain denominator.
  Dataset 2 — s3://data-sink/active/va_disability_comp_county/ (OVERWRITE)
    Disability Compensation Recipients by County, FY2019/21/23/24/25 (fips-native years).
    Socrata munges field names per year → tolerant field matcher. SCD-rating severity bands +
    age + sex preserved (severity → re-exam intensity signal). FY2020 excluded (degenerate:
    no FIPS, "Unknown" counties). GDX deferred (the JSON API resources are malformed).

Validation gates are hard-fail. Ledger: ops.labor_share_runs (reused; audit-write failure
warns, never raises). Zero LLM, zero scraping — public Socrata JSON API.

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with boto3 \\
      --with 'psycopg[binary]' python -m pipelines.reference.va_veteran_demand --smoke
    ...                        python -m pipelines.reference.va_veteran_demand
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request

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
SOURCE_TAG = "va_veteran_demand"
SOURCE_URL = "https://www.data.va.gov/resource/"
UA = "hq-data-factory-ingest/1.0 (benjaminjcrane@gmail.com)"
PAGE = 50_000

VETPOP_URI = os.environ.get("VA_VETPOP_LANCE_URI", f"s3://{BUCKET}/active/va_vetpop_county/")
VETPOP_TOTAL_URI = os.environ.get("VA_VETPOP_TOTAL_LANCE_URI", f"s3://{BUCKET}/active/va_vetpop_county_total/")
DISAB_URI = os.environ.get("VA_DISAB_LANCE_URI", f"s3://{BUCKET}/active/va_disability_comp_county/")

VETPOP_RESOURCE = "jrjd-qghv"
# Disability Compensation Recipients by County — fips-native fiscal years only.
# FY2020 (6263-7mn5) excluded: no fips_code, "Unknown" counties. GDX deferred (malformed API).
DISAB_RESOURCES = {
    2019: "k42f-3ku6", 2021: "k9i5-f2ec", 2023: "5uqy-ph6a",
    2024: "74ts-jsvb", 2025: "95an-zhhy",
}
BASE_SNAPSHOT_YEAR = 2023  # VetPop2023 base year for the national-total gate
VET_NATIONAL_BAND = (16_000_000, 20_000_000)


def _gate(name: str, ok: bool, detail: str) -> None:
    line = f"GATE {name}: {'PASS' if ok else 'FAIL'} — {detail}"
    print(line, flush=True)
    if not ok:
        raise RuntimeError(line)


# ── Socrata paginated fetch ───────────────────────────────────────────────────────────
def _socrata(resource: str, params: str = "") -> list[dict]:
    """Fetch a full Socrata resource as list[dict] via $limit/$offset paging."""
    token = os.environ.get("SOCRATA_APP_TOKEN")
    out: list[dict] = []
    offset = 0
    while True:
        url = (f"https://www.data.va.gov/resource/{resource}.json"
               f"?$limit={PAGE}&$offset={offset}{('&' + params) if params else ''}")
        req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                   **({"X-App-Token": token} if token else {})})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    page = json.load(r)
                break
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                if attempt == 4:
                    raise
                wait = 2 ** attempt
                print(f"  retry {resource} offset={offset} ({exc}); sleep {wait}s", flush=True)
                time.sleep(wait)
        if not page:
            break
        out.extend(page)
        offset += PAGE
        if len(page) < PAGE:
            break
    return out


def _i(v) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _fips(v) -> str | None:
    """5-char county FIPS, or None for unmappable buckets (Unknown / Other Foreign / 0)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s.isdigit() or s == "0":
        return None
    s = s.zfill(5)
    return s if len(s) == 5 else None


def _norm_key(k: str) -> str:
    return k.strip().strip("_").lower()


def _pick(rec: dict, *needles: str):
    """First value whose normalized key contains ALL needles (per-year Socrata munging-safe)."""
    for k, v in rec.items():
        nk = _norm_key(k)
        if all(n in nk for n in needles):
            return v
    return None


def _write_lance(table, uri: str, so: dict) -> None:
    import lance
    lance.write_dataset(table, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)


# ── schemas ───────────────────────────────────────────────────────────────────────────
def _vetpop_schema():
    import pyarrow as pa
    return pa.schema([
        ("fips", pa.string()), ("county_state", pa.string()), ("state", pa.string()),
        ("snapshot_year", pa.int32()), ("age_group", pa.string()), ("sex", pa.string()),
        ("veterans", pa.int64()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def _vetpop_total_schema():
    import pyarrow as pa
    return pa.schema([
        ("fips", pa.string()), ("county_state", pa.string()), ("state", pa.string()),
        ("snapshot_year", pa.int32()), ("veterans_total", pa.int64()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def _disab_schema():
    import pyarrow as pa
    i = pa.int64()
    return pa.schema([
        ("fiscal_year", pa.int32()), ("fips", pa.string()),
        ("state", pa.string()), ("county", pa.string()), ("recipients", i),
        ("scd_0_20", i), ("scd_30_40", i), ("scd_50_60", i), ("scd_70_90", i), ("scd_100", i),
        ("age_17_44", i), ("age_45_64", i), ("age_65_plus", i), ("male", i), ("female", i),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


# ── run ───────────────────────────────────────────────────────────────────────────────
def run(*, vetpop_uri: str = VETPOP_URI, vetpop_total_uri: str = VETPOP_TOTAL_URI,
        disab_uri: str = DISAB_URI, smoke: bool = False) -> dict:
    import pyarrow as pa

    so = _storage_options()
    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at
    src = f"{SOURCE_TAG}:data.va.gov"

    # ── VetPop county (jrjd-qghv) ──
    raw = _socrata(VETPOP_RESOURCE)
    vet_rows: list[dict] = []
    for r in raw:
        yr = _i(str(r.get("date", ""))[:4])
        vet_rows.append({
            "fips": (r.get("fips") or "").zfill(5) if r.get("fips") else None,
            "county_state": r.get("county_state"), "state": r.get("state"),
            "snapshot_year": yr, "age_group": r.get("age_group"), "sex": r.get("sex"),
            "veterans": _i(r.get("veterans")) or 0,
            "source": src, "ingested_at": ingested_at,
        })
    if smoke:  # keep base year only — enough to exercise gates fast
        vet_rows = [r for r in vet_rows if r["snapshot_year"] == BASE_SNAPSHOT_YEAR]

    bad_fips = [r["fips"] for r in vet_rows if not (r["fips"] and len(r["fips"]) == 5)]
    counties = {r["fips"] for r in vet_rows}
    nat_base = sum(r["veterans"] for r in vet_rows if r["snapshot_year"] == BASE_SNAPSHOT_YEAR)
    _gate("1-vetpop-fips", not bad_fips, f"non-5char-fips={len(bad_fips)}")
    _gate("1b-vetpop-counties", 3100 <= len(counties) <= 3200,
          f"distinct_counties={len(counties)} (expect 3100-3200)")
    _gate("1c-vetpop-national", VET_NATIONAL_BAND[0] <= nat_base <= VET_NATIONAL_BAND[1],
          f"FY{BASE_SNAPSHOT_YEAR} national veterans={nat_base:,} (expect {VET_NATIONAL_BAND})")

    # rollup: fips × snapshot_year → sum(veterans)
    agg: dict[tuple, dict] = {}
    for r in vet_rows:
        key = (r["fips"], r["snapshot_year"])
        a = agg.setdefault(key, {"fips": r["fips"], "county_state": r["county_state"],
                                 "state": r["state"], "snapshot_year": r["snapshot_year"],
                                 "veterans_total": 0, "source": src, "ingested_at": ingested_at})
        a["veterans_total"] += r["veterans"]
    total_rows = list(agg.values())
    tot_base = sum(a["veterans_total"] for a in total_rows if a["snapshot_year"] == BASE_SNAPSHOT_YEAR)
    _gate("2-rollup", tot_base == nat_base,
          f"rollup FY{BASE_SNAPSHOT_YEAR} sum={tot_base:,} == detail sum={nat_base:,}")

    # ── Disability Compensation Recipients by County ──
    disab_rows: list[dict] = []
    fy_landed: dict[int, int] = {}
    for fy, rid in sorted(DISAB_RESOURCES.items()):
        recs = _socrata(rid)
        n = 0
        for r in recs:
            fips = _pick(r, "fips")
            recip = _i(_pick(r, "total", "disability", "compensation"))
            if recip is None:
                continue
            disab_rows.append({
                "fiscal_year": fy,
                "fips": _fips(fips),
                "state": _pick(r, "state"), "county": _pick(r, "county"),
                "recipients": recip,
                "scd_0_20": _i(_pick(r, "scd", "0", "20")),
                "scd_30_40": _i(_pick(r, "scd", "30", "40")),
                "scd_50_60": _i(_pick(r, "scd", "50", "60")),
                "scd_70_90": _i(_pick(r, "scd", "70", "90")),
                "scd_100": _i(_pick(r, "scd", "100")),
                "age_17_44": _i(_pick(r, "age", "17", "44")),
                "age_45_64": _i(_pick(r, "age", "45", "64")),
                "age_65_plus": _i(_pick(r, "age", "65")),
                "male": _i(_pick(r, "male")), "female": _i(_pick(r, "female")),
                "source": f"{src}:{rid}", "ingested_at": ingested_at,
            })
            n += 1
        fy_landed[fy] = n
        print(f"  disability FY{fy} ({rid}): {n} rows", flush=True)

    d_malformed = [r for r in disab_rows if r["fips"] is not None and len(r["fips"]) != 5]
    d_unmappable = sum(1 for r in disab_rows if r["fips"] is None)  # Unknown / Other Foreign — kept
    _gate("3-disab-recipients", all(r["recipients"] is not None for r in disab_rows),
          f"null-recipient rows={sum(1 for r in disab_rows if r['recipients'] is None)}")
    _gate("3b-disab-fips", len(d_malformed) == 0,
          f"malformed-fips={len(d_malformed)}; unmappable(Unknown/foreign) kept as null={d_unmappable}")

    # ── write + index + ledger ──
    status, error_text, built = "error", None, []
    cov_vet = {"detail_rows": len(vet_rows), "counties": len(counties),
               "snapshot_years": sorted({r["snapshot_year"] for r in vet_rows}),
               "national_FY%d" % BASE_SNAPSHOT_YEAR: nat_base, "total_rows": len(total_rows)}
    cov_disab = {"rows": len(disab_rows), "fiscal_years": fy_landed,
                 "unmappable_fips_rows": d_unmappable,
                 "fy2020_excluded": "degenerate schema (no fips, Unknown counties)",
                 "gdx_deferred": "data.va.gov GDX JSON resources are malformed (single collapsed column)"}
    try:
        _write_lance(pa.Table.from_pylist(vet_rows, schema=_vetpop_schema()), vetpop_uri, so)
        built += _build_indexes(vetpop_uri, btree=["fips", "state"],
                                bitmap=["age_group", "sex", "snapshot_year"], so=so)
        _write_lance(pa.Table.from_pylist(total_rows, schema=_vetpop_total_schema()), vetpop_total_uri, so)
        built += ["total:" + b for b in _build_indexes(
            vetpop_total_uri, btree=["fips"], bitmap=["snapshot_year", "state"], so=so)]
        _write_lance(pa.Table.from_pylist(disab_rows, schema=_disab_schema()), disab_uri, so)
        built += ["disab:" + b for b in _build_indexes(
            disab_uri, btree=["fips"], bitmap=["fiscal_year", "state"], so=so)]
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("va_vetpop_county", vetpop_uri, SOURCE_URL, len(vet_rows), built,
                    cov_vet, status, error_text, started_at, completed_at)
        _record_run("va_disability_comp_county", disab_uri, SOURCE_URL, len(disab_rows), built,
                    cov_disab, status, error_text, started_at, completed_at)
        print(f"VA DEMAND SUMMARY: vetpop={cov_vet} disab={cov_disab} status={status}", flush=True)
    return {"vetpop_rows": len(vet_rows), "vetpop_total_rows": len(total_rows),
            "disab_rows": len(disab_rows), "indexes": built,
            "cov_vet": cov_vet, "cov_disab": cov_disab, "status": status}


# ── CLI ───────────────────────────────────────────────────────────────────────────────
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
    p = argparse.ArgumentParser(description="VA veteran demand-side cluster ingest (Socrata → Lance).")
    p.add_argument("--smoke", action="store_true",
                   help="base-year-only vetpop; write to throwaway _smoke_ URIs and delete after.")
    a = p.parse_args()
    uris = [VETPOP_URI, VETPOP_TOTAL_URI, DISAB_URI]
    if a.smoke:
        uris = [_smoke_uri(u) for u in uris]
    try:
        r = run(vetpop_uri=uris[0], vetpop_total_uri=uris[1], disab_uri=uris[2], smoke=a.smoke)
    finally:
        if a.smoke:
            for u in uris:
                _delete_prefix(u)
    print(f"\n=== va veteran demand summary ===\n  {r}", flush=True)


if __name__ == "__main__":
    _cli()
