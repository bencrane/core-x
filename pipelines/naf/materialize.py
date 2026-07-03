"""Phase 3 — NAF wage-schedule MATERIALIZE (landed PDFs → Lance system of record).

Reads the Phase-1 fetch manifest, routes each landed PDF to its type parser, and writes four
snapshot Lance datasets under s3://data-sink/active/ with scalar indexes and a fail-closed
verify(). Pure local out-of-core compute — pulls PDF bytes from the R2 landing zone,
pdftotext -layout, parses, collects, writes Lance. Never re-hits DCPAS. Blast-radius separated
from Phase 1: a parse change never re-fetches; a fetch flap never corrupts the SoR.

DATASETS (schedule_type → parser → dataset)
  CT, Special  -> _parse_matrix_naflns  -> naf_wage_rates          NA/NL/NS grade×step priced matrix
  AS           -> _parse_matrix_as       -> naf_wage_rates          AS/PS grade×step priced matrix
  NF, PBS      -> _parse_payband_range    -> naf_nf_payband_ranges   NF-level min/max ranges
  PBPR         -> _parse_survey           -> naf_nf_payband_survey    survey job wage points
  RSB          -> _parse_geography        -> naf_wage_area_geography  wage-area county/installation defs

Idempotent: mode="overwrite" rebuilds each dataset as a full snapshot of the landed corpus.
Parse failures never crash the run — counted as rejected/empty and logged. (~43% of Special PDFs
are a different "Special Wage Rate Ranges" memo format the matrix parser correctly returns [] for.)

CLI
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.materialize build              # all 4
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.materialize build --dataset naf_wage_rates
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.materialize build --limit 400  # stratified smoke
  doppler run -p core-x -c prd -- python3 -m pipelines.naf.materialize verify
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

from pipelines.bls.ingest import _build_indexes, _s3_client, _storage_options
from pipelines.naf.census import _record_run
from pipelines.naf import (
    _parse_geography,
    _parse_matrix_as,
    _parse_matrix_naflns,
    _parse_payband_range,
    _parse_survey,
)

BUCKET = "data-sink"
ACTIVE = os.environ.get("NAF_ACTIVE_PREFIX", f"s3://{BUCKET}/active")
LAND_MANIFEST_LOCAL = os.environ.get("NAF_FETCH_MANIFEST", "exports/naf_landing/fetch_manifest.parquet")
LAND_MANIFEST_R2 = "landing/naf/fetch_manifest.parquet"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3

ROUTER = {
    "CT":      (_parse_matrix_naflns.parse, "naf_wage_rates"),
    "Special": (_parse_matrix_naflns.parse, "naf_wage_rates"),
    "AS":      (_parse_matrix_as.parse,     "naf_wage_rates"),
    "NF":      (_parse_payband_range.parse, "naf_nf_payband_ranges"),
    "PBS":     (_parse_payband_range.parse, "naf_nf_payband_ranges"),
    "PBPR":    (_parse_survey.parse,        "naf_nf_payband_survey"),
    "RSB":     (_parse_geography.parse,     "naf_wage_area_geography"),
}


def _pa_schema():
    import pyarrow as pa
    S = pa.string
    return {
        "naf_wage_rates": pa.schema([
            ("wage_area", S()), ("naf_area", S()), ("schedule_number", pa.int32()), ("series", S()),
            ("grade", pa.int16()), ("step", pa.int8()), ("hourly_rate", pa.float64()),
            ("rate_type", S()), ("schedule_family", S()), ("footnote", pa.bool_()),
            ("subject", S()), ("effective_date", S()), ("issue_date", S()),
            ("source_pdf_filename", S()), ("ingest_ts", pa.timestamp("us")),
        ]),
        "naf_nf_payband_ranges": pa.schema([
            ("wage_area", S()), ("naf_area", S()), ("schedule_number", pa.int32()), ("nf_level", pa.int8()),
            ("min_annual", pa.float64()), ("min_hourly", pa.float64()),
            ("max_annual", pa.float64()), ("max_hourly", pa.float64()),
            ("effective_date", S()), ("source_pdf_filename", S()), ("ingest_ts", pa.timestamp("us")),
        ]),
        "naf_nf_payband_survey": pa.schema([
            ("wage_area", S()), ("naf_area", S()), ("schedule_number", pa.int32()), ("nf_level", pa.int8()),
            ("survey_job_title", S()), ("matches", pa.int32()), ("average", pa.float64()),
            ("high", pa.float64()), ("low", pa.float64()),
            ("effective_date", S()), ("issue_date", S()),
            ("source_pdf_filename", S()), ("ingest_ts", pa.timestamp("us")),
        ]),
        "naf_wage_area_geography": pa.schema([
            ("wage_area", S()), ("naf_area", S()), ("schedule_number", pa.int32()), ("row_kind", S()),
            ("agency", S()), ("installation", S()), ("county", S()), ("state", S()),
            ("applicable_schedule", S()), ("source_pdf_filename", S()), ("ingest_ts", pa.timestamp("us")),
        ]),
    }


INDEXES = {
    "naf_wage_rates": (["wage_area", "naf_area", "schedule_number"], ["series", "grade", "step", "rate_type", "schedule_family"]),
    "naf_nf_payband_ranges": (["wage_area", "naf_area", "schedule_number"], ["nf_level"]),
    "naf_nf_payband_survey": (["wage_area", "naf_area", "schedule_number"], ["nf_level"]),
    "naf_wage_area_geography": (["wage_area", "naf_area", "state"], ["row_kind"]),
}

# Grain gate keys — INCLUDE source_pdf_filename so the gate catches within-PDF double-emits
# without false-failing on legitimate cross-PDF schedule-number reuse across the 30K-PDF corpus.
# naf_area distinguishes area codes that legitimately SHARE a physical PDF (e.g. area 034 and
# its remainder area 034R both reference 034-008-CT.pdf) — without it those become false dupes.
GRAIN = {
    "naf_wage_rates": ("naf_area", "source_pdf_filename", "series", "grade", "step"),
    "naf_nf_payband_ranges": ("naf_area", "source_pdf_filename", "nf_level"),
    "naf_nf_payband_survey": ("naf_area", "source_pdf_filename", "nf_level", "survey_job_title", "average"),
    "naf_wage_area_geography": ("naf_area", "source_pdf_filename", "row_kind", "agency", "installation", "county", "state"),
}
STRICT_GRAIN = {  # hard gate on the priced matrices; warn-only on reference sidecars (legit repeats)
    "naf_wage_rates": True, "naf_nf_payband_ranges": True,
    "naf_nf_payband_survey": False, "naf_wage_area_geography": False,
}

# Hand-verified anchors from the parser-build workflow. filter dict → expected dict; numeric-aware.
# If the anchor's PDF is absent from a run (e.g. --limit smoke), that anchor is skipped.
ANCHORS = {
    "naf_wage_rates": [
        ({"source_pdf_filename": "101-045-CT.pdf", "series": "NA", "grade": 1, "step": 1}, {"hourly_rate": 16.23}),
        ({"source_pdf_filename": "101-045-CT.pdf", "series": "NL", "grade": 1, "step": 1}, {"hourly_rate": 17.85}),
        ({"source_pdf_filename": "101-045-CT.pdf", "series": "NS", "grade": 1, "step": 1}, {"hourly_rate": 20.62}),
        ({"source_pdf_filename": "101-045-CT.pdf", "series": "NS", "grade": 19, "step": 5}, {"hourly_rate": 45.00}),
        ({"source_pdf_filename": "101-045-CT.pdf", "series": "NA", "grade": 15, "step": 5}, {"hourly_rate": 32.15}),
        ({"source_pdf_filename": "004-011-AS.pdf", "series": "AS", "grade": 1, "step": 1}, {"hourly_rate": 5.23}),
        ({"source_pdf_filename": "004-011-AS.pdf", "series": "PS", "grade": 1, "step": 1}, {"hourly_rate": 4.75}),
        ({"source_pdf_filename": "004-011-AS.pdf", "series": "AS", "grade": 7, "step": 5}, {"hourly_rate": 8.56}),
    ],
    "naf_nf_payband_ranges": [
        ({"source_pdf_filename": "004-011-15-NF.pdf", "nf_level": 1},
         {"min_annual": 9920, "min_hourly": 4.75, "max_annual": 15990, "max_hourly": 7.66}),
        ({"source_pdf_filename": "070-034-60-NF.pdf", "nf_level": 1},
         {"min_annual": 15130, "min_hourly": 7.25, "max_annual": 30090, "max_hourly": 14.42}),
    ],
    "naf_nf_payband_survey": [
        ({"source_pdf_filename": "004-012-16-PR.pdf", "nf_level": 1, "survey_job_title": "SALES ASSOCIATE"},
         {"matches": 78, "average": 5.62, "high": 7.50, "low": 4.75}),
    ],
    "naf_wage_area_geography": [
        ({"source_pdf_filename": "004-030-ScheduleBack.pdf", "row_kind": "county", "county": "Lowndes County", "state": "MS"},
         {"applicable_schedule": "004"}),
    ],
}


# ── parse fan-out ──────────────────────────────────────────────────────────────────────
_tls = threading.local()


def _s3():
    if not hasattr(_tls, "s3"):
        _tls.s3 = _s3_client()
    return _tls.s3


def _parse_one(rec: dict):
    st = rec.get("schedule_type")
    route = ROUTER.get(st)
    if not route:
        return (None, [], "no_parser")
    parse, dataset = route
    try:
        body = _s3().get_object(Bucket=BUCKET, Key=rec["r2_key"])["Body"].read()
        with tempfile.NamedTemporaryFile(suffix=".pdf") as tf:
            tf.write(body)
            tf.flush()
            txt = subprocess.run(["pdftotext", "-layout", tf.name, "-"],
                                 capture_output=True, timeout=120).stdout.decode("utf-8", "replace")
        meta = {k: rec.get(k) for k in ("naf_area", "wage_area", "version", "filename", "schedule_type")}
        rows = parse(txt, meta) or []
        wa, na = rec.get("wage_area"), rec.get("naf_area")
        for r in rows:
            r.setdefault("source_pdf_filename", rec.get("filename"))
            r["naf_area"] = na                 # base area dir (variant re-issues share one base)
            if wa:                             # census cascade wage_area beats PDF-header scraping
                r["wage_area"] = wa
        return (dataset, rows, "ok" if rows else "empty")
    except Exception as exc:  # noqa: BLE001 — one bad PDF must not kill the run
        return (dataset, [], f"err:{type(exc).__name__}")


def _load_manifest() -> list[dict]:
    import pyarrow.parquet as pq
    path = LAND_MANIFEST_LOCAL
    if not os.path.exists(path):
        path = os.path.join(tempfile.gettempdir(), "naf_fetch_manifest.parquet")
        _s3_client().download_file(BUCKET, LAND_MANIFEST_R2, path)
    recs = pq.read_table(path).to_pylist()
    return [r for r in recs if r.get("status") in ("landed", "already_landed")]


# ── Lance write + verify ────────────────────────────────────────────────────────────────
def _uri(dataset: str) -> str:
    return f"{ACTIVE}/{dataset}/"


def _coerce(rows: list[dict], schema) -> list[dict]:
    now = dt.datetime.now(dt.timezone.utc)
    names = [f.name for f in schema]
    out = []
    for r in rows:
        row = {n: r.get(n) for n in names}
        row["ingest_ts"] = now
        out.append(row)
    return out


def _write_dataset(dataset: str, rows: list[dict], so: dict) -> list[str]:
    import lance
    import pyarrow as pa
    schema = _pa_schema()[dataset]
    table = pa.Table.from_pylist(_coerce(rows, schema), schema=schema)
    lance.write_dataset(table, _uri(dataset), mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)
    btree, bitmap = INDEXES[dataset]
    return _build_indexes(_uri(dataset), btree, bitmap, so)


def _match(got, val) -> bool:
    try:
        return abs(float(got) - float(val)) < 1e-6
    except (TypeError, ValueError):
        return str(got) == str(val)


def _verify(dataset: str, rows: list[dict]) -> None:
    """Fail-closed: grain gate (within-PDF uniqueness) + numeric anchor spot-checks."""
    grain = GRAIN[dataset]
    keys = [tuple(str(r.get(g)) for g in grain) for r in rows]
    dupes = len(rows) - len(set(keys))
    if dupes:
        msg = f"[verify {dataset}] grain {grain}: {dupes} duplicate rows"
        if STRICT_GRAIN[dataset]:
            from collections import Counter
            ex = [k for k, n in Counter(keys).items() if n > 1][:5]
            raise RuntimeError(msg + f" — examples: {ex}")
        print("  WARN " + msg)

    present = {r.get("source_pdf_filename") for r in rows}
    checked = skipped = warned = 0
    for filt, exp in ANCHORS.get(dataset, []):
        pdf = filt.get("source_pdf_filename")
        if pdf and pdf not in present:
            skipped += 1
            continue
        hits = [r for r in rows if all(_match(r.get(k), v) for k, v in filt.items())]
        problem = ("PDF present but NO matching row" if not hits else
                   next((f"col {c}: got {hits[0].get(c)!r} expected {v!r}"
                         for c, v in exp.items() if not _match(hits[0].get(c), v)), None))
        if problem:
            m = f"[verify {dataset}] anchor {filt}: {problem}"
            if STRICT_GRAIN[dataset]:
                raise RuntimeError(m)
            print("  WARN " + m)
            warned += 1
            continue
        checked += 1
    print(f"  [verify {dataset}] OK — {len(rows):,} rows, grain dupes={dupes}, anchors ok={checked} warn={warned} skipped={skipped}")


def _dedup(dataset: str, rows: list[dict]) -> list[dict]:
    """Collapse exact-grain duplicate rows (no-op for strict datasets that already passed verify)."""
    grain = GRAIN[dataset]
    seen, out = set(), []
    for r in rows:
        k = tuple(str(r.get(g)) for g in grain)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    if len(out) != len(rows):
        print(f"  [dedup {dataset}] collapsed {len(rows) - len(out)} exact-grain duplicate rows")
    return out


def _dedup_identical(rows: list[dict]) -> list[dict]:
    """Collapse rows identical across ALL columns — 2-page repeated tables, cross-listed shares.
    Conflicting rows (same grain, DIFFERENT values) are preserved so the grain gate still catches
    a genuine parse bug rather than silently dropping ambiguous data."""
    seen, out = set(), []
    for r in rows:
        k = tuple(sorted((c, str(v)) for c, v in r.items()))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


# ── build command ───────────────────────────────────────────────────────────────────────
def cmd_build(args) -> int:
    so = _storage_options()
    recs = _load_manifest()
    targets = set(args.dataset.split(",")) if args.dataset else None
    if targets:
        recs = [r for r in recs if ROUTER.get(r.get("schedule_type"), (None, None))[1] in targets]
    if args.limit:
        by_st: dict[str, list[dict]] = {}
        for r in recs:
            by_st.setdefault(r.get("schedule_type"), []).append(r)
        per = max(1, args.limit // max(1, len(by_st)))
        recs = [r for rs in by_st.values() for r in rs[:per]]
    print(f"[build] PDFs to parse: {len(recs):,}  workers={args.workers}  targets={targets or 'all'}  limit={args.limit or 0}")

    buckets: dict[str, list[dict]] = {d: [] for d in _pa_schema()}
    stats = {"ok": 0, "empty": 0, "no_parser": 0}
    errs: dict[str, int] = {}
    started = dt.datetime.now(dt.timezone.utc)
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for dataset, rows, status in ex.map(_parse_one, recs):
            done += 1
            if status.startswith("err:"):
                errs[status] = errs.get(status, 0) + 1
            else:
                stats[status] = stats.get(status, 0) + 1
            if dataset and rows:
                buckets[dataset].extend(rows)
            if done % 2500 == 0:
                print(f"  … parsed {done}/{len(recs)}  " + ", ".join(f"{d}={len(v):,}" for d, v in buckets.items() if v))
    print(f"[parse] {stats}  errors={errs}")

    # Collapse exact-identical duplicate rows (2-page repeated tables, cross-listed shares) before
    # the grain gate — so it trips only on CONFLICTING dupes (same grain, different values).
    for dataset in buckets:
        before = len(buckets[dataset])
        buckets[dataset] = _dedup_identical(buckets[dataset])
        if len(buckets[dataset]) != before:
            print(f"[dedup-identical {dataset}] collapsed {before - len(buckets[dataset]):,} exact-duplicate rows")

    # Fail-closed: verify ALL target datasets before writing ANY, so a verify failure can
    # never leave active/ in a partial state.
    to_write = []
    failures = []
    for dataset, rows in buckets.items():
        if targets and dataset not in targets:
            continue
        if not rows:
            print(f"[skip] {dataset}: 0 rows")
            continue
        print(f"[{dataset}] parsed {len(rows):,} rows — verifying")
        try:
            _verify(dataset, rows)
            to_write.append((dataset, rows))
        except RuntimeError as exc:
            print(f"  FAILED: {exc}")
            failures.append(str(exc))
    if failures:
        print(f"\n[ABORT] {len(failures)} dataset(s) failed fail-closed verify — active/ untouched:")
        for f in failures:
            print("  - " + f)
        return 1

    results = []
    for dataset, rows in to_write:
        rows = _dedup(dataset, rows)
        built = _write_dataset(dataset, rows, so)
        print(f"[{dataset}] wrote {_uri(dataset)}  indexes={built}")
        _record_run(dataset, _uri(dataset), LAND_MANIFEST_R2, None, len(rows), 0, built,
                    "success", None, started, dt.datetime.now(dt.timezone.utc))
        results.append((dataset, len(rows), len(built)))

    print("\n=== NAF materialize summary ===")
    for d, n, nidx in results:
        print(f"  {d:26s} rows={n:>9,}  idx={nidx}")
    return 0


def cmd_verify(args) -> int:
    import lance
    so = _storage_options()
    for dataset in _pa_schema():
        try:
            ds = lance.dataset(_uri(dataset), storage_options=so)
            print(f"  {dataset:26s} rows={ds.count_rows():>9,}")
        except Exception as exc:  # noqa: BLE001
            print(f"  {dataset:26s} MISSING/err: {exc}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NAF wage-schedule materialize (Phase 3).")
    ap.add_argument("cmd", choices=["build", "verify"])
    ap.add_argument("--dataset", default="", help="comma list of target datasets (default: all)")
    ap.add_argument("--limit", type=int, default=0, help="stratified cap across datasets (smoke)")
    ap.add_argument("--workers", type=int, default=10, help="parse threads")
    args = ap.parse_args(argv)
    if args.cmd == "build":
        return cmd_build(args)
    return cmd_verify(args)


if __name__ == "__main__":
    sys.exit(main())
