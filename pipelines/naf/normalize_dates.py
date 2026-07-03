"""Phase 5 — NAF date normalization: raw date strings → DATE32 point-in-time columns.

The NAF rate/payband datasets carry effective_date / issue_date as verbatim PDF strings in mixed
formats ('16 August 2025', '01/01/2003', '01/Jan/2001', 'November 5 2007'), some wrapped in DoD
boilerplate ('The first day of the first ... pay period beginning on or after <date>.') or trailed
by signature-block / footnote noise ('... Wage and Salary Branch', '<date>**'). Lexical sort of
these strings gives WRONG recency ('16 August 2025' < '5 August 2023'), which breaks every
point-in-time / latest-schedule query. This step adds true DATE32 parsed columns.

NON-DESTRUCTIVE: the original string columns and every other column are preserved byte-for-byte
(their Arrow arrays are carried through untouched); the parsed DATE32 columns are appended. Then the
dataset is overwritten (Lance overwrite) and its scalar indexes are rebuilt, with a new
BTREE(effective_date_parsed) added for point-in-time pushdown. Fail-closed: parse coverage on each
non-empty date column must clear the gate before any write.

  naf_wage_rates          + effective_date_parsed, issue_date_parsed
  naf_nf_payband_ranges   + effective_date_parsed
  naf_nf_payband_survey   + effective_date_parsed, issue_date_parsed

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with boto3 \
      python3 -m pipelines.naf.normalize_dates build
"""
from __future__ import annotations

import datetime as dt
import re
import sys

from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"
DATA_STORAGE_VERSION = "2.1"
MIN_PARSE_FRAC = 0.995  # of non-empty values per date column

# Datasets → (date columns present, btree, bitmap). effective_date_parsed added to BTREE for pushdown.
TARGETS = {
    "naf_wage_rates": {
        "date_cols": ["effective_date", "issue_date"],
        "btree": ["wage_area", "naf_area", "schedule_number", "effective_date_parsed"],
        "bitmap": ["series", "grade", "step", "rate_type", "schedule_family"],
    },
    "naf_nf_payband_ranges": {
        "date_cols": ["effective_date"],
        "btree": ["wage_area", "naf_area", "schedule_number", "effective_date_parsed"],
        "bitmap": ["nf_level"],
    },
    "naf_nf_payband_survey": {
        "date_cols": ["effective_date", "issue_date"],
        "btree": ["wage_area", "naf_area", "schedule_number", "effective_date_parsed"],
        "bitmap": ["nf_level"],
    },
}

_MONTHS: dict[str, int] = {}
for _i, _m in enumerate(("January February March April May June July August September October "
                         "November December").split(), 1):
    _MONTHS[_m.lower()] = _i
    _MONTHS[_m[:3].lower()] = _i
_MONTHS["sept"] = 9  # common variant

_BOILER = re.compile(r"(?i)\bbeginning on or after\b\s*")
_SIG = re.compile(r"(?i)\s*(wage\s+and\s+salary|civilian personnel|effective date|order date|"
                  r"issue date|division|branch|chief)\b.*$")
_DMY_NAME = re.compile(r"(\d{1,2})[ .,/\-]+([A-Za-z]+)[ .,/\-]+(\d{4})")   # 16 August 2025 / 01/Jan/2001
_MDY_NAME = re.compile(r"([A-Za-z]+)[ .,]+(\d{1,2})[ .,]+(\d{4})")          # November 5, 2007
_DMY_NUM = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")                        # 01/01/2003 (DD/MM/YYYY)


def parse_naf_date(s):
    """Robustly parse a NAF date string → datetime.date, or None. Handles boilerplate prefix,
    signature/footnote suffixes, and DD-Month-YYYY / Month-DD-YYYY / DD/MM/YYYY / DD/Mon/YYYY."""
    if not s:
        return None
    s = str(s).strip()
    m = _BOILER.search(s)
    if m:
        s = s[m.end():]
    s = _SIG.sub("", s).strip().strip("*").strip(" .,\t")

    def _mk(y, mo, d):
        try:
            return dt.date(int(y), int(mo), int(d))
        except (ValueError, TypeError):
            return None

    m = _DMY_NAME.search(s)
    if m and m.group(2).lower() in _MONTHS:
        return _mk(m.group(3), _MONTHS[m.group(2).lower()], m.group(1))
    m = _MDY_NAME.search(s)
    if m and m.group(1).lower() in _MONTHS:
        return _mk(m.group(3), _MONTHS[m.group(1).lower()], m.group(2))
    m = _DMY_NUM.search(s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _mk(y, mo, d) or _mk(y, d, mo)  # DD/MM then fall back MM/DD
    return None


def normalize_dataset(name: str, spec: dict) -> dict:
    import lance
    import pyarrow as pa

    so = _storage_options()
    uri = f"{ACTIVE}/{name}/"
    ds = lance.dataset(uri, storage_options=so)
    tbl = ds.to_table()
    n = tbl.num_rows

    # Idempotent: drop any pre-existing parsed columns before re-appending.
    drop = [c for c in tbl.column_names if c.endswith("_parsed")]
    if drop:
        tbl = tbl.drop(drop)

    cov = {}
    for col in spec["date_cols"]:
        if col not in tbl.column_names:
            continue
        vals = tbl.column(col).to_pylist()
        parsed = [parse_naf_date(v) for v in vals]
        nonempty = sum(1 for v in vals if v is not None and str(v).strip() != "")
        got = sum(1 for p in parsed for _ in (1,) if p is not None)
        frac = (got / nonempty) if nonempty else 1.0
        cov[col] = {"nonempty": nonempty, "parsed": got, "frac": round(frac, 5)}
        if frac < MIN_PARSE_FRAC:
            # surface a few residuals for diagnosis before failing closed
            resid = sorted({str(v)[:80] for v, p in zip(vals, parsed) if v and str(v).strip() and p is None})[:8]
            raise RuntimeError(f"GATE FAIL {name}.{col}: parsed {got}/{nonempty} ({frac:.3%}) < "
                               f"{MIN_PARSE_FRAC:.1%}. residual e.g. {resid}")
        tbl = tbl.append_column(f"{col}_parsed", pa.array(parsed, type=pa.date32()))

    if tbl.num_rows != n:
        raise RuntimeError(f"GATE FAIL {name}: row count changed {n} -> {tbl.num_rows}")

    lance.write_dataset(tbl, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    built = _build_indexes(uri, spec["btree"], spec["bitmap"], so)
    written = lance.dataset(uri, storage_options=so).count_rows()
    if written != n:
        raise RuntimeError(f"POST-WRITE FAIL {name}: {written} != {n}")

    print(f"[{name}] {written:,} rows; parsed { {c: v['frac'] for c, v in cov.items()} }; indexes={built}")
    return {"dataset": name, "rows": written, "coverage": cov, "indexes": built}


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="NAF date normalization → DATE32 (Phase 5).")
    ap.add_argument("cmd", nargs="?", default="build", choices=["build"])
    ap.parse_args(argv)
    print("=== NAF date normalization ===")
    results = [normalize_dataset(name, spec) for name, spec in TARGETS.items()]
    print("\n=== summary ===")
    for r in results:
        print(f"  {r['dataset']:24s} rows={r['rows']:>9,}  "
              + " ".join(f"{c}={v['frac']:.4f}" for c, v in r["coverage"].items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
