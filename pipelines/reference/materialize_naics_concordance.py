"""Reference loader — naics_concordance: the official Census NAICS vintage concordance chain,
normalized to edge grain. One row per (from_vintage, from_code, to_code) directed revision edge,
covering 2002->2007->2012->2017->2022. This is Layer 0 alongside dec_code_domain_ref: the mapping
every historical NAICS code resolves through to reach the current (2022) vintage.

WHY  FPDS transactions carry the NAICS vintage in force at action time, frozen forever. The L1
     spine holds 1,7xx distinct NAICS codes of which ~550 are not in the single-vintage (2022)
     naics_reference — ~15% of coded transactions. Pooled/historical NAICS×PSC analyses silently
     fragment the same activity across vintages (541712 vs 541713/14/15; 517311 vs 517111) unless
     they resolve through this concordance. naics_reference.change_indicator flags THAT a code
     changed; this dataset holds FROM->TO.

GRAIN  1 row per (from_vintage, from_code, to_code). A 1:1 revision is one row; a split is N rows
       sharing from_code; a merge is N rows sharing to_code. Titles ride along verbatim from the
       Census files (the from_title of a split row describes the specific piece that moved).
SoR    s3://data-sink/active/naics_concordance/   (Lance v2.1; reference, mode=overwrite)
SOURCE Census "Full Concordance" workbooks (census.gov/naics/concordances), one per revision:
       2002->2007, 2007->2012, 2012->2017, 2017->2022. (The reverse 2022->2017 workbook is the
       same edge set transposed and is deliberately not ingested.)

COLS   from_vintage, from_code, from_title, to_vintage, to_code, to_title, relation
       ('identical'|'one_to_one'|'split'|'merge'|'complex'), source_file, source_url, ingested_at.
KEYS   BTREE (from_code, to_code); BITMAP (from_vintage, to_vintage, relation).

    doppler run -p core-x -c prd -- uv run --no-project --with pylance --with pandas \
        --with openpyxl --with xlrd python3 \
        pipelines/reference/materialize_naics_concordance.py <smoke|build|verify> [--source-dir D]
      smoke  — download+parse all four workbooks, run every fail-closed gate, write a _sample
               copy, print the report. NO active/ write. Exit 1 on any gate failure.
      build  — same gates, then overwrite active/naics_concordance/ + build indices.
      verify — read active/ back; grain + sentinel + index sanity.
      --source-dir — read the four workbooks from a local directory instead of census.gov
               (files named exactly as in FILES below).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import tempfile
import urllib.request
from collections import Counter, defaultdict

ACTIVE = "s3://data-sink/active"
REF_URI = os.environ.get("NAICS_CONCORDANCE_URI", f"{ACTIVE}/naics_concordance/")
SAMPLE_URI = os.environ.get("NAICS_CONCORDANCE_SAMPLE_URI", f"{ACTIVE}/_sample/naics_concordance/")
DATA_STORAGE_VERSION = "2.1"
BASE_URL = "https://www.census.gov/naics/concordances"

# (from_vintage, to_vintage, filename, pandas engine)
FILES = [
    ("2002", "2007", "2002_to_2007_NAICS.xls", "xlrd"),
    ("2007", "2012", "2007_to_2012_NAICS.xls", "xlrd"),
    ("2012", "2017", "2012_to_2017_NAICS.xlsx", "openpyxl"),
    ("2017", "2022", "2017_to_2022_NAICS.xlsx", "openpyxl"),
]

BTREE_INDEXES = ["from_code", "to_code"]
BITMAP_INDEXES = ["from_vintage", "to_vintage", "relation"]

CODE_RE = re.compile(r"^\d{6}$")
PER_FILE_FLOOR = 900          # every full concordance enumerates ~1,050+ 6-digit rows
DROP_SHARE_CEILING = 0.05     # >5% unparseable rows in a workbook = shape drift, abort

# Fail-closed sentinels — known revision facts; a parse that loses any of these aborts.
SENTINELS = [
    # (from_vintage, from_code, required subset of to_codes)
    ("2002", "111110", {"111110"}),                     # stable code, every chain link
    ("2007", "111110", {"111110"}),
    ("2012", "111110", {"111110"}),
    ("2017", "111110", {"111110"}),
    ("2012", "541711", {"541713", "541714"}),            # 2017 R&D split (biotech line)
    ("2012", "541712", {"541713", "541715"}),            # 2017 R&D split (phys/eng line)
    ("2017", "517311", {"517111"}),                      # the 2022 telecom recode
    ("2017", "336111", {"336110"}),                      # 2022 auto mfg merge
]

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
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


def _paths(source_dir: str | None) -> dict[str, str]:
    if source_dir:
        return {fn: os.path.join(source_dir, fn) for _, _, fn, _ in FILES}
    d = tempfile.mkdtemp(prefix="naics_concordance_")
    out = {}
    for _, _, fn, _ in FILES:
        p = os.path.join(d, fn)
        url = f"{BASE_URL}/{fn}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as r, open(p, "wb") as f:
            f.write(r.read())
        log(f"downloaded {url} -> {os.path.getsize(p):,} bytes")
        out[fn] = p
    return out


def _code(v) -> str | None:
    """Normalize a Census cell to a 6-digit code string, else None."""
    if v is None:
        return None
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.replace(",", "")
    if not s or not s.replace(".", "").isdigit():
        return None
    s = str(int(float(s)))
    return s if CODE_RE.fullmatch(s) else None


def _parse(paths: dict[str, str]) -> tuple[list[dict], list[str]]:
    import pandas as pd

    problems: list[str] = []
    rows: list[dict] = []
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    for from_v, to_v, fn, engine in FILES:
        df = pd.read_excel(paths[fn], engine=engine, header=None)
        # header text row is index 2 in every workbook; data starts at 3; edges live in cols 0-3
        body = df.iloc[3:, :4]
        parsed = dropped = 0
        pairs: list[dict] = []
        for _, r in body.iterrows():
            fc, tc = _code(r.iloc[0]), _code(r.iloc[2])
            if fc is None and tc is None:
                continue  # blank spacer / footnote row
            if fc is None or tc is None:
                dropped += 1
                continue
            parsed += 1
            pairs.append({
                "from_vintage": from_v, "from_code": fc,
                "from_title": (str(r.iloc[1]).strip() if r.iloc[1] is not None else None),
                "to_vintage": to_v, "to_code": tc,
                "to_title": (str(r.iloc[3]).strip() if r.iloc[3] is not None else None),
                "source_file": fn, "source_url": f"{BASE_URL}/{fn}",
                "ingested_at": ingested_at,
            })
        if parsed < PER_FILE_FLOOR:
            problems.append(f"{fn}: parsed {parsed} rows < floor {PER_FILE_FLOOR}")
        if parsed and dropped / max(parsed + dropped, 1) > DROP_SHARE_CEILING:
            problems.append(f"{fn}: dropped {dropped}/{parsed + dropped} rows > {DROP_SHARE_CEILING:.0%}")
        log(f"{fn}: {parsed} edges ({dropped} dropped)")
        # dedup within file (Census repeats no edges today; gate if that drifts)
        seen = set()
        for p in pairs:
            k = (p["from_code"], p["to_code"])
            if k in seen:
                continue
            seen.add(k)
            rows.append(p)

    # relation classification per revision file
    for from_v, to_v, fn, _ in FILES:
        sub = [r for r in rows if r["source_file"] == fn]
        fan_out = Counter(r["from_code"] for r in sub)
        fan_in = Counter(r["to_code"] for r in sub)
        for r in sub:
            split = fan_out[r["from_code"]] > 1
            merge = fan_in[r["to_code"]] > 1
            if split and merge:
                r["relation"] = "complex"
            elif split:
                r["relation"] = "split"
            elif merge:
                r["relation"] = "merge"
            elif r["from_code"] == r["to_code"]:
                r["relation"] = "identical"
            else:
                r["relation"] = "one_to_one"
    return rows, problems


def _gate(rows: list[dict]) -> list[str]:
    problems: list[str] = []
    by_edge = defaultdict(set)
    for r in rows:
        by_edge[(r["from_vintage"], r["from_code"])].add(r["to_code"])
        if not CODE_RE.fullmatch(r["from_code"]) or not CODE_RE.fullmatch(r["to_code"]):
            problems.append(f"malformed code pair {r['from_code']}->{r['to_code']} in {r['source_file']}")
    for from_v, from_c, required in SENTINELS:
        got = by_edge.get((from_v, from_c), set())
        if not required.issubset(got):
            problems.append(f"sentinel fail: {from_v} {from_c} -> {sorted(got)} missing {sorted(required - got)}")
    keys = [(r["from_vintage"], r["from_code"], r["to_code"]) for r in rows]
    if len(keys) != len(set(keys)):
        problems.append("grain violation: duplicate (from_vintage, from_code, to_code)")
    return problems


def _to_table(rows: list[dict]):
    import pyarrow as pa

    schema = pa.schema([
        ("from_vintage", pa.string()), ("from_code", pa.string()), ("from_title", pa.string()),
        ("to_vintage", pa.string()), ("to_code", pa.string()), ("to_title", pa.string()),
        ("relation", pa.string()), ("source_file", pa.string()), ("source_url", pa.string()),
        ("ingested_at", pa.string()),
    ])
    cols = [f.name for f in schema]
    return pa.table({c: [r.get(c) for r in rows] for c in cols}, schema=schema)


def _write(uri: str, tbl, so: dict, index: bool):
    import lance

    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        storage_options=so)
    ds = lance.dataset(uri, storage_options=so)
    if index:
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"  BTREE  ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"  BITMAP ✓ {col}")
    return ds


def _report(rows: list[dict]) -> dict:
    by_file = Counter(r["source_file"] for r in rows)
    by_rel = Counter(r["relation"] for r in rows)
    return {
        "total_edges": len(rows),
        "edges_per_file": dict(sorted(by_file.items())),
        "relation_mix": dict(sorted(by_rel.items())),
        "sentinel_541712_2017": sorted({r["to_code"] for r in rows
                                        if r["from_vintage"] == "2012" and r["from_code"] == "541712"}),
        "sentinel_517311_2022": sorted({r["to_code"] for r in rows
                                        if r["from_vintage"] == "2017" and r["from_code"] == "517311"}),
    }


def _args() -> tuple[str, str | None]:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    src = None
    if "--source-dir" in sys.argv:
        src = sys.argv[sys.argv.index("--source-dir") + 1]
    return cmd, src


def smoke():
    _, src = _args()
    rows, problems = _parse(_paths(src))
    problems += _gate(rows)
    rep = _report(rows)
    if problems:
        print(json.dumps({"status": "GATE_FAIL", "problems": problems, "report": rep}, indent=2))
        sys.exit(1)
    _write(SAMPLE_URI, _to_table(rows), _r2_so(), index=False)
    print(json.dumps({"status": "GATES_PASS", "sample_uri": SAMPLE_URI, "report": rep}, indent=2))


def build():
    _, src = _args()
    rows, problems = _parse(_paths(src))
    problems += _gate(rows)
    if problems:
        raise RuntimeError("GATE_FAIL: " + "; ".join(problems))
    tbl = _to_table(rows)
    _write(REF_URI, tbl, _r2_so(), index=True)
    log(f"DONE → {REF_URI} rows={tbl.num_rows}")
    print(json.dumps({"status": "BUILT", "uri": REF_URI, "rows": tbl.num_rows,
                      "report": _report(rows)}, indent=2))


def verify():
    import lance

    ds = lance.dataset(REF_URI, storage_options=_r2_so())
    t = ds.scanner(columns=["from_vintage", "from_code", "to_code", "relation"]).to_table().to_pylist()
    keys = [(r["from_vintage"], r["from_code"], r["to_code"]) for r in t]
    split_2012 = sorted({r["to_code"] for r in t if r["from_vintage"] == "2012" and r["from_code"] == "541712"})
    try:
        idx = [getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    print(json.dumps({
        "uri": REF_URI, "rows": ds.count_rows(),
        "grain_unique": len(keys) == len(set(keys)) == ds.count_rows(),
        "vintage_pairs": sorted({(r["from_vintage"]) for r in t}),
        "sentinel_541712_2017": split_2012,
        "indices": idx,
    }, indent=2))


def main():
    cmd, _ = _args()
    fn = {"smoke": smoke, "build": build, "verify": verify}.get(cmd)
    if not fn:
        print(f"unknown command: {cmd} (smoke|build|verify)")
        sys.exit(2)
    fn()


if __name__ == "__main__":
    main()
