"""Reference loader — psc_successor_map: the official GSA old->new PSC mapping, extracted from
Appendix 7 ("PSC Crosswalk from Previous Version of Manual") of the October 2020 PSC Manual — the
only machine-recoverable successor mapping GSA publishes. One row per (old_psc, new_psc) directed
edge. Layer 0 alongside dec_code_domain_ref / naics_concordance: retired PSC codes in the FPDS
spine resolve through this map; WHEN a code lived/died stays in psc_reference (start/end dates) —
this dataset holds only FROM->TO.

WHY  The 2020-10-30 IT overhaul end-dated 68 service + 30 product IT PSCs (D3xx et al -> DA/DB/
     DC/DD/7xxx) and recoded all 721 R&D PSCs; legacy vehicles keep reporting old codes for years
     (both series live simultaneously in the spine). psc_reference knows codes' lifespans but has
     no successor pointer; without this map, pooled NAICS×PSC analyses fragment the same activity
     across the recode boundary.

GRAIN  1 row per (old_psc, new_psc) edge. new_psc is NULL where GSA ended a code with no
       successor ('----' in the manual). Splits are N rows sharing old_psc; merges N rows sharing
       new_psc; old==new rows are revisions/renames carried by the manual's cumulative table.
SoR    s3://data-sink/active/psc_successor_map/   (Lance v2.1; reference, mode=overwrite)
SOURCE October 2020 PSC Manual PDF (acquisition.gov), Appendix 7. Two physical tables:
       pages ~312-313 "(NewPSC, Old PSC)" — the Oct-2020 release edges (segment 'release_oct2020');
       pages ~314+   "(Previous, Current)" — the cumulative historical crosswalk (segment
       'historical_cumulative'). Column order flips between segments; the parser normalizes both
       to old->new. Where the same edge appears in both segments the release row wins.

COLS   old_psc, new_psc, rationale, mapping_segment, is_terminal, is_self, is_split, is_merge,
       page_number, source, source_url, manual_effective_date, ingested_at.
KEYS   BTREE (old_psc, new_psc); BITMAP (mapping_segment, is_terminal, is_split, is_merge).

    doppler run -p core-x -c prd -- uv run --no-project --with pylance --with pdfplumber python3 \
        pipelines/reference/materialize_psc_successor_map.py <smoke|build|verify> [--source F.pdf]
      smoke  — download+parse the manual, run every fail-closed gate, write a _sample copy,
               print the report. NO active/ write. Exit 1 on any gate failure.
      build  — same gates, then overwrite active/psc_successor_map/ + build indices.
      verify — read active/ back; grain + sentinel + index sanity.
      --source — parse a local copy of the manual PDF instead of downloading.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import tempfile
import urllib.request
from collections import Counter

ACTIVE = "s3://data-sink/active"
REF_URI = os.environ.get("PSC_SUCCESSOR_MAP_URI", f"{ACTIVE}/psc_successor_map/")
SAMPLE_URI = os.environ.get("PSC_SUCCESSOR_MAP_SAMPLE_URI", f"{ACTIVE}/_sample/psc_successor_map/")
PSC_REFERENCE_URI = os.environ.get("PSC_REFERENCE_URI", f"{ACTIVE}/psc_reference/")
DATA_STORAGE_VERSION = "2.1"
SOURCE_URL = "https://www.acquisition.gov/sites/default/files/manual/October%202020%20PSC%20Manual.pdf"
MANUAL_EFFECTIVE_DATE = "2020-10-30"

BTREE_INDEXES = ["old_psc", "new_psc"]
BITMAP_INDEXES = ["mapping_segment", "is_terminal", "is_split", "is_merge"]

PSC_RE = re.compile(r"^[A-Z0-9]{4}$")
TERMINAL_TOKENS = {"----", "---", "--", "N/A", "NA", "NONE"}
DISTINCT_OLD_FLOOR = 2500     # cumulative crosswalk carries ~2,870 distinct old codes
RD_OLD_EXPECTED = 721         # the documented count of end-dated R&D PSCs
REF_MEMBERSHIP_FLOOR = 0.99   # codes on either side must exist in psc_reference

# Fail-closed sentinels — documented 2020 IT recode facts; losing any aborts the build.
SENTINELS = [
    ("D301", "DC01"),
    ("D302", "DA01"),
    ("D303", "DH01"),
    ("D305", "DB10"),
    ("D306", "DD01"),
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


def _pdf_path(source: str | None) -> str:
    if source:
        return source
    p = os.path.join(tempfile.mkdtemp(prefix="psc_manual_"), "psc_manual_oct2020.pdf")
    req = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=300) as r, open(p, "wb") as f:
        f.write(r.read())
    log(f"downloaded {SOURCE_URL} -> {os.path.getsize(p):,} bytes")
    return p


def _clean(v) -> str:
    return re.sub(r"\s+", " ", (v or "").strip())


def _parse(pdf_path: str) -> tuple[list[dict], list[str]]:
    import pdfplumber

    problems: list[str] = []
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    pdf = pdfplumber.open(pdf_path)

    # locate the Appendix 7 body (heading page, past the TOC)
    start = None
    for i, page in enumerate(pdf.pages):
        if i < 50:
            continue
        text = page.extract_text() or ""
        if re.search(r"Appendix\s*7\s*[–-]\s*PSC Crosswalk", text, re.I):
            start = i
            break
    if start is None:
        return [], ["Appendix 7 heading not found in manual PDF"]
    log(f"Appendix 7 starts on page {start + 1}")

    edges: dict[tuple[str, str | None], dict] = {}
    direction: str | None = None  # 'new_first' | 'old_first'
    malformed = 0
    for i in range(start, len(pdf.pages)):
        tbl = pdf.pages[i].extract_table()
        if not tbl:
            break
        for row in tbl:
            if not row or len(row) < 2:
                continue
            c0, c1 = _clean(row[0]), _clean(row[1])
            rationale = _clean(row[2]) if len(row) > 2 else ""
            h0 = c0.lower().replace("\n", "")
            if h0.startswith("newpsc"):
                direction = "new_first"
                continue
            if h0.startswith("previous"):
                direction = "old_first"
                continue
            if direction is None:
                continue
            new_c, old_c = (c0, c1) if direction == "new_first" else (c1, c0)
            old_c, new_c = old_c.upper(), new_c.upper()
            if old_c in TERMINAL_TOKENS:  # never observed; guard anyway
                malformed += 1
                continue
            terminal = new_c in TERMINAL_TOKENS
            if not PSC_RE.fullmatch(old_c) or (not terminal and not PSC_RE.fullmatch(new_c)):
                malformed += 1
                continue
            key = (old_c, None if terminal else new_c)
            seg = "release_oct2020" if direction == "new_first" else "historical_cumulative"
            if key in edges:
                if edges[key]["mapping_segment"] == "historical_cumulative" and seg == "release_oct2020":
                    edges[key]["mapping_segment"] = seg
                    edges[key]["rationale"] = rationale or edges[key]["rationale"]
                continue
            edges[key] = {
                "old_psc": old_c, "new_psc": None if terminal else new_c,
                "rationale": rationale or None, "mapping_segment": seg,
                "is_terminal": terminal, "is_self": (not terminal and old_c == new_c),
                "page_number": i + 1, "source": "October 2020 PSC Manual, Appendix 7",
                "source_url": SOURCE_URL, "manual_effective_date": MANUAL_EFFECTIVE_DATE,
                "ingested_at": ingested_at,
            }
    rows = list(edges.values())
    fan_out = Counter(r["old_psc"] for r in rows)
    fan_in = Counter(r["new_psc"] for r in rows if r["new_psc"])
    for r in rows:
        r["is_split"] = fan_out[r["old_psc"]] > 1
        r["is_merge"] = bool(r["new_psc"]) and fan_in[r["new_psc"]] > 1
    if malformed > len(rows) * 0.05:
        problems.append(f"malformed rows {malformed} > 5% of {len(rows)} parsed edges")
    log(f"parsed {len(rows)} distinct edges ({malformed} malformed cells skipped)")
    return rows, problems


def _ref_codes(so: dict) -> set[str] | None:
    import lance

    try:
        t = lance.dataset(PSC_REFERENCE_URI, storage_options=so).scanner(
            columns=["psc_code"]).to_table()
        return {str(v) for v in t.column("psc_code").to_pylist() if v}
    except Exception as e:  # noqa: BLE001
        log(f"psc_reference not readable ({e}); membership gate degraded to WARN")
        return None


def _gate(rows: list[dict], ref_codes: set[str] | None) -> list[str]:
    problems: list[str] = []
    old_codes = {r["old_psc"] for r in rows}
    if len(old_codes) < DISTINCT_OLD_FLOOR:
        problems.append(f"distinct old codes {len(old_codes)} < floor {DISTINCT_OLD_FLOOR}")
    rd_self = {r["old_psc"] for r in rows if r["old_psc"].startswith("A") and r["is_self"]}
    if len(rd_self) != RD_OLD_EXPECTED:
        problems.append(f"R&D self-mapped old-code count {len(rd_self)} != documented {RD_OLD_EXPECTED}")
    rd_all = {c for c in old_codes if c.startswith("A")}
    if len(rd_all) < RD_OLD_EXPECTED:
        problems.append(f"R&D old-code count {len(rd_all)} < documented {RD_OLD_EXPECTED}")
    edge_set = {(r["old_psc"], r["new_psc"]) for r in rows}
    for old_c, new_c in SENTINELS:
        if (old_c, new_c) not in edge_set:
            problems.append(f"sentinel fail: {old_c}->{new_c} missing")
    keys = [(r["old_psc"], r["new_psc"]) for r in rows]
    if len(keys) != len(set(keys)):
        problems.append("grain violation: duplicate (old_psc, new_psc)")
    if ref_codes is not None:
        for side, vals in [("old", [r["old_psc"] for r in rows]),
                           ("new", [r["new_psc"] for r in rows if r["new_psc"]])]:
            hit = sum(1 for v in vals if v in ref_codes)
            share = hit / max(len(vals), 1)
            if share < REF_MEMBERSHIP_FLOOR:
                problems.append(f"{side}_psc membership in psc_reference {share:.3%} < {REF_MEMBERSHIP_FLOOR:.0%}")
    return problems


def _to_table(rows: list[dict]):
    import pyarrow as pa

    schema = pa.schema([
        ("old_psc", pa.string()), ("new_psc", pa.string()), ("rationale", pa.string()),
        ("mapping_segment", pa.string()), ("is_terminal", pa.bool_()), ("is_self", pa.bool_()),
        ("is_split", pa.bool_()), ("is_merge", pa.bool_()), ("page_number", pa.int64()),
        ("source", pa.string()), ("source_url", pa.string()),
        ("manual_effective_date", pa.string()), ("ingested_at", pa.string()),
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
    by_seg = Counter(r["mapping_segment"] for r in rows)
    return {
        "total_edges": len(rows),
        "distinct_old": len({r["old_psc"] for r in rows}),
        "distinct_new": len({r["new_psc"] for r in rows if r["new_psc"]}),
        "segments": dict(sorted(by_seg.items())),
        "terminal_edges": sum(1 for r in rows if r["is_terminal"]),
        "self_edges": sum(1 for r in rows if r["is_self"]),
        "splits": sum(1 for r in rows if r["is_split"]),
        "rd_old_codes": len({r["old_psc"] for r in rows if r["old_psc"].startswith("A")}),
        "sentinels": {f"{o}->{n}": (o, n) in {(r["old_psc"], r["new_psc"]) for r in rows}
                      for o, n in SENTINELS},
    }


def _args() -> tuple[str, str | None]:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    src = None
    if "--source" in sys.argv:
        src = sys.argv[sys.argv.index("--source") + 1]
    return cmd, src


def smoke():
    _, src = _args()
    so = _r2_so()
    rows, problems = _parse(_pdf_path(src))
    problems += _gate(rows, _ref_codes(so))
    rep = _report(rows)
    if problems:
        print(json.dumps({"status": "GATE_FAIL", "problems": problems, "report": rep}, indent=2))
        sys.exit(1)
    _write(SAMPLE_URI, _to_table(rows), so, index=False)
    print(json.dumps({"status": "GATES_PASS", "sample_uri": SAMPLE_URI, "report": rep}, indent=2))


def build():
    _, src = _args()
    so = _r2_so()
    rows, problems = _parse(_pdf_path(src))
    problems += _gate(rows, _ref_codes(so))
    if problems:
        raise RuntimeError("GATE_FAIL: " + "; ".join(problems))
    tbl = _to_table(rows)
    _write(REF_URI, tbl, so, index=True)
    log(f"DONE → {REF_URI} rows={tbl.num_rows}")
    print(json.dumps({"status": "BUILT", "uri": REF_URI, "rows": tbl.num_rows,
                      "report": _report(rows)}, indent=2))


def verify():
    import lance

    ds = lance.dataset(REF_URI, storage_options=_r2_so())
    t = ds.scanner(columns=["old_psc", "new_psc", "mapping_segment", "is_terminal"]).to_table().to_pylist()
    keys = [(r["old_psc"], r["new_psc"]) for r in t]
    edge_set = set(keys)
    try:
        idx = [getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    print(json.dumps({
        "uri": REF_URI, "rows": ds.count_rows(),
        "grain_unique": len(keys) == len(set(keys)) == ds.count_rows(),
        "sentinels": {f"{o}->{n}": (o, n) in edge_set for o, n in SENTINELS},
        "segments": dict(Counter(r["mapping_segment"] for r in t)),
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
