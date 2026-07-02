"""L1 derived — sca_wd_rates: structured occupation-rate rows parsed from SCA wage-determination
rate registers.

Parses the verbatim plaintext register stored in each SCA wage determination into one
rate-bearing occupation row per (wd_id, occupation_code). SCA ONLY — DBA registers are a
different format and are excluded at the source filter (wd_type = 'SCA').

SOURCE   sam_wd_rate_documents (Lance SoR, s3://data-sink/active/sam_wd_rate_documents).
         1,521 SCA docs, every `document` non-null. The register is a fixed-width-ish text banner
         + occupation listing + fringe block. Read-only Lance scan of (wd_id, revision_number,
         document); every row is parsed in Python (block parser + fringe extractor). No landing
         fetch, no external HTTP.

VALIDATOR dol_sca_occupations (s3://data-sink/active/dol_sca_occupations, 502 codes). Used ONLY to
         MEASURE the extracted-code match rate in the fail-closed gate — never to filter rows. 23
         legacy transportation codes (92xxx) + 99948 legitimately live outside the directory and
         are kept; live match rate is 99.97% of RAW (pre-collapse) rows.

PARSE    Block parser (mandatory — a line parser silently drops ~14.5k wrapped-title rows). A record
         starts at a `^\\d{5} - ` line and appends continuation lines (wrapped titles / footnote+rate
         carried onto line 2-3) until an end-of-line rate `\\d+\\.\\d{2}` appears; the block closes
         the instant it captures a rate, so trailing prose cannot corrupt it. Excluded:
           - family/group headers  (code ends `000`, never rated),
           - conformance / no-rate blocks  (`(see N)` code rows with no rate — 585 of them),
           - interspersed prose  (no `\\d{5} - ` prefix; absorbed harmlessly, never a false row).
         Footnote is the parenthesized `(see N)` token (N∈{1,2}), captured as an int, nullable.
         Transport-quote artifacts (four literal double-quotes) in titles are collapsed to one `"`.

FRINGE   Per-WD, denormalized onto every row of the WD. Anchored at
         `ALL OCCUPATIONS LISTED ABOVE RECEIVE THE FOLLOWING BENEFITS:`.
           health_welfare_hourly         — `HEALTH & WELFARE ... $<n> per hour` (2-OR-3 decimals;
                                            $16.375 3-decimal values are NOT truncated; a
                                            parenthesized region label e.g. `(Hawaii):` is skipped).
                                            Varies per WD ($5.55 std, $1.00 PR fast-food, $16.375,
                                            $2.42 HI, $10.72). NEVER hardcoded. NULL on 7 docs.
                                            MULTI-VALUE SAFETY: a few docs carry two distinct standard
                                            H&W figures in the fringe block — footnote-scoped
                                            (`1) ... $5.55` vs `2) ... $6.11`) or region-scoped
                                            (mainland $5.55 vs Hawaii $2.42). The per-WD model cannot
                                            attribute those per row, so rather than stamp a
                                            confidently-wrong single figure on every row, such docs
                                            emit health_welfare_hourly = NULL and are counted by the
                                            gate (docs_multi_hw). See D2.
           health_welfare_eo13706_hourly — second H&W for EO-13706 sick-leave contracts. NULL on the
                                            351 docs with no EO line. A negative lookahead on the
                                            standard-H&W regex prevents it grabbing the EO figure.

GRAIN    1 row per (wd_id, occupation_code). The register natively prices 14 codes multiple times
         (sub-location / vessel-type variants — 215 collisions across those docs). To hold the stated
         (wd_id, occupation_code) grain + BTREE, collisions are collapsed DETERMINISTICALLY: prefer an
         explicit base/"ALL LOCATIONS" designation variant when present, otherwise fall back to the
         MINIMUM hourly_wage variant (the SCA rate floor for that code in that WD). This is a lossy,
         accepted tradeoff (grounding option b): 215 non-base priced variants are intentionally
         dropped to hold the (wd_id, occupation_code) grain. Live-measured publish count: 371,408 rows
         (371,623 raw − 215 collapsed). FAIL-CLOSED before write: count == distinct(wd_id,
         occupation_code); code-match ≥ 0.98 measured on RAW rows; hourly_wage non-null ≥ 0.98
         measured on the RAW pre-skip population (emitted / (emitted + rate-less code blocks)); row
         count inside the measured band.

TABLE    s3://data-sink/active/sca_wd_rates/    BTREE: [wd_id, occupation_code]
         wd_id, occupation_code (5-char), title, hourly_wage (DOUBLE), footnote (int, nullable),
         health_welfare_hourly (DOUBLE, nullable), health_welfare_eo13706_hourly (DOUBLE, nullable),
         wd_revision (int, from revision_number), source, ingested_at.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sca_wd_rates build

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 -m pipelines.reference.materialize_sca_wd_rates verify
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys

# Reuse the repo's R2 plumbing + Lance index builder by import — do NOT re-implement.
from pipelines.bls.ingest import _build_indexes, _storage_options

ACTIVE = "s3://data-sink/active"

# Source Lance datasets (SoR) — env-overridable for staging.
SRC_URI = os.environ.get("SAM_WD_RATE_DOCUMENTS_URI", f"{ACTIVE}/sam_wd_rate_documents")
DOL_OCC_URI = os.environ.get("DOL_SCA_OCCUPATIONS_URI", f"{ACTIVE}/dol_sca_occupations")

# Output dataset.
TARGET_URI = os.environ.get("SCA_WD_RATES_URI", f"{ACTIVE}/sca_wd_rates/")

DATA_STORAGE_VERSION = "2.1"       # net-new Lance dataset -> pin current default storage version
SOURCE = "SCA_WD_RATE_REGISTER"

BTREE = ["wd_id", "occupation_code"]
BITMAP: list[str] = []

# ── Publish band — live-measured on the full 1,521-SCA population (2026-07-02) ─────────
# Raw rate rows 371,623; 215 collapsed on the 14 multi-priced-code docs (base-then-MIN rule) ->
# 371,408 distinct (wd_id, occupation_code) pairs == published rows. Band is a tight tolerance
# so a parser regression (dropped wrapped rows, mis-scoped listing region, DBA leakage) trips it.
EXPECTED_ROWS = 371_408
ROW_BAND = (368_000, 375_000)

# Gate thresholds.
MIN_CODE_MATCH = 0.98      # non-family extracted RAW codes present in dol_sca_occupations (~.9997)
MIN_WAGE_NONNULL = 0.98    # emitted rows / (emitted + rate-less code blocks) (measured ~1.0)

# ── Ground-truth anchors (hand-verified verbatim register lines) — asserted in verify() ─
# (wd_id, occupation_code) -> (hourly_wage, health_welfare_hourly, health_welfare_eo13706_hourly)
ANCHORS: dict[tuple[str, str], tuple[float, float, float | None]] = {
    ("2017-0649", "07080"): (10.87, 1.00, None),
    ("2017-0649", "07090"): (9.88, 1.00, None),
    ("2017-0655", "07080"): (11.36, 1.00, None),
    ("2017-0655", "07090"): (10.33, 1.00, None),
    ("2015-4007", "01011"): (18.02, 5.55, 5.09),
    ("2015-4009", "01011"): (18.52, 5.55, 5.09),
    ("2015-4009", "01020"): (28.63, 5.55, 5.09),
    ("2015-5885", "01011"): (16.62, 5.55, 5.09),
}

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


# ── register text-format grammar ──────────────────────────────────────────────────────
# A code line: 5-digit code, " - ", then the rest (title, optional footnote, optional rate).
_CODE_LINE_RE = re.compile(r"^(\d{5}) - (.*)$")
# The assembled-block matcher: title (non-greedy) → optional (see N) footnote → EOL rate.
_ROW_RE = re.compile(
    r"^(?P<code>\d{5}) - (?P<title>.+?)"
    r"(?:\s+\(see (?P<footnote>\d+)\))?"
    r"\s+(?P<rate>\d+\.\d{2})\s*$"
)
# Listing region bounds.
_LISTING_HEADER_RE = re.compile(r"OCCUPATION CODE\s*-\s*TITLE.*RATE", re.I)
_FRINGE_ANCHOR = "ALL OCCUPATIONS LISTED ABOVE RECEIVE THE FOLLOWING BENEFITS:"
# Standard H&W $ per hour — negative-lookahead EO 13706 so it never grabs the EO figure;
# `[^\n]*?` skips a parenthesized region label; `\d+\.\d{2,3}` keeps 3-decimal ($16.375) intact.
# Uses finditer in parse_fringe to detect the multi-value (footnote/region-split) case.
_HW_RE = re.compile(
    r"HEALTH\s*&\s*WELFARE(?![^\n]*EO\s*13706)[^\n]*?\$\s*(\d+\.\d{2,3})\s*per\s*hour",
    re.I,
)
# EO 13706 second H&W $ per hour — matched independently.
_HW_EO_RE = re.compile(
    r"HEALTH\s*&\s*WELFARE[^\n]*?EO\s*13706[^\n]*?\$\s*(\d+\.\d{2,3})\s*per\s*hour",
    re.I,
)
# Base/all-locations designation — preferred variant on grain collapse.
_BASE_TITLE_RE = re.compile(r"ALL\s+LOCATIONS", re.I)


def _collapse_quotes(s: str) -> str:
    """Collapse transport-quote escaping (four literal double-quotes -> one) and trim."""
    return s.replace('""""', '"').replace('""', '"').strip()


def parse_fringe(document: str) -> tuple[float | None, float | None, bool]:
    """Extract the per-WD standard H&W and the EO-13706 second H&W ($/hour). Both nullable.

    Returns (health_welfare_hourly, eo_hourly, multi_hw). D2: when the fringe block carries >1
    distinct standard-H&W figure (footnote-scoped `1)`/`2)` or region-scoped mainland/Hawaii), the
    per-WD model cannot attribute a value per row, so health_welfare_hourly is returned NULL and
    multi_hw=True (counted by the gate) rather than stamping a confidently-wrong single figure."""
    idx = document.find(_FRINGE_ANCHOR)
    tail = document[idx:] if idx != -1 else document
    hw_vals = [float(m.group(1)) for m in _HW_RE.finditer(tail)]
    eo = _HW_EO_RE.search(tail)
    distinct_hw = set(hw_vals)
    multi_hw = len(distinct_hw) > 1
    hw = None if (multi_hw or not hw_vals) else hw_vals[0]
    return (
        hw,
        float(eo.group(1)) if eo else None,
        multi_hw,
    )


def parse_rows(document: str) -> tuple[list[dict], int]:
    """Block parser over the occupation listing region.

    A record starts at a `^\\d{5} - ` line and appends continuation lines until an end-of-line
    rate appears; the block closes the instant it captures a rate (so trailing prose after a rated
    row cannot corrupt it). Family headers (*000) and rate-less conformance blocks emit no row.
    Returns (rated rows pre-grain-collapse, count of non-family code blocks that closed WITHOUT a
    rate). The rate-less count feeds the D4 non-tautological wage_nonnull gate."""
    lines = document.splitlines()

    # Confine to the occupation listing: from the column header down to the fringe anchor. This
    # keeps prose after the listing (uniform allowance, directory boilerplate) out of the parser.
    start = 0
    for i, ln in enumerate(lines):
        if _LISTING_HEADER_RE.search(ln):
            start = i + 1
            break
    end = len(lines)
    for i in range(start, len(lines)):
        if _FRINGE_ANCHOR in lines[i]:
            end = i
            break
    region = lines[start:end]

    rows: list[dict] = []
    cur_code: str | None = None
    cur_parts: list[str] = []
    cur_closed = False
    rateless = 0

    def flush_rateless() -> None:
        """Count a non-family code block that ended without ever capturing a rate (conformance /
        no-rate block). Family headers (*000) are never rated by design and are not counted."""
        nonlocal rateless
        if cur_code is not None and not cur_closed and not cur_code.endswith("000"):
            rateless += 1

    def close_if_rate() -> None:
        """If the assembled current block ends in an EOL rate, emit it and mark the block closed."""
        nonlocal cur_closed
        if cur_code is None or cur_closed or cur_code.endswith("000"):
            return
        block = " ".join(p.strip() for p in cur_parts if p.strip())
        m = _ROW_RE.match(f"{cur_code} - {block}")
        if not m:
            return
        rows.append({
            "occupation_code": cur_code,
            "title": _collapse_quotes(m.group("title")),
            "footnote": int(m.group("footnote")) if m.group("footnote") else None,
            "hourly_wage": float(m.group("rate")),
        })
        cur_closed = True

    for ln in region:
        m = _CODE_LINE_RE.match(ln)
        if m:
            # a new code line ends the previous block; if it never rated, tally it as rate-less
            flush_rateless()
            cur_code = m.group(1)
            cur_parts = [m.group(2)]
            cur_closed = False
            close_if_rate()          # single-line rated row closes immediately
        elif cur_code is not None and not cur_closed:
            if not ln.strip():
                continue             # blank line: soft boundary, keep accumulating the title
            cur_parts.append(ln)
            close_if_rate()          # continuation may carry the footnote+rate (wrapped title)
    flush_rateless()                 # final open block
    return rows, rateless


def _collapse_grain(rows: list[dict]) -> list[dict]:
    """Collapse per-(wd,code) collisions deterministically (D3): prefer an explicit base/"ALL
    LOCATIONS" designation variant when present, else the MIN hourly_wage variant (SCA rate floor).
    Ties on wage keep the first-seen (stable, register order). Non-base variants are intentionally
    dropped to hold the (wd_id, occupation_code) grain — an accepted lossy tradeoff."""
    best: dict[str, dict] = {}
    for r in rows:
        c = r["occupation_code"]
        prev = best.get(c)
        if prev is None:
            best[c] = r
            continue
        r_base = bool(_BASE_TITLE_RE.search(r["title"]))
        p_base = bool(_BASE_TITLE_RE.search(prev["title"]))
        if r_base and not p_base:
            best[c] = r
        elif p_base and not r_base:
            continue
        elif r["hourly_wage"] < prev["hourly_wage"]:
            best[c] = r
    return list(best.values())


def _parse_document(wd_id: str, wd_revision, document: str) -> tuple[list[dict], int, bool]:
    """Parse one register -> (collapsed fringe-denormalized rows, raw rate-row count, multi_hw flag).

    The raw count is the pre-collapse rated-row total, used by the gate to measure code-match on the
    honest (un-gamed) population (D5)."""
    raw, _rateless = parse_rows(document)
    if not raw:
        return [], 0, False
    hw, eo, multi_hw = parse_fringe(document)
    collapsed = _collapse_grain(raw)
    try:
        rev = int(wd_revision) if wd_revision is not None else None
    except (TypeError, ValueError):   # D7: a non-numeric revision must not abort the whole build
        rev = None
    out = []
    for r in collapsed:
        out.append({
            "wd_id": wd_id,
            "occupation_code": r["occupation_code"],
            "title": r["title"],
            "hourly_wage": r["hourly_wage"],
            "footnote": r["footnote"],
            "health_welfare_hourly": hw,
            "health_welfare_eo13706_hourly": eo,
            "wd_revision": rev,
        })
    return out, len(raw), multi_hw


def build() -> dict:
    import lance
    import pyarrow as pa

    so = _storage_options()

    # Register the Lance dataset OBJECT and scan only the projected SCA slice into Python.
    src = lance.dataset(SRC_URI, storage_options=so)
    tbl = src.scanner(
        columns=["wd_id", "revision_number", "document"],
        filter="wd_type = 'SCA'",
    ).to_table()
    n_docs = tbl.num_rows
    log(f"scanned {n_docs:,} SCA docs from {SRC_URI}")

    wd_ids = tbl.column("wd_id").to_pylist()
    revs = tbl.column("revision_number").to_pylist()
    docs = tbl.column("document").to_pylist()

    # Validator code universe — measured, never used to filter.
    occ = lance.dataset(DOL_OCC_URI, storage_options=so)
    code_universe = {
        c for c in occ.scanner(columns=["occupation_code"]).to_table()
        .column("occupation_code").to_pylist() if c
    }
    log(f"loaded {len(code_universe):,} validator codes from {DOL_OCC_URI}")

    records: list[dict] = []
    docs_with_rows = 0
    docs_multi_hw = 0
    raw_rate_rows = 0
    # D5: code-match is measured on the RAW (pre-collapse) rated codes so the grain collapse cannot
    # flatter the ratio by preferentially dropping unmatched collision variants.
    raw_code_total = 0
    raw_code_matched = 0
    now = dt.datetime.now(dt.timezone.utc)
    for wd_id, rev, doc in zip(wd_ids, revs, docs):
        if doc is None:
            continue
        # Re-parse raw once for the honest code-match denominator (cheap vs the double count risk).
        raw_rows, _rl = parse_rows(doc)
        for rr in raw_rows:
            raw_code_total += 1
            if rr["occupation_code"] in code_universe:
                raw_code_matched += 1
        parsed, n_raw, multi_hw = _parse_document(wd_id, rev, doc)
        raw_rate_rows += n_raw
        if multi_hw:
            docs_multi_hw += 1
        if parsed:
            docs_with_rows += 1
        records.extend(parsed)
    for r in records:
        r["source"] = SOURCE
        r["ingested_at"] = now

    rows = len(records)
    log(f"parsed {rows:,} rate rows (raw {raw_rate_rows:,}) across "
        f"{docs_with_rows:,}/{n_docs:,} SCA docs; multi-HW docs={docs_multi_hw:,}")

    # -- FAIL-CLOSED gate — structural 1:1 + honest quality floors + band -----------------
    pairs = [(r["wd_id"], r["occupation_code"]) for r in records]
    n_distinct = len(set(pairs))
    n_null_key = sum(1 for w, c in pairs if not w or not c)
    # D5: code-match on RAW rated rows (family headers are never emitted, so all raw rows are codes).
    code_match = raw_code_matched / raw_code_total if raw_code_total else 0.0
    # D4: wage_nonnull is meaningful only against the pre-skip population — emitted rated rows over
    # (emitted + rate-less code blocks). isinstance(float) is a tautology, so measure the real thing.
    rateless_total = raw_rate_rows == 0 and 0 or None  # placeholder overwritten below
    # recompute rate-less across corpus (single dedicated pass, reuses parse_rows output shape)
    rateless_total = 0
    for doc in docs:
        if doc is None:
            continue
        _rr, rl = parse_rows(doc)
        rateless_total += rl
    wage_nonnull = raw_rate_rows / (raw_rate_rows + rateless_total) if (raw_rate_rows + rateless_total) else 0.0
    # D6: single-pass docs-with-H&W (was O(docs × rows)).
    docs_hw = len({r["wd_id"] for r in records if r["health_welfare_hourly"] is not None})

    if n_null_key:
        raise RuntimeError(f"GATE FAIL: {n_null_key} rows with NULL wd_id/occupation_code")
    if rows != n_distinct:
        raise RuntimeError(
            f"GATE FAIL: grain not 1:1 — rows={rows} != distinct(wd_id,occupation_code)={n_distinct}")
    if code_match < MIN_CODE_MATCH:
        raise RuntimeError(
            f"GATE FAIL: code-match {code_match:.4f} < {MIN_CODE_MATCH} "
            f"({raw_code_matched}/{raw_code_total} raw codes in dol_sca_occupations)")
    if wage_nonnull < MIN_WAGE_NONNULL:
        raise RuntimeError(
            f"GATE FAIL: hourly_wage non-null {wage_nonnull:.4f} < {MIN_WAGE_NONNULL} "
            f"({raw_rate_rows}/{raw_rate_rows + rateless_total} rated vs rate-less code blocks)")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        raise RuntimeError(
            f"GATE FAIL: row count {rows} outside measured band {ROW_BAND} "
            f"(expected ~{EXPECTED_ROWS})")
    log(f"GATE PASS: rows={rows:,} distinct={n_distinct:,} code_match={code_match:.4f} "
        f"wage_nonnull={wage_nonnull:.4f} multi_hw_docs={docs_multi_hw:,} band={ROW_BAND}")

    # Assemble the typed Arrow table (explicit schema so nullable DOUBLE/int columns are stable).
    schema = pa.schema([
        ("wd_id", pa.string()),
        ("occupation_code", pa.string()),
        ("title", pa.string()),
        ("hourly_wage", pa.float64()),
        ("footnote", pa.int32()),
        ("health_welfare_hourly", pa.float64()),
        ("health_welfare_eo13706_hourly", pa.float64()),
        ("wd_revision", pa.int32()),
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    table = pa.Table.from_pylist(records, schema=schema)

    lance.write_dataset(table, TARGET_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    log(f"wrote Lance -> {TARGET_URI}")
    built = _build_indexes(TARGET_URI, BTREE, BITMAP, so)

    ds = lance.dataset(TARGET_URI, storage_options=so)
    written = ds.count_rows()
    if written != rows:
        raise RuntimeError(f"POST-WRITE FAIL: lance {written} != assembled {rows}")

    results = {
        "uri": TARGET_URI,
        "sca_docs": n_docs,
        "docs_with_rows": docs_with_rows,
        "rows": written,
        "raw_rate_rows": raw_rate_rows,
        "distinct_wd_code_pairs": n_distinct,
        "grain_1to1": written == n_distinct,
        "code_match_rate": round(code_match, 4),
        "wage_nonnull_rate": round(wage_nonnull, 4),
        "docs_with_hw": docs_hw,
        "docs_multi_hw": docs_multi_hw,
        "columns": table.schema.names,
        "indexes": built,
        "source": SOURCE,
    }
    print(json.dumps(results, indent=2, default=str))
    return results


def verify() -> dict:
    import lance

    so = _storage_options()
    ds = lance.dataset(TARGET_URI, storage_options=so)
    rows = ds.count_rows()

    key = ds.scanner(columns=["wd_id", "occupation_code"]).to_table()
    pairs = list(zip(key.column("wd_id").to_pylist(), key.column("occupation_code").to_pylist()))
    n_distinct = len(set(pairs))

    # D1: Lance names scalar indices `<col>_idx`; a name-suffix guess (`str(i).endswith(col)`) is
    # False for `wd_id_idx`.endswith(`wd_id`). Assert on the index's declared `fields` instead.
    raw_indices = ds.list_indices()
    idx_names = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                 for i in raw_indices]
    idx_fields = set()
    for i in raw_indices:
        fields = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
        for f in (fields or []):
            idx_fields.add(f)

    # Spot-check the 8 hand-verified anchors: (wd_id, code) -> rate + doc H&W triple.
    wd_set = sorted({wd for wd, _ in ANCHORS})
    wd_lit = ", ".join(f"'{w}'" for w in wd_set)
    spot = ds.scanner(
        columns=["wd_id", "occupation_code", "hourly_wage",
                 "health_welfare_hourly", "health_welfare_eo13706_hourly"],
        filter=f"wd_id IN ({wd_lit})",
    ).to_table().to_pylist()
    got = {(r["wd_id"], r["occupation_code"]):
           (r["hourly_wage"], r["health_welfare_hourly"], r["health_welfare_eo13706_hourly"])
           for r in spot}

    anchor_report = {}
    for k, exp in ANCHORS.items():
        anchor_report["/".join(k)] = {"expected": exp, "got": got.get(k)}

    out = {
        "uri": TARGET_URI,
        "rows": rows,
        "distinct_wd_code_pairs": n_distinct,
        "grain_1to1": rows == n_distinct,
        "columns": ds.schema.names,
        "indices": idx_names,
        "index_fields": sorted(idx_fields),
        "wage_nonnull": ds.count_rows(filter="hourly_wage IS NOT NULL"),
        "hw_nonnull": ds.count_rows(filter="health_welfare_hourly IS NOT NULL"),
        "eo_nonnull": ds.count_rows(filter="health_welfare_eo13706_hourly IS NOT NULL"),
        "anchors": anchor_report,
    }
    print(json.dumps(out, indent=2, default=str))

    # -- FAIL-LOUD verify — grain, index presence, every anchor triple ---------------------
    errors: list[str] = []
    if rows != n_distinct:
        errors.append(f"grain broken: rows={rows} != distinct(wd_id,occupation_code)={n_distinct}")
    if not (ROW_BAND[0] <= rows <= ROW_BAND[1]):
        errors.append(f"row count {rows} outside band {ROW_BAND}")
    # BTREE must exist on BOTH key columns — asserted against declared index fields (D1).
    for col in BTREE:
        if col not in idx_fields:
            errors.append(f"BTREE {col} index absent — index_fields={sorted(idx_fields)}")
    for k, exp in ANCHORS.items():
        g = got.get(k)
        if g is None:
            errors.append(f"anchor {k} missing from published table")
        elif g != exp:
            errors.append(f"anchor {k} expected {exp} got {g}")
    if errors:
        raise RuntimeError("VERIFY FAIL: " + " | ".join(errors))
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
