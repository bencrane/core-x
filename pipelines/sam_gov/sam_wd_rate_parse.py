"""SAM.gov Wage Determination rate-register parser → sam_wd_rates_structured.

Gap-1 of the labor wiring plan (LABOR_x_GOVCON_CROSSWALK_GTM.md §"Not yet wired"):
turn the 5,757 plaintext rate registers in `sam_wd_rate_documents.document` into
join-able `(wd_id, occupation/classification, wage, fringe)` rows.

Deterministic — ZERO LLM anywhere. Both register types are WHD system output with
stable anchors (verified across the full corpus 2026-07-11):

  SCA (1,521 docs): blank/next-code delimited entries under the literal header
      `OCCUPATION CODE - TITLE  FOOTNOTE  RATE`, each `NNNNN - Title ... RATE`,
      possibly wrapped across lines. Fringe (Health & Welfare) is a DOC-LEVEL
      anchored block (`HEALTH & WELFARE: $NN.NN`), occasionally footnote-split
      into multiple rates. Computer occupations may carry `(see 1)` instead of a
      numeric rate (the SCA computer-exemption note) — kept as rate-NULL rows.
  DBA (4,236 docs): dot-leader craft lines `CLASSIFICATION....$ RATE  FRINGE`
      inside identifier blocks (`ELEC0026-002 06/01/2023` = union locals,
      `SUxx....` = survey rates); classifications wrap onto the previous line.

RECONCILIATION DOCTRINE (fail-closed, zero silent drops):
  every SCA code-line and every DBA dot-rate line becomes EXACTLY ONE output row
  — entries without a parseable rate are emitted with wage_rate NULL and a
  parse_note ('see_footnote' | 'no_rate'), never dropped. Per-document
  `rows == candidates` is asserted; any document that fails goes verbatim into
  `sam_wd_rates_residuals` and flips run status to 'partial'.

Run:
    doppler run -p core-x -c prd -- \
      uv run --with pylance --with pyarrow --with 'psycopg[binary]' \
      python pipelines/sam_gov/sam_wd_rate_parse.py --run
Smoke:
    ... --run --limit 20 --out-uri s3://data-sink/active/_smoke_wd_rates/
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

from pipelines.bls.ingest import (  # noqa: E402 — fleet R2/index plumbing, verbatim
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _storage_options,
)

BUCKET = "data-sink"
DOCS_URI = os.environ.get(
    "SAM_WD_RATE_DOCS_URI", f"s3://{BUCKET}/active/sam_wd_rate_documents/")
OUT_URI = os.environ.get(
    "SAM_WD_RATES_URI", f"s3://{BUCKET}/active/sam_wd_rates_structured/")
RESIDUAL_URI = os.environ.get(
    "SAM_WD_RATES_RESIDUAL_URI", f"s3://{BUCKET}/active/sam_wd_rates_residuals/")

FEED = "sam_wd_rates_structured"
SOURCE = "parsed from sam_wd_rate_documents.document (deterministic, no LLM)"

# ── SCA grammar ──────────────────────────────────────────────────────────────
# A code line opens an entry; continuations extend it until a rate terminates it,
# a blank line ends it, or the next code line opens a new one.
SCA_CODE_START = re.compile(r"^\s*(\d{5})\s*-\s*(.*)$")
SCA_RATE_TAIL = re.compile(r"\s{2,}\d+\.\d{2}\s*$")
SCA_ENTRY = re.compile(
    r"^(?P<title>.*?)(?:\s*\((?P<foot>\d+(?:\s*,\s*\d+)*)\))?\s{2,}(?P<rate>\d+\.\d{2})\s*$")
SCA_SEE = re.compile(r"^(?P<title>.*?)\s*\(\s*see\s+(?P<ref>\d+[A-Za-z]?)\s*\)\s*$", re.I)
# Doc-level Health & Welfare: same-line, or wrapped onto the next line.
HW_INLINE = re.compile(r"HEALTH\s*&\s*WELFARE\s*:?[^\n]*?\$?\s*(\d+\.\d+)", re.I)
HW_WRAP = re.compile(r"HEALTH\s*&\s*WELFARE[^\n]*\n[^\n]*?\$?\s*(\d+\.\d+)", re.I)

# ── DBA grammar ──────────────────────────────────────────────────────────────
DBA_BLOCK = re.compile(r"^\s*\*?\s*([A-Z]{2,6}\d{4}-\d{3})\s+(\d{2}/\d{2}/\d{4})\s*$")
# `CLASSIFICATION....$ RATE [junk-word...] FRINGE` — fringe may carry a
# thousands comma (annualized amounts) or a trailing % (percent-of-rate fringe);
# an interposed footnote word ("Employee...") is tolerated and discarded.
DBA_LINE = re.compile(
    r"^(?P<pre>.{0,70}?)\.{3,}\s*\$\s*(?P<rate>\d+\.\d{2})"
    r"(?:\s+(?:[A-Za-z][A-Za-z .]*?\.{0,3}\s+)?(?P<fringe>[\d,]+\.\d+%?))?\s*$")
DBA_CAND = re.compile(r"\.{3,}\s*\$\s*\d+\.\d{2}")
DBA_SEP = re.compile(r"^[-=_\s]+$")


def parse_sca(doc: str) -> tuple[list[dict], int, list[str]]:
    """Line state machine. Returns (rows, candidate_count, unreconciled_samples)."""
    rows: list[dict] = []
    issues: list[str] = []
    cand = 0
    cur: tuple[str, list[str]] | None = None

    def close() -> None:
        nonlocal cur
        if cur is None:
            return
        code, parts = cur
        joined = " ".join(p.strip() for p in parts).strip()
        # Joining strips the >=2-space column gap before the rate; rebuild it
        # explicitly when the entry's last physical line ended in a rate.
        tail = parts[-1].rstrip()
        if SCA_RATE_TAIL.search(tail):
            m_tail = re.search(r"(\d+\.\d{2})\s*$", tail)
            body = re.sub(r"\s*" + re.escape(m_tail.group(1)) + r"\s*$", "", joined)
            joined = body.rstrip() + "  " + m_tail.group(1)
        m = SCA_ENTRY.match(joined)
        if m:
            rows.append({"occupation_code": code, "classification_title": m.group("title").strip(),
                         "footnote_ref": m.group("foot"), "wage_rate": float(m.group("rate")),
                         "parse_note": "ok"})
        else:
            ms = SCA_SEE.match(joined)
            if ms:
                rows.append({"occupation_code": code,
                             "classification_title": ms.group("title").strip(),
                             "footnote_ref": ms.group("ref"), "wage_rate": None,
                             "parse_note": "see_footnote"})
            else:
                # family headers and rate-less area listings: keep, never drop
                rows.append({"occupation_code": code, "classification_title": joined,
                             "footnote_ref": None, "wage_rate": None,
                             "parse_note": "no_rate"})
        cur = None

    for line in doc.splitlines():
        mc = SCA_CODE_START.match(line)
        if mc:
            close()
            cand += 1
            cur = (mc.group(1), [mc.group(2).rstrip()])
            if SCA_RATE_TAIL.search(mc.group(2).rstrip()) or SCA_SEE.match(mc.group(2).strip()):
                close()
            continue
        if cur is not None:
            if not line.strip():
                close()
            else:
                cur[1].append(line.rstrip())
                if SCA_RATE_TAIL.search(line.rstrip()):
                    close()
    close()
    if len(rows) != cand:  # structurally impossible by construction; assert anyway
        issues.append(f"rows={len(rows)} != candidates={cand}")
    return rows, cand, issues


def parse_dba(doc: str) -> tuple[list[dict], int, list[str]]:
    """Dot-leader craft lines inside union/survey identifier blocks."""
    rows: list[dict] = []
    issues: list[str] = []
    lines = doc.splitlines()
    block = bdate = None
    cand = len(DBA_CAND.findall(doc))
    for i, line in enumerate(lines):
        mb = DBA_BLOCK.match(line)
        if mb:
            block, bdate = mb.group(1), mb.group(2)
            continue
        m = DBA_LINE.match(line)
        if not m:
            if DBA_CAND.search(line):
                issues.append(line.rstrip()[:160])
            continue
        title = m.group("pre").strip()
        # wrapped classification: one-line lookback for the leading fragment
        if i > 0:
            prev = lines[i - 1]
            ps = prev.strip()
            if (ps and "$" not in prev and "..." not in prev
                    and not DBA_BLOCK.match(prev) and not DBA_SEP.match(prev)
                    and not ("Rates" in prev and "Fringes" in prev)):
                title = (ps + " " + title).strip()
        fr = m.group("fringe")
        rows.append({
            "occupation_code": None,
            "classification_title": title,
            "footnote_ref": None,
            "wage_rate": float(m.group("rate")),
            "fringe": float(fr.replace(",", "").rstrip("%")) if fr else None,
            "fringe_is_pct": bool(fr and fr.endswith("%")),
            "block_id": block,
            "block_date": bdate,
            "rate_source": ("dba_survey" if block and block.startswith("SU")
                            else "dba_union" if block else "dba_unattributed"),
            "parse_note": "ok" if title else "empty_title",
        })
    return rows, cand, issues


def _schema():
    import pyarrow as pa
    return pa.schema([
        ("wd_id", pa.string()), ("full_reference_number", pa.string()),
        ("revision_number", pa.string()), ("wd_type", pa.string()),
        ("occupation_code", pa.string()), ("classification_title", pa.string()),
        ("footnote_ref", pa.string()), ("wage_rate", pa.float64()),
        ("fringe", pa.float64()), ("fringe_is_pct", pa.bool_()),
        ("hw_rate", pa.float64()), ("hw_rates_all", pa.string()),
        ("block_id", pa.string()), ("block_date", pa.string()),
        ("rate_source", pa.string()), ("parse_note", pa.string()),
        ("document_sha256", pa.string()), ("source", pa.string()),
        ("parsed_at", pa.string()),
    ])


def _residual_schema():
    import pyarrow as pa
    return pa.schema([
        ("wd_id", pa.string()), ("wd_type", pa.string()), ("reason", pa.string()),
        ("candidate_ct", pa.int64()), ("parsed_ct", pa.int64()),
        ("samples", pa.string()), ("document_sha256", pa.string()),
        ("parsed_at", pa.string()),
    ])


def _record_run(stats: dict, dsn: str | None) -> None:
    """ops.sam_wage_determination_runs, same table as sam_wd_manifest (dict-based)."""
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops.* write.", flush=True)
        return
    try:
        import psycopg
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute(
                """
                INSERT INTO ops.sam_wage_determination_runs
                  (feed,status,active_total,wd_rows,county_rows,sca,dba,cba,
                   stateless_wds,dedup_dropped,api_calls,wd_uri,county_uri,
                   indexes_built,stats,error,started_at,completed_at)
                VALUES (%(feed)s,%(status)s,%(active_total)s,%(wd_rows)s,NULL,
                   %(sca)s,%(dba)s,NULL,NULL,NULL,NULL,%(wd_uri)s,NULL,
                   %(indexes_built)s,%(stats)s,%(error)s,%(started_at)s,%(completed_at)s)
                """,
                {**stats, "stats": json.dumps(stats.get("stats", {}))},
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must never mask a good parse
        print(f"WARN: ops.* write failed: {exc}", flush=True)


def run(out_uri: str, residual_uri: str, docs_uri: str, limit: int | None) -> int:
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _storage_options()
    docs = lance.dataset(docs_uri, storage_options=so).to_table().to_pylist()
    if limit:
        docs = docs[:limit]
    print(f"parse: {len(docs)} rate documents from {docs_uri}", flush=True)

    out_rows: list[dict] = []
    residuals: list[dict] = []
    n_sca = n_dba = 0
    parsed_at = started.isoformat()
    for d in docs:
        wd_id, wd_type, text = d["wd_id"], d["wd_type"], d["document"] or ""
        base = {"wd_id": wd_id, "full_reference_number": d.get("full_reference_number"),
                "revision_number": str(d.get("revision_number")), "wd_type": wd_type,
                "document_sha256": d.get("document_sha256"), "source": SOURCE,
                "parsed_at": parsed_at}
        if wd_type == "SCA":
            rows, cand, issues = parse_sca(text)
            hws = HW_INLINE.findall(text) or HW_WRAP.findall(text)
            hw0 = float(hws[0]) if hws else None
            hw_all = json.dumps([float(h) for h in hws]) if len(hws) > 1 else None
            for r in rows:
                out_rows.append({**base, **r, "fringe": None, "fringe_is_pct": None,
                                 "hw_rate": hw0, "hw_rates_all": hw_all,
                                 "block_id": None, "block_date": None,
                                 "rate_source": "sca_register"})
            n_sca += len(rows)
        else:
            rows, cand, issues = parse_dba(text)
            for r in rows:
                out_rows.append({**base, **r, "hw_rate": None, "hw_rates_all": None})
            n_dba += len(rows)
        if issues or (cand > 0 and len(rows) != cand):
            residuals.append({"wd_id": wd_id, "wd_type": wd_type,
                              "reason": "reconciliation_mismatch",
                              "candidate_ct": cand, "parsed_ct": len(rows),
                              "samples": json.dumps(issues[:5]),
                              "document_sha256": d.get("document_sha256"),
                              "parsed_at": parsed_at})

    status = "ok" if not residuals else "partial"
    print(f"parsed rows: {len(out_rows)} (SCA {n_sca} / DBA {n_dba}); "
          f"unreconciled docs: {len(residuals)}", flush=True)

    tbl = pa.Table.from_pylist(out_rows, schema=_schema())
    lance.write_dataset(tbl, out_uri, mode="overwrite", storage_options=so,
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE)
    built = _build_indexes(out_uri,
                           btree=["wd_id", "occupation_code", "full_reference_number"],
                           bitmap=["wd_type", "rate_source", "parse_note"], so=so)
    print(f"wrote {tbl.num_rows} rows → {out_uri} (indexes: {built})", flush=True)

    rtbl = pa.Table.from_pylist(residuals, schema=_residual_schema())
    lance.write_dataset(rtbl, residual_uri, mode="overwrite", storage_options=so,
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE)
    print(f"wrote {rtbl.num_rows} residual docs → {residual_uri}", flush=True)

    _record_run({
        "feed": FEED, "status": status, "active_total": len(docs),
        "wd_rows": len(out_rows), "sca": n_sca, "dba": n_dba,
        "wd_uri": out_uri, "indexes_built": built, "error": None,
        "started_at": started, "completed_at": dt.datetime.now(dt.timezone.utc),
        "stats": {"residual_docs": len(residuals),
                  "no_rate_rows": sum(1 for r in out_rows if r["parse_note"] == "no_rate"),
                  "see_footnote_rows": sum(1 for r in out_rows if r["parse_note"] == "see_footnote"),
                  "empty_title_rows": sum(1 for r in out_rows if r["parse_note"] == "empty_title"),
                  "hw_missing_docs": sum(1 for d in docs if d["wd_type"] == "SCA"
                                         and not (HW_INLINE.search(d["document"] or "")
                                                  or HW_WRAP.search(d["document"] or "")))},
    }, os.environ.get("HQX_DB_URL_POOLED"))
    return 0 if status == "ok" else 1


def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--docs-uri", default=DOCS_URI)
    p.add_argument("--out-uri", default=OUT_URI)
    p.add_argument("--residual-uri", default=RESIDUAL_URI)
    a = p.parse_args()
    if not a.run:
        p.print_help()
        sys.exit(2)
    sys.exit(run(a.out_uri, a.residual_uri, a.docs_uri, a.limit))


if __name__ == "__main__":
    _cli()
