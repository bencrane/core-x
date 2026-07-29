"""Materialize OBBA (P.L. 119-21) apportionment-footnote attributions → Lance SoR.

WHAT THIS IS
    The One Big Beautiful Bill Act (OBBA, P.L. 119-21) is a reconciliation act, so USAspending's
    DEFC tagging cannot isolate its dollars (verified: no DEFC exists for 119-21 — see the federal
    appropriations run record). But OMB apportionment FOOTNOTES cite the enacting statute per line,
    and 570 footnote rows (329 distinct texts, FY2025-26, 111 TAFS) in `omb_apportionment_footnotes`
    cite P.L. 119-21 with section/paragraph granularity and explicit dollar amounts. This module
    lands those OBBA-attributed dollars as a queryable dataset.

METHOD (recorded for audit)
    1. LLM STRUCTURED EXTRACTION over each distinct OBBA-citing footnote → (section, paragraph,
       amount, amount_verbatim, purpose) tuples, extracting ONLY amounts the footnote binds to
       P.L. 119-21 (not co-cited laws).
    2. INDEPENDENT ADVERSARIAL VERIFICATION of every extraction (re-read raw footnote; correct
       amount↔law mis-bindings; the 21 multi-law footnotes are the risk surface).
    The verified extractions are the committed input of record at
    `docs/reference/data/obba_apportionment_extractions.json` (this module does NOT call an LLM).
    3. DETERMINISTIC VERBATIM GATE (here): every amount_verbatim must occur literally in its source
       footnote (normalized for $/comma/space/case, so "$100M" and "$2,275,000,000" both pass) —
       a hallucination backstop. Amounts failing the gate are dropped and logged.
    4. `is_primary_citation` flags one row per distinct (section, paragraph, amount) so a SUM over
       primary rows yields the distinct-statutory-line total WITHOUT the transfer-chain /
       multi-account double-count (gross sum over all rows is ~1.8x the distinct total).

DEFENSIBLE FIGURE
    Distinct statutory lines (is_primary_citation, net): ~$245.7B (positive-only ~$248.8B),
    FY2025 ~$72.7B / FY2026 ~$173.0B, 619 lines across 78 accounts. This is the apportionment-
    VISIBLE OBBA subset for the first two fiscal years — NOT OBBA's full multi-year budgetary
    effect (that is the CBO score, a separate ingest). Land it, label it; do not conflate the two.

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \\
      --with 'psycopg[binary]' python -m pipelines.reference.materialize_obba_attributions
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import uuid

os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")

from pipelines.bls.ingest import (  # noqa: E402
    DATA_STORAGE_VERSION, MAX_BYTES_PER_FILE, MAX_ROWS_PER_FILE, _build_indexes, _storage_options,
)

BUCKET = "data-sink"
URI = f"s3://{BUCKET}/active/omb_obba_attributions/"
FOOTNOTES_URI = f"s3://{BUCKET}/active/omb_apportionment_footnotes/"
ARTIFACT = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "reference", "data",
                        "obba_apportionment_extractions.json")


def _t(s) -> str:
    """Normalize a token/text for verbatim comparison ($/comma/space/case-insensitive)."""
    return re.sub(r"[\s,$]", "", str(s)).lower()


def _record_run(run_id, rows, distinct_line, gross, rejects, recon_fails) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ledger.", flush=True)
        return
    try:
        import psycopg

        with psycopg.connect(dsn, autocommit=True) as c, c.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ops.federal_appropriations_ingest_runs (
                    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, run_id uuid NOT NULL,
                    stream text NOT NULL, resolved_url text, source_bytes bigint, rows_written bigint,
                    datasets jsonb, started_at timestamptz, finished_at timestamptz,
                    status text NOT NULL CHECK (status IN ('running','completed','failed')),
                    disposition text, notes text, recorded_at timestamptz NOT NULL DEFAULT now());""")
            cur.execute("""
                INSERT INTO ops.federal_appropriations_ingest_runs
                    (run_id, stream, resolved_url, source_bytes, rows_written, datasets,
                     started_at, finished_at, status, disposition, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (str(run_id), "obba_apportionment_attributions", "derived:omb_apportionment_footnotes",
                 None, rows, json.dumps({"omb_obba_attributions": rows}),
                 dt.datetime.now(dt.timezone.utc), dt.datetime.now(dt.timezone.utc), "completed", "ok",
                 f"OBBA P.L.119-21 apportionment attributions; distinct_line=${distinct_line:,.0f}; "
                 f"gross=${gross:,.0f}; gate_rejects={rejects}; recon_fails={recon_fails}"))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger write failed: {exc}", flush=True)


def materialize(smoke: bool = False) -> dict:
    import duckdb
    import lance
    import pyarrow as pa

    so = _storage_options()
    verified = json.load(open(os.path.abspath(ARTIFACT)))

    # key -> footnote_text, and text -> (tafs set, fy set), from the apportionment-footnote SoR
    con = duckdb.connect()
    con.register("fn", lance.dataset(FOOTNOTES_URI, storage_options=so).to_table())
    key_text, tafs_by_text, fy_by_text = {}, {}, {}
    for file_id, fy, tafs, fnum, text in con.execute("""
        select file_id, fiscal_year, tafs, footnote_number, footnote_text
        from fn where footnote_text ilike '%119-21%'
    """).fetchall():
        key_text[f"{file_id}|{tafs}|{fnum}|{fy}"] = text
        tafs_by_text.setdefault(text, set()).add(tafs)
        fy_by_text.setdefault(text, set()).add(int(fy))

    rows, rejected, recon_fail = [], [], []
    ingested_at = dt.datetime.now(dt.timezone.utc)
    seen_dedup, seen_line = set(), set()
    for f in verified:
        text = key_text.get(f["key"])
        if text is None:
            continue
        tnorm = _t(text)
        primary_tafs = f["key"].split("|")[1]
        comp_sum = 0.0
        for a in (f.get("corrected_attributions") or []):
            amt, vb = a.get("amount_usd"), a.get("amount_verbatim")
            if amt is None or not vb:
                rejected.append({"key": f["key"], "reason": "no amount"}); continue
            vtok = _t(vb)
            if not (vtok and (vtok in tnorm or vtok.lstrip("-") in tnorm)):
                rejected.append({"key": f["key"], "reason": "verbatim not in source", "vb": vb}); continue
            comp_sum += float(amt)
            sec, par = str(a.get("section", "") or ""), str(a.get("paragraph", "") or "")
            dedup = (text, sec, par, round(float(amt), 2))
            if dedup in seen_dedup:
                continue
            seen_dedup.add(dedup)
            line_key = (sec, par, round(float(amt), 2))
            is_primary = line_key not in seen_line
            seen_line.add(line_key)
            all_tafs = sorted(tafs_by_text.get(text, {primary_tafs}))
            fys = sorted(fy_by_text.get(text, set()))
            rows.append({
                "law": "119-21", "section": sec or None, "paragraph": par or None,
                "amount_usd": float(amt), "amount_verbatim": vb,
                "purpose": (a.get("purpose") or None), "bound_confidence": a.get("bound_confidence"),
                "is_primary_citation": is_primary, "primary_tafs": primary_tafs,
                "all_tafs": "|".join(all_tafs), "tafs_count": len(all_tafs),
                "fiscal_years": "|".join(str(y) for y in fys),
                "primary_fiscal_year": (min(fys) if fys else None),
                "footnote_text": text[:4000],
                "method": "apportionment_footnote:llm_extract+adversarial_verify+verbatim_gate",
                "source": "omb_apportionment_footnotes", "ingested_at": ingested_at,
            })
        st = f.get("stated_total_usd") or 0
        if st and comp_sum and abs(st - comp_sum) > max(1000, 0.02 * st):
            recon_fail.append({"key": f["key"], "stated_total": st, "component_sum": comp_sum})

    schema = pa.schema([
        ("law", pa.string()), ("section", pa.string()), ("paragraph", pa.string()),
        ("amount_usd", pa.float64()), ("amount_verbatim", pa.string()), ("purpose", pa.string()),
        ("bound_confidence", pa.string()), ("is_primary_citation", pa.bool_()),
        ("primary_tafs", pa.string()), ("all_tafs", pa.string()), ("tafs_count", pa.int32()),
        ("fiscal_years", pa.string()), ("primary_fiscal_year", pa.int32()),
        ("footnote_text", pa.string()), ("method", pa.string()), ("source", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    uri = URI.replace("/active/", "/smoke/") if smoke else URI
    tbl = pa.Table.from_pylist(rows, schema=schema)
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=so)
    _build_indexes(uri, btree=["section", "primary_fiscal_year"], bitmap=[], so=so)

    con.register("r", tbl)
    distinct_line = con.execute("select coalesce(sum(amount_usd),0) from r where is_primary_citation").fetchone()[0]
    gross = con.execute("select coalesce(sum(amount_usd),0) from r").fetchone()[0]
    n_lines = con.execute("select count(*) from r where is_primary_citation").fetchone()[0]

    # gates
    if not smoke:
        assert len(rejected) == 0, f"{len(rejected)} verbatim-gate rejects (should be 0 after the gate fix)"
        assert len(recon_fail) == 0, f"{len(recon_fail)} stated-total reconciliation failures"
        assert distinct_line > 100e9, f"distinct-line OBBA total ${distinct_line:,.0f} implausibly low"
        secs = {r["section"] for r in rows}
        assert "20004" in secs and "1181" in secs, "expected OBBA sections missing"

    _record_run(uuid.uuid4(), len(rows), distinct_line, gross, len(rejected), len(recon_fail))
    print(f"omb_obba_attributions: {len(rows)} rows ({n_lines} distinct lines) -> {uri}", flush=True)
    print(f"  distinct-line OBBA (net): ${distinct_line:,.0f}  |  gross: ${gross:,.0f}  |  "
          f"rejects={len(rejected)} recon_fails={len(recon_fail)}", flush=True)
    return {"rows": len(rows), "distinct_lines": n_lines, "distinct_line_usd": distinct_line,
            "gross_usd": gross, "uri": uri}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--smoke", action="store_true", help="write to smoke/ throwaway URI")
    materialize(smoke=ap.parse_args().smoke)


if __name__ == "__main__":
    main()
