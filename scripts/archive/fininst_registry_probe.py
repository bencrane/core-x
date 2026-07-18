#!/usr/bin/env python3
"""Read-only probe of candidate FINANCIAL-INSTITUTION REGISTRY datasets in the Lance SoR.

Goal: find datasets carrying a CANONICAL government designation (FDIC cert, NCUA charter,
Fed RSSD, SEC CIK, FINRA/SEC CRD, GLEIF LEI, federal-regulator agency code, charter/entity
type) that could deterministically classify UCC secured parties (lenders) — bank vs credit
union vs RIA vs BDC vs fund — WITHOUT an LLM.

For each target dataset: typed schema, row count, committed indices, and auto-detected
IDENTIFIER columns (3 non-null samples + null density) and CLASSIFICATION columns
(frequency GROUP BY when distinct ≤ cap). Strictly non-mutating.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/fininst_registry_probe.py > /tmp/fininst.json
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sys
import time

import duckdb
import lance
import pyarrow as pa

BUCKET = "data-sink"
TIMEOUT_S = 90
CLASS_CAP = 220  # GROUP BY a class column only when distinct ≤ this

# (label, schema_only) — the financial-institution registry shortlist from the live catalog.
TARGETS = [
    ("hmda_panels", False),          # HMDA reporter panel — federal regulator (agency) + RSSD
    ("crosswalk_hmda_gleif", False), # built HMDA-lender → LEI crosswalk
    ("hmda_lar", True),              # loan-level (huge) — schema only
    ("sec_adv_part1", False),        # SEC Form ADV Part 1 — RIAs (CRD/SEC#, adviser/fund flags)
    ("sec_adv_w", False),            # SEC Form ADV-W / wide
    ("edgar_cik_map", False),        # SEC CIK registry (canonical SEC entity id)
    ("edgar_form_d", False),         # Reg D private offerings — industryGroup (Banking/Pooled Fund/…)
    ("gleif_l1_entities", False),    # GLEIF LEI L1 — entity category / legal form / status
    ("gleif_l2_relationships", True),# LEI parent/child — schema only
    ("sba_7a", False),               # SBA 7(a) — originating lender names
    ("sba_504", False),              # SBA 504 — CDC / third-party lender names
    ("nmls_state_entity_counts", False),  # confirm: aggregate vs per-entity roster
    ("form5500_sch_a_carrier", True),     # insurance carriers (tangential) — schema only
    ("companies", True),             # generic firmographic spine — schema only (peek for type cols)
]

IDENT_RE = re.compile(r"(rssd|fdic|ncua|\bcert\b|cert_|\bcik\b|\bcrd\b|\blei\b|lei_|charter_?num|"
                      r"_id$|^id$|_num$|sec_?number|sec_?no|filer|registration_?num)", re.I)
CLASS_RE = re.compile(r"(agency|regulator|charter_?type|entity_?type|institution_?type|org_?type|"
                      r"category|categor|industry|naics|\bsic\b|_type$|^type$|_class|status|"
                      r"form_?type|filing_?type|segment|kind|legal_?form|fund|adviser|registrant)", re.I)


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET temp_directory='/tmp/duck_spill_fi';")
    con.execute("PRAGMA threads=8;")
    return con


def probe(label, schema_only, so) -> dict:
    uri = f"s3://{BUCKET}/active/{label}/"
    ds = lance.dataset(uri, storage_options=so)
    fields = [(f.name, f.type) for f in ds.schema]
    names = [n for n, _ in fields]
    rec = {
        "label": label, "uri": uri, "rows": ds.count_rows(),
        "n_cols": len(fields),
        "schema": [{"name": n, "type": str(t)} for n, t in fields],
        "indices": [], "identifiers": {}, "classifications": {},
        "ident_cols": [n for n in names if IDENT_RE.search(n)],
        "class_cols": [n for n in names if CLASS_RE.search(n)],
    }
    try:
        for i in ds.list_indices():
            d = dict(i) if not isinstance(i, dict) else i
            rec["indices"].append({"type": d.get("type") or d.get("index_type"),
                                   "fields": d.get("fields") or d.get("columns")})
    except Exception as e:  # noqa: BLE001
        rec["indices_error"] = str(e)[:120]

    if schema_only:
        rec["note"] = "schema_only"
        return rec

    ident = rec["ident_cols"]
    klass = rec["class_cols"]
    proj = sorted(set(ident + klass))
    if not proj:
        return rec

    def _run():
        reader = ds.scanner(columns=proj).to_reader()
        con = _con()
        try:
            con.register("r", reader)
            con.execute("CREATE TEMP TABLE t AS SELECT * FROM r")
            n = con.execute("SELECT count(*) FROM t").fetchone()[0]
            for c in ident:
                nn = con.execute(f'SELECT count("{c}") FROM t').fetchone()[0]
                samples = con.execute(
                    f'SELECT DISTINCT CAST("{c}" AS VARCHAR) v FROM t '
                    f'WHERE "{c}" IS NOT NULL LIMIT 4').fetchall()
                rec["identifiers"][c] = {
                    "non_null": int(nn), "null_pct": round(100 * (n - nn) / n, 2) if n else None,
                    "distinct_le": None, "samples": [s[0] for s in samples]}
            for c in klass:
                nd = con.execute(f'SELECT count(DISTINCT "{c}") FROM t').fetchone()[0]
                entry = {"distinct": int(nd)}
                if nd <= CLASS_CAP:
                    rows = con.execute(
                        f'SELECT COALESCE(CAST("{c}" AS VARCHAR), \'∅ NULL\') v, count(*) n '
                        f'FROM t GROUP BY 1 ORDER BY n DESC').fetchall()
                    entry["groups"] = [{"value": v, "rows": int(x)} for v, x in rows[:40]]
                else:
                    s = con.execute(
                        f'SELECT DISTINCT CAST("{c}" AS VARCHAR) v FROM t '
                        f'WHERE "{c}" IS NOT NULL LIMIT 6').fetchall()
                    entry["samples"] = [r[0] for r in s]
                rec["classifications"][c] = entry
        finally:
            con.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            fut.result(timeout=TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            rec["error"] = f"timeout>{TIMEOUT_S}s"
        except Exception as e:  # noqa: BLE001
            rec["error"] = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    return rec


def main() -> int:
    so = storage_options()
    out = []
    for label, schema_only in TARGETS:
        t0 = time.time()
        try:
            rec = probe(label, schema_only, so)
        except Exception as e:  # noqa: BLE001
            rec = {"label": label, "error": f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"}
        out.append(rec)
        print(f"{label:<26} rows={rec.get('rows')!s:>10} ident={len(rec.get('ident_cols', []))} "
              f"class={len(rec.get('class_cols', []))} ({round(time.time()-t0,1)}s)",
              file=sys.stderr, flush=True)
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
