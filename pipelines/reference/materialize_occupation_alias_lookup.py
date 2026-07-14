"""occupation_alias_lookup — the reverse-map entry hop: free-text role names → SOC/SCA codes.

Closes labor-wiring delta 1 (docs/reference/STAFFING_GTM_LABOR_WIRING.md): the only unbuilt
link in the pre-call → award chain. A staffing agency's answer to "what roles do you staff"
("travel nurses", "cleared network engineers") must resolve to SOC/SCA codes before the
reverse-lookup through naics_psc_labor_profile_categories can reach awards. This dim
materializes the full landed title corpus as ONE normalized alias table:

    free text  ──normalize──►  alias_norm  ──this table──►  (code_type, code)
                                                             ├─ soc → combos (674 in layer)
                                                             └─ sca → soc bridge carried inline

Sources (all Lance, s3://data-sink/active/):
  onet_occupation_data            1,016   SOC primary titles          → title_source='primary'
  onet_sample_of_reported_titles  7,953   reported job titles         → 'reported'
  onet_job_titles                57,543   alternate titles (+short)   → 'alternate' / 'short'
  dol_sca_occupations               502   SCA taxonomy titles         → 'sca_primary'
  sca_soc_crosswalk                 424   SCA→SOC bridge (tier/confidence), carried inline
  naics_psc_labor_profile_categories      reachability flag: in_combo_layer = the code
                                          appears in at least one ranked combo profile

Alias explosion (deterministic): O*NET titles of the shape "X (Y)" land as BOTH X and Y
(the parenthetical is the spelled-out form — "Travel RN (Travel Registered Nurse)");
short_title lands as its own alias. Normalization: lowercase, punctuation → space, collapse
whitespace. Grain: 1/(alias_norm, code_type, code) — one alias CAN map to several codes
(honest ambiguity, e.g. "Superintendent"); rank at query time by title_source priority
(primary > sca_primary > reported > alternate > short) then in_combo_layer.

Matching doctrine (query-time, not baked): exact alias_norm probe first; then token-subset /
LIKE against alias_norm; fuzzy/LLM only as last resort. The table is the corpus, not the
matcher.

Ledger: ops.labor_share_runs (generic shape, dataset-name-keyed — same as naics_labor_share).

    doppler run -p core-x -c prd -- uv run --with pylance --with pyarrow --with duckdb \\
      --with boto3 --with 'psycopg[binary]' \\
      python -m pipelines.reference.materialize_occupation_alias_lookup --smoke   # then full
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
from collections import Counter

from pipelines.bls.ingest import (  # noqa: E402 — fleet plumbing, do not reimplement
    DATA_STORAGE_VERSION,
    MAX_BYTES_PER_FILE,
    MAX_ROWS_PER_FILE,
    _build_indexes,
    _s3_client,
    _storage_options,
)
from pipelines.reference.labor_share_ingest import _record_run  # shared ops ledger

BUCKET = "data-sink"
A = f"s3://{BUCKET}/active"
SOURCE_TAG = "materialize_occupation_alias_lookup"
OUT_URI = os.environ.get("OCCUPATION_ALIAS_LOOKUP_URI", f"{A}/occupation_alias_lookup/")

_SOURCE_PRIORITY = {"primary": 1, "sca_primary": 2, "reported": 3, "alternate": 4, "short": 5}
_PAREN_RE = re.compile(r"^(?P<outer>[^()]+?)\s*\((?P<inner>[^()]{2,})\)\s*$")
_NORM_RE = re.compile(r"[^a-z0-9]+")

# ── validation gate bounds ──────────────────────────────────────────────────────────
GATE_MIN_ROWS = 60_000
GATE_SOC_COMBO_COVERAGE = 0.95     # combo-layer SOCs reachable by ≥1 alias
GATE_SCA_COMBO_COVERAGE = 0.95     # combo-layer SCAs reachable by ≥1 alias
# anchor lookups: alias_norm → expected code must be present
GATE_ANCHORS = [("travel rn", "soc", "29-1141"),
                ("network engineer", "soc", "15-1241"),
                ("software developer", "soc", "15-1252")]


def _gate(name: str, ok: bool, detail: str) -> None:
    print(f"GATE {name}: {'PASS' if ok else 'FAIL'} — {detail}", flush=True)
    if not ok:
        raise RuntimeError(f"gate failed: {name} — {detail}")


def _norm(s: str) -> str | None:
    n = _NORM_RE.sub(" ", s.lower()).strip()
    return n or None


def _variants(title: str | None) -> list[str]:
    """A verbatim title → its alias variants (self + parenthetical split)."""
    if not title or not title.strip():
        return []
    t = title.strip()
    m = _PAREN_RE.match(t)
    if m:
        return [t, m.group("outer").strip(), m.group("inner").strip()]
    return [t]


def _schema():
    import pyarrow as pa
    return pa.schema([
        ("alias", pa.string()), ("alias_norm", pa.string()), ("n_tokens", pa.int32()),
        ("code_type", pa.string()), ("code", pa.string()),
        ("occupation_title", pa.string()), ("title_source", pa.string()),
        ("source_priority", pa.int32()), ("in_combo_layer", pa.bool_()),
        ("bridged_soc_code", pa.string()), ("bridge_tier", pa.string()),
        ("bridge_confidence", pa.string()),
        ("sca_entry_type", pa.string()),
        ("source", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


def run(*, storage_options: dict | None = None, uri: str = OUT_URI) -> dict:
    import lance
    import pyarrow as pa

    so = storage_options or _storage_options()
    started_at = dt.datetime.now(dt.timezone.utc)
    ingested_at = started_at
    status, error_text, built = "error", None, []
    rows: list[dict] = []
    cov: dict = {}
    try:
        def tbl(name, columns=None, filter=None):  # noqa: A002
            return lance.dataset(f"{A}/{name}/", storage_options=so) \
                .scanner(columns=columns, filter=filter).to_table().to_pylist()

        occ = tbl("onet_occupation_data", columns=["o_net_soc_code", "soc_code", "title"])
        rep = tbl("onet_sample_of_reported_titles",
                  columns=["soc_code", "title", "reported_job_title"])
        alt = tbl("onet_job_titles", columns=["soc_code", "title", "job_title", "short_title"])
        sca = tbl("dol_sca_occupations",
                  columns=["occupation_code", "occupation_title", "entry_type"])
        xw = tbl("sca_soc_crosswalk",
                 columns=["occupation_code", "soc_code", "tier", "confidence"])
        cat = tbl("naics_psc_labor_profile_categories", columns=["soc_code", "sca_code"])

        combo_socs = {r["soc_code"] for r in cat if r["soc_code"]}
        combo_scas = {r["sca_code"] for r in cat if r["sca_code"]}
        # canonical title per 6-digit SOC = the '.00' base-occupation row (8-digit O*NET
        # specializations like 29-1141.04 must not shadow the base title)
        soc_title: dict[str, str] = {}
        for r in occ:
            if (r["o_net_soc_code"] or "").endswith(".00") or r["soc_code"] not in soc_title:
                soc_title[r["soc_code"]] = r["title"]
        bridge = {r["occupation_code"]: r for r in xw}

        # dedup on (alias_norm, code_type, code) keeping the highest-priority source
        best: dict[tuple, dict] = {}

        def add(alias: str, code_type: str, code: str, canonical: str | None,
                title_source: str, sca_entry_type: str | None = None) -> None:
            norm = _norm(alias)
            if not norm or not code:
                return
            key = (norm, code_type, code)
            prio = _SOURCE_PRIORITY[title_source]
            cur = best.get(key)
            if cur and cur["source_priority"] <= prio:
                return
            b = bridge.get(code) if code_type == "sca" else None
            best[key] = {
                "alias": alias.strip(), "alias_norm": norm, "n_tokens": len(norm.split()),
                "code_type": code_type, "code": code,
                "occupation_title": canonical, "title_source": title_source,
                "source_priority": prio,
                "in_combo_layer": code in (combo_socs if code_type == "soc" else combo_scas),
                "bridged_soc_code": b["soc_code"] if b else None,
                "bridge_tier": b["tier"] if b else None,
                "bridge_confidence": str(b["confidence"]) if b and b["confidence"] is not None else None,
                "sca_entry_type": sca_entry_type,
                "source": SOURCE_TAG, "ingested_at": ingested_at,
            }

        for r in occ:
            for v in _variants(r["title"]):
                add(v, "soc", r["soc_code"], r["title"], "primary")
        for r in rep:
            for v in _variants(r["reported_job_title"]):
                add(v, "soc", r["soc_code"], soc_title.get(r["soc_code"], r["title"]), "reported")
        for r in alt:
            canonical = soc_title.get(r["soc_code"], r["title"])
            for v in _variants(r["job_title"]):
                add(v, "soc", r["soc_code"], canonical, "alternate")
            for v in _variants(r["short_title"]):
                add(v, "soc", r["soc_code"], canonical, "short")
        for r in sca:
            for v in _variants(r["occupation_title"]):
                add(v, "sca", r["occupation_code"], r["occupation_title"], "sca_primary",
                    sca_entry_type=r["entry_type"])

        rows = sorted(best.values(), key=lambda r: (r["alias_norm"], r["code_type"], r["code"]))

        # ── gates ──
        n = len(rows)
        _gate("row-count", n >= GATE_MIN_ROWS, f"rows={n} (≥ {GATE_MIN_ROWS})")
        soc_covered = {r["code"] for r in rows if r["code_type"] == "soc"} & combo_socs
        sca_covered = {r["code"] for r in rows if r["code_type"] == "sca"} & combo_scas
        _gate("combo-soc-coverage", len(soc_covered) / len(combo_socs) >= GATE_SOC_COMBO_COVERAGE,
              f"{len(soc_covered)}/{len(combo_socs)} combo SOCs alias-reachable")
        _gate("combo-sca-coverage", len(sca_covered) / len(combo_scas) >= GATE_SCA_COMBO_COVERAGE,
              f"{len(sca_covered)}/{len(combo_scas)} combo SCAs alias-reachable")
        by_key = {(r["alias_norm"], r["code_type"]): None for r in rows}
        anchor_misses = [(a, ct, c) for a, ct, c in GATE_ANCHORS
                         if (a, ct, c) not in best]
        _gate("anchors", not anchor_misses,
              f"misses={anchor_misses or 'none'} over {len(GATE_ANCHORS)} anchors "
              f"({len(by_key)} distinct alias_norm×type)")
        sca_bridged = sum(1 for r in rows if r["code_type"] == "sca" and r["bridged_soc_code"])
        _gate("sca-bridge-carried", sca_bridged > 0,
              f"{sca_bridged} sca alias rows carry a bridged soc_code")

        table = pa.Table.from_pylist(rows, schema=_schema())
        lance.write_dataset(table, uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
        built = _build_indexes(uri, btree=["alias_norm", "code"],
                               bitmap=["code_type", "title_source", "in_combo_layer"], so=so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        print(f"FATAL occupation_alias_lookup: {exc}", flush=True)
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        cov = {
            "rows": len(rows),
            "distinct_alias_norm": len({r["alias_norm"] for r in rows}),
            "by_source": {k: sum(1 for r in rows if r["title_source"] == k)
                          for k in _SOURCE_PRIORITY},
            "soc_rows": sum(1 for r in rows if r["code_type"] == "soc"),
            "sca_rows": sum(1 for r in rows if r["code_type"] == "sca"),
            "in_combo_rows": sum(1 for r in rows if r["in_combo_layer"]),
            "ambiguous_aliases": len({k for k, c in Counter(
                (r["alias_norm"], r["code_type"]) for r in rows).items() if c > 1}),
        }
        _record_run("occupation_alias_lookup", uri,
                    "composed:onet_titles+dol_sca_occupations(+sca_soc bridge)",
                    len(rows), built, cov, status, error_text, started_at, completed_at)
        print(f"OCCUPATION_ALIAS_LOOKUP SUMMARY: {cov} status={status}", flush=True)
    return {"dataset": "occupation_alias_lookup", "rows": len(rows), "indexes": built,
            "coverage": cov, "status": status}


def _cli() -> None:
    p = argparse.ArgumentParser(
        description="Compose occupation_alias_lookup (O*NET + SCA titles → SOC/SCA entry hop).")
    p.add_argument("--smoke", action="store_true",
                   help="write to a throwaway _smoke_ URI and delete it after.")
    a = p.parse_args()
    uri = f"{A}/_smoke_occupation_alias_lookup/" if a.smoke else OUT_URI
    result = run(uri=uri)
    if a.smoke:
        s3 = _s3_client()
        prefix = uri.split(f"{BUCKET}/", 1)[1]
        keys = [o["Key"] for page in s3.get_paginator("list_objects_v2")
                .paginate(Bucket=BUCKET, Prefix=prefix) for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET,
                              Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
        print(f"smoke cleanup: deleted {len(keys)} objects under {prefix}", flush=True)
    print(f"\n=== occupation_alias_lookup summary ===\n  {result}", flush=True)


if __name__ == "__main__":
    _cli()
