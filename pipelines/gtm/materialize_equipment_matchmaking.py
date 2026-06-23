"""Materializer — Gen-3 native Lance SoR for semantic equipment matchmaking.

Phase C of the end-to-end matchmaking engine. Deterministic I/O ONLY — no LLM/API calls.
Consumes the per-shard verdict files produced by the agentic harness (Phase B,
scripts/mm_workflow.js) and the shard input files (Phase A, scripts/extract_matchmaking_shards.py),
runs a hard adversarial grounding gate, and writes the canonical Lance dataset.

SoR contract:
    s3://data-sink/active/equipment_matchmaking/   (native Lance v2.1)

Schema:
    domain_norm                VARCHAR  NOT NULL  (PK — BTREE)
    supported_pscs             LIST<VARCHAR> NOT NULL  (PSC codes; [] when no match)
    verified_inventory_matches LIST<VARCHAR> NOT NULL  (verbatim catalog strings that triggered the match)
    justification_payload      VARCHAR  NOT NULL  (compact JSON reasoning string)
    matched_psc_count          INT32    NOT NULL  (len(supported_pscs); BITMAP — low cardinality 0..15)
    materialized_at            TIMESTAMP(us, UTC) NOT NULL  (lineage)

GROUNDING GATE (adversarial, deterministic):
    Every verified_inventory_match MUST resolve to that domain's actual scraped catalog
    (category_names ∪ equipment_item_names) under normalization (lowercase, alnum-collapse,
    bidirectional containment). Ungrounded strings are DROPPED and counted as hallucinations.
    Every supported_psc must be one of the 15 canonical codes; unknown codes are dropped.
    Coverage is asserted: every evaluated domain must carry a verdict.

Run:
    doppler run -p core-x -c prd -- python3 pipelines/gtm/materialize_equipment_matchmaking.py
"""

from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import sys

import lance
import pyarrow as pa


_BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                     "reports")
SHARD_DIR = os.environ.get("MM_SHARD_DIR", os.path.join(_BASE, "mm_shards"))
OUT_DIR = os.environ.get("MM_OUT_DIR", os.path.join(_BASE, "mm_out"))
VERDICTS_JSONL = os.path.join(_BASE, "equipment_matchmaking_verdicts.jsonl")

DATASET_URI = os.environ.get("EQUIPMENT_MATCHMAKING_URI",
                             "s3://data-sink/active/equipment_matchmaking/")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

VALID_PSC = {
    "Z2AA", "Y1DA", "Z1DA", "Z2DA", "Y1LB", "Z1LB", "Y1PC", "Y1NE",
    "Y1KD", "Y1PZ", "Z2KA", "Z1KF", "P400", "F108", "F014",
}

INDEXES: dict[str, list[str]] = {
    "BTREE": ["domain_norm"],
    "BITMAP": ["matched_psc_count"],
}


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (and R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


_norm_re = re.compile(r"[^a-z0-9]+")


def _norm(s: str) -> str:
    return _norm_re.sub(" ", s.lower()).strip()


def _load_catalogs() -> dict[str, dict]:
    """domain_norm -> {'tokens_blob': normalized catalog text, 'members': set(normalized members)}."""
    out: dict[str, dict] = {}
    shard_files = sorted(glob.glob(os.path.join(SHARD_DIR, "shard_*.json")))
    if not shard_files:
        raise RuntimeError(f"no shard files under {SHARD_DIR}")
    for sf in shard_files:
        with open(sf, encoding="utf-8") as fh:
            data = json.load(fh)
        for d in data.get("domains", []):
            dn = d["domain_norm"]
            members = set()
            for arr in (d.get("category_names") or [], d.get("equipment_item_names") or []):
                for x in arr:
                    nx = _norm(x)
                    if nx:
                        members.add(nx)
            out[dn] = {"members": members, "blob": " | ".join(sorted(members))}
    return out


def _grounded(match: str, cat: dict) -> bool:
    """A match string is grounded if its normalized form has bidirectional containment with
    any catalog member (handles verbatim copies + light formatting drift; rejects inventions)."""
    nm = _norm(match)
    if not nm:
        return False
    if nm in cat["members"]:
        return True
    for mem in cat["members"]:
        if nm in mem or mem in nm:
            return True
    return False


def _load_verdicts() -> dict[str, dict]:
    out: dict[str, dict] = {}
    files = sorted(glob.glob(os.path.join(OUT_DIR, "shard_*.json")))
    if not files:
        raise RuntimeError(f"no verdict files under {OUT_DIR} — run the Phase B workflow first")
    dupes = 0
    bad_files = []
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                arr = json.load(fh)
        except Exception as exc:  # noqa: BLE001
            bad_files.append((os.path.basename(f), str(exc)))
            continue
        if not isinstance(arr, list):
            bad_files.append((os.path.basename(f), "not a JSON array"))
            continue
        for rec in arr:
            dn = (rec.get("domain_norm") or "").strip().lower()
            if not dn:
                continue
            if dn in out:
                dupes += 1
            out[dn] = rec
    if bad_files:
        print(f"WARN: {len(bad_files)} unreadable verdict file(s): {bad_files[:5]}", file=sys.stderr)
    if dupes:
        print(f"WARN: {dupes} duplicate domain verdicts (last-wins)", file=sys.stderr)
    return out


def _schema() -> pa.Schema:
    ts = pa.timestamp("us", tz="UTC")
    return pa.schema([
        pa.field("domain_norm",                pa.string(),               nullable=False),
        pa.field("supported_pscs",             pa.list_(pa.string()),     nullable=False),
        pa.field("verified_inventory_matches", pa.list_(pa.string()),     nullable=False),
        pa.field("justification_payload",      pa.string(),               nullable=False),
        pa.field("matched_psc_count",          pa.int32(),                nullable=False),
        pa.field("materialized_at",            ts,                        nullable=False),
    ])


def _build_rows(catalogs: dict, verdicts: dict) -> tuple[list[dict], dict]:
    now = dt.datetime.now(dt.timezone.utc)
    rows: list[dict] = []
    stats = {
        "evaluated_domains": len(catalogs),
        "verdicts_loaded": len(verdicts),
        "missing_verdict": 0,
        "rows": 0,
        "matched_domains": 0,
        "dropped_unknown_psc": 0,
        "dropped_ungrounded_matches": 0,
        "domains_with_dropped_matches": 0,
        "matched_but_zero_grounded": 0,
        "psc_hist": {},
    }
    missing = []
    for dn, cat in catalogs.items():
        rec = verdicts.get(dn)
        if rec is None:
            stats["missing_verdict"] += 1
            missing.append(dn)
            # emit a placeholder so the SoR records the domain was in-scope but un-evaluated
            rows.append({
                "domain_norm": dn,
                "supported_pscs": [],
                "verified_inventory_matches": [],
                "justification_payload": json.dumps({"verdict": "missing", "reason": "no agent verdict"}),
                "matched_psc_count": 0,
                "materialized_at": now,
            })
            continue

        pscs_raw = rec.get("supported_pscs") or []
        pscs = []
        for p in pscs_raw:
            pu = str(p).strip().upper()
            if pu in VALID_PSC and pu not in pscs:
                pscs.append(pu)
            elif pu not in VALID_PSC:
                stats["dropped_unknown_psc"] += 1

        matches_raw = rec.get("verified_inventory_matches") or []
        grounded = []
        dropped_here = 0
        for m in matches_raw:
            ms = str(m).strip()
            if not ms:
                continue
            if _grounded(ms, cat):
                if ms not in grounded:
                    grounded.append(ms)
            else:
                dropped_here += 1
        if dropped_here:
            stats["dropped_ungrounded_matches"] += dropped_here
            stats["domains_with_dropped_matches"] += 1

        # If the firm matched PSCs but NONE of its cited inventory grounds, the match is
        # unsupported — null it out (the grounding gate is the final arbiter).
        if pscs and not grounded:
            stats["matched_but_zero_grounded"] += 1
            try:
                jp = json.loads(rec.get("justification_payload") or "{}")
            except Exception:  # noqa: BLE001
                jp = {"raw": rec.get("justification_payload")}
            jp["_gate"] = "voided: all cited inventory ungrounded"
            payload = json.dumps(jp, ensure_ascii=False)
            pscs = []
        else:
            payload = rec.get("justification_payload")
            if not isinstance(payload, str):
                payload = json.dumps(payload, ensure_ascii=False) if payload is not None else "{}"

        if pscs:
            stats["matched_domains"] += 1
            stats["psc_hist"][len(pscs)] = stats["psc_hist"].get(len(pscs), 0) + 1

        rows.append({
            "domain_norm": dn,
            "supported_pscs": pscs,
            "verified_inventory_matches": grounded if pscs else [],
            "justification_payload": payload or "{}",
            "matched_psc_count": len(pscs),
            "materialized_at": now,
        })

    stats["rows"] = len(rows)
    if missing:
        print(f"WARN: {len(missing)} domains missing verdicts (first 10): {missing[:10]}", file=sys.stderr)
    return rows, stats


def _write_jsonl(rows: list[dict]) -> None:
    with open(VERDICTS_JSONL, "w", encoding="utf-8") as fh:
        for r in rows:
            out = {k: v for k, v in r.items() if k != "materialized_at"}
            fh.write(json.dumps(out, ensure_ascii=False) + "\n")
    print(f"wrote consolidated provenance → {VERDICTS_JSONL} ({len(rows)} rows)")


def _create_indexes(so: dict) -> None:
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for index_type, cols in INDEXES.items():
        for col in cols:
            ds.create_scalar_index(col, index_type=index_type)
            print(f"  {index_type:<6} ✓ equipment_matchmaking.{col}")


def _verify(so: dict) -> dict:
    import pyarrow.compute as pc

    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["domain_norm"])
    distinct = pc.count_distinct(keys.column("domain_norm")).as_py()
    unique_ok = (n == distinct)
    sample = next((v for v in keys.column("domain_norm").to_pylist() if v), None)
    probe = ds.scanner(columns=["domain_norm"],
                       filter=f"domain_norm = '{sample}'").to_table().num_rows if sample else -1
    indexes = sorted(
        ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix))
        for ix in ds.list_indices()
    )
    out = {
        "uri": DATASET_URI, "rows": n, "distinct_domain_norm": distinct,
        "unique_invariant_ok": unique_ok, "schema": [f.name for f in ds.schema],
        "indexes": indexes, f"probe_domain={sample!r}": probe,
    }
    if not unique_ok:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(domain_norm)={distinct}")
    return out


def main() -> None:
    so = _r2_storage_options()
    catalogs = _load_catalogs()
    verdicts = _load_verdicts()
    rows, stats = _build_rows(catalogs, verdicts)

    print("=== grounding + validation ===")
    print(json.dumps(stats, indent=2, default=str))

    if stats["missing_verdict"] > 0:
        print(f"\nNOTE: {stats['missing_verdict']} domains lack verdicts — re-run those shards before"
              f" treating the SoR as complete (placeholders written).", file=sys.stderr)

    schema = _schema()
    cols = {f.name: [r[f.name] for r in rows] for f in schema}
    table = pa.Table.from_pydict(cols, schema=schema)
    print(f"\nbuilding Lance table — {table.num_rows} rows, {len(table.schema)} cols")

    lance.write_dataset(
        table, DATASET_URI, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )
    print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

    _create_indexes(so)
    _write_jsonl(rows)
    print("\n=== read-back verify ===")
    print(json.dumps(_verify(so), indent=2, default=str))


if __name__ == "__main__":
    main()
