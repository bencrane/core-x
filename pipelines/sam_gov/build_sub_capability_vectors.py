"""Build worker — govcon_sub_capability_vectors: the SUBAWARDEE capability-vector text substrate
(build plan PHASE 5 sub leg; docs/plans/SUBAWARDEE_CAPABILITY_BUILDOUT_PLAN.md §4.1).

THE TEXT-STAMP HALF ONLY. This module dedups + chunks the subawardees' self-reported subaward
descriptions into the frozen vectors dataset with `embedding = NULL`; the EXISTING GPU embed harness
(`sam_attachment_embed_modal.py`, sub_caps sink) fills `embedding` + builds IVF_PQ. Split exactly like
the chunk sinks: extract/chunk here, embed/index there. No embedding code lives in this file.

    SOURCE   usaspending_api_fresh/contract_subaward (the 90-day fresh sub→prime fact, sub-self-reported
             subaward_description). Window-as-data: re-pointing SUBAWARD_SOURCE_URI at the 5y
             usaspending/subaward_search is an overwrite, not a new table (operator decision: 90-day first).
    GRAIN    (subawardee_uei, description_chunk_ix). Per-(sub, distinct-description) dedup → ≤1,200-char
             whitespace chunks → per-sub text-dedup → per-sub ordinal. Per-UEI concat is REJECTED (p99
             concat = 78,225 chars; ~1,080 prolific subs would truncate at bge's 512-token ceiling).
    SoR      s3://data-sink/active/govcon_sub_capability_vectors/ (Lance v2.1; derived, OVERWRITE).
             No window suffix beyond the inherited `_90day`; the window is DATA (created_at + run_id).
             Frozen schema + assert_schema (govcon_gtm_schemas.py) BEFORE the first commit.
    IDEMPOTENT  overwrite-mode snapshot. `chunk_id = sha256(subawardee_uei|chunk_ix|text)[:24]`;
             per-sub chunk order = (len DESC, text) so chunk_ix + content-hash are stable across re-runs.
             `verify --content-hash` hashes the business columns (excl. embedding + run/created stamps),
             so a re-run over the same upstream is provably zero-delta.
    CUI EGRESS INVARIANT  `subaward_description` is the firm's OWN SAM subaward report — sub-self-reported,
             NOT solicitation CUI — safe to embed and surface (same posture asserted in
             build_subawardee_capability_profiles.py). NO doc_scope-derived / solicitation-chunk text
             enters this sink.
    INDICES  BTREE(subawardee_uei) — the sole prefilter/point-lookup key. NO BTREE(chunk_id) (anti-pattern
             #7 / #3177: nothing point-looks-up chunk_id; the egress chain resolves it by string-split).
             IVF_PQ(embedding) is built by the embed harness, not here.

    doppler run -- .venv/bin/python pipelines/sam_gov/build_sub_capability_vectors.py <cmd>
      build                   dedup + chunk + overwrite (text + NULL embedding) + BTREE(subawardee_uei)
      verify                  row count, grain, char-len + null-embedding stats, index list, CUI checks
      verify --content-hash   + business-column content hash (idempotency proof)
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    SUB_CAPABILITY_VECTORS_URI, sub_capability_vectors_schema, assert_schema)
from pipelines.sam_gov.sam_attachment_extract_90day import _r2_storage_options  # noqa: E402

import os  # noqa: E402

ACTIVE = "s3://data-sink/active"
# Source of the description text. 90-day fresh feed by default (operator decision 2026-06-16);
# override to the 5y resume with GOVCON_SUBAWARD_SOURCE_URI=…/usaspending/subaward_search/.
SUBAWARD_SOURCE_URI = os.environ.get(
    "GOVCON_SUBAWARD_SOURCE_URI", f"{ACTIVE}/usaspending_api_fresh/contract_subaward/")
SUB_UEI_COL = os.environ.get("GOVCON_SUBAWARD_SUB_UEI_COL", "subawardee_uei")
DESC_COL = os.environ.get("GOVCON_SUBAWARD_DESC_COL", "subaward_description")

DATA_STORAGE_VERSION = "2.1"
BTREE_INDEXES = ["subawardee_uei"]
CHUNK_CHARS = 1200            # ≈300 tokens — well under bge's 512-token wall
EMBED_DIM = 1024

DUCK_MEM = "8GB"
DUCK_TMP = "/tmp/sub_capability_vectors_duckdb"


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


def _chunk_text(s: str, limit: int = CHUNK_CHARS) -> list[str]:
    """Whitespace-greedy split into ≤limit-char pieces. Lossless modulo whitespace normalization
    (these are flat work-item / SKU lists, not prose — no boundary-quote concern). A single token
    longer than limit (a spaceless SKU dump) is hard-split."""
    s = " ".join(s.split())               # collapse whitespace runs; deterministic
    if not s:
        return []
    if len(s) <= limit:
        return [s]
    out: list[str] = []
    cur = ""
    for tok in s.split(" "):
        if len(tok) > limit:
            if cur:
                out.append(cur); cur = ""
            for i in range(0, len(tok), limit):
                out.append(tok[i:i + limit])
            continue
        if not cur:
            cur = tok
        elif len(cur) + 1 + len(tok) <= limit:
            cur += " " + tok
        else:
            out.append(cur); cur = tok
    if cur:
        out.append(cur)
    return out


def _assemble(so):
    """Deterministic assembly → (arrow_table, stats). Lance scanner projects ONLY the two text columns
    (never an embedding) → DuckDB dedup to per-(sub, distinct-description) pairs → Python chunk +
    per-sub text-dedup + ordinal → arrow cast to the frozen schema with embedding = NULL."""
    import lance
    import pyarrow as pa

    src = lance.dataset(SUBAWARD_SOURCE_URI, storage_options=so)
    cols = set(src.schema.names)
    if SUB_UEI_COL not in cols or DESC_COL not in cols:
        raise RuntimeError(
            f"source {SUBAWARD_SOURCE_URI} missing {SUB_UEI_COL!r}/{DESC_COL!r}; has {sorted(cols)[:20]}…")
    src_tbl = src.scanner(columns=[SUB_UEI_COL, DESC_COL]).to_table()
    log(f"source rows={src_tbl.num_rows:,} ({SUBAWARD_SOURCE_URI})")

    con = _duck()
    con.execute("PRAGMA threads=1")        # deterministic aggregation order (idempotency / zero-delta)
    con.register("csub", src_tbl)
    pairs = con.execute(f"""
        SELECT {SUB_UEI_COL} AS sub_uei, {DESC_COL} AS d, CAST(count(*) AS INTEGER) AS n
        FROM csub
        WHERE {SUB_UEI_COL} IS NOT NULL AND {DESC_COL} IS NOT NULL
          AND length(trim({DESC_COL})) > 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    con.close()
    log(f"distinct (sub, description) pairs={len(pairs):,}")

    # chunk + per-sub text-dedup (sum provenance counts) + deterministic per-sub ordinal
    per_sub: dict[str, dict[str, int]] = {}
    for sub_uei, d, n in pairs:
        agg = per_sub.setdefault(sub_uei, {})
        for ch in _chunk_text(d):
            agg[ch] = agg.get(ch, 0) + int(n)

    sub_uei_l, ix_l, cid_l, text_l, clen_l, nsrc_l = [], [], [], [], [], []
    for sub_uei in sorted(per_sub):
        ordered = sorted(per_sub[sub_uei].items(), key=lambda kv: (-len(kv[0]), kv[0]))
        for ix, (ch, n) in enumerate(ordered):
            cid = hashlib.sha256(f"{sub_uei}|{ix}|{ch}".encode("utf-8")).hexdigest()[:24]
            sub_uei_l.append(sub_uei); ix_l.append(ix); cid_l.append(cid)
            text_l.append(ch); clen_l.append(len(ch)); nsrc_l.append(n)

    n = len(sub_uei_l)
    if n == 0:
        raise RuntimeError("zero chunks assembled — check the source URI / column names")
    tbl = pa.table({
        "subawardee_uei": pa.array(sub_uei_l, pa.string()),
        "description_chunk_ix": pa.array(ix_l, pa.int32()),
        "chunk_id": pa.array(cid_l, pa.string()),
        "text": pa.array(text_l, pa.large_string()),
        "char_len": pa.array(clen_l, pa.int32()),
        "n_source_subawards": pa.array(nsrc_l, pa.int32()),
        "embedding": pa.nulls(n, pa.list_(pa.float32(), EMBED_DIM)),
        "model_id": pa.nulls(n, pa.string()),
        "model_revision": pa.nulls(n, pa.string()),
    })
    stats = {
        "chunks": n,
        "distinct_subs": len(per_sub),
        "distinct_chunk_ids": len(set(cid_l)),
        "max_char_len": max(clen_l),
        "over_1200": sum(1 for c in clen_l if c > CHUNK_CHARS),   # expect 0 (hard-split guarantees ≤limit)
    }
    return tbl, stats


def _stamp(tbl, so):
    import pyarrow as pa
    schema = sub_capability_vectors_schema()
    run_id = f"sub_caps_text_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    created_at = dt.datetime.now(dt.timezone.utc)
    n = tbl.num_rows
    tbl = tbl.append_column("run_id", pa.array([run_id] * n, pa.string()))
    tbl = tbl.append_column("created_at", pa.array([created_at] * n, pa.timestamp("us", tz="UTC")))
    tbl = tbl.select(schema.names).cast(schema)
    return tbl, run_id, created_at


def build():
    import lance
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    log("assembling subawardee capability vectors (text substrate; embedding = NULL) …")
    tbl, stats = _assemble(so)
    log(f"assembled chunks={stats['chunks']:,} · distinct_subs={stats['distinct_subs']:,} "
        f"· distinct_chunk_ids={stats['distinct_chunk_ids']:,} · max_char_len={stats['max_char_len']:,} "
        f"· over_1200={stats['over_1200']:,}")
    if stats["chunks"] != stats["distinct_chunk_ids"]:
        raise RuntimeError(
            f"CHUNK_ID COLLISION: {stats['chunks']} chunks but {stats['distinct_chunk_ids']} distinct "
            f"chunk_ids — the (sub_uei, chunk_ix, text) hash must be unique.")
    if stats["over_1200"]:
        raise RuntimeError(f"{stats['over_1200']} chunks exceed {CHUNK_CHARS} chars — chunker invariant broken")

    tbl, run_id, created_at = _stamp(tbl, so)
    schema = sub_capability_vectors_schema()
    if not tbl.schema.equals(schema, check_metadata=False):
        raise RuntimeError("assembled table schema != frozen sub_capability_vectors_schema()")

    log(f"writing OVERWRITE → {SUB_CAPABILITY_VECTORS_URI}  (run {run_id})")
    lance.write_dataset(tbl, SUB_CAPABILITY_VECTORS_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    assert_schema(SUB_CAPABILITY_VECTORS_URI, schema, so)
    ds = lance.dataset(SUB_CAPABILITY_VECTORS_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"  BTREE  ✓ {col}")
    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    log(f"DONE → {SUB_CAPABILITY_VECTORS_URI} chunks={stats['chunks']:,} in {elapsed:.0f}s "
        f"(embedding NULL — run sam_attachment_embed_modal.py sub_caps to fill + IVF_PQ)")
    return {**stats, "run_id": run_id, "built_at": created_at.isoformat(), "elapsed_s": round(elapsed, 1)}


def _content_hash(con, rel: str) -> str:
    """Stable over business columns only — excludes embedding (NULL pre-embed) + run/created stamps."""
    cols = ["subawardee_uei", "description_chunk_ix", "chunk_id", "text", "char_len", "n_source_subawards"]
    proj = ", ".join(cols)
    return con.execute(
        f"WITH b AS (SELECT {proj} FROM {rel}) "
        f"SELECT md5(string_agg(md5(to_json(b)), '' ORDER BY subawardee_uei, description_chunk_ix)) FROM b"
    ).fetchone()[0]


def verify(content_hash: bool = False):
    import lance
    so = _r2_storage_options()
    ds = lance.dataset(SUB_CAPABILITY_VECTORS_URI, storage_options=so)
    schema = sub_capability_vectors_schema()
    assert_schema(SUB_CAPABILITY_VECTORS_URI, schema, so)
    rows = ds.count_rows()
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("v", ds.scanner(columns=[
        "subawardee_uei", "description_chunk_ix", "chunk_id", "text", "char_len", "n_source_subawards"]).to_table())
    distinct_subs = con.execute("SELECT count(DISTINCT subawardee_uei) FROM v").fetchone()[0]
    distinct_cids = con.execute("SELECT count(DISTINCT chunk_id) FROM v").fetchone()[0]
    dup_grain = con.execute(
        "SELECT count(*) FROM (SELECT subawardee_uei, description_chunk_ix, count(*) c "
        "FROM v GROUP BY 1,2 HAVING c > 1)").fetchone()[0]
    over = con.execute(f"SELECT count(*) FROM v WHERE char_len > {CHUNK_CHARS}").fetchone()[0]
    out = {
        "uri": SUB_CAPABILITY_VECTORS_URI, "rows": rows,
        "distinct_subs": distinct_subs, "distinct_chunk_ids": distinct_cids,
        "chunk_id_unique": distinct_cids == rows,            # expect True
        "grain_dupes": dup_grain,                            # expect 0
        "over_1200_chars": over,                             # expect 0
        "embedding_null": ds.count_rows(filter="embedding IS NULL"),
        "embedding_filled": ds.count_rows(filter="embedding IS NOT NULL"),
        "columns": len(schema.names), "indices": sorted(idx), "schema_assert": "PASS",
    }
    if content_hash:
        out["content_hash_excl_embedding_stamps"] = _content_hash(con, "v")
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


def main():
    ap = argparse.ArgumentParser(description="build govcon_sub_capability_vectors text substrate")
    ap.add_argument("cmd", choices=["build", "verify"])
    ap.add_argument("--content-hash", action="store_true", help="(verify) include business-column content hash")
    args = ap.parse_args()
    if args.cmd == "build":
        build()
    else:
        verify(content_hash=args.content_hash)


if __name__ == "__main__":
    main()
