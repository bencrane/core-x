"""Additive in-place patch — restore contract_end_date values nulled by the original
now+2y upper bound (ingest #311). NO full re-ingestion.

Method (exact, not the non-unique ATN+Phase+Agency composite): reconstruct each affected
row's STORED sbir_surrogate_id by re-running the ORIGINAL projection
(`_build_sql(ced_upper='2028-06-07')` — the boundary in force at the v15 ingest, which
included contract_end_date in the surrogate hash AS NULL for these rows), and carry the
true lower-bound-only date alongside (`emit_true_ced`). Then `merge_insert` on
sbir_surrogate_id overwrites ONLY contract_end_date for the matched rows, and the 14 scalar
indices are rebuilt (merge_insert relocates matched rows -> they become unindexed).

  doppler run -- python pipelines/sbir/patch_contract_end_date.py [--apply]

Default is DRY-RUN (reconstruct + gate + report, no write). Pass --apply to mutate.

Known, intentional caveat: the stored surrogate for these rows was hashed with
contract_end_date = NULL; after this patch the column is populated, so the surrogate no
longer fingerprints the row. The surrogate is treated as a stable opaque PK post-mint; a
future full re-ingest (now lower-bound-only by default) re-aligns everything under overwrite
semantics. Keyed-update by surrogate is exactly what the directive specifies.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ingest  # noqa: E402

# Boundary in force at the v15 ingest (2026-06-07 + 2y). Hardcoded so reconstruction is
# faithful regardless of when this script is re-run.
ORIG_CED_UPPER = "2028-06-07"
SOURCE = os.environ.get("SBIR_PATCH_SOURCE", f"s3://{ingest.BUCKET}/landing/sbir/award_data.csv")


def _reconstruct(local_source: str):
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("SET memory_limit='8GB';")
    os.makedirs(f"{ingest.SCRATCH}/spill", exist_ok=True)
    con.execute(f"SET temp_directory='{ingest.SCRATCH}/spill';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(ingest._macros())
    sql = ingest._build_sql(local_source, ced_upper=ORIG_CED_UPPER, emit_true_ced=True)
    con.execute("CREATE TABLE recon AS " + sql)
    # affected = original window nulled it (contract_end_date IS NULL) but the lower-bound
    # value is a real date (contract_end_date_true IS NOT NULL).
    rows = con.execute(
        "SELECT sbir_surrogate_id, contract_end_date_true "
        "FROM recon WHERE contract_end_date IS NULL AND contract_end_date_true IS NOT NULL "
        "ORDER BY sbir_surrogate_id"
    ).fetchall()
    dup = con.execute(
        "SELECT count(*) - count(DISTINCT sbir_surrogate_id) FROM ("
        "  SELECT sbir_surrogate_id FROM recon "
        "  WHERE contract_end_date IS NULL AND contract_end_date_true IS NOT NULL)"
    ).fetchone()[0]
    con.close()
    return rows, dup


def _live_null_ced_ids(ds):
    import duckdb

    con = duckdb.connect(":memory:")
    con.register("rdr", ds.scanner(columns=["sbir_surrogate_id", "contract_end_date"]).to_reader())
    con.execute("CREATE TABLE liveced AS SELECT * FROM rdr")  # materialize: reader is single-pass
    con.unregister("rdr")
    ids = set(r[0] for r in con.execute(
        "SELECT sbir_surrogate_id FROM liveced WHERE contract_end_date IS NULL").fetchall())
    total_nonnull = con.execute(
        "SELECT count(*) FROM liveced WHERE contract_end_date IS NOT NULL").fetchone()[0]
    con.close()
    return ids, total_nonnull


def _record_patch(so_dsn_row: dict) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ledger write.")
        return
    try:
        import psycopg

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("ALTER TABLE ops.sbir_awards_runs "
                        "ADD COLUMN IF NOT EXISTS operation text DEFAULT 'ingest';")
            cur.execute(
                """INSERT INTO ops.sbir_awards_runs
                   (dataset_uri, source_file, rows, distinct_pk, exact_dup_rows,
                    indices, status, error, started_at, completed_at, operation)
                   VALUES (%(dataset_uri)s, %(source_file)s, %(rows)s, %(distinct_pk)s,
                    %(exact_dup_rows)s, %(indices)s, %(status)s, %(error)s,
                    %(started_at)s, %(completed_at)s, %(operation)s)""", so_dsn_row)
            conn.commit()
        print("ledger: ops.sbir_awards_runs patch row written.")
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ledger write failed: {exc}")


def run(apply: bool) -> dict:
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = ingest._so()
    local = ingest._fetch_source(SOURCE)

    print(f"reconstruct original projection (ced_upper={ORIG_CED_UPPER}) …")
    affected, dup = _reconstruct(local)
    print(f"reconstructed affected rows: {len(affected)} (duplicate surrogate ids: {dup})")
    if dup != 0:
        raise SystemExit(f"ABORT: {dup} duplicate surrogate ids among affected rows")
    if not affected:
        print("nothing to restore; exiting.")
        return {"affected": 0}

    ds = lance.dataset(ingest.DATASET_URI, storage_options=so)
    v_before, n_before = ds.version, ds.count_rows()
    live_null_ids, nonnull_before = _live_null_ced_ids(ds)
    print(f"live v{v_before}: rows={n_before:,} contract_end_date_nonnull={nonnull_before:,} "
          f"null={len(live_null_ids):,}")

    # ── integrity gate: every reconstructed id must currently exist + be NULL in live ──
    recon_ids = [sid for sid, _ in affected]
    missing = [sid for sid in recon_ids if sid not in live_null_ids]
    if missing:
        raise SystemExit(
            f"ABORT: {len(missing)} reconstructed surrogate(s) are not NULL-ced in live "
            f"(drift or already patched). e.g. {missing[:3]}")
    print(f"integrity gate PASS: all {len(recon_ids)} targets exist in live with ced NULL.")

    if not apply:
        print("\nDRY RUN — no write. Sample restorations:")
        for sid, d in affected[:5]:
            print(f"  {sid[:24]}…  -> {d}")
        return {"affected": len(affected), "applied": False}

    # ── apply: merge_insert ONLY (sbir_surrogate_id, contract_end_date) ──
    patch_tbl = pa.table({
        "sbir_surrogate_id": pa.array(recon_ids, pa.string()),
        "contract_end_date": pa.array([d for _, d in affected], pa.date32()),
    })
    print(f"merge_insert {patch_tbl.num_rows} rows …")
    ds.merge_insert("sbir_surrogate_id").when_matched_update_all().execute(patch_tbl)
    ds = lance.dataset(ingest.DATASET_URI, storage_options=so)

    print("rebuild scalar indices (merge_insert relocated matched rows) …")
    built = ingest._create_indexes(ds, so)
    ds = lance.dataset(ingest.DATASET_URI, storage_options=so)
    v_after = ds.version

    # ── post-write verification ──
    import duckdb
    con = duckdb.connect(":memory:")
    con.register("rdr", ds.scanner(columns=["sbir_surrogate_id", "contract_end_date"]).to_reader())
    con.execute("CREATE TABLE v AS SELECT * FROM rdr")  # materialize: reader is single-pass
    con.unregister("rdr")
    n_after = con.execute("SELECT count(*) FROM v").fetchone()[0]
    dpk = con.execute("SELECT count(DISTINCT sbir_surrogate_id) FROM v").fetchone()[0]
    nonnull_after = con.execute("SELECT count(*) FROM v WHERE contract_end_date IS NOT NULL").fetchone()[0]
    # confirm each target now carries its expected date
    con.register("p", patch_tbl)
    mism = con.execute(
        "SELECT count(*) FROM p JOIN v USING (sbir_surrogate_id) "
        "WHERE v.contract_end_date IS DISTINCT FROM p.contract_end_date").fetchone()[0]
    con.close()

    untrained = []
    for ix in ds.list_indices():
        st = ds.stats.index_stats(ix["name"])
        if st.get("num_unindexed_rows", 0) != 0:
            untrained.append((ix["name"], st.get("num_unindexed_rows")))

    ok = (n_after == n_before and dpk == n_after and
          nonnull_after == nonnull_before + len(affected) and mism == 0 and not untrained)
    print(f"\nrows {n_before:,}->{n_after:,}  distinct_pk={dpk:,}  "
          f"ced_nonnull {nonnull_before:,}->{nonnull_after:,} (+{nonnull_after-nonnull_before})  "
          f"date_mismatches={mism}  untrained_idx={untrained}")

    completed = dt.datetime.now(dt.timezone.utc)
    _record_patch({
        "dataset_uri": ingest.DATASET_URI, "source_file": os.path.basename(local),
        "rows": len(affected), "distinct_pk": dpk, "exact_dup_rows": n_after - dpk,
        "indices": f"v{v_before}->v{v_after}; reindexed {len(built.get('BTREE',[]))+len(built.get('BITMAP',[]))}; "
                   f"restored {len(affected)} contract_end_date",
        "status": "success" if ok else "error",
        "error": None if ok else f"gate fail: untrained={untrained} mism={mism}",
        "started_at": started, "completed_at": completed,
        "operation": "restore_contract_end_date",
    })

    if not ok:
        raise SystemExit("POST-WRITE GATE FAILED — inspect dataset")
    return {"affected": len(affected), "applied": True, "version_before": v_before,
            "version_after": v_after, "ced_nonnull_before": nonnull_before,
            "ced_nonnull_after": nonnull_after, "rows": n_after, "distinct_pk": dpk,
            "indices": built}


def main() -> None:
    ap = argparse.ArgumentParser(description="Restore contract_end_date (additive, in-place)")
    ap.add_argument("--apply", action="store_true", help="mutate the dataset (default: dry-run)")
    args = ap.parse_args()
    import json

    print(json.dumps(run(args.apply), indent=2, default=str))


if __name__ == "__main__":
    main()
