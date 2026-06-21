#!/usr/bin/env python3
"""Wave-1 worklist + CUI-coverage check — SB>$500K STAGE-READY (chunked, not extracted).

Materializes the stage-ready resource_id allow-list (for --resource-ids-file scoping of the
regex extract + LLM select phases) AND verifies the marking pass scanned those chunks
(content_marking populated, not NULL) so the PASS marking report genuinely covers them —
otherwise staging would be CUI egress.

Writes: /tmp/stage3_sb500k_stageready.ids  (one resource_id per line)

    doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/stage3_sb500k_stageready_ids.py
"""
from __future__ import annotations
import json, os, sys
import duckdb, lance

def so():
    ep = os.environ.get("R2_ENDPOINT")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}

A = "s3://data-sink/active"
MANIFESTS = [f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
    f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
    f"{A}/sam_opps_attachment_manifest_sb500k/"] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
WINNERS=f"{A}/sam_opps_attachment_manifest_winners/"; FILES=f"{A}/sam_attachment_files/"; REQ=f"{A}/govcon_award_requirements/"
GAA=f"{A}/govcon_active_awards/"; SCOPE=f"{A}/govcon_scope_vectors/"; PRICING=f"{A}/govcon_pricing/"; UNKNOWN=f"{A}/govcon_unknown/"
UNI_A="s3://data-sink/sam-gov-opps/active/"; UNI_R="s3://data-sink/sam-gov-opps/archived/"
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"
OUT_IDS = "/tmp/stage3_sb500k_stageready.ids"

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/_s3prep_spill", exist_ok=True)
    con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_s3prep_spill'")

    al = []
    for i, uri in enumerate(MANIFESTS):
        try: con.register(f"m{i}", lance.dataset(uri, storage_options=s)); al.append(f"m{i}")
        except Exception: pass
    con.execute(f"""CREATE TEMP TABLE man AS SELECT {N.format(c='solicitation_number')} AS sol_norm, resource_id
        FROM ({' UNION ALL '.join(f'SELECT solicitation_number, resource_id FROM {a}' for a in al)}) WHERE resource_id IS NOT NULL""")
    con.register("winners", lance.dataset(WINNERS, storage_options=s))
    con.execute("""CREATE TEMP TABLE man_awardkey AS SELECT DISTINCT resource_id, k FROM (
        SELECT resource_id, contract_award_unique_key AS k FROM winners WHERE contract_award_unique_key IS NOT NULL AND resource_id IS NOT NULL
        UNION ALL SELECT resource_id, unnest(award_keys) AS k FROM winners WHERE award_keys IS NOT NULL AND resource_id IS NOT NULL) WHERE k IS NOT NULL""")
    con.register("uni_a", lance.dataset(UNI_A, storage_options=s)); con.register("uni_r", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE piid_sol AS SELECT DISTINCT upper(trim(award_number)) AS piid, {N.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM uni_a UNION ALL SELECT award_number, solicitation_number FROM uni_r)
        WHERE award_number IS NOT NULL AND trim(award_number) <> '' AND solicitation_number IS NOT NULL""")
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE ga AS SELECT contract_award_unique_key AS k,
        coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cur_val, upper(trim(business_size_code)) AS bsc,
        {N.format(c='solicitation_identifier')} AS fpds_sol, upper(trim(award_id_piid)) AS piid
        FROM gaa WHERE (active_current OR active_potential)""")
    con.execute("""CREATE TEMP TABLE award_sol AS SELECT g.k, g.cur_val, g.bsc,
        coalesce(g.fpds_sol, (SELECT min(p.sol) FROM piid_sol p WHERE p.piid=g.piid)) AS sol_norm FROM ga g""")
    con.register("files", lance.dataset(FILES, storage_options=s))
    con.execute("CREATE TEMP TABLE dl AS SELECT DISTINCT resource_id FROM files WHERE status='downloaded'")
    con.register("req", lance.dataset(REQ, storage_options=s))
    con.execute("CREATE TEMP TABLE ex AS SELECT DISTINCT resource_id FROM req WHERE resource_id IS NOT NULL")
    con.register("scope", lance.dataset(SCOPE, storage_options=s)); con.register("pricing", lance.dataset(PRICING, storage_options=s)); con.register("unknown", lance.dataset(UNKNOWN, storage_options=s))
    con.execute("""CREATE TEMP TABLE ch AS SELECT DISTINCT resource_id FROM (
        SELECT resource_id FROM scope WHERE resource_id IS NOT NULL UNION SELECT resource_id FROM pricing WHERE resource_id IS NOT NULL
        UNION SELECT resource_id FROM unknown WHERE resource_id IS NOT NULL)""")
    # cohort resource_ids (SB>$500K via Sol# ∪ winners award-key)
    con.execute("""CREATE TEMP TABLE cohA_sol AS SELECT DISTINCT sol_norm FROM award_sol WHERE bsc='S' AND cur_val>5e5 AND sol_norm IS NOT NULL""")
    con.execute("""CREATE TEMP TABLE cohA_key AS SELECT DISTINCT k FROM award_sol WHERE bsc='S' AND cur_val>5e5""")
    con.execute("""CREATE TEMP TABLE cohA_res AS SELECT DISTINCT resource_id FROM (
        SELECT m.resource_id FROM man m JOIN cohA_sol cs ON m.sol_norm=cs.sol_norm
        UNION SELECT ma.resource_id FROM man_awardkey ma JOIN cohA_key ck ON ma.k=ck.k)""")
    # STAGE-READY = cohort ∩ chunked ∩ downloaded ∩ NOT extracted
    con.execute("""CREATE TEMP TABLE stageready AS
        SELECT resource_id FROM cohA_res
        WHERE resource_id IN (SELECT resource_id FROM ch)
          AND resource_id IN (SELECT resource_id FROM dl)
          AND resource_id NOT IN (SELECT resource_id FROM ex)""")
    ids = [r[0] for r in con.execute("SELECT resource_id FROM stageready ORDER BY resource_id").fetchall()]
    with open(OUT_IDS, "w") as f:
        f.write("\n".join(ids) + ("\n" if ids else ""))

    # ── CUI coverage: were these chunks scanned? content_marking NULL = unscanned ──
    cm = con.execute("""
        WITH cc AS (
            SELECT resource_id, content_marking FROM scope WHERE resource_id IN (SELECT resource_id FROM stageready)
            UNION ALL SELECT resource_id, content_marking FROM pricing WHERE resource_id IN (SELECT resource_id FROM stageready)
            UNION ALL SELECT resource_id, content_marking FROM unknown WHERE resource_id IN (SELECT resource_id FROM stageready))
        SELECT count(*) AS chunks,
               count(*) FILTER (WHERE content_marking IS NULL) AS chunks_unscanned_null,
               count(*) FILTER (WHERE content_marking IS NOT NULL AND len(content_marking) > 0) AS chunks_marked_cui,
               count(DISTINCT resource_id) AS rids,
               count(DISTINCT resource_id) FILTER (WHERE content_marking IS NULL) AS rids_with_unscanned_chunk,
               count(DISTINCT resource_id) FILTER (WHERE content_marking IS NOT NULL AND len(content_marking) > 0) AS rids_with_cui_marking
        FROM cc""").df().to_dict("records")[0]

    print(json.dumps({"stageready_ids_written": len(ids), "out_file": OUT_IDS,
                      "cui_coverage": cm}, indent=2, default=str))

if __name__ == "__main__":
    main()
