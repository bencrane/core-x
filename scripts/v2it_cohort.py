#!/usr/bin/env python3
"""Build the IT/Professional v1-done re-extraction cohort + shard into waves.

v1-done = ledger llm_state='done' AND prompt_hash IN the 3 v1 controlled-vocab hashes
(excludes the 799 already v2-freeform under f3567fc8). IT/Professional = the resource's
solicitation maps to an active award with psc_code starting 'D' (IT services) or 'R'
(professional/admin support). Writes the full id list + per-wave shard files (hash%NWAVES).

    doppler run -p core-x -c prd -- uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/v2it_cohort.py
"""
from __future__ import annotations
import json, os, sys
import duckdb, lance

def so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}
A = "s3://data-sink/active"
LEDGER=f"{A}/govcon_requirements_extract_ledger/"; GAA=f"{A}/govcon_active_awards/"
MANIFESTS = [f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/", f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
    f"{A}/sam_opps_attachment_manifest_sb500k/"] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
UNI_A="s3://data-sink/sam-gov-opps/active/"; UNI_R="s3://data-sink/sam-gov-opps/archived/"
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"
V1_HASHES = ("f19feb090a8c08612fe3a13739e752719e6202f44220112ee19298472d8f60d3",
             "425b3bfea680d3cdd8a61e1ec3b9078102be7363e9409f695fd7031e5e1d98d4",
             "1798fde01d566b6f1e4a447ec385b1f6ebb9ec2ed3b445deb69bb0dc9862947d")
NWAVES = int(os.environ.get("NWAVES", "16"))
OUT_ALL = "/tmp/v2it_all.ids"; OUT_WAVE = "/tmp/v2it_wave_{:02d}.ids"

def main():
    s = so(); con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    os.makedirs("/tmp/_v2it_spill", exist_ok=True); con.execute("SET memory_limit='12GB'"); con.execute("SET temp_directory='/tmp/_v2it_spill'")
    con.register("L", lance.dataset(LEDGER, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE v1done AS SELECT DISTINCT resource_id FROM L
        WHERE llm_state='done' AND prompt_hash IN {V1_HASHES} AND resource_id IS NOT NULL""")
    # manifest: resource -> sol_norm
    al=[]
    for i,u in enumerate(MANIFESTS):
        try: con.register(f"m{i}", lance.dataset(u, storage_options=s)); al.append(f"m{i}")
        except Exception: pass
    con.execute(f"""CREATE TEMP TABLE man AS SELECT DISTINCT resource_id, {N.format(c='solicitation_number')} AS sol_norm
        FROM ({' UNION ALL '.join(f'SELECT resource_id, solicitation_number FROM {a}' for a in al)})
        WHERE resource_id IS NOT NULL AND solicitation_number IS NOT NULL""")
    # PIID recovery for awards lacking FPDS sol
    con.register("ua", lance.dataset(UNI_A, storage_options=s)); con.register("ur", lance.dataset(UNI_R, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE piid_sol AS SELECT DISTINCT upper(trim(award_number)) AS piid, {N.format(c='solicitation_number')} AS sol
        FROM (SELECT award_number, solicitation_number FROM ua UNION ALL SELECT award_number, solicitation_number FROM ur)
        WHERE award_number IS NOT NULL AND trim(award_number)<>'' AND solicitation_number IS NOT NULL""")
    # active awards -> resolved sol + psc first char, IT/Prof = D or R
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE itprof_sol AS
        WITH g AS (SELECT {N.format(c='solicitation_identifier')} AS fpds_sol, upper(trim(award_id_piid)) AS piid,
                          substr(nullif(upper(trim(psc_code)),''),1,1) AS pfx,
                          upper(trim(business_size_code)) AS bsc,
                          coalesce(TRY_CAST(current_total_value_of_award AS DOUBLE),0) AS cv
                   FROM gaa WHERE (active_current OR active_potential))
        SELECT DISTINCT coalesce(fpds_sol,(SELECT min(p.sol) FROM piid_sol p WHERE p.piid=g.piid)) AS sol_norm, pfx, bsc, cv
        FROM g g WHERE pfx IN ('D','R')""")
    # cohort resources = v1done ∩ (manifest sol -> IT/Prof award sol)
    con.execute("""CREATE TEMP TABLE itres AS
        SELECT DISTINCT v.resource_id FROM v1done v
        JOIN man m ON v.resource_id = m.resource_id
        JOIN itprof_sol i ON m.sol_norm = i.sol_norm""")
    ids = [r[0] for r in con.execute("SELECT resource_id FROM itres ORDER BY resource_id").fetchall()]
    with open(OUT_ALL, "w") as f: f.write("\n".join(ids) + ("\n" if ids else ""))
    # shard by hash % NWAVES (deterministic disjoint waves)
    con.execute(f"CREATE TEMP TABLE waves AS SELECT resource_id, (abs(hash(resource_id)) % {NWAVES}) AS w FROM itres")
    wave_sizes = {}
    for w in range(NWAVES):
        wids = [r[0] for r in con.execute(f"SELECT resource_id FROM waves WHERE w={w} ORDER BY resource_id").fetchall()]
        with open(OUT_WAVE.format(w), "w") as f: f.write("\n".join(wids) + ("\n" if wids else ""))
        wave_sizes[w] = len(wids)
    diag = {
        "v1done_total": con.execute("SELECT count(*) FROM v1done").fetchone()[0],
        "it_prof_cohort": len(ids), "out_all": OUT_ALL, "nwaves": NWAVES,
        "wave_sizes": wave_sizes,
        "by_psc_prefix": con.execute("""SELECT i.pfx, count(DISTINCT v.resource_id) n FROM v1done v
            JOIN man m ON v.resource_id=m.resource_id JOIN itprof_sol i ON m.sol_norm=i.sol_norm GROUP BY 1 ORDER BY n DESC""").df().to_dict("records"),
        "sb_subset": con.execute("""SELECT count(DISTINCT v.resource_id) FROM v1done v JOIN man m ON v.resource_id=m.resource_id
            JOIN itprof_sol i ON m.sol_norm=i.sol_norm WHERE i.bsc='S'""").fetchone()[0],
        "sb_gt500k_subset": con.execute("""SELECT count(DISTINCT v.resource_id) FROM v1done v JOIN man m ON v.resource_id=m.resource_id
            JOIN itprof_sol i ON m.sol_norm=i.sol_norm WHERE i.bsc='S' AND i.cv>5e5""").fetchone()[0],
    }
    print(json.dumps(diag, indent=2, default=str))

if __name__ == "__main__":
    main()
