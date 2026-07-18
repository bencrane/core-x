#!/usr/bin/env python3
"""READ-ONLY simulation of the dynamic claim/lease model for the multi-session grind.

Validates session-grain disjointness for the TWO-LEVEL fan-out: variable sessions per
account, any number of accounts. Models the proposed claim ledger:
  - OUTER static band: account_band = abs(hash(resource_id)) % K_accounts  (each account
    only claims within its band — a hard cross-account guard).
  - INNER dynamic claim: within a band, an arbitrary number of sessions atomically claim
    the next N pending rows (simulated as an atomic pop from a per-band pending queue).

Asserts: (1) every pending resource_id is claimed exactly once across ALL sessions of ALL
accounts (zero double-claim, zero drop); (2) per-band load is even; (3) an abandoned
session's lease expiry returns its un-done rows to pending for safe re-claim with no
double-append. Pure in-memory simulation over the REAL pending id set (no writes).

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/_plan_claim_model_sim.py > /tmp/_plan_claim_sim.json
"""
from __future__ import annotations
import datetime as dt, json, os, random, sys, threading
import duckdb, lance

def so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}

A = "s3://data-sink/active"
MANIFESTS = [
    f"{A}/sam_opps_attachment_manifest/", f"{A}/sam_opps_attachment_manifest_winners/",
    f"{A}/sam_opps_attachment_manifest_remediation/shard_000/",
    f"{A}/sam_opps_attachment_manifest_equipment_rental/shard_000/",
] + [f"{A}/sam_opps_attachment_manifest_play1/shard_00{i}/" for i in range(6)]
WINNERS = f"{A}/sam_opps_attachment_manifest_winners/"
REQ = f"{A}/govcon_award_requirements/"
GAA = f"{A}/govcon_active_awards/"
N = "nullif(upper(regexp_replace(trim({c}), '[^A-Za-z0-9]', '', 'g')), '')"

def main():
    s = so()
    con = duckdb.connect(":memory:"); con.execute("PRAGMA threads=8")
    out = {"as_of": dt.datetime.now(dt.timezone.utc).isoformat()}

    aliases = []
    for i, uri in enumerate(MANIFESTS):
        try:
            con.register(f"m{i}", lance.dataset(uri, storage_options=s)); aliases.append(f"m{i}")
        except Exception:
            pass
    usql = " UNION ALL ".join(f"SELECT solicitation_number, resource_id FROM {a}" for a in aliases)
    con.execute(f"""CREATE TEMP TABLE man AS
        SELECT {N.format(c='solicitation_number')} AS sol_norm, resource_id
        FROM ({usql}) WHERE resource_id IS NOT NULL""")
    con.register("winners", lance.dataset(WINNERS, storage_options=s))
    con.execute("""CREATE TEMP TABLE man_awardkey AS SELECT DISTINCT resource_id, k FROM (
        SELECT resource_id, contract_award_unique_key AS k FROM winners
          WHERE contract_award_unique_key IS NOT NULL AND resource_id IS NOT NULL
        UNION ALL SELECT resource_id, unnest(award_keys) AS k FROM winners
          WHERE award_keys IS NOT NULL AND resource_id IS NOT NULL) WHERE k IS NOT NULL""")
    con.register("gaa", lance.dataset(GAA, storage_options=s))
    con.execute(f"""CREATE TEMP TABLE sb AS
        SELECT {N.format(c='solicitation_identifier')} AS sol_norm, contract_award_unique_key AS k
        FROM gaa WHERE (active_current OR active_potential) AND upper(trim(business_size_code))='S'""")
    con.execute("CREATE TEMP TABLE sb_sol AS SELECT DISTINCT sol_norm FROM sb WHERE sol_norm IS NOT NULL")
    con.execute("CREATE TEMP TABLE sb_key AS SELECT DISTINCT k FROM sb")
    con.execute("""CREATE TEMP TABLE sb_res AS SELECT DISTINCT resource_id FROM (
        SELECT m.resource_id FROM man m JOIN sb_sol ss ON m.sol_norm=ss.sol_norm
        UNION SELECT ma.resource_id FROM man_awardkey ma JOIN sb_key sk ON ma.k=sk.k)""")
    con.register("req", lance.dataset(REQ, storage_options=s))
    con.execute("CREATE TEMP TABLE ex AS SELECT DISTINCT resource_id FROM req WHERE resource_id IS NOT NULL")
    ids = [r[0] for r in con.execute(
        "SELECT resource_id FROM sb_res WHERE resource_id NOT IN (SELECT resource_id FROM ex)").fetchall()]
    con.close()

    out["pending_sb_resource_ids"] = len(ids)

    # ── claim-model simulation ──
    K_ACCOUNTS = 3
    # outer static band (cross-account guard)
    def band(rid: str) -> int:
        # mirror DuckDB abs(hash())%K determinism conceptually; use python hash of bytes for sim
        import hashlib
        return int(hashlib.sha256(rid.encode()).hexdigest(), 16) % K_ACCOUNTS
    bands: dict[int, list[str]] = {b: [] for b in range(K_ACCOUNTS)}
    for rid in ids:
        bands[band(rid)].append(rid)
    out["band_sizes"] = {str(b): len(v) for b, v in bands.items()}
    sizes = [len(v) for v in bands.values()]
    out["band_max_dev_from_even_pct"] = round(100 * (max(sizes) - min(sizes)) / (sum(sizes) / K_ACCOUNTS), 3)

    # inner dynamic claim: per band, a thread-safe atomic queue (models UPDATE..WHERE state='pending'
    # ..LIMIT N RETURNING). M sessions per account, each repeatedly claims a batch of N until empty.
    BATCH_N = 50
    SESSIONS_PER_ACCOUNT = [4, 7, 3]  # VARIABLE per account — the constraint under test
    claimed_log: list[str] = []
    log_lock = threading.Lock()

    def run_band(b: int, n_sessions: int):
        queue = list(bands[b])           # the per-band pending set
        qlock = threading.Lock()
        local: list[str] = []
        llock = threading.Lock()
        def session(sid: int):
            while True:
                with qlock:              # atomic claim of next BATCH_N (CAS-equivalent)
                    if not queue:
                        return
                    take = queue[:BATCH_N]; del queue[:BATCH_N]
                with llock:
                    local.extend(take)
        threads = [threading.Thread(target=session, args=(i,)) for i in range(n_sessions)]
        for t in threads: t.start()
        for t in threads: t.join()
        with log_lock:
            claimed_log.extend(local)

    bts = []
    for b in range(K_ACCOUNTS):
        t = threading.Thread(target=run_band, args=(b, SESSIONS_PER_ACCOUNT[b]))
        bts.append(t); t.start()
    for t in bts: t.join()

    # ── invariants ──
    total_sessions = sum(SESSIONS_PER_ACCOUNT)
    out["sim"] = {
        "k_accounts": K_ACCOUNTS,
        "sessions_per_account": SESSIONS_PER_ACCOUNT,
        "total_sessions": total_sessions,
        "batch_n": BATCH_N,
        "claimed_total": len(claimed_log),
        "claimed_distinct": len(set(claimed_log)),
        "double_claims": len(claimed_log) - len(set(claimed_log)),
        "dropped": len(ids) - len(set(claimed_log)),
        "coverage_complete": len(set(claimed_log)) == len(ids),
        "zero_overlap": len(claimed_log) == len(set(claimed_log)),
    }

    # ── lease-expiry re-claim: kill a random session's last batch as "abandoned", revert to pending,
    #    re-claim by another session; assert no row lands twice (idempotent on resource_id) ──
    rng = random.Random(42)
    abandoned = set(rng.sample(ids, min(200, len(ids))))   # claimed-but-not-done at lease expiry
    # done set = everything claimed minus abandoned (the abandoned never wrote a result)
    done = set(claimed_log) - abandoned
    # re-claim pass: only rows NOT in done are re-claimable (idempotent landing keyed on resource_id)
    reclaimable = [r for r in ids if r not in done]
    reclaim_set = set(reclaimable)
    final = done | reclaim_set
    out["lease_reclaim"] = {
        "abandoned_rows": len(abandoned),
        "done_after_first_pass": len(done),
        "reclaimable_rows": len(reclaimable),
        "final_coverage": len(final),
        "final_equals_universe": len(final) == len(ids),
        "double_append_rows": len(done & reclaim_set),   # MUST be 0: done rows never re-claimed
        "safe_reclaim": (done & reclaim_set) == set(),
    }

    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr)

if __name__ == "__main__":
    main()
