"""04 — Tier segmentation Tier0/Tier1/Tier2 + nesting (coordinator addition).

Tier0 = gate-OFF base lookalike families (corporate-family-deduped reps).
Tier1 = of those, primes that demonstrably sub out at all.
Tier2 = of those, primes that sub out to recipients whose OWN PRIME HISTORY
        sits in the target's work-shape NAICS (the strongest signal; the
        gtm_prime_subout_by_recipient_code 'awarded_prime_contracts_in_code' lens).

Demonstrates the nesting HAZARD: if Tier1 is drawn from gtm_prime_sub_pairs but
Tier2 from gtm_prime_subout_by_recipient_code, the two sub-out marts DISAGREE
and Tier2 is NOT a subset of Tier1 (Tier2 can exceed Tier1_5y). Nesting holds
only when Tier1 and Tier2 come from ONE source (the recipient-code cube).
All tier definitions are lifetime / mart-relative => date-stable per artifact.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/Users/benjamincrane/core-x")
from lib_sidecar import q, cand_sql, inlist, shape_naics, PIN_ARTIFACT, SAMPLE
from apps.catalyst_api.src.routers.sub_dossier_v1 import (
    family_key, is_jv_name, sql_market_hydrate, sql_jv_flags)

print(f"# pinned artifact: {PIN_ARTIFACT}\n")

def reps_gate_off(u):
    rows = q(cand_sql(u, select="c.uei, c.wt"), limit=25000)
    ueis = [r["uei"] for r in rows]
    tf = q(f"SELECT e.uei,e.legal_business_name,h.ultimate_parent_uei FROM gtm_sam_entities e "
           f"LEFT JOIN entity_hierarchy h USING(uei) WHERE e.uei='{u}'")[0]
    tfam = family_key(u, tf.get("ultimate_parent_uei"), tf.get("legal_business_name"))
    hyd = {}
    for i in range(0, len(ueis), 6000):
        for r in q(sql_market_hydrate(ueis[i:i+6000]), limit=6100, pin=False):
            hyd[r["uei"]] = r
    jv = set()
    for i in range(0, len(ueis), 6000):
        jv |= {r["uei"] for r in q(sql_jv_flags(ueis[i:i+6000]), limit=6100, pin=False)}
    seen = set(); out = []
    for r in rows:
        mu = r["uei"]; h = hyd.get(mu, {}); nm = h.get("legal_business_name")
        fam = family_key(mu, h.get("ultimate_parent_uei"), nm)
        if fam == tfam or (mu in jv) or is_jv_name(nm) or fam in seen:
            continue
        seen.add(fam); out.append(mu)
    return out

def members(reps, sql_tmpl):
    got = set()
    for i in range(0, len(reps), 6000):
        got |= {list(r.values())[0] for r in q(sql_tmpl(reps[i:i+6000]), limit=6100)}
    return got

print(f"{'name':<24}{'Tier0':>7}{'T1_pairs5y':>11}{'T1_cube':>8}{'T2_cube':>8}"
      f"{'  T2<=T1_pairs?':>15}{'  T2<=T1_cube<=T0?':>18}")
for u, name in SAMPLE[:4]:
    reps = reps_gate_off(u); repset = set(reps); sn = shape_naics(u)
    t1_pairs = members(reps, lambda ch:
        f"SELECT DISTINCT prime_uei FROM gtm_prime_sub_pairs "
        f"WHERE prime_uei IN ({inlist(ch)}) AND edge_dollars_5y>0")
    t1_cube = members(reps, lambda ch:
        f"SELECT DISTINCT prime_awardee_uei FROM gtm_prime_subout_by_recipient_code "
        f"WHERE prime_awardee_uei IN ({inlist(ch)}) AND recipient_code_source='awarded_prime_contracts_in_code' "
        f"AND recipient_code_type='naics' AND context_code_type='naics' AND subaward_amt_total>0")
    t2_cube = members(reps, lambda ch:
        f"SELECT DISTINCT prime_awardee_uei FROM gtm_prime_subout_by_recipient_code "
        f"WHERE prime_awardee_uei IN ({inlist(ch)}) AND recipient_code_source='awarded_prime_contracts_in_code' "
        f"AND recipient_code_type='naics' AND context_code_type='naics' "
        f"AND recipient_code IN ({inlist(sn)}) AND subaward_amt_total>0")
    naive = t2_cube <= t1_pairs                       # the tempting-but-wrong nesting
    unified = t2_cube <= t1_cube <= repset            # the correct nesting
    print(f"{name[:24]:<24}{len(reps):>7}{len(t1_pairs):>11}{len(t1_cube):>8}{len(t2_cube):>8}"
          f"{str(naive):>15}{str(unified):>18}")
print("\n  Tier0-Tier1 gap = base lookalikes with NO demonstrated FSRS sub-out (neutral segment).")
print("  naive nesting (Tier2 cube vs Tier1 pairs) is FALSE -> unify the sub-out source.")
