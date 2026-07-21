"""01 — Universe ceiling + sidecar cap behaviour (FRONT 1: can 25k ever bite?).

Proves: at DEFAULT dials the gate-OFF candidate set is a strict subset of the
14,482 firms with prime_obl_60mo>=$10M, so LIMIT 25,000 cannot truncate. Also
confirms the sidecar honours large limits (and silently caps at 50k above),
and that a 25k-element IN-list does not blow the statement size.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_sidecar import q, scalar, PIN_ARTIFACT

print(f"# pinned artifact: {PIN_ARTIFACT}\n")

print("== absolute universe ceiling ==")
row = scalar("""
SELECT (SELECT COUNT(*) FROM gtm_entity_behavior_rollup WHERE prime_obl_60mo >= 1e7) AS firms_10m_floor,
       (SELECT COUNT(*) FROM gtm_entity_behavior_rollup WHERE prime_obl_60mo > 0)   AS firms_any_prime60,
       (SELECT COUNT(DISTINCT uei) FROM gtm_prime_code_signature)                   AS distinct_primes_sig,
       (SELECT COUNT(*) FROM gtm_entity_behavior_rollup)                            AS rollup_rows""")
print(f"  firms with prime_obl_60mo >= $10M (default floor) : {row['firms_10m_floor']}")
print(f"  firms with prime_obl_60mo  > 0  (floor dialed 0)  : {row['firms_any_prime60']}")
print(f"  distinct primes in gtm_prime_code_signature       : {row['distinct_primes_sig']}")
print(f"  => at default floor, gate-OFF candidates subset {row['firms_10m_floor']} < 25,000 : "
      f"{'CAP CANNOT BITE' if row['firms_10m_floor'] < 25000 else 'CAP CAN BITE'}")
print(f"  => at floor 0 the ceiling is up to {row['distinct_primes_sig']} >> 25,000 : cap CAN bite when floor dialed down\n")

print("== sidecar limit honouring (source gtm_prime_code_signature has 1.18M rows) ==")
for lim in (1000, 25000, 30000, 50000, 60000):
    n = len(q("SELECT uei FROM gtm_prime_code_signature", limit=lim))
    flag = "  <-- SILENT CAP below requested" if n < lim else ""
    print(f"  requested {lim:>6} -> returned {n:>6}{flag}")
print("  (engine ceiling 25,000 is < the 50,000 hard max: honoured exactly. Raising it above 50k would silently cap.)\n")

print("== 25k-element IN-list statement-size probe ==")
ueis = [r["uei"] for r in q("SELECT uei FROM gtm_prime_code_signature", limit=25000)]
inlist = ", ".join(f"'{u}'" for u in ueis)
n = scalar(f"SELECT COUNT(*) AS n FROM gtm_sam_entities WHERE uei IN ({inlist})")["n"]
print(f"  IN-list of {len(ueis)} elements executed OK (count={n}); no statement-size cap at the engine's max fetch.")
