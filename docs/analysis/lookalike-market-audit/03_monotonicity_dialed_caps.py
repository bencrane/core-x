"""03 — Dialed-down floor breaks the '25k is enough' proof, and the
monotonicity property test passes on a KNOWN-WRONG (capped) number (FRONT 1 +
guardrail critique).

Shows:
  (a) c_raw(gate OFF) for Carahsoft as market_prime_floor / min_lane_hits move
      into their VALID dial ranges — it climbs past 25,000.
  (b) at floor=0/min_lane_hits=1 the engine's market.total is total_capped=True
      and materially undercounts, YET total(require_subout ON) <= total(OFF)
      still holds — i.e. the proposed monotonicity invariant is GREEN on a
      number the engine itself flags as capped/wrong.
"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/Users/benjamincrane/core-x")
from lib_sidecar import q, cand_sql, scalar, PIN_ARTIFACT
from apps.catalyst_api.src.routers.sub_dossier_v1 import build

U = "DT8KJHZXVJH5"  # Carahsoft — broadest sample firm
print(f"# pinned artifact (raw side): {PIN_ARTIFACT}\n")

print("== (a) c_raw(gate OFF, pre-dedup) vs the 25,000 fetch ceiling, Carahsoft ==")
# True universe size via COUNT(*) over the candidate query (not a row fetch, so
# the sidecar's 50k row cap never floors the number).
for floor, mh in [(1e7, 2), (1e6, 2), (0, 2), (0, 1)]:
    inner = cand_sql(U, mh=mh, floor=floor, select="c.uei")
    n = scalar(f"SELECT COUNT(*) AS n FROM ({inner}) t")["n"]
    bite = "  <-- exceeds 25k: ENGINE FETCH TRUNCATES the total" if n > 25000 else ""
    print(f"  market_prime_floor={floor:>11.0f}  min_lane_hits={mh}  ->  true c_raw={n}{bite}")

print("\n== (b) engine at floor=0 / min_lane_hits=1: capped total + monotonicity ==")
off = asyncio.run(build({"uei": U, "dials": {"market_prime_floor": 0, "min_lane_hits": 1}}))
on  = asyncio.run(build({"uei": U, "dials": {"market_prime_floor": 0, "min_lane_hits": 1, "require_subout": True}}))
mo, so = off["market"]["total"], on["market"]["total"]
print(f"  gate OFF: market.total={mo}  total_capped={off['market']['total_capped']}  (true post-dedup is far larger)")
print(f"  gate ON : market.total={so}  total_capped={on['market']['total_capped']}")
print(f"  monotonicity (ON <= OFF): {'HOLDS' if so <= mo else 'VIOLATED'}  "
      f"<-- but OFF is total_capped=True, so a GREEN monotonicity test here certifies a WRONG number")
