"""Phase 4 parity gate: 14 phrase fixtures through phrase_compiler, Lance vs sidecar executor."""
import json, os, sys, time
sys.path.insert(0, "/Users/benjamincrane/core-x/.claude/worktrees/objective-wozniak-5ded4e")

from apps.catalyst_api.src import phrase_compiler

PHRASES = [
    "construction companies that received a code A mod in the last 90 days",
    "construction companies that received a code Y mod over $5m in the last year",
    "companies over $10m that primed in 236220",
    "dsbs companies in VA that delivered subs under naics 236220",
    "companies that primed in 541690 and also sub",
    "sub-only companies with inferred primeable 541330",
    "active dsbs companies in VA with inferred subbable psc R499",
    "companies with inferred primeable 236220 that received a code G mod in the last 90 days",
    "construction companies with awards expiring within 180 days that received a code G mod in the last 90 days",
    "companies with awards expiring within 90 days",
    "awards over $5m expiring within 365 days",
    "awards from gsa in psc D302 acted in the last 90 days",
    "actions over $5m in naics 237310 in the last 90 days",
    "actions with a subcontracting plan in psc R499 in the last 180 days",
]

def totals(env):
    """Extract comparable numbers from a phrase execution envelope."""
    r = env.get("result") or {}
    return {k: r.get(k) for k in ("total", "total_rows", "distinct_recipients") if r.get(k) is not None}

results = []
for phrase in PHRASES:
    row = {"phrase": phrase}
    try:
        plan = phrase_compiler.compile_phrase(phrase)
        for mode, flag in (("lance", ""), ("sidecar", "on")):
            os.environ["QUERY_SIDECAR_EXECUTE"] = flag
            t0 = time.monotonic()
            env = phrase_compiler.execute_plan(plan["plan"])
            row[mode] = {"ms": round((time.monotonic()-t0)*1000, 1), **totals(env)}
        lt = {k: v for k, v in row["lance"].items() if k != "ms"}
        st = {k: v for k, v in row["sidecar"].items() if k != "ms"}
        row["parity"] = "OK" if lt == st else f"MISMATCH {lt} vs {st}"
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
    results.append(row)
    print(json.dumps(row))

n_ok = sum(1 for r in results if r.get("parity") == "OK")
print(f"\nPARITY: {n_ok}/{len(results)} OK")
