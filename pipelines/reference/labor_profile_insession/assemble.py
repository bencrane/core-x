"""Step 5 — ASSEMBLE: per-call result files -> the module's --agent-results array.

Source of truth = the per-call JSON files the in-session Opus/xhigh subagents wrote to
<scratch>/results/. Validates per-call completeness (all expected PSCs present) before including a
call, and NORMALIZES each result object's PSC key to `psc_code` (agents occasionally emit `psc`).

NO API, NO dataset writes — just marshals local files into <scratch>/agent_results.json, which
`materialize_naics_psc_labor_profile.py retrieve --agent-results <that file>` consumes.

Env: NPLP_SCRATCH (default /tmp/nplp).
"""
from __future__ import annotations

import json
import os

SCRATCH = os.environ.get("NPLP_SCRATCH", "/tmp/nplp")
RESULTS = os.path.join(SCRATCH, "results")


def _psc_of(r: dict) -> str:
    return str(r.get("psc_code") or r.get("psc") or "")


def main() -> None:
    expected = json.load(open(f"{SCRATCH}/expected.json"))

    agent_results = []
    incomplete, bad = [], []
    ok = combos = 0
    for cid, exp_list in expected.items():
        path = f"{RESULTS}/{cid}.json"
        if not os.path.exists(path):
            incomplete.append((cid, "missing file"))
            continue
        try:
            obj = json.load(open(path))
        except Exception as e:  # noqa: BLE001
            bad.append((cid, f"json parse: {e}"))
            continue
        results = obj.get("results")
        if not isinstance(results, list):
            bad.append((cid, "no results array"))
            continue
        exp = set(exp_list)
        got = {_psc_of(r) for r in results}
        miss = exp - got
        if miss:
            incomplete.append((cid, f"missing {len(miss)}/{len(exp)} pscs"))
            continue
        clean = []
        for r in results:
            p = _psc_of(r)
            if p in exp:
                rr = dict(r)
                rr["psc_code"] = p       # normalize: psc -> psc_code
                rr.pop("psc", None)
                clean.append(rr)
        agent_results.append({"custom_id": cid, "results": clean})
        ok += 1
        combos += len(exp)

    out = f"{SCRATCH}/agent_results.json"
    json.dump(agent_results, open(out, "w"))
    print(f"assembled: calls={ok}/{len(expected)}  combos={combos}")
    print(f"incomplete={len(incomplete)}  bad={len(bad)}")
    for c in (incomplete + bad)[:40]:
        print("  ", c)
    print(f"-> {out}")
    if incomplete or bad:
        print("NOTE: NOT all calls assembled — re-drive the missing cids before retrieve "
              "(the fail-closed combo gate will reject a partial file).")


if __name__ == "__main__":
    main()
