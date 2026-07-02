"""Step 4 — VALIDATE: check every result file present so far against expected + vocab.

Reports running coverage toward the worklist's combo count, integrity defects (for re-drive), and
quality drift. Read-only; NO API; NO dataset writes.

A "defect" cid should be re-driven (re-run the in-session agent for that call) before assembly —
the fail-closed combo gate in `retrieve` will refuse a partial/invalid agent_results file.

Env: NPLP_SCRATCH (default /tmp/nplp).
"""
from __future__ import annotations

import json
import os
import statistics
from collections import Counter

from ._util import load_module

M = load_module()

SCRATCH = os.environ.get("NPLP_SCRATCH", "/tmp/nplp")
RESULTS = os.path.join(SCRATCH, "results")


def _psc_of(r: dict) -> str:
    return str(r.get("psc_code") or r.get("psc") or "")


def main() -> None:
    expected = json.load(open(f"{SCRATCH}/expected.json"))
    cids = json.load(open(f"{SCRATCH}/cids.json"))
    total_combos = sum(len(v) for v in expected.values())
    sca_codes, _, soc_codes, _, _, _ = M._vocabularies()
    sca_set, soc_set = set(sca_codes), set(soc_codes)

    present = [c for c in cids if os.path.exists(f"{RESULTS}/{c}.json")]
    absent = [c for c in cids if not os.path.exists(f"{RESULTS}/{c}.json")]
    defects = []
    combos_ok = play_t = play_f = offp = cat_total = 0
    soc_bad = sca_bad = fld_bad = 0
    cats_play, conf_c, role_c = [], Counter(), Counter()

    for cid in present:
        try:
            o = json.load(open(f"{RESULTS}/{cid}.json"))
        except Exception as e:  # noqa: BLE001
            defects.append((cid, f"parse:{str(e)[:30]}"))
            continue
        results = o.get("results") or []
        exp, got = set(expected[cid]), {_psc_of(r) for r in results}
        if exp - got:
            defects.append((cid, f"missing {len(exp - got)}"))
        if got - exp:
            defects.append((cid, f"extra {len(got - exp)}"))
        for r in results:
            if _psc_of(r) not in exp:
                continue
            combos_ok += 1
            isp, cats = bool(r.get("is_labor_play")), (r.get("categories") or [])
            if isp:
                play_t += 1
                cats_play.append(len(cats))
            else:
                play_f += 1
            for c in cats:
                cat_total += 1
                if any(k not in c for k in ("confidence", "role_class", "soc_code",
                                            "off_pattern", "sca_code")):
                    fld_bad += 1
                if c.get("soc_code") not in soc_set:
                    soc_bad += 1
                if c.get("sca_code") is not None and c.get("sca_code") not in sca_set:
                    sca_bad += 1
                role_c[c.get("role_class")] += 1
                conf_c[c.get("confidence")] += 1
                offp += bool(c.get("off_pattern"))

    pct = lambda a, b: f"{100 * a / b:.1f}%" if b else "n/a"  # noqa: E731
    print(f"FILES {len(present)}/{len(cids)}   absent {len(absent)}")
    print(f"COMBOS covered {combos_ok}/{total_combos}")
    print(f"DEFECTS {len(defects)}  {defects[:12]}")
    print(f"field-missing {fld_bad}   SOC-bad {soc_bad}   SCA-bad {sca_bad}")
    tot = play_t + play_f
    if tot:
        print(f"is_play {pct(play_t, tot)}   cats/play mean="
              f"{statistics.mean(cats_play):.2f}   off_pattern {pct(offp, cat_total)}")
        print(f"confidence high/med/low "
              f"{conf_c.get('high', 0)}/{conf_c.get('medium', 0)}/{conf_c.get('low', 0)}")
    if defects or absent or fld_bad or soc_bad or sca_bad:
        print("NOT clean — re-drive the listed cids and re-validate before assemble/retrieve.")


if __name__ == "__main__":
    main()
