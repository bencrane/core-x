"""Designation / subcontracting-plan detector over solicitation text.

Loads `reference/designation_lexicon.json` and applies boundary-aware regex with
proximity-negation. Pure stdlib (json, re) — no deps, no network. Matches are
solicitation REFERENCES (opp_text_ref__<stem>), not firm attributes; presence != truth.

Self-test (the Phase-1 regex-mechanics gate):
    python3 pipelines/sam_gov/designation_match.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

LEXICON_PATH = Path(__file__).parent / "reference" / "designation_lexicon.json"


def _compile(patterns, flags=re.IGNORECASE):
    return re.compile("|".join(f"(?:{p})" for p in patterns), flags)


def load(path=LEXICON_PATH):
    lex = json.loads(Path(path).read_text())
    mp = lex["match_policy"]
    sp = lex["subcontracting_plan"]
    comp = {
        "policy": mp,
        "neg": _compile(mp["negation_cues"]),
        "stems": [(d["stem"], d["short"], _compile(d["patterns"])) for d in lex["designations"]],
        "setaside_ref": _compile(lex["set_aside_reference"]["patterns"]),
        "subk_clause": _compile(sp["clause_present"]["patterns"]),
        "subk_submit": _compile(sp["section_l_submit"]["patterns"]),
        "subk_support": _compile(sp["support_clauses"]["patterns"]),
        "subk_pct": _compile([sp["goals_table"]["percent_pattern"]]),
        "subk_goaltok": _compile([sp["goals_table"]["goal_token_pattern"]]),
        "goals_prox": sp["goals_table"]["goals_proximity_chars"],
    }
    return lex, comp


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _mid(a, b):
    return (a + b) // 2


def detect(text: str, comp: dict) -> dict:
    """Return {designations: [{stem, short, start, end, context, snippet}], subcontracting: {...}}.
    context ∈ {binding, listed, negated}. Offsets are in whitespace-normalized space."""
    t = _norm(text)
    pol = comp["policy"]
    negwin = pol["negation_window_chars"]
    neg_mids = [_mid(m.start(), m.end()) for m in comp["neg"].finditer(t)]

    hits = []
    for stem, short, rx in comp["stems"]:
        for m in rx.finditer(t):
            s, e = m.start(), m.end()
            c = _mid(s, e)
            negated = any(abs(c - nm) <= negwin for nm in neg_mids)
            hits.append({"stem": stem, "short": short, "start": s, "end": e,
                         "context": "negated" if negated else "binding",
                         "snippet": t[max(0, s - 40):e + 40]})

    # "listed" overlay: a cluster of >= N distinct stems in a window is a clause-matrix
    # enumeration, not a binding set-aside — downgrade (unless already negated).
    lw, lmin = pol["listed_window_chars"], pol["listed_min_distinct_stems"]
    for h in hits:
        if h["context"] == "negated":
            continue
        c = _mid(h["start"], h["end"])
        near = {x["stem"] for x in hits if abs(_mid(x["start"], x["end"]) - c) <= lw // 2}
        if len(near) >= lmin:
            h["context"] = "listed"

    pct_mids = [_mid(m.start(), m.end()) for m in comp["subk_pct"].finditer(t)]
    tok_mids = [_mid(m.start(), m.end()) for m in comp["subk_goaltok"].finditer(t)]
    prox = comp["goals_prox"]
    goals = any(abs(p - tk) <= prox for p in pct_mids for tk in tok_mids)

    subk = {
        "clause_present": bool(comp["subk_clause"].search(t)),
        "section_l_submit": bool(comp["subk_submit"].search(t)),
        "support_clause": bool(comp["subk_support"].search(t)),
        "goals_table": goals,
    }
    subk["required"] = subk["clause_present"] and (subk["goals_table"] or subk["section_l_submit"])
    return {"designations": hits, "subcontracting": subk}


# ─────────────────────────── self-test (Phase-1 gate) ───────────────────────────

def _stems(res, ctx=None):
    return {h["stem"] for h in res["designations"] if ctx is None or h["context"] == ctx}


def _selftest() -> int:
    _, comp = load()
    fails = []

    def check(name, cond):
        if not cond:
            fails.append(name)
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")

    # A — single binding set-aside
    a = detect("This acquisition is a 100% Service-Disabled Veteran-Owned Small Business "
               "(SDVOSB) set-aside under FAR 52.219-27.", comp)
    check("A: SDVOSB present", "service_disabled_veteran_owned_business" in _stems(a))
    check("A: SDVOSB binding (not listed/negated)",
          "service_disabled_veteran_owned_business" in _stems(a, "binding"))
    check("A: SDVOSB did not double-fire VOSB", "veteran_owned_business" not in _stems(a))

    # B — negated enumeration
    b = detect("This requirement is unrestricted and full and open. The following set-aside "
               "programs do not apply: HUBZone, SDVOSB, WOSB, and 8(a).", comp)
    for stem in ("historically_underutilized_business_zone_hubzone_firm",
                 "service_disabled_veteran_owned_business",
                 "women_owned_small_business", "c8a_program_participant"):
        check(f"B: {stem} negated", stem in _stems(b, "negated"))

    # C — subcontracting plan required (clause + submit + goals)
    c = detect("The Contractor shall submit a Small Business Subcontracting Plan in accordance "
               "with FAR 52.219-9. Goals: Small Business 25.0%, SDVOSB 3%, HUBZone 3%, WOSB 5%.", comp)
    check("C: clause_present", c["subcontracting"]["clause_present"])
    check("C: section_l_submit", c["subcontracting"]["section_l_submit"])
    check("C: goals_table", c["subcontracting"]["goals_table"])
    check("C: required==True", c["subcontracting"]["required"])

    # D — acronym collision guard
    d = detect("Deliver to Building 8A, Room 210. The SDB sample must be returned. "
               "Veteran status is not required for delivery personnel.", comp)
    check("D: 8(a) NOT fired on 'Building 8A'", "c8a_program_participant" not in _stems(d))
    check("D: SDB NOT fired on bare 'SDB'", "small_disadvantaged_business" not in _stems(d))
    check("D: VOSB NOT fired on 'Veteran status'", "veteran_owned_business" not in _stems(d))

    # E — clause-matrix listing -> 'listed', not binding
    e = detect("Set-aside categories referenced in Section L include: Small Disadvantaged "
               "Business, HUBZone, Service-Disabled Veteran-Owned Small Business, Women-Owned "
               "Small Business, and 8(a).", comp)
    check("E: >=1 designation marked 'listed'", len(_stems(e, "listed")) >= 1)
    check("E: none spuriously negated", len(_stems(e, "negated")) == 0)

    print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(_selftest())
