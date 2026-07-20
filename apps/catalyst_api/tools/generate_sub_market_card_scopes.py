#!/usr/bin/env python3
"""Generate ``src/sub_market_card_scopes.json`` — the sub-under market card scopes.

The sub-market cards (gc Markets, subcontract lane) scope each card as an
explicit set of PRIME award combos (naics6 × psc4) — the demand-side markets
subawardees sub under. Authority: the Phase-0 composition
``~/Desktop/hq/data-cache/submarkets/sub_market_cards_v1.json`` (family rules →
$-ranked pair lists at ≥99% family coverage, measured + reconciled at the
stamped artifact; adversarially verified by independent query shapes).

This tool copies the SCOPE fields (pairs, titles, members_are, family_rule,
reconciliation) — never the measured anatomy, which the router computes live.

Re-run (this machine): python3 tools/generate_sub_market_card_scopes.py
"""
from __future__ import annotations

import json
from pathlib import Path

SRC = Path.home() / "Desktop/hq/data-cache/submarkets/sub_market_cards_v1.json"
OUT = Path(__file__).resolve().parent.parent / "src" / "sub_market_card_scopes.json"


def main() -> None:
    src = json.loads(SRC.read_text())
    cards = {}
    for slug, c in src["cards"].items():
        cards[slug] = {
            "title": c["title"],
            "members_are": c["members_are"],
            "family_rule": c["family_rule"],
            "pairs": sorted(map(tuple, c["pairs"])),
            "verified": {
                "n_pairs": c["n_pairs"],
                "pair_coverage_pct": c["reconciliation"]["pair_coverage_pct"],
                "sub_usd": c["anatomy"]["sub_usd"],
                "n_subs": c["anatomy"]["n_subs"],
                "artifact": src["artifact"],
            },
            # v1.1 cohort segments: named pair-subsets (work clusters) with
            # dominance membership + deterministic vetted-mart language.
            "cohorts": {
                k: {
                    "family_name": co["family_name"],
                    "one_liner": co["one_liner"],
                    "language_phrases": co["language_phrases"],
                    "pairs": sorted(map(tuple, co["pairs"])),
                    "share_of_card_pct": co["share_of_card_pct"],
                    "stats": co["stats"],
                }
                for k, co in c.get("cohorts", {}).items()
            },
        }
    OUT.write_text(json.dumps(
        {"generated_from": "hq/data-cache/submarkets/sub_market_cards_v1.json",
         "window": src["window"], "definition": src["definition"], "cards": cards},
        indent=1, sort_keys=True) + "\n")
    print(f"wrote {OUT}: {len(cards)} cards, "
          f"{sum(len(c['pairs']) for c in cards.values())} pairs")


if __name__ == "__main__":
    main()
