#!/usr/bin/env python3
"""Generate ``src/capital_card_scopes.json`` — the capital-card scope OVERLAY.

The capital card catalog (30 cards, gc Markets tab; durable record
``~/Desktop/hq/data-cache/capital/card_catalog_v1.json`` v1.4) scopes each card
as a (NAICS6, PSC4) pair-set. The 22 canonical cards resolve their BASE pairs
from hqx pg ``gtm.market_collections`` at runtime (the standing authority —
market_slice_v1 reads it exactly as market_collections_v1 does). Everything pg
does NOT carry lives in this overlay:

  extensions   the 63 uncovered pairs re-homed into 4 canonical cards
               (catalog ``corrections_v1_3.extensions``, verbatim)
  cards        pair-sets for the 6 non-pg cards:
               - 5 uncovered-sweep cards, derived from
                 ``uncovered_sweep_v1.json`` by the cluster rules below and
                 VERIFIED against the baked card numbers (see each card's
                 ``verified`` block: live sidecar replay reproduced the
                 fixture's firms/unfin_B at artifact 20260718T021418Z)
               - training-education-services (catalog ``new_card``, verbatim)
               and the 2 staffing-lens cards (non-additive cross-cut views)
               snapshotted from Lance ``staffing_market_collections``.

Sweep-card derivation rules (pair ∈ uncovered_pairs, i.e. in NO collection):
  federal-it-solutions            NAICS4 ∈ {5415,5182,5171,5132} × PSC D*
  applied-research-development    NAICS4 ∈ {5417,5413,5415,5416} × PSC A*
  architect-engineering-services  NAICS4 = 5413 × PSC C*
  facility-maintenance-repair     NAICS2 = 23 or NAICS4 ∈ {5612,5617} × PSC Z*
  utility-services                NAICS3 = 221 × PSC S*
Rule identification: pair-count + dollar subset-sum against the baked catalog
(exact matches), then live sidecar reconciliation of the resulting cohorts.

KNOWN DELTA (disclosed, unresolved): facility-maintenance-repair's exact
bake-time base pair list was never persisted; the derived rule reproduces the
baked pair COUNT (96) but the live cohort (base ∪ extensions) measures
956 firms / $6.157B vs the baked 939 / $6.011B (+1.8% firms, +2.4% $) at the
same pinned artifact — the true base is a proper subset that cannot be uniquely
recovered from the durable records. Every other card (29/30) reconciles
exactly. If the operator re-bakes the catalog, regenerate and re-verify.

Re-run (this machine): doppler run -p core-x -c prd -- python3 tools/generate_capital_card_scopes.py
Inputs are the hq vault caches + Lance; output is committed (schema-as-code).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

HQ = Path.home() / "Desktop/hq/data-cache/capital"
OUT = Path(__file__).resolve().parent.parent / "src" / "capital_card_scopes.json"

NAMES = {
    "federal-it-solutions": "IT Solutions & Services",
    "applied-research-development": "Applied Research & Development",
    "architect-engineering-services": "Architecture & Engineering Design",
    "facility-maintenance-repair": "Facility Maintenance & Repair",
    "utility-services": "Utility Services",
    "training-education-services": "Training & Education Services",
    "finance-accounting-staffing": "Finance & Accounting Services",
    "logistics-supply-chain-staffing": "Logistics & Supply-Chain Services",
}

SWEEP_RULES = {
    "federal-it-solutions": lambda n, p: n[:4] in ("5415", "5182", "5171", "5132") and p[0] == "D",
    "applied-research-development": lambda n, p: n[:4] in ("5417", "5413", "5415", "5416") and p[0] == "A",
    "architect-engineering-services": lambda n, p: n[:4] == "5413" and p[0] == "C",
    "facility-maintenance-repair": lambda n, p: (n[:2] == "23" or n[:4] in ("5612", "5617")) and p[0] == "Z",
    "utility-services": lambda n, p: n[:3] == "221" and p[0] == "S",
}


def main() -> None:
    catalog = json.loads((HQ / "card_catalog_v1.json").read_text())
    sweep = json.loads((HQ / "uncovered_sweep_v1.json").read_text())
    uncovered = [(str(n), str(p)) for n, p, *_ in sweep["uncovered_pairs"]]

    extensions = {
        slug: {"pairs": sorted((str(n), str(p)) for n, p in blk["pairs"]), "why": blk["why"]}
        for slug, blk in catalog["corrections_v1_3"]["extensions"].items()
    }

    cards: dict[str, dict] = {}
    for slug, rule in SWEEP_RULES.items():
        pairs = sorted({(n, p) for n, p in uncovered if rule(n, p)})
        fx = catalog["cards"][slug]
        verified: dict = {"n_pairs": fx["n_pairs"], "firms_unfin": fx["firms_unfin"],
                          "unfin_B": fx["unfin_B"], "artifact": catalog["artifact"]}
        if slug == "facility-maintenance-repair":
            verified["known_delta"] = (
                "live base∪ext = 956 firms/$6.157B vs baked 939/$6.011B; "
                "true base pair list unrecoverable from durable records (see tool docstring)"
            )
        cards[slug] = {
            "title": NAMES[slug],
            "lens": "uncovered-sweep",
            "pairs": pairs,
            "verified": verified,
        }
        assert len(pairs) == fx["n_pairs"], f"{slug}: {len(pairs)} != {fx['n_pairs']}"

    te_slug, te = next(iter(catalog["corrections_v1_3"]["new_card"].items()))
    cards[te_slug] = {
        "title": NAMES[te_slug],
        "lens": "canonical",
        "pairs": sorted((str(n), str(p)) for n, p in te["pairs"]),
        "verified": {"firms_unfin": catalog["cards"][te_slug]["firms_unfin"],
                     "unfin_B": catalog["cards"][te_slug]["unfin_B"],
                     "artifact": catalog["artifact"]},
    }

    # staffing-lens cross-cut cards: snapshot from Lance
    import lance  # noqa: PLC0415

    storage = {
        "aws_access_key_id": os.environ.get("R2_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.environ.get("R2_SECRET_ACCESS_KEY", ""),
        "aws_endpoint": os.environ.get("R2_ENDPOINT", ""),
        "aws_region": "auto",
    }
    ds = lance.dataset("s3://data-sink/active/staffing_market_collections", storage_options=storage)
    t = ds.to_table().to_pylist()
    for slug in ("finance-accounting-staffing", "logistics-supply-chain-staffing"):
        pairs = sorted({(r["naics_code"], r["psc_code"]) for r in t if r["slug"] == slug})
        fx = catalog["cards"][slug]
        cards[slug] = {
            "title": NAMES[slug],
            "lens": "staffing",
            "pairs": pairs,
            "verified": {"firms_unfin": fx["firms_unfin"], "unfin_B": fx["unfin_B"],
                         "artifact": catalog["artifact"]},
        }

    out = {
        "generated_from": "hq/data-cache/capital catalog v1.4 + uncovered_sweep_v1 + Lance staffing_market_collections",
        "catalog_artifact": catalog["artifact"],
        "extensions": extensions,
        "cards": cards,
    }
    OUT.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    n_ext = sum(len(e["pairs"]) for e in extensions.values())
    print(f"wrote {OUT}: {len(cards)} overlay cards, {n_ext} extension pairs across {len(extensions)} canonical cards")


if __name__ == "__main__":
    main()
