"""Bake — drill-region demo stats + archetype work orders (drillDemo.ts source; as-run 2026-07-26).

Grain: one entry per DISTINCT demo_region among active deals (seeded set below; extend the
DRILLS dict — state tier = state list, region tier = county FIPS from
reference/demo_region_catalog).

Per drill region:
  firms   — firms >=$500K FY23-25; median award over awards >=$250K (floor is a
            positioning choice: medians are ~3x any floor in this power-law book);
            book growth FY25/FY23 - 1; first-time = UEIs whose first in-region
            action >= 2024-10-01
  active  — award_geo_state, is_terminated=false AND current_end_date>=today: sum
            obligated, distinct UEIs; flow-down = per-award NAICS -> KLEMS industry ->
            reference/equipment_flowdown_factors.equipment_related_share (v1). NEVER a
            flat factor (gc-hq-new#168 fixed exactly that inconsistency).
  outlook — region share = region FY23-25 obligations / national; uplift = $785B OBBA x
            share x 0.40 ramp (v1 assumption, stated on-card as estimate)
  window  — active + uplift; equipment = total x region FY23-25 factor-weighted ratio
  orders  — 3 fixed archetypes (roads/newbuild/repair; combos below), real awards
            $25-250M in-region (fallback floor $5M when thin) via txn_events_combo;
            names bridge_sam_pdl -> entity_profile_gold; active counts
            contractor_award_summary.

Output: JSON to stdout in the drillDemo.ts shape — paste/generate into gc-hq-new
apps/platform-app/src/map/drillDemo.ts (keep its GENERATED header).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_drill_demo.py
"""
from __future__ import annotations

ARCH={
 "roads":   [("237310","Y1LB"),("237990","Y1KA"),("237990","Y1KB"),("237990","Y1PZ"),("237310","Z2LB")],
 "newbuild":[("236220","Y1JZ"),("236220","Y1PZ"),("236220","Y1AA"),("236220","Y1AZ"),("236220","Y1DA")],
 "repair":  [("236220","Z2JZ"),("236220","Z2AA"),("236220","Z1JZ"),("236220","Z2AZ")],
}
FACE={"roads":"Highway, road and bridge construction — earthmoving, grading, structures, paving.",
      "newbuild":"New construction of federal facilities — sitework, foundations, vertical build-out.",
      "repair":"Repair and modernization of existing federal facilities — structural, mechanical, sitework."}
# Seeded deals' drill regions (2026-07-26). Region-tier county lists come from
# reference/demo_region_catalog (demo_region -> county_fips).
DRILLS_STATE={"TX":["TX"],"CO":["CO"],"IN + MI":["IN","MI"]}
DRILLS_REGION=["southern california","northeast ohio","western TX","central california"]
OBBA=785e9; RAMP=0.40; FY=("DATE '2022-10-01'","DATE '2025-09-30'")

# Implementation intentionally mirrors the session run: see git history of
# gc-hq-new drillDemo.ts and core-x docs/reference/DEMO_NARRATIVE_BAKES.md.
# The full executable version of each stage exists in this file's sibling scripts;
# the queries are documented above and in the doc — an agent re-running this bake
# should compose them per region and emit the DRILL_DEMO record.

if __name__=="__main__":
    raise SystemExit("Doc-bearing skeleton: compose the documented queries per region "
                     "(see docstring + DEMO_NARRATIVE_BAKES.md) and regenerate drillDemo.ts.")
