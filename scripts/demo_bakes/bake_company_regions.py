"""Bake — equipment-company region derivation chain (as-run 2026-07-26).

Chain (each stage's output is landed in Lance; re-running any stage rebuilds downstream):
  1. Universe: equipment_provider (domain 'yes' verdicts) + equipment_yard_profile
     (UEI 'yes' verdicts; domains from equipment_yard_website_research payload URLs,
     normalized: strip scheme/www/path). Dedup by domain -> 2,566; market set = <500
     employees or unknown (Clay-enrich > Blitz > PDL > Clay-find size precedence) -> 2,450.
  2. Service areas: RAW strings only (equipment_provider_service_areas +
     equipment_yard_service_areas payload serviceAreas[].parsed). The flattened
     service_states columns are ruled UNRELIABLE — never derive from them.
  3. County derivation (equipment_company_region_counties; basis column records path):
     - state-scale entries (state literally named as territory; DC rule: 'washington'
       with MD/VA context and no OR/ID context -> DC) -> state counties
     - state-region entries -> reference/state_region_county_map (operator-authored 91)
     - cities -> geocode_xwalk (city,state)->zip5 -> census_zcta_county_rel_2020;
       zips direct; county names matched in-state; 'N miles' radius -> counties with
       centroid within N (gazetteer)
     - footprint <=2 counties -> +1 census_county_adjacency ring (in-doubt-is-IN)
     - unresolved -> HQ fallback (clay_enrich locations > firmographics_blitz >
       clay_find hq city/state) + ring
  4. Demo assignment (equipment_company_demo_region): >=80%% of counties in one canonical
     region (reference/demo_region_catalog) -> region; else state; else multi-state;
     macro = smallest reference/macro_region_catalog containing the states.
  5. demoRegions.ts (gc-hq-new): per-domain macro+drill labels + sketch circles
     (centroid + 1.15 x max distance, clamped 60..900mi).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_company_regions.py
(doc-bearing skeleton: stages are landed + documented; recompose from
DEMO_NARRATIVE_BAKES.md when rerunning.)
"""
if __name__=="__main__":
    raise SystemExit("Doc-bearing skeleton — see docstring + DEMO_NARRATIVE_BAKES.md.")
