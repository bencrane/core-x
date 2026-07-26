# demo_bakes — ONE-BUTTON derivation scripts for the ⌘B demo numbers

Every number on the gc-hq-new Explore rehearsal walk is computed by a script in this
directory. Methodology authority: `docs/reference/DEMO_NARRATIVE_BAKES.md`.

**ENCAPSULATION RULE (operator-ruled 2026-07-26):** these scripts are the single source
of truth for the demo's numbers. Any change to a method, factor, floor, or window is
made HERE and re-run — never as ad-hoc session queries. Baked TS artifacts in gc-hq-new
(`apps/platform-app/src/map/`) are OUTPUTS: regenerate, never hand-edit.

All scripts: `doppler run -p core-x -c prd -- python3 scripts/demo_bakes/<script>`
(run from this directory so `_shared.py` imports; `GC_HQ_APP` overrides the default
`~/Desktop/gc-hq-new` checkout for TS outputs).

| Script | Writes | Verified one-button |
|---|---|---|
| `bake_company_regions.py` | equipment_company_region_counties, equipment_company_demo_region (Lance) + demoRegions.ts | 2026-07-26 (reproduced 2,450 / 248,407) |
| `bake_macro_region_econ.py` | reference/macro_region_econ (Lance); macroEcon.ts values derive from it | 2026-07-26 |
| `bake_drill_demo.py` | drillDemo.ts (reads reference/equipment_flowdown_factors + demo_region_catalog) | 2026-07-26 (reproduced CO $64B/~$2.7B) |
| `bake_cost_structure.py` | reference/cost_structure_vectors, cost_structure_weighted, ecec_comp_components, equipment_flowdown_factors (Lance) | 2026-07-26 |
| `bake_industry_shape.py` | INDUSTRY_SHAPE block in rehearsal.ts (rewrites in place) | 2026-07-26 (parity-gated) |
| `bake_equipment_prone.py` | EQUIPMENT_PRONE block in rehearsal.ts (six-bucket narrowing page) | 2026-07-26 (post mapping-audit) |

Order when rebuilding everything: cost_structure → industry_shape → equipment_prone →
macro_region_econ → company_regions → drill_demo. Sidecar snapshot staleness moves numbers slightly between
rebuilds — that is expected; the artifact stamp rides every sidecar response.
