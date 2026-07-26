# demo_bakes — as-run derivation scripts for the ⌘B demo numbers (2026-07-26)

Extracted from the authoring session, deterministic, doppler-cred'd (`-p core-x -c prd`).
Each script owns the mart(s) it writes; the methodology authority is
`docs/reference/DEMO_NARRATIVE_BAKES.md`. Order matters only where noted.

| Script | Writes |
|---|---|
| `bake_company_regions.py` | equipment_company_region_counties + demo/macro assignment (+ demoRegions.ts) |
| `bake_macro_region_econ.py` | reference/macro_region_econ (+ macroEcon.ts) |
| `bake_drill_demo.py` | drill firms/active/outlook/window/work-orders (+ drillDemo.ts) |
| `bake_cost_structure.py` | reference/cost_structure_vectors, cost_structure_weighted, ecec_comp_components, equipment_flowdown_factors |
| `bake_industry_shape.py` | INDUSTRY_SHAPE sector rollup (rehearsal.ts values) |

These are faithful records of what ran — refactor before extending, but any rerun must
preserve the parity gates documented in each script's docstring.
