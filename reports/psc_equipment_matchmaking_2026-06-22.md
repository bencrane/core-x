# Semantic Equipment Matchmaking — 12-Firm Sample

**Date:** 2026-06-22
**PSC dictionary:** `s3://data-sink/active/reference/psc_equipment_mapping/` — 15 rows (ground truth)
**Yard catalogs:** `s3://data-sink/active/equipment_catalog/` — 4,358 rows · 3,096 distinct domains w/ inventory signal
**Sample:** 12 distinct `domain_norm`, all `confidence=high`, ranked by inventory depth
**Match rule:** semantic intersection of yard (`category_names ∪ equipment_item_names`) with each PSC `required_equipment`; a PSC is included when the yard stocks ≥1 *signature* machine for it (incidental-only overlaps excluded to suppress false positives). Match strength (STRONG / MODERATE / WEAK / LOW) is flagged inline in the JSON justifications.

Full structured verdicts (exact requested schema): [`psc_equipment_matchmaking_2026-06-22.json`](psc_equipment_matchmaking_2026-06-22.json)
Reproducible Step-2 extractor: [`scripts/_sample_equipment_catalog.py`](../scripts/_sample_equipment_catalog.py)

---

## Summary

| # | Domain | Yard archetype | # PSCs | Matched PSC codes |
|---|--------|----------------|:------:|-------------------|
| 1 | a2zrentals.com | Light/medium general yard | 3 | Z2AA, Z2DA, P400 |
| 2 | exiusa.com | Geophysical/NDT instruments | 0 | — |
| 3 | eswagner.com | Heavy-civil contractor fleet | 12 | Y1DA, Y1LB, Z1LB, Y1PC, Y1NE, Y1KD, Y1PZ, Z2KA, Z1KF, P400, F108, F014 |
| 4 | unitedrentals.com | National full-line catalog | 15 | ALL 15 |
| 5 | accessrentalsllc.com | General contractor yard | 12 | Z2AA, Y1DA, Z1DA, Z2DA, Y1PC, Y1NE, Y1PZ, Z2KA, Z1KF, P400, F108, F014 |
| 6 | brookhollowrental.com | Light homeowner/contractor | 4 | Z2AA, Z1DA, Z2DA, P400 |
| 7 | statelinemachine.com | Heavy-equip PARTS vendor | 0 | — |
| 8 | seawayrentalcorp.com | General rental yard | 10 | Z2AA, Y1DA, Z1DA, Z2DA, Y1PC, Y1NE, Y1PZ, P400, F108, F014 |
| 9 | lps-inc.com | Sawmill machinery dealer | 1 | F014 (LOW) |
| 10 | partyrentaltx.com | Party/event rental | 0 | — |
| 11 | nimblems.com | Broad construction marketplace | 15 | ALL 15 |
| 12 | creativesoundandlighting.com | Event AV/production | 0 | — |

---

## Per-firm notes

**1 · a2zrentals.com** — Mini excavators + skid steers (66/72") + breakers + compaction rollers + gens + light towers. Serves office reno (Z2AA), hospital reno/interior demo (Z2DA), and building demolition (P400). No aerial lifts, no large iron → screened out of heavy-civil PSCs.

**2 · exiusa.com** — `[]`. Subsurface-imaging instrument house (GPR, magnetometers, seismic, LiDar, resistivity). Sensors, not construction iron; not even the construction-survey class (total stations / GPS rovers) is present.

**3 · eswagner.com** — Heavy-civil powerhouse: crawler + RT cranes, D4–D10 dozers, excavators to 375L, graders, scrapers, pavers, rollers, water trucks, caisson/pile drills, marine barge. Matches 12 PSCs (everything except the three aerial/interior-light-iron codes Z2AA/Z1DA/Z2DA). Strongest highway (Y1LB), land (Y1PC), mine-subsidence (Y1KD, drill rigs present), and dam/dredge (Z2KA, marine+pile fleet) coverage in the sample.

**4 · unitedrentals.com** — National full-line catalog with aerial + chillers + earthmoving + cranes + traffic. **Clean sweep of all 15 PSCs**, mostly STRONG with exact-name coverage. The canonical superset yard.

**5 · accessrentalsllc.com** — Broad general yard (aerial, telehandlers, track loaders, mini-ex, dozers, grapples/breakers, dewatering pumps, AC/chiller-class). 12 PSCs. Screened out of the highway codes (Y1LB/Z1LB — no graders/pavers/water trucks) and mine-subsidence (Y1KD — no drill rig).

**6 · brookhollowrental.com** — Light yard with tow-behind bucket (boom) lifts, scissor lift, Bobcat skid/mini-ex + breaker. Serves the building-interior cluster (Z2AA, Z1DA, Z2DA, P400). Too light for earthmoving/heavy-civil PSCs.

**7 · statelinemachine.com** — `[]`. Wear-parts & reman-component vendor (teeth, edges, undercarriage, hydraulic pumps, rubber tracks). Category labels name graders/pavers/sweepers but carry *parts*, not deployable machines. Correctly rejected — this is the classic false-positive a naive keyword match would trip on.

**8 · seawayrentalcorp.com** — General yard: trackhoes (32–40HP), 71HP CAT dozer, backhoes, skid steers, telescopic forklifts, towable boom + scissor lifts, brush cutter. 10 PSCs. Screened out of highway, mine, and dam/dredge (no graders/pavers, no drill rigs, no marine/long-reach).

**9 · lps-inc.com** — Sawmill / wood-processing machinery dealer; `equipment_item_names` empty (category-level only). Single **LOW** F014 adjacency on "Harvesters and Processors / Hogs and Wood Grinders / Wood Chipper – Mobile" vs Forestry Mulchers. Surfaced as a semantic adjacency, not a confirmed deployable match — stationary mill lines, not field rental iron.

**10 · partyrentaltx.com** — `[]`. Tables, chairs, tents, catering, AV. Zero construction equipment.

**11 · nimblems.com** — Full United-style construction marketplace (aerial, earthmoving, cranes, chillers, crushers/screens, grade control). **All 15 PSCs.** Second clean-sweep yard in the sample.

**12 · creativesoundandlighting.com** — `[]`. Event AV/staging house. Its logistics tail (generators, fencing, barricades, temporary facilities) is event-grade, not construction iron — non-qualifying.

---

## Engine read-out

- **Clean sweeps (15/15):** `unitedrentals.com`, `nimblems.com` — full-line catalogs / marketplaces.
- **Heavy-civil specialist (12):** `eswagner.com` — the highest-value federal-construction match in the sample (owns the rare signature iron: drill rigs, pile hammers, marine, graders, pavers).
- **General yards (10–12):** `accessrentalsllc.com`, `seawayrentalcorp.com` — broad but screened out of pavers/graders/drill-rig PSCs.
- **Building-interior light yards (3–4):** `a2zrentals.com`, `brookhollowrental.com` — office/hospital reno + demolition only.
- **Correct zero-matches (4):** `exiusa.com` (geophysical instruments), `statelinemachine.com` (parts vendor), `partyrentaltx.com` (event), `creativesoundandlighting.com` (AV) — each a domain a keyword matcher would mis-fire on; the signature-machine threshold rejects them.
- **Edge case (1):** `lps-inc.com` — sawmill dealer, single LOW F014 adjacency flagged, not asserted.
