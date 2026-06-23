# Full-Database Semantic Equipment Matchmaking — Run Report

**Date:** 2026-06-22
**SoR materialized:** `s3://data-sink/active/equipment_matchmaking/` (native Lance v2.1)
**Engine:** end-to-end agentic — 129 native Claude subagents (no Anthropic-API/SDK calls), deterministic Python for Lance/R2 I/O only.

## Inputs
- **Ground truth:** `s3://data-sink/active/reference/psc_equipment_mapping/` — 15 PSC codes × `required_equipment`.
- **Targets:** `s3://data-sink/active/equipment_catalog/` — 4,358 rows → **3,096 distinct `domain_norm`** with a populated `category_names` ∪ `equipment_item_names`.

## Pipeline (3 phases)
| Phase | Artifact | Role |
|---|---|---|
| A — extract/shard | [`scripts/extract_matchmaking_shards.py`](../scripts/extract_matchmaking_shards.py) | dedup by domain (richest row), filter to signal, 129 shards × 24 |
| B — agentic reasoning | [`scripts/mm_workflow.js`](../scripts/mm_workflow.js) | 129 subagents; per-domain signature-machine matchmaking + bouncer + verbatim grounding |
| C — materialize + gate | [`pipelines/gtm/materialize_equipment_matchmaking.py`](../pipelines/gtm/materialize_equipment_matchmaking.py) | deterministic grounding gate → Lance overwrite + BTREE/BITMAP |

Reasoning spend: 129 agents · 6.38M subagent tokens · 537 tool-uses · ~19.8 min wall-clock · 0 failed shards.

## Output schema
`domain_norm` (PK, BTREE) · `supported_pscs` LIST\<VARCHAR\> · `verified_inventory_matches` LIST\<VARCHAR\> (verbatim catalog strings) · `justification_payload` VARCHAR (compact JSON) · `matched_psc_count` INT32 (BITMAP) · `materialized_at` TS.

## Grounding gate (adversarial, deterministic)
Every `verified_inventory_match` must resolve to the domain's real scraped catalog under normalized bidirectional containment; ungrounded strings are dropped, and any PSC match whose cited inventory is *entirely* ungrounded is voided.
- **7** ungrounded strings dropped across **6** domains (**0.06%** hallucination rate).
- **0** matches voided · **0** unknown PSC codes · **0** domains missing a verdict (full 3,096 coverage).

## Results
- **3,096** evaluated · **1,452 matched (46.9%)** · **1,644 bouncer-rejected (53.1%)**.
- Rejections are the load-bearing result: parts/reman vendors, event/party rental, AV/stage-production, survey/NDT instrument houses, stationary processing-line dealers, and homeowner hand-tool shops — each returns `[]`.

### PSC demand coverage (how many of the 3,096 yards can serve each code)
| Code | PSC | Yards | % |
|---|---|---:|---:|
| Y1PZ | Other Non-Building Facilities | 924 | 29.8% |
| Y1DA | Hospital Construction | 855 | 27.6% |
| Z2AA | Office Repair/Alteration | 734 | 23.7% |
| Z2DA | Hospital Repair/Alteration | 670 | 21.6% |
| P400 | Demolition | 660 | 21.3% |
| F108 | Environmental Remediation | 639 | 20.6% |
| Y1PC | Unimproved Land / Site Prep | 496 | 16.0% |
| Z2KA | Dam / Dredging Repair | 414 | 13.4% |
| Y1LB | Highway Construction | 313 | 10.1% |
| Y1NE | Water Supply Facilities | 305 | 9.9% |
| Z1DA | Hospital Maintenance | 231 | 7.5% |
| Z1LB | Highway Maintenance | 228 | 7.4% |
| Y1KD | Mine Subsidence Control | 226 | 7.3% |
| F014 | Tree Thinning | 214 | 6.9% |
| Z1KF | Dredging Maintenance | 96 | 3.1% |

Curve is correct: broad-requirement codes (Y1PZ/Y1DA) top out; marine/specialist codes (Z1KF) are scarce.

### Perfect-coverage yards (15/15) — national full-line fleets
`unitedrentals.com`, `hercrentals.com`, `catrentalstore.com`, `fabickcat.com`, `foleyeq.com`, `quinncompany.com`, `stowerscat.com`, `wyomingcat.com`, `airportequipmentrentals.com`, `accessrentalsllc.com` — face-valid (the actual national/Cat-dealer rental networks).

## Provenance
Consolidated per-domain verdicts: [`reports/equipment_matchmaking_verdicts.jsonl`](equipment_matchmaking_verdicts.jsonl) (3,096 rows). Shard inputs + raw agent outputs (`reports/mm_shards/`, `reports/mm_out/`) are regenerable scratch (gitignored).

## Calibration note
The distributed agents apply Rule 1 ("≥1 signature machine") slightly more inclusively than the hand-tuned 12-firm sample pass ([`psc_equipment_matchmaking_2026-06-22.md`](psc_equipment_matchmaking_2026-06-22.md)) — e.g. a yard with mini excavators + smooth-drum rollers matches earthmoving codes a stricter human pass screened out. This is faithful to the directive's literal rule, and all cited inventory is grounding-verified. To tighten precision, raise the threshold (require ≥2 signature classes, or weight by machine tonnage/count) — but that is a policy change, not a correctness fix.
