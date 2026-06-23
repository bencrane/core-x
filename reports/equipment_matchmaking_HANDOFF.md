# Equipment Matchmaking — Handoff & Audit Brief

> For the next agent / reviewer. Self-contained: explains what was built, where it lives, how to
> query it, real worked examples, how to independently verify it, and where the soft spots are.

---

## 0. TL;DR

A new Lance dataset maps **3,096 scraped equipment-rental-yard websites** to the **15 federal
construction PSC codes** each yard can credibly serve. For every yard it records *which* PSCs,
*which exact machines* triggered each match, and *why*.

- **System of record:** `s3://data-sink/active/equipment_matchmaking/` (native Lance v2.1, R2)
- **3,096 domains evaluated · 1,452 matched (47%) · 1,644 rejected (53%)**
- **Reasoning was done by 129 Claude subagents, not an API script** (that was the explicit ask).
- **Every cited machine is verified against the yard's real catalog** — measured hallucination
  rate **0.06%** (7 strings out of ~11k dropped).
- Shipped in PR [#629](https://github.com/bencrane/core-x/pull/629) (merged, commit `f006f62`).

> **Update 2026-06-23:** the PSC dictionary was expanded **15 → 19 codes** (added Z1AA, Y1AA, Y1JZ, Z2JZ —
> office + misc-building work) and the engine re-run. Current state: **1,467 matched** domains;
> `matched_psc_count` now ranges 0–19. Mechanics below are unchanged — only the dictionary grew. See
> `docs/reference/GOVCON_EQUIPMENT_RENTAL_GOLDEN_OVERLAP.md §8` for the uplift.

**Is it good?** The bouncer makes sharp, defensible calls (see §5–6). The one judgment knob you
might disagree with is **how inclusive a "match" is** — the agents follow the literal rule
("yard stocks ≥1 signature machine for the PSC"), which is broader than a hand-tuned analyst would
be. That's a *policy* choice, not a bug. See §7.

---

## 1. What problem this solves

Upstream we have:
- `active/reference/psc_equipment_mapping/` — 15 PSC codes, each with a `required_equipment` list
  (e.g. `Y1LB Construction of Highways` needs Motor Graders, Smooth Drum Compactors, Pavers, …).
- `active/equipment_catalog/` — scraped catalogs for thousands of company domains
  (`category_names`, `equipment_item_names`).

This dataset is the **join**: for each yard, which federal construction contract categories could
it supply equipment for? That powers the Equipment-Rental GTM motion (target the right yards for
the right USAspending PSC demand).

---

## 2. Where everything lives

| Thing | Path |
|---|---|
| **Lance SoR** | `s3://data-sink/active/equipment_matchmaking/` |
| Phase A — extract & shard | `scripts/extract_matchmaking_shards.py` |
| Phase B — agentic harness | `scripts/mm_workflow.js` (Claude Code Workflow) |
| Phase C — grounding gate + materialize | `pipelines/gtm/materialize_equipment_matchmaking.py` |
| Consolidated verdicts (committed provenance) | `reports/equipment_matchmaking_verdicts.jsonl` (3,096 rows) |
| Run report | `reports/equipment_matchmaking_2026-06-22.md` |
| 12-firm hand-validation (the earlier sample) | `reports/psc_equipment_matchmaking_2026-06-22.md` |
| Scratch (regenerable, **gitignored**) | `reports/mm_shards/`, `reports/mm_out/` |

---

## 3. The data model

| Column | Type | Meaning |
|---|---|---|
| `domain_norm` | VARCHAR (PK, BTREE) | normalized yard domain, e.g. `unitedrentals.com` |
| `supported_pscs` | LIST\<VARCHAR\> | matched PSC codes, e.g. `["Y1LB","Z2AA"]`; `[]` if rejected |
| `verified_inventory_matches` | LIST\<VARCHAR\> | **verbatim** catalog strings that triggered matches |
| `justification_payload` | VARCHAR | compact JSON: `{archetype, verdict, rejected_reason?, per_psc{}}` |
| `matched_psc_count` | INT32 (BITMAP) | `len(supported_pscs)` — fast filter for "full-line" vs "specialist" |
| `materialized_at` | TIMESTAMP | lineage |

**The 15 PSC codes** (from `psc_equipment_mapping`):
`Z2AA` office reno · `Y1DA` hospital constr · `Z1DA` hospital maint · `Z2DA` hospital reno ·
`Y1LB` highway constr · `Z1LB` highway maint · `Y1PC` land/site prep · `Y1NE` water supply ·
`Y1KD` mine subsidence · `Y1PZ` other non-building · `Z2KA` dam/dredge repair · `Z1KF` dredge maint ·
`P400` demolition · `F108` env remediation · `F014` tree thinning.

### How to query it (copy-paste)

```python
import lance, os
so = {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
      "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
      "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
      "region": "auto"}
ds = lance.dataset("s3://data-sink/active/equipment_matchmaking/", storage_options=so)

# One yard
ds.scanner(filter="domain_norm = 'unitedrentals.com'").to_table().to_pylist()

# All yards that can serve highway construction (BITMAP-accelerated prefilter, then list check)
import pyarrow.compute as pc
t = ds.scanner(filter="matched_psc_count > 0").to_table()
y1lb = [r for r in t.to_pylist() if "Y1LB" in r["supported_pscs"]]
```

Run anything with creds via: `doppler run -p core-x -c prd -- python3 yourscript.py`.
Or just read `reports/equipment_matchmaking_verdicts.jsonl` directly (no R2 needed) — same rows.

---

## 4. How it was built (and why this way)

The directive forbade "a standard Python script that hits the Anthropic API." So:

1. **Phase A (Python, deterministic):** dedup catalog to 3,096 distinct domains (keep richest row),
   keep only those with a populated `category_names`/`equipment_item_names`, sort, cut into
   **129 shards of 24 domains**.
2. **Phase B (agentic):** a Claude Code **Workflow** spawned **129 subagents**, one per shard.
   Each agent got the 15-PSC dictionary + matching rules + a "bouncer" spec, reasoned over its 24
   domains, and wrote a verdict file. **No API key, no `anthropic` SDK — native Claude reasoning.**
3. **Phase C (Python, deterministic):** load all verdicts, run the **grounding gate**, write Lance
   + BTREE/BITMAP indexes + the consolidated JSONL.

**The grounding gate is the key correctness mechanism.** Agents were told to copy machine names
*verbatim* from the catalog. Phase C re-checks every `verified_inventory_match` against the
domain's actual scraped catalog (normalized, bidirectional containment). Ungrounded strings are
dropped; if *all* of a yard's cited machines for its matches are ungrounded, the match is **voided**.
Outcome: 7 dropped / 6 domains (**0.06%**), 0 voided. Translation: the agents almost never invented
equipment, and the pipeline would have caught them if they had.

---

## 5. Worked examples (real records from the dataset)

**Full-line Cat dealer — matches all 15 (`catrentalstore.com`):**
> `per_psc.Y1LB`: "Motor Graders + Compactors + Wheel Loaders + Water Trucks + Pavers; STRONG."
> `per_psc.F014`: "Mulchers + Track Loaders + Dozers; STRONG."

**Heavy-civil contractor fleet — 9 PSCs (`eswagner.com`):** matched on real grounded iron —
`D10R CAT DOZER`, `375L CAT EXCAVATOR`, `16H CAT ATS/GPS MOTOR GRADER`, `CS 563E CAT SMOOTH DRUM ROLLER`,
`740 CAT ARTICULATED TRUCK`. Self-labels weak matches honestly (`Z1KF dredge maintenance WEAK`).

**Light yard — 3 PSCs (`brookhollowrental.com`):** `Bobcat Skidsteer`, `Scissor Lift`,
`40' Bucket Lift`, `Mini Excavators` → office reno (Z2AA), hospital reno (Z2DA), remediation (F108, WEAK).

**Edge case — 1 PSC (`lps-inc.com`):** a sawmill-machinery dealer. Most of its catalog is stationary
plant (debarkers, kilns, planers) → correctly *ignored*. But `Wood Chipper - Mobile`,
`Harvesters and Processors`, `Hogs and Wood Grinders` are genuine mobile forestry iron → **F014 only**.
This is exactly the nuanced call you'd want.

**Rejections (`supported_pscs = []`) with reasons:**
- `statelinemachine.com` — "Inventory is exclusively spare parts (pistons, bucket teeth, cutting
  edges, undercarriage, hydraulic pumps as parts)… whole machines not rented."
- `partyrentaltx.com` — "Tables, chairs, linens, tents, catering… no construction machines."
- `exiusa.com` — "GPR, magnetometers, seismic geophones, LiDar… instrument house."

---

## 6. Why I trust the bouncer (unbiased random sample)

A deterministic every-290th-row slice — these are cases the engine had to judge cold:

| Domain | Verdict | What the bouncer caught |
|---|---|---|
| `astlaser.com` | ❌ reject | surgical/medical lasers — medical device rental, not construction |
| `buymj.com` | ❌ reject | sells **fixed overhead/bridge cranes**, not mobile RT/crawler cranes |
| `oilfieldmarinecontractors.com` | ❌ reject | **vessels** (airboats/crewboats) ≠ amphibious excavators/swamp buggies |
| `spot-coolers.com` | ❌ reject | portable AC/heaters only — **no aerial iron to pair**, so no Z1DA on chillers alone |
| `fp-usa.com` | ❌ reject | postage meters / mailroom hardware |
| `imagequestks.com` | ❌ reject | office/production printers |
| `maedaamerica.com` | ✅ Y1DA only | specialty **mini/crawler crane** maker — matched only the crane PSC |
| `rentalex.com` | ✅ 5 PSCs | aerial-heavy yard, grounded on real `JLG 600AJ`, `GENIE S80`, `SKYJACK SJ6832` |
| `deanrentalenterprises.us` | ✅ 11 PSCs | heavy-civil w/ `Long Reach Excavators` |

The three bolded rejects are the tell: a naive keyword matcher would have matched "crane", "marine",
and "chiller". The semantic bouncer rejected all three for the right reasons. That's the signal this
was done with judgment, not string-matching.

---

## 7. The one thing to scrutinize: match inclusiveness (calibration)

The rule the agents followed: **"include a PSC if the yard stocks ≥1 signature machine for it."**
That is the directive's literal Rule 1. It produces *broad* matching:

- `a2zrentals.com` matched **8** PSCs — including `Y1DA`/`Y1PZ`/`F108` flagged **WEAK** because it has
  *mini* excavators. A strict human analyst (see the earlier 12-firm pass) gave it **3**.
- Neither is "wrong." The agent is faithful to the written rule; the analyst applied a higher bar.

**Distribution sanity check (this is healthy, not inflated):**

| matched_psc_count | # yards |
|---|---|
| 0 (rejected) | 1,644 |
| 1–2 | 319 |
| 3–5 | 639 |
| 6–9 | 349 |
| 10–15 | 145 |

Mode is 3 PSCs; the 145 yards at 10–15 are dominated by real national/Cat-dealer fleets (United,
Herc, Cat Rental Store, Fabick, Foley, Quinn, Stowers, Wyoming Cat). Face-valid.

**If you want tighter precision**, the lever is a *policy change* in the Phase B prompt + a re-run:
- require **≥2 distinct signature classes** per PSC (kills WEAK single-machine matches), and/or
- treat `WEAK`-labeled matches as non-matches in Phase C (cheap — the strength is already in
  `justification_payload.per_psc`; you can downgrade without re-running the agents).

The strength tags (STRONG/MODERATE/WEAK) are already recorded per-PSC, so you can re-derive a
stricter `supported_pscs` from the existing data without spending a single token. That's the
cheapest first experiment.

---

## 8. Known limitations / honest caveats

1. **Inclusiveness is policy, not truth** — see §7. Decide your bar before using `supported_pscs` raw.
2. **Catalog quality is upstream** — `equipment_catalog` mixes true rental yards with dealers, parts
   vendors, and odd verticals. The bouncer handles most, but garbage-in still constrains the ceiling.
   A few domains had only generic category headers and no items → auto-rejected as "no inventory."
3. **`required_equipment` is a 15-row hand seed** — the ground truth itself is small/curated. If a PSC's
   required list is incomplete, matches inherit that gap.
4. **No tonnage / count weighting** — "has a mini excavator" and "has fifty 50-ton excavators" both
   count as one signature match. Fine for *eligibility*, not for *capacity ranking*.
5. **Strength tags are the agent's judgment**, not a calibrated score. Use them as ordinal hints.

---

## 9. If you're auditing this — do these

1. **Re-run the grounding check yourself** (the cheap, high-value one):
   for N random matched domains, confirm every `verified_inventory_match` appears in that domain's
   `equipment_catalog` row. Phase C already does this; re-derive it independently to trust it.
2. **Eyeball 20 rejects** in `verified_inventory_matches == []` — confirm they're genuinely non-yards.
   The random sample in §6 is the template.
3. **Stress the inclusiveness bar:** pull all `matched_psc_count >= 10`, confirm they're real full-line
   fleets (they are). Then pull `matched_psc_count == 1`, confirm the single match is defensible.
4. **Compare to the hand pass:** `reports/psc_equipment_matchmaking_2026-06-22.md` evaluated 12 firms
   by hand. Diff those 12 against the dataset — the deltas are all inclusiveness (§7), no contradictions.

---

## 10. Re-running / regenerating

```bash
# Phase A — re-extract shards from current equipment_catalog
doppler run -p core-x -c prd -- python3 scripts/extract_matchmaking_shards.py

# Phase B — re-run the agentic harness (Claude Code, /workflows). Defaults baked into the script.
#   (launched via the Workflow tool against scripts/mm_workflow.js)

# Phase C — grounding gate + Lance overwrite
doppler run -p core-x -c prd -- python3 pipelines/gtm/materialize_equipment_matchmaking.py
```

Phase C is **idempotent** (full overwrite + uniqueness assertion on `domain_norm`). The Workflow is
**resumable** — re-running re-uses cached agent results for unchanged shards. To re-evaluate only
specific shards, pass `{shardIds:[...]}` to the workflow.

**To tighten precision without re-running agents:** edit Phase C to read
`justification_payload.per_psc[code]` and drop codes tagged `WEAK` before writing
`supported_pscs`. One pass, zero tokens.
