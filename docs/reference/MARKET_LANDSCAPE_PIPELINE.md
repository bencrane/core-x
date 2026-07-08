# MARKET_LANDSCAPE_PIPELINE — sub-spend allocation, capability set, audience ladder

**Status:** spec, validated once by hand (Bowler Pons `H53YKU2UV2K5`, testing-page v22–v28, 2026-07-08).
**Executable references:** `hq:design-artifacts/testing-page/_build/` — `sub_spend_allocation.py` (V1→V4), `audience_ladder.py`, `vehicle_expiry.py`, `cust_trend.py`, `market_dynamics.py` (probes + page builders; committed to the hq repo).
**Companion spec:** `docs/reference/MARKET_FOCUS_PIPELINE.md` (PR #1084) — the recognition section; read it first for format and for the standard-practice doctrine (§6 there) this spec inherits.
**Governing rulings:** `docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md` §0/§0.1; the 2026-07-08 epistemic rulings (stamps classify the buyer's procurement, never the subcontracted task; capability hierarchy = own primes > SAM declarations > subbed-under stamps); the 2026-07-08 operator-ratified rulings restated in §2 below.
**Session handoff:** `hq:directives/2026-07-08-market-landscape-HANDOFF.md`.

---

## 0. Purpose

Three linked constructions, one shared substrate, all per-subject:

1. **Sub-spend allocation** ("What The Sub Spend Buys," v22–v27): for each of the subject's
   paying primes, the prime's trailing-5yr sub-out wallet decomposed by the *type of work* each
   paid firm does — labeled from the paid firm's own record, never from the program stamp alone.
2. **Subject capability set** (v27): the subject-intrinsic family set that defines the gold
   slice, the competitor census, and the market bridge — one definition, three surfaces.
3. **Audience ladder** ("Market Landscape," v28): the lookalike buyer universe, cut by cut, from
   every prime winning the subject's anchor work down to a named shortlist already paying
   subject-shaped firms.

Framing doctrine (operator-ratified): slices are stated flat as "% of spend allocated to a type
of work"; method disclosed once in a muted chnote; totals sum to 100%; sales asset, not academic;
less-conservative-but-directionally-defensible preferred. Reader-facing copy uses only shape/work
language — family codes live in chnotes and probes.

## 1. Output contract (as shipped, v24–v28)

| Emitted element | Form |
|---|---|
| Per-buyer work-type donuts (v25–v27) | One buyer at a time (gold-chip tabs); top-8 named work-type slices + other + unallocated; wallet total in the hole; gold = contiguous capability-set block, legend split "In [subject]'s line of work / Outside it"; subject stat strip + contested-pool stat per pane |
| Contested pool + named census (v27) | Per buyer: pool $ = wallet slices inside the capability set, % of wallet, firm count, subject's share; top named rivals with $ |
| Dependencies cards (v24) | Concentration-comp format: tag / bold fact / mono identifier / consequence (BUYER / WORK / VEHICLE / LIVE TODAY) |
| Market Landscape tab (v28) | Bridge (five buyers → three shapes, condensed archetype cards) → ladder funnel with archetype splits per cut → cut cards in natural language → The List (named shortlist with $) |

Register (operator-enforced, hard): declarative; no second person; no meta/provenance chatter on
cards; interpretive content flagged once ("Interpretive read — …"); measured numbers only.
Window labeling: analysis windows are 60mo trailing; reader-facing labels say "past 5 yrs."

## 2. Definitions (operator-ratified 2026-07-08 — canonical, do not re-derive)

**family_key** (`docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md` ruling):
`NAICS[:4] × (PSC[0] if alpha else PSC[:2])`, e.g. `5616xJ`, `3399x42`.

**Standard floor** (family "held" by a firm): own prime $ in the family ≥ $100K (lifetime or
60mo) OR ≥ 5% of the firm's own lifetime prime book.

**Subject capability set** (adversarial ruling; probe V4): family ∈ set iff
- **tier 1 — prime-proven:** subject's own LIFETIME prime $ in the family clears the standard
  floor; OR
- **tier 2 — market-proven:** programs stamped with the family carry ≥ 10% of the subject's
  lifetime sub revenue;
- SAM-declarations × pairing-matrix only as a cold-start fallback when both tiers are empty.

Validated: Bowler = 14 families (13 prime-proven + `5616xJ` market-proven). The plain-language
sentence: "everything the government has paid them for — six figures+ as prime, or the kinds of
programs carrying a tenth+ of everything they've been paid as a sub."

**Anchor families:** the stamp families on the SUBJECT's own subawards — the programs the subject
has ridden. Distinct from the capability set; "anchor" is an adjective; per-subject property.
(Bowler: `5616xJ 5413xJ 5413xN 5413xA 3399x42 2362xY`.)

**Shapes/archetypes:** named clusters of the subject's anchor-family set, computed per subject
(Bowler: integrators `5616xJ/5413xJ/5413xN/5413xA` · suppliers `3399x42` · builders `2362xY`).
Queued refinement: a `mixed` tag when the top cluster carries < 50% of a prime's anchor $.

**Epistemic ground (inherited, never violated):** the stamp on subbed-under money classifies the
buyer's procurement, never the subbed task. Recipient shape (the paid firm's own record) is the
primary evidence of what sub money bought; being repeatedly paid inside family-X programs is
market-position evidence, not capability evidence.

## 3. Inputs

- `subject_uei` (single entity) + its buyer set (from the Market Focus pipeline S1).
- R2 Lance datasets (all under `s3://data-sink/active/`; storage_options carry
  `client_max_retries: "8"`):
  - `usaspending_subaward_canonical` — subawards (buyer, subawardee, $, action date, prime-award
    NAICS/PSC stamp, sub-side PoP state/county).
  - `gtm_prime_combo_lanes` — every firm's own prime family profile (`prime_obl_60mo`,
    `prime_obl_lifetime` per NAICS×PSC lane).
  - `gtm_prime_farmout_combo_lanes` — per-firm farm-out $ per lane (60mo / lifetime).
  - `gtm_prime_pop_lanes` — per-firm own-award PoP lanes (uei × state × county FIPS).
  - `gtm_naics_psc_pairs` — NAICS×PSC pairing substrate (PR #1082): real-award
    `obligated_lifetime` / `n_awards_lifetime` per pair + `is_psctool_suggested` overlay.
  - `sam_master_entities` — SAM registrations: declared `naics_codes`, `primary_naics`,
    `psc_codes`.
- Invocation: `doppler run --project core-x --config prd -- /Users/benjamincrane/core-x/.venv/bin/python <probe>.py` from `hq:design-artifacts/testing-page/_build/`.

## 4. Pipeline A — sub-spend allocation cascade (deterministic end-to-end)

*Reference:* `_build/sub_spend_allocation.py` (V2 = own-record cascade, V3 = declaration tier).

**Grain:** (buyer, subawardee, program-family) — the program family `P` is the stamp family of
the prime award the money rode under. Each grain row's whole $ goes to exactly ONE work-type
label. Window: trailing 60mo of `subaward_action_date` (a parallel per-calendar-year pass feeds
the gold trend).

**Cascade (first match wins):**

| Step | Label source | Condition |
|---|---|---|
| 1. `exact` | `P` itself | the paid firm holds `P` (standard floor) |
| 2. `naics4` | firm's largest floored family sharing `P`'s NAICS4 | tie-break: lifetime $ desc |
| 3. `psccls` | firm's largest floored family sharing `P`'s PSC class | tie-break: lifetime $ desc |
| 4. `dominant` | firm's dominant family: dollar-weighted, 60mo first then lifetime (`ROW_NUMBER` over `(m60>0) DESC, m60 DESC, life DESC, fk`) | firm has an own prime record but no floored family relating to `P` |
| 5. `declaration` (V3) | SAM-declared codes × real-award pairing matrix, then the SAME cascade vs `P` (exact → NAICS4 → PSC-class → declaration-dominant). Candidates: NAICS+PSC declarers = their declared cross-product kept where the pair exists in `gtm_naics_psc_pairs` real-award data (or is psctool-suggested), weighted by the pair's `obligated_lifetime`; NAICS-only declarers = each declared NAICS × its top-3 real-award PSC pairings | firm has NO own prime record but a SAM registration with usable declarations |
| 6. `gray` | `__UNATTR__` | no own record, no SAM registration / no supported pairs |

**Validated mix (Bowler's five buyers):** the declaration tier collapsed unallocated 4.5% → **0.37% of $**
(Serco 9.5→1.0%, ADS 3.8→0.3%, Sev1Tech fully allocated). Gold slices and subject shares are
invariant under the tier (it only labels previously-gray firms).

**Derived measures (same probe):**
- **Gold slice** per buyer = wallet $ whose label ∈ subject capability set (§5).
- **Per-year gold trend** = gold $ and share-of-wallet by calendar year (Serco: 1.0% → 13.7%,
  '22→'25, while the wallet fell $451M→$100M).
- **Contested pool** per buyer = capability-set wallet slices: pool $, % of wallet, distinct
  firms, subject's $ and share (Serco $688M = 61% of wallet · 241 firms · Bowler 2.4%;
  Sev1Tech 81% / Bowler 32% largest recipient; Alutiiq 52%; ADS 12%; URS 0.5%).
- **Named census** per buyer = top firms in the contested pool excl. the subject, with $, family,
  and cascade step.

## 5. Pipeline B — subject capability set (deterministic)

*Reference:* `_build/sub_spend_allocation.py` V4 section; re-derived identically in
`_build/audience_ladder.py`.

Tier 1 from `gtm_prime_combo_lanes` (subject's own lanes, lifetime $, standard floor). Tier 2
from `usaspending_subaward_canonical` (subject-as-subawardee, stamp-family shares of lifetime sub
$, ≥ 10% bar). Union, tier recorded per family (`MIN(tier)` on collision — prime-proven wins).

Consumers (one definition, three surfaces): donut gold blocks (§4), competitor census (§4),
ladder cut 5 (§6). **Materialization candidate (queued):** firm-capability labels as a real R2
mart, since three surfaces recompute them.

## 6. Pipeline C — audience ladder (deterministic)

*Reference:* `_build/audience_ladder.py`. Anchors grouped by shape (§2). Anchor primes + subject
excluded from every cut. Validated counts: **2,703 → 1,464 → 513 (+97 stale parked) → 248 → 200.**

| Cut | Rule | Datasets |
|---|---|---|
| 1 — winning anchor work now | own prime 60mo $ > 0 in any anchor family AND total 60mo book ≥ floor = **max($10M, smallest anchor prime's 5yr book)**. Shape assigned per firm by its largest anchor-family group (integrator ≥ supplier ≥ builder on ties) | `gtm_prime_combo_lanes` |
| 2 — footprint overlap | own-award PoP county FIPS ∈ subject counties OR their sub-side ride county ∈ subject counties (name-match on state+county) | `gtm_prime_pop_lanes`, `usaspending_subaward_canonical` |
| 3 — they hand work out | disclosed farm-out 60mo > 0. Lifetime-only disclosers are the **stale tier — parked, not deleted** (+97) | `gtm_prime_farmout_combo_lanes` |
| 4 — they hand out THIS work, here | anchor-family farm-out 60mo > 0 AND (anchor-stamped sub rides land in subject counties OR own PoP in subject counties). Emits deal-size distribution per sub relationship, 5yr: median $195K · p25 $70K · p75 $719K | `gtm_prime_farmout_combo_lanes`, `usaspending_subaward_canonical`, `gtm_prime_pop_lanes` |
| 5 — they already pay subject-shaped firms | ≥ 1 anchor-ride subawardee whose OWN prime families intersect the subject capability set (subject excluded). Emits $ to shaped firms ($2.16B) inside the anchor farm-out pool ($12.96B) | + `gtm_prime_combo_lanes` (subawardee profiles), capset (§5) |
| The List | top cut-5 survivors by $ already flowing to subject-shaped firms: name, shape, book, anchor farm-out, to-shaped $ + firm count (BAE $165M, Lockheed, SRC, BL Harbert, Raytheon, L3Harris, Northrop, SAIC, RQ Construction, Oasis, ManTech…) | — |

**Floor phrasing rule:** cut 1's floor is stated on-page as a credential ("at least the size of
the smallest current buyer"), never as methodology.

**Queued extension (approved in principle, not run):** aggregate sub-out donut for the top-200 —
full wallets through the §4 cascade, per-shape splits; probable capstone.

## 7. Determinism boundary

| Step | Nature |
|---|---|
| Allocation cascade V2/V3, capability set V4, ladder cuts 1–5, all derived measures | Deterministic SQL over Lance datasets — scriptable end-to-end, byte-reproducible given the datasets and `W0` |
| Shape naming (cluster labels "integrator/supplier/builder") | LLM/operator judgment at setup, then frozen per subject; membership assignment is deterministic |
| Family → plain-language work-type labels on donut slices | In-session placeholders — table NOT ratified; treat as pending |
| List-row identity clauses (display names) | Queued: record-derived first, website enrichment for displayed names only, activity-gated |
| Page copy/framing | Operator register rules (§1) — judgment, bounded by the doctrine |

Nondeterminism note: DuckDB `MEDIAN`/`quantile_cont` over the same table is stable; counts shift
only when the underlying marts rebuild. Re-runs against rebuilt marts must re-emit the page
numbers — never patch one number in place.

## 8. Standard practice (inherited from MARKET_FOCUS_PIPELINE §6)

Every element on a serving page is affiliated with a named pipeline — a probe script in the
page's `_build/` (durable, committed to hq, runnable under doppler + the core-x venv) for numeric
content, or a spec like this one for interpretive content. Page HTML comes from an immutable
`build_vN.py` chain (copy → patch → bump → gallery). New probes never live only in a session
scratchpad.

## 9. Validation run traceability (Bowler Pons, v22–v28)

| On-page element | Derivation |
|---|---|
| Per-buyer donuts + unallocated 0.37% | §4 cascade V2+V3, 60mo window |
| Gold blocks + "In Bowler's line of work / Outside it" legend | §5 capability set (14 families) ∩ §4 labels |
| Serco gold trend 1.0% → 13.7% ('22→'25) | §4 per-year pass |
| Contested pools + named census | §4 derived measures over §5 set |
| Dependencies cards (BUYER 88% / WORK 64% / VEHICLE → Feb '29 / LIVE $1.9M → Aug '28) | `_build/vehicle_expiry.py` + composition probes |
| Ladder 2,703→1,464→513(+97)→248→200 + splits | §6 cuts 1–5 |
| Deal-size median $195K (p25 $70K, p75 $719K) | §6 cut 4 |
| $2.16B to subject-shaped firms / $12.96B pool | §6 cut 5 |
| The List | §6 List query |

## 10. Script inventory (hq:design-artifacts/testing-page/_build/)

| Script | Role |
|---|---|
| `sub_spend_allocation.py` | §4 + §5 in one file: V1 firm-global (superseded) → V2 cascade → per-year gold trend → SAM gray-recovery coverage → V3 declaration tier → V4 capability set + contested pools + named census |
| `audience_ladder.py` | §6: capset re-derivation, cuts 1–5, deal-size stats, The List |
| `vehicle_expiry.py` | Dependencies inputs: FPDS PoP end + parent-IDV ordering-window end per subbed-under award; live/closed rollups |
| `cust_trend.py` | Per-buyer per-year prime book + sub-out (context panel for the allocation section) |
| `market_dynamics.py` | Market-wide farm-out per anchor family (The Dynamics tab, v18) |
| `build_v22.py … build_v28.py` | Page emit chain for the versions this spec covers (immutable; each execs its predecessor and patches `src_py`) |
