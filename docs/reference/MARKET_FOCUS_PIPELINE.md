# MARKET_FOCUS_PIPELINE — recognition section from websites × award record

**Status:** spec, validated once by hand (Bowler Pons `H53YKU2UV2K5`, testing-page v14–v16, 2026-07-08).
**Executable references:** `hq:design-artifacts/testing-page/_build/` (probes + page builders; committed to the hq repo).
**Governing rulings:** `docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md` §0/§0.1; the 2026-07-08 epistemic rulings (stamps classify the buyer's procurement, never the subcontracted task; `sub_naics` is a broker join-copy of the prime award's FPDS NAICS; capability hierarchy = own primes > SAM declarations > subbed-under stamps).

---

## 0. Purpose

Declared NAICS do not say what kind of firm an entity is, and no subaward-level combo exists —
the stamp on subbed-under money classifies the buyer's program, and the subbed task "runs the
gamut." This pipeline produces the **Market Focus** section: a recognition statement — *the
target market is [buyer archetypes] running [kinds of programs] that hand off [Z, the subject's
trade]* — derived from the subject's paid record plus the public websites of its paying primes.

- **Recognition, not sizing.** Deliberately disjoint from the audience funnel / Market Expansion
  machinery. No counts, no per-prime dollar detail in the emitted section.
- **Never per-relationship work attribution.** The output characterizes the buyer *class*; it
  never asserts what work was performed under any specific prime or award (§2.2 ruling).

## 1. Output contract (as shipped in testing-page v16)

| Emitted section | Form |
|---|---|
| The Market, In One Statement | One-paragraph market definition + one-paragraph "every paid relationship fits this shape" |
| The Buyer Archetypes | 2–4 cards: shape name · what these firms are/win · why they don't self-perform the slice |
| What They Need When They Win | 3-column table: Who / They win / They need — the archetype→need join, no dollars |
| (nix-pile) The Work That Gets Handed Off | Trade chips + Z statement; retained pending operator cull |

Register (operator-enforced): declarative; no second person; no meta/provenance chatter on cards
("Source:", "their website says", "fetched" are banned); interpretive content flagged once with
"Interpretive read — …" in the section why-line; record vs read boundary preserved.

## 2. Inputs

- `subject_uei` (single entity).
- R2 Lance datasets: `usaspending_subaward_canonical`, `usaspending_fpds_canonical_txn`
  (both under `s3://data-sink/active/`; run adjacent to R2, `client_max_retries: "8"`).
- Fetch capability (HTTP) + web-search fallback.

## 3. Steps

### S1 — Buyer set (deterministic)
Query `usaspending_subaward_canonical` for `subawardee_uei = subject`, group by
`prime_awardee_uei`: lifetime $, subaward count, span (min/max action date).
The set is **exhaustive when small** (Bowler: 5 buyers total — no ranking involved); for
long-tailed subjects, cut at cumulative ≥95% of lifetime sub $ and disclose the cut.
*Reference:* `_build/bowler_pons.py`, `_build/prime_subout_totals.py`.

### S2 — Record facts per buyer (deterministic)
For each buyer: (a) the NAICS×PSC stamps its payments to the subject rode under (join subaward →
its prime award's FPDS stamp — the only combo system that exists); (b) the buyer's own per-year
prime book and sub-out book, calendar years, trailing 5 (`_build/cust_trend.py`); (c) registration
liveness (active/wound-down — e.g., negative prime book, UEI superseded).

### S3 — Subject Z (mechanical fetch + LLM extract, gated)
Fetch the subject's website; extract self-described service lines. **Corroboration gate:** keep
only lines consistent with the subject's own prime-lane stamp families (paid-to-do evidence).
Z = the intersection, stated as a trade (Bowler: engineer/install/sustain physical-security
electronics — access control, intrusion detection, video, alarm/detection — at defense
installations; corroborated by 5616xJ, 5413xJ/N/J063, 3341x63/3342x63 lanes).

### S4 — Buyer websites (mechanical fetch + LLM extract, gated)
**Activity gate (eliminates the temporal-mismatch class — no M&A/rebrand reasoning anywhere):**
fetch websites ONLY for buyers with live paid activity (registration active AND sub-out dollars
in the trailing 24mo). Wound-down registrations contribute archetype evidence from S2 record
facts only — a dead UEI's website describes a different firm by definition.
Per active buyer: fetch → extract what-it-is / what-it-wins / what-it-self-performs.
**Fallbacks (both exercised in validation):** HTTP 403/block → web search over public reporting;
301 off-domain → follow once, note only.

### S5 — Synthesis (LLM, prompt-encodable; the non-deterministic core)
1. **Cluster** buyers into 2–4 archetypes by *why the archetype does not self-perform Z*
   (integrators own program management, not trades; distributors have no field bench;
   construction/IT primes carry it as subordinate scope). Heterogeneity is expected and is a
   feature — the invariant is Z, not the buyer type.
2. **One-statement:** "The market is [program kinds, from S2 stamps] primes whose contracts
   carry [Z] that the prime does not self-perform."
3. **Needs join:** per archetype, project Z into its program types → "They win / They need" rows.

### Gates (deterministic, applied to S5 output)
- **Paid-instance gate:** an archetype (and its needs-row) exists only if ≥1 subject subaward
  sits inside that archetype's programs. No paid instance → no row.
- **Traceability gate:** every $ figure on the card traces to an S1/S2 query output; every firm
  characterization traces to an S4 extract or public reporting. Nothing free-floating.
- **Attribution gate:** no sentence of the form "did X work for prime P."

## 4. Determinism boundary

| Step | Nature |
|---|---|
| S1, S2, gates | Deterministic queries/checks — scriptable end-to-end |
| S3/S4 fetch + fallback | Mechanical with defined branching |
| S3/S4 extraction, S5 clustering & phrasing | LLM judgment — repeatable in kind, not byte-identical; ships as a prompt-and-fallback spec |

## 5. Validation run traceability (v16, Bowler Pons)

| On-page element | Derivation |
|---|---|
| "Every paid relationship on the record fits this shape" | S1 (5 buyers exhaustive) ∩ S2 stamps ∩ S4 archetypes |
| INTEGRATOR card (Serco/Alutiiq-shaped) | S2 stamps (5616xJ/5413xJ·N Navy task orders) + Serco public reporting (site 403) + alutiiq.com |
| SUPPLIER card (ADS-shaped) | S2 stamps (3399x42 DLA vehicles) + adsinc.com |
| BUILDER card (URS/Sev1Tech-shaped) | S2 stamps (2362xY; 5413xJ063) + firm identity; under the S4 activity gate these two would be record-only |
| "What They Need When They Win" rows | S5 needs join; paid-instance gate satisfied per row (S1 dollars per buyer) |
| Trade chips / Z | S3: bowlerpons.com service lines ∩ prime-lane families |
| Customer Trajectories trend grid (v13) | `_build/cust_trend.py` per-year series, verbatim |

## 6. Standard practice (operator directive, 2026-07-08)

**Every element on a serving page is affiliated with a named pipeline — either an existing
script/query or a spec like this one that a future agent can construct.** Concretely:

1. Numeric content → a probe script in the page's `_build/` (durable, committed to hq), runnable
   under `doppler run --project core-x --config prd -- <core-x venv python>`.
2. Interpretive content → a pipeline spec in `core-x/docs/reference/` naming inputs, steps,
   gates, and the determinism boundary.
3. Page HTML → a `build_vN.py` in `_build/` (immutable version chain; copy → patch → bump →
   gallery). The builder embeds the measured values with their source noted.
4. New probes never live only in a session scratchpad.

## 7. Script inventory for this pipeline (hq:design-artifacts/testing-page/_build/)

| Script | Role |
|---|---|
| `cust_trend.py` | S2: per-buyer per-year prime book + sub-out (R2 Lance, DuckDB) |
| `bowler_pons.py`, `prime_subout_totals.py` | S1/S2: buyer set, lifetime $, stamps, spans |
| `build_v13.py` | Trajectories table → trend grid (small multiples, per-row scales) |
| `build_v14.py` | Market Focus tab (S3–S5 output as shipped) + Prime Relationships cleanup |
| `build_v15.py` | Need-lines folded into archetype cards (reverted by operator feedback) |
| `build_v16.py` | Cards restored to v14 form + "What They Need When They Win" connector table |
