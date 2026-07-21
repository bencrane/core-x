# Sidecar Gap Report — SBIR Phase II → Phase III / first non-SBIR DoD prime crossover

- **Date:** 2026-07-20
- **Artifact stamp:** `query-sidecar/query_sidecar_20260720T025249Z.duckdb` (106 tables, built 2026-07-20T02:52:49Z)
- **Topic:** SBIR Phase II → Phase III (or first non-SBIR DoD prime) transition detection + sizing for software/IT firms

---

## Gap entry

1. **Intent** — Identify software/IT companies that recently transitioned from an SBIR Phase II
   award into their first Phase III (or first non-SBIR) DoD prime contract; size the cohort over
   the trailing 18 months (volume of firms, avg contract value); and map those transitions to
   commercial domains.

2. **Why not the sidecar** — **missing column(s).** The FPDS element that authoritatively encodes
   SBIR/STTR phase — `research` (SR1/SR2/SR3 = SBIR Phase I/II/III; ST1/ST2/ST3 = STTR) — is not
   projected into ANY of the 106 sidecar tables. A full `information_schema.columns` scan for
   `sbir|sttr|phase|research|program|solicit|idv` returns only `solicitation_identifier` /
   `solicitation_date` (award_descriptions), `idv_type_code`, and subawardee-profile solicitation
   tags — none of which encode phase. The `type_of_set_aside_code='SBP'` value on
   `txn_events_combo` is the only SBIR-adjacent flag and it is unusable as a proxy: it does not
   distinguish Phase I/II/III, and Phase III awards carry NO set-aside by rule (sole-source
   derivative justification), so the transition event lands entirely outside the SBP slice. In
   an 18-month DoD software/IT window only **26 UEIs** carried `SBP` vs **3,987** total —
   confirming the set-aside code cannot reconstruct the phase ladder. Phase-crossover detection
   also requires per-UEI award-sequence ordering keyed on the phase field, which no mart carries.

3. **What I ran instead** — sidecar only, for the bounded-adjacent sizing (NOT the transition):
   `SELECT CASE WHEN type_of_set_aside_code='SBP' … END, count(DISTINCT uei), count(DISTINCT
   award_key), sum(obligation), avg(obligation) FROM txn_events_combo WHERE action_date >=
   '2025-01-20' AND awarding_agency_code IN ('097','021','017','057') AND (naics 5415/5112/5182/5191
   OR psc D3%/70%/DA%/DJ%) GROUP BY 1`. The transition-isolating query could not be composed —
   the phase field does not exist in the sidecar. Authoritative answer requires the FPDS 392-col
   canonical Lance spine (`usaspending_fpds_canonical_txn`), which carries `research`.

4. **Cost** — sidecar sizing queries: ~6–10 s each (unpruned agency+naics predicate on 108M-row
   `txn_events_combo`). The real question was **not answerable at any cost** on the current
   artifact — hard structural miss, not a slow scan.

5. **Recurrence** — **recurring.** SBIR→Phase-III crossover is a durable GTM motion (transitioning
   firms = high-intent, newly-primeable, commercial-dual-use targets). This will be re-asked per
   agency, per NAICS vertical, and on a rolling trailing-window basis.

---

## Footer — rank (recurrence × cost)

Single gap, **HIGH** rank: recurring GTM motion × currently-unanswerable (structural column miss).
Demand is for the FPDS `research` (SBIR/STTR phase) element projected onto the txn grain, plus a
per-UEI phase-ladder state so Phase II→III (and first-non-SBIR-prime) crossover events are
first-class. Promotion candidate: add `research` (+ derived `sbir_phase`) to `gtm_txn_events_slim`
/ `txn_events_combo`, and a `gtm_sbir_phase_ladder` entity mart (uei-grain: last_phase2_date,
first_phase3_date, first_nonsbir_prime_date, crossover_flag). Report demand only — disposition set
by the `sidecar-gaps` promotion cycle.

---

## Disposition (2026-07-20 build cycle · artifact `query_sidecar_20260721T020734Z.duckdb`, 107 tables)

**Verdict: PARK (structural) — feasibility CONFIRMED, deferred to its own cycle.**

Probe result (doctrine: claims are guesses until probed): `usaspending_fpds_canonical_txn` Lance
(v19, 392 cols) **does carry `research` AND `research_description`** — the SBIR/STTR phase element
exists upstream. The report's core claim is TRUE; the signal is available.

Why parked rather than shipped this cycle:
- The entity-grain deliverable (`gtm_sbir_phase_ladder`: last_phase2_date / first_phase3_date /
  first_nonsbir_prime_date / crossover_flag) is a **new structural mart** — a self-contained thought.
- The txn-column half (`sbir_phase` derived from `research`) would ride the `txn_events_combo`
  `combo_fact` scan, but that touches the **108M-row critical-path fact**; bolting an unrelated
  structural thought onto it in the same cycle as the `us_software_companies` promotion mixes
  blast radius for no shared benefit. Two clean thoughts beat one risky combined build.

**Build spec for next cycle (ready to execute):** extend `_COMBO_SRC_COLS` + `_COMBO_FACT_SQL`
with a CASE `sbir_phase` off `research` (SR1/2/3→SBIR I/II/III, ST1/2/3→STTR I/II/III), then a new
aggregate mart from the canonical keyed `uei` computing the phase-ladder dates + crossover flag.
Kept in the active ledger intent; no build fired this cycle.
