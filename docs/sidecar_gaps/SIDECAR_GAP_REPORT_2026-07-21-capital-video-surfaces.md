# SIDECAR GAP REPORT — 2026-07-21 — capital-video surfaces (pay lens, firm flip, outlays)

- **Date:** 2026-07-21
- **Artifact:** `query-sidecar/query_sidecar_20260721T020734Z.duckdb` (107 tables, built 2026-07-21T02:07:34Z)
- **Session topic:** capital-video instrument build — how-it-pays lens (financing pie),
  award⇄firm flip drawer, green financed-contrast dots, paid-% strategy sweep.

---

## Entry 1 — paid-% (outlays) by payment combo on the in-force book

1. **Intent** — "How much of the obligated money is already paid out?" — obligated vs
   outlaid $ and paid-% grouped by contract-financing class × pricing class, in-force
   awards only (the "float is live" beat; also wanted per-award/per-firm later for the
   drawers).
2. **Why not the sidecar** — `missing table`: outlays structurally cannot exist in the
   sidecar (FPDS records obligations only; outlays are Treasury File-C, surfaced only on
   the USAspending prime-award API product). The pulled copy lives on Lance at
   `s3://data-sink/active/usaspending_api_fresh/contract_prime_award` (v48) and is NOT
   in the sidecar. Columns needed: `total_obligated_amount`, `total_outlayed_amount`,
   `contract_financing_code`, `type_of_contract_pricing_code`,
   `period_of_performance_current_end_date` (+ OBBBA/IIJA/COVID supplemental
   obligation/outlay columns sighted in the same schema for the forward-wave trace).
3. **What I ran instead** — pylance `to_table()` on the 5 columns above → duckdb
   aggregate: financing-class × pricing-class GROUP BY over
   `period_of_performance_current_end_date >= today`. Dataset hit:
   `usaspending_api_fresh/contract_prime_award` (744,000-row capped pull; 119,873
   in-force rows aggregated).
4. **Cost** — ~40s wall end-to-end (schema probe + column pull + aggregate), two runs
   (first hit a VARCHAR-amount cast error). 744k rows scanned → 12 result rows.
   Caveat carried: the pull is capped and recent-activity-filtered — aggregates are
   directional, not citation-grade.
5. **Recurrence** — **recurring**: the paid-%/float numbers are slated for the video
   narrative, the award drawer (per-award obligated-vs-outlaid clock), and the firm
   drawer (paid-% of the firm's book). Every future ask re-runs a Lance scan until an
   award-grain outlay spine exists. Same lane also carries the OBBBA/IIJA supplemental
   trace (asked this session as "what's on the table").

## Entry 2 — award profile point-reads exceed the interactive budget (missing sort)

1. **Intent** — click-an-award → full award profile (the drawer read), now demo-critical
   via the green contrast dots.
2. **Why not the sidecar** — `missing sort (too slow unpruned)`: served BY the sidecar,
   but the txn tables backing the profile's ledger/subaward legs are not
   award-key-sorted; probes ride uei-pruning (core-x #1299). For mega-prime UEIs
   (Lockheed-class) the UEI slice barely prunes.
3. **What I ran instead** — the standard catalyst `/market-slice/award` path; no
   alternate store. Mitigations shipped this session: BFF timeout 30s→90s
   (gc-hq-new #97) + manual pre-warm curl of the five green-dot awards.
4. **Cost** — 26.9s cold for `CONT_AWD_N0001922F2503` (Lockheed); 11–18s cold for the
   other four greens; one demo-path 500 (timeout) before the mitigation. Repeats warm.
5. **Recurrence** — **recurring and now demo-critical**: this is fresh demand evidence
   for the already-logged `2026-07-21-award-key-probes.md` gap (award-key-sorted txn
   projection). Green-dot clicks on camera are exactly the interaction that gap was
   parked awaiting.

---

## Ranking (recurrence × cost)

1. **Entry 1 (outlay spine)** — highest: recurring across video, both drawers, and the
   OBBBA trace; each ask is a fresh Lance scan; upstream pull is capped so the numbers
   are also *wrong-ish* until the lane runs (uncap → in-force filter → award-grain
   spine).
2. **Entry 2 (award-key sort)** — high: 10–30s cold on a camera-facing click; already
   ledgered, this session converts it from "promote on demand evidence" to demanded.

(Related, already ledgered separately this session: `2026-07-21-firm-contact-channels.md`
— person-grain contact channels for the firm drawer's people section.)

---

## DISPOSITION (2026-07-22)

- **Entry 1 (outlay spine / paid-%):** PARKED. Blocked upstream of the sidecar — the api_fresh pull
  is `last_modified`-windowed (half the active book by count) and the bulk `award_search` snapshot is
  six weeks stale with a stalled delta. Protocol path (per operator ruling): a NEW reconciled
  award-grain spine (bulk `award_search` ∪ `api_fresh`, argmax(last_modified), never upserting the
  immutable bulk — the transactions-spine pattern), parity-checked to the 255,901 active keys, THEN
  promoted. For the video, outlay aggregates are frozen as page constants with floor framing — no
  sidecar dependency. Promote the spine on the next data-leg cycle.
- **Entry 2 (award-key probes):** PROMOTED + shipped — see `2026-07-21-award-key-probes.md`
  disposition. Four companions + the award_key_pfx pruning leg; /award drawer 13–27s → 0.81s.
