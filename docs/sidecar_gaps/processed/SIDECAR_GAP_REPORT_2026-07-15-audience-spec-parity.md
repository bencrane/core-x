# Sidecar gap report — 2026-07-15 · audience-spec / laser-in parity

- **Date:** 2026-07-15
- **Artifact:** `query-sidecar/query_sidecar_20260714T230548Z.duckdb` (89 tables, ready)
- **Session topic:** Market-tab audience-spec redesign (rare-structure-hq) — the form is being
  reframed as a demand-side-partner "audience spec" instrument: geo × $ windows (sub / prime /
  combined) × active book × "N actions of type X within window Y", with live entity counts as
  each axis is answered. Operator direction: this counting engine plausibly precedes or replaces
  phrase/ for audience definition, so the sidecar (which backs catalyst) is the parity target.
  Entries below are the audience-spec shapes the sidecar cannot serve cleanly today.

---

## 1. Combined federal $ per entity (sub + prime, windowed)

1. **Intent** — "Entities with ≥ $X total federal revenue (sub + prime combined) in the last
   12/24/60 months" — partners don't care about the sub/prime split by default and should not
   need client-side addition.
2. **Why not the sidecar** — missing column(s): `gtm_entity_behavior_rollup` carries
   `prime_obl_12mo/24mo/36mo/60mo/lifetime` and `sub_amt_24mo/60mo/lifetime` but no
   `total_amt_*` columns, and the sub-side windows are missing 12mo/36mo so even a client-side
   sum can't be built for those windows.
3. **What I ran instead** — nothing yet (design phase); the equivalent audience-mart columns
   were confirmed absent by reading `scripts/build_gtm_audience_marts.py` in core-x.
4. **Cost** — n/a this session; recurs as per-query `COALESCE(a,0)+COALESCE(b,0)` boilerplate
   and is impossible for 12/36mo windows.
5. **Recurrence** — recurring: every audience-spec count and every "how big is their federal
   book" question.

## 2. Entity-grain audience spine (geo × $ × active in one table)

1. **Intent** — "How many entities fit: PoP in TX/FL, ≥ $1M 24mo, active award book ≥ $500K"
   — one count, answered live while a partner talks.
2. **Why not the sidecar** — wrong grain / spread across tables: the axes live in three places
   (`gtm_sam_entities` → physical_state/cage/naics; `gtm_prime_pop_lanes` → PoP by lane, no
   single `primary_pop_state`; `gtm_entity_behavior_rollup` → $ windows + active book). Every
   audience count is a 3-way join with a lane-collapse subquery; there is no entity-grain
   audience spine table (the Lance `gtm_audience_entities` mart exists but is NOT in the
   sidecar manifest).
3. **What I ran instead** — nothing yet; shape confirmed by DESCRIBE of the three tables.
4. **Cost** — n/a this session; recurs as a ~15-line join every audience question, with the
   lane-collapse making "primary PoP state" ambiguous per query author.
5. **Recurrence** — recurring: this is the base query of the audience-spec surface.

## 3. Cross-entity "actions of type X within window Y" (interactive)

1. **Intent** — "Entities with ≥ N actions of type C (terminations / option exercises / mods /
   new awards…) in the last 90 days" — the laser-in axis, cross-entity.
2. **Why not the sidecar** — missing sort (too slow unpruned): `gtm_txn_recipient_month_rollup`
   has the exact grain (uei × action_type_code × plan_class × naics × psc × agency × month →
   n_actions, obligation_sum) but is sorted `uei` — an audience query enters by
   action_type + month, so it full-scans 34M rows.
3. **What I ran instead** — `SELECT COUNT(DISTINCT uei) FROM gtm_txn_recipient_month_rollup
   WHERE action_type_code='C' AND month >= DATE '2026-04-01'` on serving.
4. **Cost** — 2,001 ms; ~34.1M rows scanned → 1 row returned (7,329 distinct UEIs). Usable
   one-shot, ~10× too slow for count-as-you-type against a compound spec.
5. **Recurrence** — recurring: every laser-in clause, fired on each spec keystroke/change.

## 4. Audience-mart chain lacks the active-book axis (correctly-elsewhere note)

1. **Intent** — "Active award $ / count / expiry timing as audience filters" for the Market tab.
2. **Why not the sidecar** — not a sidecar gap: `gtm_entity_behavior_rollup` already carries
   `active_obl`, `active_award_ct`, `earliest_pop_end`, `pop_expiring_180d_ct`. The gap is in
   the Postgres-mirrored `gtm_audience_entities` mart (built by
   `scripts/build_gtm_audience_marts.py`) that serves the Market tab today — recorded here so
   the build cycle decides ONE serving home for audience counts instead of growing both.
3. **What I ran instead** — read the mart build source; DESCRIBE'd the rollup on serving.
4. **Cost** — n/a; architectural fork risk, not query cost.
5. **Recurrence** — recurring until the audience counting engine's serving home is decided.

---

## Ranking (recurrence × cost)

1. **Entry 2** — every audience count pays the 3-way join; blocks the live-count UX entirely.
2. **Entry 3** — 2s per clause, fired repeatedly; the defining laser-in axis.
3. **Entry 1** — cheap to express but unanswerable for 12/36mo combined windows.
4. **Entry 4** — no query cost, but the fork decision gates where 1–3 land.

Demand only — no proposed solutions in entries per §7.

---

## Disposition (build cycle 2026-07-15, artifact query_sidecar_20260715T215456Z, 91 tables)

| Entry | Verdict | Shipped | Measured |
|---|---|---|---|
| 1 combined totals | Promote (column-grain, rode the build) | `total_amt_12/24/60mo/lifetime` derived cols on `gtm_audience_entities` | expressible in one predicate; 36mo window PARKED (absent from source mart — needs an audience-mart rebuild first) |
| 2 audience spine | Promote (structural, operator-directed demand) | `gtm_audience_entities` (2.03M rows, Tier A, sort uei, all 69 mart cols + 4 derived) | TX × ≥$1M-24mo count: 43 ms, single table (was: 3-way join) |
| 3 laser-in sort | Promote (structural sort copy) | `txn_recipient_month_by_type` (34M, sort action_type_code, month) | 2,001 ms → 9.6 ms (209×), identical result (7,329 UEIs) |
| 4 serving-home fork | Documented | Sidecar is the serving home for audience counts; the Postgres-mirror chain remains the Market tab's transport until re-pointed | n/a |

**Adjacency riders:** all 69 source-mart columns ride entry 2's copy (bands,
designation flags, people-coverage counts, naics_2..6) — every foreseeable
same-session follow-up (name it, split by state, check coverage) is answerable.
**Parked (structural-gated):** `total_amt_36mo` (source mart lacks sub_amt_36mo);
named-signal event table (terminations/novations as first-class signals) — the
month rollup serves these via action_type codes; cage_code on the audience mart
(needs audience-mart rebuild, tracked with 36mo).
