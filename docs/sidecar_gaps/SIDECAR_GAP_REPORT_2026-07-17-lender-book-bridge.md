# SIDECAR GAP REPORT — 2026-07-17 — lender-book bridge

- **Date:** 2026-07-17
- **Artifact at session start:** `query_sidecar_20260717T234653Z` (104 tables)
- **Session topic:** capital-provider lender-book portrait (hq
  `design-artifacts/capital-providers/lender-book/`, v1–v3) — per-lender
  lead-magnet page reading a lender's full CA/CO UCC debtor book against the
  federal record.

## Entry 1 — lender → full filing book

1. **Intent** — "Give me every UCC filing naming lender X as secured party,
   with debtor identity/geo and filing state" — the root extraction for every
   lender-book page; feeds all downstream uei-keyed cuts.
2. **Why not the sidecar** — wrong grain / missing sort. Lender→filings is only
   expressible by normalizing + LIKE-scanning `secured_parties` across all of
   `ucc_filings_all` (sorted `ucc_state, debtor_name_norm` — no pruning on the
   lender axis). `ucc_lenders_all` has the lender grain but no filing linkage.
3. **What I ran instead** — superset prefilter
   (`regexp_replace(upper(secured_parties),…) LIKE '%<key>%'`) + exact
   per-party normalization via `unnest(string_split(…))`, replicating
   `sam_ucc_debtor_overlap.py::_LK`; then filing-grain fetch to the client.
4. **Cost** — ~4.0–4.6 s per lender per query shape (the page runs the scan
   1–3×); 7.7M rows scanned vs ~10k returned (CNB). Hard failure mode: books
   over 50k filings (JPMORGAN CHASE BANK: 160k) exceed the API row cap —
   mega-lenders are unservable on this path.
5. **Recurrence** — recurring by construction: once per lender per generation,
   and the lead-magnet path implies every lender in `ucc_lenders_all` (135k)
   is a potential probe. Operator-directed promotion (2026-07-17 session).

## Build scope block (pre-build, per adjacency sweep)

**Ships from demand (structural, operator-directed):**
- `ucc_lender_filings` — lender_key-grain filing bridge, local off
  `ucc_filings_all`, sorted `(lender_key, uei)`. Grain
  1/(lender_key, ucc_state, filing_id, debtor_key); GROUP BY collapses
  same-lender spelling variants on one filing. Normalization expression kept
  in lockstep with `_LK` (sam_ucc_debtor_overlap.py).

**Rides as adjacency (column-grain, one line each):**
- Full debtor identity/geo (`debtor_name/_norm/_city/_state/_zip`, `is_org`,
  `sos_entity_key`, `in_sam`, `uei`) — every book read wants the roster
  without a join-back.
- All filing attributes (`filing_class`, `terminated`, `is_active_financing`,
  `is_lease`, `first/last_filing_date`, `lapse_date`, `n_secured_parties`) —
  the page's book-side cuts (active split, year trend, lease/tax split,
  recency) compute from these in one probe.
- `lender_name` (raw party as filed) — display + normalization audit.

**Stays gated (structural, parked with rationale):**
- Blobs `secured_parties` + `collateral_text` — both remain one pure-equality
  join away on `(ucc_state, filing_id, debtor_key)` vs `ucc_filings_all`;
  duplicating them into the exploded grain grows the artifact ~1GB for detail
  no first-read needs. Promote a collateral text path only on demonstrated
  recurrence of "against what" at lender grain.
- Filing-key sort copy of `ucc_filings_all` (for ms-class blob join-backs) —
  hash join of a one-lender probe set against the 7.7M local table is already
  seconds-class warm; not worth a third copy of the corpus today.
- Debtor-key-sorted copy of the bridge ("who else lends to this debtor set" /
  co-lender overlap) — answerable warm via hash join of one lender's debtor
  keys; promote only if competitor-overlap becomes a page section.

**Next-question simulation (each answerable post-build):** book roster ✓ ·
active/lapsed split ✓ · filings-by-year trend ✓ · SAM slice → uei joins ✓ ·
lender aggregates (`ucc_lenders_all`) ✓ · lender_class at read time (equality
join to `ucc_lenders_all`) ✓ · collateral text (equality join-back, parked) ✓.

## Disposition

*(appended post-build)*
