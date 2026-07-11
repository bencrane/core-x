# SIDECAR GAP REPORT — 2026-07-10 — funding-tab PDL match

Artifact at session: `query-sidecar/query_sidecar_20260709T214133Z.duckdb` (48 tables).
Topic: on-call funding-tab investigation (v18 era) — enrichment coverage of the
unattributed subaward-receiver population.

## Entry 1 — PDL company-match coverage for a UEI population

1. **Intent:** Of the 12,574 unattributed DoD subaward receivers (no usable prime
   record; from the top-5 signature allocation of the FY23–25 DoD sub flow), what
   share match a PDL company — overall and among the 7,812 with a SAM domain?
2. **Why not the sidecar:** missing table — no PDL identity/bridge table in the
   artifact. `firmographics_blitz` (domain-keyed) is served, but the SAM↔PDL
   identity bridge (`bridge_sam_pdl`: uei, duns, pdl_company_id, normalized_domain)
   and `pdl_companies` / `pdl_normalized_companies` exist only on Lance.
3. **What I ran instead:** exported the 12,574 (uei, normalized_domain, recv) via
   /sql to TSV; opened `s3://data-sink/active/bridge_sam_pdl` with lance
   (storage_options region=auto), `to_table()` into DuckDB (801,831 rows, 7 cols;
   only uei + pdl_company_id needed), LEFT JOIN on uei.
4. **Cost:** sidecar export ~12 s (12,574 rows); Lance open + full-table pull +
   join ~40 s wall; 801,831 rows scanned vs 12,574 needed.
5. **Recurrence:** recurring shape — "does population X have enrichment identity
   coverage (PDL / LinkedIn / domain)" is now a standard funnel-sizing question;
   this session alone chained domain → LinkedIn → PDL on the same population.

Footer: one gap this session so far. Rank: medium-high recurrence × medium cost —
a served `bridge_sam_pdl` slim (uei, pdl_company_id, normalized_domain; sorted by
uei) would make every such coverage question a single sidecar statement, joining
cleanly against `gtm_sam_entities` and `firmographics_blitz`. Demand only — no
solution prescribed.

---

## Disposition (gap-pass-4, 2026-07-10)

Schema probes preceding the verdict: all candidate datasets sized and DESCRIBEd on Lance
(bridge 801,831 × 7; `pdl_normalized_companies` 35.4M × 15 with `linkedin_slug`;
icypeas company/person families 6–15k each; LinkedIn bridges 53k/821/33k).

| # | Verdict | What shipped |
|---|---|---|
| 1 | **Promoted (as the identity/enrichment coverage LAYER, not one bridge)** | 9 tables: `bridge_sam_pdl` (uei-sorted, all 7 cols — the entry's exact ask), `pdl_normalized_companies` (35.4M, pdl_company_id-sorted; `linkedin_slug` = the company LinkedIn URL), `icypeas_company_scrapes` + `icypeas_dsbs_company_profiles`, `icypeas_person_profiles` + `icypeas_person_profile_scrapes`, `bridge_dsbs_pdl_linkedin`, `dsbs_poc_linkedin` + `exa_person_linkedin_candidates`. Raw/JSON blob columns excluded from projections. Guide gains an Identity/enrichment §3 section + a one-pass coverage-funnel §4 pattern |

Sweep rationale (recorded): the entry's own recurrence field names the chain "domain →
LinkedIn → PDL on the same population", and the operator's next questions after reading
the coverage answer were verbatim "same q for icypeas … and any company linkedin url
table?" — the layer IS the demand shape; shipping only the bridge would have been a
failed cycle. Deliberately excluded: `pdl_companies` (raw 35M twin of normalized;
linkedin_slug ≡ linkedin_url), a second domain-sorted PDL copy (gated until felt —
domain-anchored matching is a seconds-class scan).

Artifact: `query_sidecar_20260710T200418Z.duckdb` — 61 tables, 1.232B rows, 45.16 GiB,
61/61 parity (ops ledger run 16), serving swapped. Coverage smoke (the entry's shape,
sidecar-only, ONE statement): 33,352 never-primed FY23+ subaward receivers → 20,481
PDL-matched (61%) → 20,481 with LinkedIn slug → 4,053 icypeas-scraped, **12.4s cold
first-touch** vs the ~52s export + Lance-pull + local-join chain.

**Disk flag (escalated):** artifact 45.16 GiB; blue-green peak ~90 GiB vs the 100 GB
Render disk. Grow the disk BEFORE the next artifact growth — this is no longer advisory.

---

## Addendum — Entry 2 (appended by a second session AFTER gap-pass-4 archived this file)

1. **Intent:** plain-language work phrases for the top combos per identity bucket
   (tab-07 "In Plain Language") — 78 (naics, psc) pairs.
2. **Why not the sidecar:** missing tables — `naics_psc_deliverable` (20,998,
   what_was_done_v2) and `naics_psc_labor_profile` (16,291, work_summary), Lance-only.
3. **What I ran instead:** lance open of both datasets, to_table() into DuckDB,
   join against a 78-pair CSV (naics_code, psc_code, what_was_done, work_summary).
4. **Cost:** ~60 s wall, two full-dataset pulls (37k rows) vs 78 rows needed.
5. **Recurrence:** recurring — every consumable/story section at combo grain.

### Addendum disposition (gap-pass-5)

**Promoted** — served by the combo-language layer (4 tables) shipped in gap-pass-5;
full disposition in `SIDECAR_GAP_REPORT_2026-07-10-shedding-combo-outreach.md`.
Process note: entries appended to ARCHIVED reports risk being missed — new demand
goes in a NEW dated file (README convention); this one was caught because the
shedding report cross-referenced it.
