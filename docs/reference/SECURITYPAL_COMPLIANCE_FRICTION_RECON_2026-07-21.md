# SecurityPal — GovCon compliance-friction recon

- **Date:** 2026-07-21
- **Artifact:** query-sidecar `query_sidecar_20260721T020734Z.duckdb` (107 tables)
- **Client:** SecurityPal (securitypal.com) — hybrid AI + expert platform that clears enterprise
  security questionnaires / TPRM assessments / compliance audits; unblocks stalled B2B/SaaS deals.
- **Thesis:** Federal security compliance (NIST SP 800-171, CMMC L2, DFARS flow-downs, prime SCRM)
  is exponentially harder than commercial. Federal awards data pinpoints which commercial software
  vendors are hitting that wall *now* — SecurityPal's targeting substrate.
- **Deliverable tables (HQX Postgres):** `gtm.securitypal_segment_a` (1,579), `gtm.securitypal_segment_b`
  (470), `gtm.securitypal_segment_c` (360) — actionable lists with POC. This doc is the reproducible
  analysis; the tables are the hand-off assets (POC PII lives in the DB, not git).

The core move: you don't need a contract to see the compliance wall — you need to see who is being
**forced through someone else's security review right now.** Three data shapes expose that.

---

## Software classifier (used by all segments)

A sub/vendor UEI is "commercial software/IT" if **either**:
- **Commercial-universe** — its domain ∈ `us_software_companies` (173,119-domain curated US
  software/SaaS set), bridged via `gtm_sam_entities.normalized_domain`. The higher-confidence
  "commercial SaaS" signal.
- **Federal NAICS** — `primary_naics` ∈ {5415xx, 513210/5132, 511210, 5182xx, 5191xx}. (Note the
  NAICS-2022 move of Software Publishers 511210→513210 — include both.)

Caveats that bound every number below: the uei→domain bridge resolves **~73%** of registrants
(commercial-universe counts are floors); the software universe is ~6% non-US (irrelevant to a
US-defense join); `requires_cmmc` is extracted for only **17%** of subs (Segment C is a floor).

---

## Segment A — the Multi-Prime Questionnaire Treadmill  ★ pilot

**Signal:** commercial software/IT vendors actively subcontracting under **≥3 distinct federal
primes**, active since 2024-01-20.

**Why SecurityPal:** every prime runs its own TPRM program → N primes = N separate supplier
security questionnaires (SIG/SIG-Lite, DFARS flow-down reps, CMMC attestations, SCRM packets) —
the 300-question wall, multiplied and recurring. This is the purest wedge: structural, repeating,
non-differentiating labor.

**TAM (18-mo activity):** **1,579** vendors — **665** in the commercial-SaaS universe, 1,116
NAICS-software, **1,579 (100%) with a contactable POC**, 228 also scope-flagged clearance/CMMC.
ICP note: the raw top-by-prime-count is mega-VARs (SHI 122, WWT 109) — filter `in_commercial_universe`
+ `active_prime_ct` (mid-market 3–20) for B2B-SaaS fit. **Materialized:** `gtm.securitypal_segment_a`.

## Segment B — the Civilian-to-Defense Border-Crossers  ★ highest urgency

**Signal:** established commercial software vendors (prior **civilian** federal footprint) whose
**first-ever DoD award** landed in the last 18 months.

**Why SecurityPal:** the instant a vendor touches DoD/CUI, DFARS 252.204-7012 attaches → mandatory
NIST SP 800-171 self-assessment (110 controls → SPRS score) + CMMC L2 on the runway. A civilian
SaaS company doing this cold, with a contract blocked on it — no internal compliance muscle, buy
not build.

**TAM:** 1,292 software vendors first-touched DoD in 18 mo; **470** are true crossers (prior
civilian federal), **141** of those in the commercial-SaaS universe; **153** already have ≥$100k
DoD obligated in 12 mo (money at stake now). **Materialized:** `gtm.securitypal_segment_b` (470
true crossers, POC via `sam_pocs`; example: Domino Data Lab, $16.5M DoD/12 mo).

## Segment C — the Scope-Verified CMMC/Clearance-Gated Subs  ★ surgical

**Signal:** software subs whose extracted solicitation scope explicitly states a clearance/CMMC
requirement (`requires_clearance`/`requires_cmmc`).

**Why SecurityPal:** zero inference — the requirement is in the scope text. Highest precision seed
list. Coverage-limited (17% scope extraction) → a floor.

**TAM:** **360** software subs flagged (314 active). Scaling the `govcon_scope_vectors` extraction
would lift this materially. **Materialized:** `gtm.securitypal_segment_c` (360 rows, `is_active`
flag, `req_cert_tags` / `req_clearance_level_max`).

---

## Segment summary

| Segment | Trigger | TAM (software) | Contactable | Best slice |
|---|---|---|---|---|
| A · Multi-prime treadmill | ≥3 concurrent primes | **1,579** | 1,579 (100%) | 665 commercial-universe; 228 compliance-flagged |
| B · Civilian→DoD crossers | first DoD award in 18 mo | **470** true crossers | high | 153 with ≥$100k DoD/12 mo; 141 commercial-universe |
| C · Scope-verified gated | scope states CMMC/clearance | **360** (314 active) | 360 (100%) | floor — coverage-limited |

Segments overlap (a vendor can be a crossing multi-prime sub); treat as ~2,000 deduped high-intent
accounts, not a sum. All figures are trailing-18-mo; ~20-30% higher on a 24-mo window.

---

## Reproducible SQL (query-sidecar, warm)

Segment A (the deliverable) — one native join, measured 312 ms:

```sql
SELECT g.sub_uei, g.sub_name, s.normalized_domain AS domain, u.industry,
       g.n_teaming_primes, g.teaming_prime_names, g.poc_full_name, g.poc_title,
       (u.domain IS NOT NULL) AS in_commercial_universe
FROM govcon_subawardee_profiles g
LEFT JOIN gtm_sam_entities s      ON s.uei = g.sub_uei
LEFT JOIN us_software_companies u ON u.domain = s.normalized_domain
WHERE g.n_teaming_primes >= 3 AND g.teaming_last_action_date >= DATE '2024-01-20'
  AND (u.domain IS NOT NULL OR s.primary_naics LIKE '5415%' OR s.primary_naics LIKE '5132%'
       OR s.primary_naics LIKE '5182%' OR s.primary_naics LIKE '5191%' OR s.primary_naics='511210')
ORDER BY g.n_teaming_primes DESC, g.teaming_dollars_5y DESC NULLS LAST;
```

Segment B (crossers): `gtm_txn_events_slim` first-DoD-touch (agencies 097/021/017/057) filtered to
`first_dod >= 2024-01-20`, joined to `gtm_entity_behavior_rollup` for the prior-civilian test
(`first_action_date < first_dod`) + `sam_pocs` for a contact. Segment C: `govcon_subawardee_profiles`
`requires_cmmc OR requires_clearance`, software-filtered. Both are **materialized** (tables B/C
above) with the full definition captured in each table's `COMMENT`. All three run warm against the
published artifact — no Lance scan, no cross-system hand-join (that native join-ability is the
2026-07-20 `us_software_companies` sidecar promotion; guide §4 pattern (n)).

---

## Infrastructure verdict

- **Prime→sub mapping + commercial-footprint linking:** native and warm today. `us_software_companies`
  is a sidecar mart; the software-membership test is a single join.
- **Segment A/B extractable and now materialized;** Segment C is real but coverage-bounded by scope
  extraction (17%).
- **To operationalize as a standing product:** a `gtm_compliance_friction` entity mart
  (uei-grain: `n_active_primes`, `first_dod_touch_date`, `dod_obl_ramp`, compliance flags,
  `is_commercial_software`, domain, POC, composite `friction_score`) — all inputs are warm; parked
  as the next structural build (see `docs/sidecar_gaps/processed/SIDECAR_GAP_REPORT_2026-07-20-compliance-friction-securitypal.md`).

**Recommended first move:** Segment A pilot — `gtm.securitypal_segment_a`, filtered to
`in_commercial_universe = true` and `active_prime_ct BETWEEN 3 AND 20`, sorted by prime count.
