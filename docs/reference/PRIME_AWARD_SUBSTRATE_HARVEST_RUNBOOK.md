# Prime-Award Substrate Harvest — Offline Bridge + Phase 1 (executed)

Executes the substrate-harvesting pipeline scoped in
`PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md`: materialize the offline translation
bridge, resolve notice multiplicity to the highest-value document host, sweep the
live attachment-discovery endpoint, and land an isolated attachment manifest joined
to the prime-award financials. **Stage 3 (PDF byte download) is out of scope.**

Run 2026-06-07, `pylance 7 / duckdb 1.5`, R2 creds from `core-x/prd`. Code:
`pipelines/sam_gov/sam_opps_attachment_manifest_90day_winners.py`.

---

## 0. Result

| | |
|---|---|
| **Landed dataset** | `s3://data-sink/active/sam_opps_attachment_manifest_90day_winners/` (Lance v2.1) |
| **Grain** | one row per (notice, attachment) |
| **Rows** | **155,183** attachment pointers · 22 columns |
| **Distinct notices** | 41,963 · **Distinct resource_ids** | 155,183 (all unique) |
| **BTREE indices** | `notice_id`, `sol_norm`, `contract_award_unique_key`, `solicitation_identifier`, `resource_id` |
| **Verification** | deterministic PASS · live-drift 15/15 · adversarial 3/3 (65 independent samples) |

No PDF bytes were downloaded. `download_url` is a constructed, unauthenticated
pointer; the bytes are fetched in a later stage.

---

## 1. The executed funnel

| Stage | Measure | Result |
|---|---|---:|
| **Universe** | distinct prime awards (90-day API-fresh feed) | 1,229,191 |
| **1 — Bridge (offline)** | FPDS distinct normalized solnums | 148,359 |
| | SAM opps joined (active 77,683 ∪ archived 2,839,948) | 2,885,872 |
| | **resolved solnums (inner join)** | **49,248 (33.2%)** |
| **2 — Multiplicity** | winners (1 best notice / solnum) | 49,248 |
| | primary targets (Combined/Solicitation/Presol) | 37,547 |
| **3 — Harvest (live)** | notices swept | 49,248 |
| | with attachments | 41,963 (**85.2%**) |
| | empty (no substrate) | 7,242 |
| | errors (exhausted retries) | 43 (**0.09%**) |
| **4 — Sink** | attachment pointers landed | **155,183** (~3.7 / substrate notice) |

The drop-off is front-loaded (Sol# availability 17.4% → translation 33.2%);
substrate yield on resolved solnums is high (85.2%).

---

## 2. Design decisions (why this is correct, not just convenient)

- **Translation is an offline join, not a live search.** `api.sam.gov` SI-NONFED
  caps ~10 req/day (429, resets UTC midnight); 49 K probes is infeasible. The bridge
  joins our own `sam-gov-opps` bulk — the diagnostic proved it reproduces the live
  hit rate within 2 solnums.
- **Normalization on both sides** (`upper`; strip `[^A-Z0-9]`) absorbs FPDS↔SAM
  formatting drift (dashes/spaces). Verified: 0 normalization mismatches in audit.
- **Rank on `base_type`, not `notice_type`.** A solicitation that gets awarded flips
  its `notice_type` to "Award Notice" but keeps `base_type` = its original posting
  type — which is where the PWS/SOW attachments live. Ranking on `notice_type` would
  mis-demote 8,400+ awarded solicitations. Hierarchy: Combined Synopsis/Solicitation
  > Solicitation > Presolicitation > Special Notice > Modification > Justification >
  Award Notice > Sources Sought. Award Notice / Sources Sought are chosen only when
  no higher tier exists (7,063 + 959 solnums fall back; flagged `is_primary_target=false`).
- **Live harvest is single-threaded from a residential IP** at 0.12 s pace
  (datacenter egress is 429'd). Crash-safe + **resumable** via a JSONL checkpoint —
  the run survived ~5 session resumes and continued from the checkpoint each time.

---

## 3. Manifest schema

`resource_id`, `file_name`, `mime_type`, `size_bytes`, `access_level`,
`attachment_order`, `download_url` · `notice_id`, `solicitation_number`, `sol_norm`,
`notice_type`, `base_type`, `notice_posted_date`, `notice_title`,
`classification_code`, `naics_code`, `is_primary_target` · **`solicitation_identifier`,
`contract_award_unique_key`, `award_keys[]`, `award_count`** · `harvested_at`.

The last block is the explicit FPDS linkage. `award_keys[]` is the full list of prime
awards sharing the solnum (a solnum can spawn many award keys); `contract_award_unique_key`
is the representative (max `action_date`). All four key fields are 100% non-null.

---

## 4. Content profile

| | |
|---|---|
| **MIME** | `.pdf` 91,252 · `.docx` 24,697 · null-mime 24,566 · `.xlsx` 9,181 · `.zip` 1,783 · `.doc` 1,447 · others |
| **Access** | `public` 152,146 · **`private` 3,037 (2.0%)** — auth-gated, not anonymously downloadable |
| **size_bytes** | 0-null; **4,830 ≥10 MB** (lower-bound corrupt); 24,570 declared 0 (≈ the null-mime set) |
| **Primary-target split (notices w/ substrate)** | primary 32,921 · fallback 9,042 |

---

## 5. Verification (triangulated, all PASS)

- **Deterministic** (read-back vs bridge intermediate): rowcount/cols/indices,
  all key fields non-null, `download_url` well-formed and embeds `resource_id`, one
  winner per solnum, **ranking fidelity — 0 winners with a passed-over higher-tier
  sibling, 0 Award/Sources-Sought winners where a primary tier exists**.
- **Live-vs-manifest drift** (seed 7, 15 notices): **15/15 exact** resource_id-set
  match incl. an 8-attachment notice — parsing/landing correct.
- **Independent adversarial workflow** (3 agents, raw-source re-derivation, 65 samples):
  - *Join re-derive* (25): every winner is the min-`base_type`-rank notice for its
    solnum; multiplicity genuinely exercised (one solnum had 51 sibling notices).
  - *Award-trace* (25): every `(sol_norm, contract_award_unique_key)` resolves to ≥1
    real FPDS prime transaction.
  - *Live re-probe* (15, independent seed 31337): 15/15, counts 1–57 matched exactly.

**Operational note from the audit:** do NOT materialize the 2.88M-row archived opps
set into DuckDB and aggregate — it stalls. Push the predicate / solnum IN-list into
the scan. Codified in the pipeline; recorded here for downstream consumers.

---

## 6. Consuming the manifest

Join substrate to financials directly on the indexed keys:

```sql
-- every downloadable file for a given prime award
SELECT m.file_name, m.mime_type, m.download_url
FROM   sam_opps_attachment_manifest_90day_winners m
WHERE  m.contract_award_unique_key = :award_key;

-- high-value PWS/SOW hosts only (skip award-notice fallbacks)
SELECT * FROM sam_opps_attachment_manifest_90day_winners
WHERE  is_primary_target AND mime_type = '.pdf' AND access_level = 'public';
```

Caveats: filter `access_level='public'` before any byte fetch (3,037 are gated);
treat `size_bytes` as a lower bound (enforce real size via `Content-Length` at fetch,
never as a storage budget above 10 MB).

---

## 7. Next stage (not run here)

Stage 3 byte download → `s3://data-sink/landing/sam_attachments/<notice_id>/`, driven
off `download_url` where `access_level='public'`, single-threaded/residential, real
size enforced at fetch (see `sam_attachment_download.py`).
