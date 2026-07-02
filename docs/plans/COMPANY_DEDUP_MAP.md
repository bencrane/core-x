# Company Dedup Crosswalk — `company_id → canonical_company_id`

**Status:** LANDED (2026-07-02). Worker `pipelines/resolution/company_dedup_map.py`.
**Dataset:** `s3://data-sink/active/company_dedup_map/` (Lance v2.1, overwrite-snapshot).
**Ops ledger:** `ops.company_dedup_map_runs`.

The §3.1 deliverable of the GTM identity refactors — the complement to `entity_hierarchy` (§3.2).
A **non-destructive** bridge that collapses TRUE duplicate company rows (one real company logged
under two `company_id`s) WITHOUT fusing distinct subsidiaries. `companies_canonical` is never
touched; downstream joins this map for a deduped view, and the `company_source_platforms` sidecar
re-groups by the same join with **zero re-ingest**.

Dedup and hierarchy are complementary: **dedup** collapses one company logged twice; **hierarchy**
relates distinct subsidiaries under one parent. A merge that fused two UEIs the hierarchy calls
distinct subsidiaries would be a defect — the build gates against exactly that.

---

## 1. The rule — UEI-first, two-tier

Company dedup is two problems that look identical and must be handled oppositely: **true
duplicates** (same real company, two `company_id`s — merge) vs **subsidiary look-alikes** (distinct
companies sharing a parent's domain/LinkedIn — never merge). Live grain analysis (2026-07-02,
117,037 companies) resolved the rule:

| tier | population | rule | rationale |
|---|---|---|---|
| 1 | rows WITH `uei` (72,154, 62%) | merge on **exact `uei`** | `uei` is the authoritative federal entity key; distinct UEIs never merge — a **hard guarantee**, not a name heuristic |
| 2 | rows WITHOUT `uei` (39,005) | `coalesce(company_linkedin_url, normalized_domain) + legal_name_base(name_norm(company_name))` | two rows merge only if they share BOTH the contact key AND the canonical name base |
| — | neither key present (5,878) | singleton (own `company_id`) | no cross-source key |

`canonical_company_id = min(company_id)` per group (single namespace — always a valid
`company_id` that joins straight back to `companies_canonical`). `company_id` stays the 1:1 legacy PK.

### Why UEI-first beats the handoff's uniform name-gate
The subsidiary-fusion hazard is concentrated in Alaska-Native / tribal holdings (chenega.com,
cherokee-federal.com, bowhead.com, aleutfederal.com …) where dozens of DISTINCT awardable UEIs
share one domain and LinkedIn. Measured live: a naive **domain-only** merge would fuse **2,522**
distinct UEIs; **LinkedIn-only**, 2,288. The UEI-first rule fuses **0** — those subsidiaries carry
distinct UEIs, so Tier 1 never merges them. The name-gate is only the fallback for the non-federal
tail, which makes a holding-company blocklist unnecessary (two different firms at one domain don't
merge — different names).

**Result:** ~**2,007 merges → 115,030 canonical companies** (719 by exact UEI, 1,288 by name-gate).

---

## 2. Grain & schema

One row per `company_id` (117,037), 1:1 with `companies_canonical`.

| Column | Type | Index | Meaning |
|---|---|---|---|
| `company_id` | string | BTREE | legacy 1:1 key (PK) |
| `canonical_company_id` | string | BTREE | merge-group representative = `min(company_id)` |
| `is_canonical` | bool | | `company_id == canonical_company_id` (the group's kept row) |
| `method` | string | BITMAP | `uei` \| `linkedin_domain_name` \| `singleton` |
| `blocking_key` | string | | the group key (audit) |
| `group_size` | int32 | | members in the canonical group |
| `snapshot_date` | date | | recompute stamp |

Deduped view: `companies_canonical ⨝ company_dedup_map ON company_id`, group by
`canonical_company_id`. Provenance re-groups the same way with no re-ingest.

---

## 3. Fail-closed build gates

1. **Grain** — `rows == distinct company_id == companies_canonical rows`, no null `company_id`.
2. **FUSION** — **0** canonical groups span >1 distinct `uei` (the entity_hierarchy-fusion safety;
   satisfied by the UEI-gate by construction, and asserted).

Deterministic: every collapse breaks frequency ties with `row_number() … ORDER BY cnt DESC, key ASC`.

---

## 4. Verified end-to-end (2026-07-02, adversarial 4-verifier pass)

- **Integrity:** grain 117,037; canonical reps all real company_ids; **fusion_violations = 0** across 2,007 merges.
- **Reproducibility:** independent re-derivation from `companies_canonical` (via `core.name_norm`) matches the live dataset on all 117,037 rows — **0 canonical mismatches**, identical 115,030 groups. The committed code reproduces the data exactly.
- **Coverage:** zero orphans; `companies_canonical` / `company_source_platforms` / `company_dedup_map` are identical sets at 117,037, exactly one map row per `company_id`.

Non-federal handling (e.g. the 24,398 dex staffing agencies): 100% domain-covered → all flow through
Tier 2, and same-generic-name firms (five distinct "Recruiting Solutions" on five domains) are
correctly kept apart by the domain gate.

---

## 5. Rebuild / rollback

```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/resolution/company_dedup_map.py <init_ops|build|reindex|verify>
```

Overwrite-snapshot; prior Lance version retained as the anchor. `reindex` rebuilds indexes in place.
Rollback = drop the map (nothing downstream is destructively coupled) or restore a prior version.
