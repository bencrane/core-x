# Overture Places — v3 Resolution-Keys Re-Ingest Directive

**Audience:** an AI agent executing against the `core-x` data plane, cold, with no prior context.
**Companions:** [docs/overture_places_optimization_directive.md](overture_places_optimization_directive.md) (the v2 migration this builds on) · [docs/overture_places_structural_diagnostic.md](overture_places_structural_diagnostic.md) (the read-only diagnostic).
**Target SoR:** `s3://data-sink/active/overture_places/` (Gen-3 Lance, R2). 16,273,123 rows.
**Nature:** a **one-shot, source-RE-INGEST** that rebuilds the SoR from the Overture source **pinned to the same release the live SoR was built from (`2026-05-20.0`)** so v3 is a true row-superset of v2, carrying the deterministic resolution keys (`domain`, `phone`, `street`) the v2 ingest dropped. The SoR URI does **not** change.
**Authority:** the engineering decisions in §2 are **locked** — execute the runbook in §6. The re-ingest **overwrites the SoR**; it is gated (build → LOCAL hard-verify → R2 backup → publish → post-publish verify → restore-on-fail), re-run-safe (clean no-op if already v3), ledgered, and reversible.

> Every load-bearing claim below carries the command that produced it and the actual output. Numbers are from probes run **2026-06-06** against the live v2 SoR (read-only) and the **anonymous** Overture source, on the verification venv `/tmp/overture_diag/venv/bin/python` (duckdb 1.5.3 + spatial/httpfs, pylance 7.0.0, pyarrow 24.0.0). Sample = source file `part-00000` of the pinned release, US subset = **1,039,583 rows**.

---

## 1. Mission

The v2 SoR is a geographic + name skeleton. It dropped the **deterministic match keys** an external record (CRM / company / enrichment row) needs to resolve to a place: the registrable **domain**, the canonical **phone**, and the full **street**. Fuzzy name + geo is not a join key. v3 restores them, indexes the deterministic two, and proves an external record can be resolved by `domain = …` / `phone = …` through a Lance scalar index.

**The hard architectural fact (confirmed, §2/D1):** these fields are **not in the v2 SoR** — they exist **only in the Overture source**. The v2 `optimize.py` reads the SoR and is structurally incapable of recovering a dropped column. So this is a **re-ingest from source**, not an in-place transform. To avoid vintage drift, the re-ingest is **pinned to `2026-05-20.0`** — the exact release the live SoR was built from (proven below) — so v3 has the **same rows** as v2, plus the new columns.

**Non-negotiables (safety contract):**
- **Row-superset, same release:** output row count and `DISTINCT(id)` MUST equal the v2 baseline **16,273,123** for the pinned release. No silent filtering.
- **Build → verify → publish:** the v3 dataset is built and HARD-verified on LOCAL disk; R2 is mutated only after the gate passes.
- **Backup before wipe:** the current v2 prefix is server-side-copied to a backup before the publish wipe; post-publish verification failure triggers automatic restore.
- **Coverage gate:** the verify step asserts `domain`/`phone`/`street` non-null coverage floors — a re-ingest that silently lost the keys fails before publish.
- **Ledgered:** terminal state in `ops.overture_places_runs` (`write_path='reingest_v3'`).

---

## 2. Locked decision ledger (rationale + gathered evidence)

### D1 — Re-ingest from source, pinned to `2026-05-20.0`. NOT an in-place transform.

The dropped fields are absent from the SoR and present in the source; the only way to add them is to re-read source. Pin to the live SoR's own release so v3 is a true superset.

**Evidence — v2 SoR schema (read-only R2) lacks every resolution key, and its release is `2026-05-20.0`:**
```
$ cd /Users/benjamincrane/core-x && doppler run -- /tmp/overture_diag/venv/bin/python /tmp/overture_diag/v3_probe1_schemas.py
# v2_sor.schema = {id, longitude, latitude, hilbert, region, locality, postcode, name, category, confidence}   (10 cols)
# v2_sor.rows   = 16273123
# v2_sor.metadata.release_tag = "2026-05-20.0"   ← the live SoR's release
# v2_sor.missing_resolution_keys = [addresses, phones, websites, socials, emails, operating_status, taxonomy, brand]   ← ALL absent
```

**Evidence — the source (pinned release) HAS them, with the exact field shapes the SQL targets:**
```
# source.release = "2026-05-20.0"  · file_count = 16 · total = 9.99 GiB   (the release exists, in full)
# source.schema (relevant):
#   addresses  STRUCT(freeform VARCHAR, locality, postcode, region, country)[]   ← addresses[1].freeform = street
#   websites   VARCHAR[]            phones VARCHAR[]   emails VARCHAR[]   socials VARCHAR[]
#   taxonomy   STRUCT("primary" VARCHAR, hierarchy VARCHAR[], alternates VARCHAR[])
#   brand      STRUCT(wikidata VARCHAR, ...)        operating_status VARCHAR        basic_category VARCHAR
#   geometry   GEOMETRY('OGC:CRS84')   names STRUCT("primary", ...)   categories STRUCT("primary", alternate[])
```
**Conclusion:** in-place is impossible; re-ingest pinned to `2026-05-20.0` is correct and yields a same-rows superset. The `optimize.py` path is retained for *structural* re-optimization of an existing SoR; it is **not** the tool here.

### D2 — Carry resolution keys: `domain` (normalized), `phone` (normalized), `street` (raw freeform), `taxonomy` (richer category leaf).

Coverage + resolution value justify each; the second tier (email/social/operating_status/brand) is dropped (D6). Measured US-subset coverage:

```
$ cd /Users/benjamincrane/core-x && doppler run -- /tmp/overture_diag/venv/bin/python /tmp/overture_diag/v3_probe2_normalize.py
# US sample rows: 1,039,583
#   name 100.00%  locality 99.82%  region 99.74%  postcode 97.08%
#   street(freeform) 96.61%   taxonomy.primary 95.37%   category.primary 95.37%   basic_category 93.56%
#   phone 92.02%   website 81.65%   social 67.66%   operating_status 51.50%   email 44.32%   brand.wikidata 1.76%
```

| Key | Coverage | Resolution value | Verdict |
|---|---:|---|---|
| `street` (raw `addresses[1].freeform`) | 96.61% | High — full street line; address blocking | **KEEP** raw (D4) |
| `taxonomy` (`taxonomy.primary`) | 95.37% | Medium — richer than `category`; disambiguation, not a join key | **KEEP** as data (no index) |
| `phone` (normalized) | 92.02% raw → 91.83% after norm | **Highest — deterministic key** | **KEEP + BTREE** (D3) |
| `domain` (normalized) | 81.65% raw → 81.64% after norm | **Highest — deterministic key** | **KEEP + BTREE** (D3) |

### D3 — `domain` + `phone` get BTREE scalar indexes (the deterministic keys). `street`/`taxonomy` stay as unindexed data.

A scalar BTREE on `domain`/`phone` turns an external-record lookup into an index probe (proven §10). `street` is high-cardinality free text whose exact form rarely matches a CRM string verbatim; it is a *blocking/scoring* field, not an index probe target, so it ships raw without an index (a normalized blocking key is discussed in D4 and deferred). `taxonomy` is a refinement attribute, not a join key.

### D4 — `street` = raw `freeform`, no normalized blocking key in v3.

How street is actually matched: never as `street = '<crm string>'` (formatting variance kills exact match — `2015 S. Tuttle Ave. Suite A` vs `2015 South Tuttle Avenue Ste A`). It is used **after** a domain/phone/geo block narrows candidates, as a fuzzy/normalized comparison computed at query time. A persisted normalized key (upper, strip punctuation, collapse whitespace) was tested:
```
# probe output (sample): "2015 S. Tuttle Ave. Suite A" -> "2015 S  TUTTLE AVE. SUITE A"
```
It does not survive real-world abbreviation variance (`Ave`↔`Avenue`, `Ste`↔`Suite`, `S`↔`South`) and a half-normalized key gives false confidence. A correct USPS-style address normalizer is a separate component (out of scope). **Decision:** ship `street` raw; do address normalization downstream/at query time. Documented as a deploy-time follow-on, not a v3 gap.

### D5 — `domain` normalization = regex eTLD+1 with a multi-part-TLD carve and a final validation gate. Primary domain only (websites[1]), scalar `string`, BTREE.

A full public-suffix list (PSL) is **not** justified for US data — multi-part TLDs are vanishingly rare:
```
$ # from v3_probe2_normalize.py — domain.tld_analysis
# total_domains 853,646 · multipart_tld (co.uk-style) 842 · multipart_pct 0.0986%
# top suffixes: .com 729,510 · .org 46,028 · .net 25,072 · .us 7,730 · .biz 5,682 · .gov 4,735 · .edu 3,662 · .co 2,418 · .io 1,099
```
A regex eTLD+1 that takes the last 2 labels (3 when the penultimate is a multi-part suffix token AND the last is a 2-letter ccTLD) is correct for **99.90%**; the 0.0986% co.uk-class is handled by the carve. A PSL dependency (network fetch or a vendored 15k-line table refreshed out of band) is unwarranted complexity for <0.1% of rows.

**Primary vs full list:** websites is `VARCHAR[]` but multi-value is rare — `multi_website 4,812` of 848,834 (0.57%), `max_websites 2`. A scalar primary `domain` (from `websites[1]`) is the right index target; a full domain list would 1.006× the rows for a 0.57% gain and complicate the index. **Decision:** scalar primary `domain`, BTREE.

**Hardened, tested (15/15 edge asserts pass; the shipped `_transform.domain_sql` form):**
```
$ cd /Users/benjamincrane/core-x && doppler run -- /tmp/overture_diag/venv/bin/python /tmp/overture_diag/v3_probe8_module.py
# domain_all_ok: True
#   https://www.massagebook.com/biz/x         -> massagebook.com
#   http://www.http://craigsrestaurant.com/   -> craigsrestaurant.com   (embedded double-scheme)
#   https://www.gob.pe/consulado              -> gob.pe                 (bare multi-part ccTLD)
#   https://shop.example.co.uk/path?q=1       -> example.co.uk          (3-label multi-part)
#   http://34.70.88.107/                      -> NULL                   (IPv4 literal rejected)
#   http://www.miracleinstitute/              -> NULL                   (no dot)
#   HTTP://WWW.Walgreens.COM                  -> walgreens.com          (case)
#   http://store.t-mobile.com/x               -> t-mobile.com           (hyphen preserved)
# domain coverage (hardened, full sample): 81.64% of US rows · 99.98% valid-rate of present websites · 448,684 distinct
```

### D6 — Second-tier fields: DROP `email`, `social`, `operating_status` (as a column), `brand`. KEEP `taxonomy` (data only).

Not reflexively keeping everything — each judged on coverage × resolution value:

| Field | Coverage | Resolution value | Verdict |
|---|---:|---|---|
| `taxonomy.primary` | 95.37% | Refines `category` for disambiguation | **KEEP** (data, no index) |
| `email` | 44.32% | Weak/noisy join key (shared `info@`, role addresses); low coverage | **DROP** |
| `social` | 67.66% | Heterogeneous URLs (fb/ig/x), no canonical form, weak key | **DROP** |
| `operating_status` | 51.50% | A *filter signal*, not a resolution key; see D7 | **DROP as column** (closed handled at ingest, D7) |
| `brand.wikidata` | 1.76% | Near-empty in US data | **DROP** |

Dropping these keeps the row footprint lean (each added string column is real cost — see §3 footprint).

### D7 — Closed places: KEEP all rows (do NOT filter). Row count is preserved.

Closed places are a tiny fraction; dropping them would break the row-superset contract for ~0.04% gain and lose places a stale CRM record may still legitimately resolve to.
```
$ cd /Users/benjamincrane/core-x && doppler run -- /tmp/overture_diag/venv/bin/python /tmp/overture_diag/v3_probe6_idxcost.py
# closed-family sample: permanently_closed 340 · closed 40 · temporarily closed 2  = 382 rows
# closed_pct_sample: 0.0367%   projected to full SoR: ~5,980 rows
```
**Decision:** keep every row; do not persist `operating_status`. The row count stays **16,273,123**. (If a consumer later needs open/closed filtering, that is a v4 column add, not a v3 row drop.)

### D8 — Safety harness modeled on `optimize.py`: build → LOCAL hard-verify (incl. coverage + pushdown) → server-side R2 backup of current v2 → wipe+publish → post-publish verify → restore-on-fail → ledger. dryrun/apply split.

A destructive overwrite of the canonical SoR without backup + verify-before-publish is unacceptable. The v3 worker reuses `optimize.py`'s proven harness verbatim (server-side `CopyObject` backup = no egress; boto3 uniform-part publish = R2-compliant; auto-restore on any post-publish failure). The `places.py::run` ingest has **no** backup/verify/restore and is **not** used for this destructive re-ingest.

### D9 — Same URI, same fragment sizing (1,048,576 rows / 90 GiB), same storage version (2.1), same `(region, hilbert)` sort.

Stable downstream addressing; v3 is a superset re-sort + column add, not a topology change. All v2 structural wins (Hilbert sort key, region BITMAP, constant-demotion to metadata, confidence float32) are preserved.

### D10 — Go-forward parity: `_transform.py` + `places.py` emit v3, else the next monthly ingest reverts the SoR to v2.

The shared transform is the single source of truth; both the one-shot worker and the recurring ingest import it. Edits in §5.3.

---

## 3. Target v3 schema + index set + projected footprint

### Schema `overture_places.v3` — 14 per-row columns (was 10)

| # | Column | Type | vs v2 |
|---|---|---|---|
| 1 | `id` | `string` | unchanged (GERS UUID; plane-wide join key) |
| 2 | `longitude` | `double` | unchanged |
| 3 | `latitude` | `double` | unchanged |
| 4 | `hilbert` | `uint32` | unchanged (sort key) |
| 5 | `region` | `string` | unchanged (USPS-normalized) |
| 6 | `locality` | `string` | unchanged |
| 7 | `postcode` | `string` | unchanged |
| 8 | `name` | `string` | unchanged |
| 9 | `category` | `string` | unchanged |
| 10 | `taxonomy` | `string` | **NEW** — `taxonomy.primary` |
| 11 | `confidence` | `float` | unchanged (float32) |
| 12 | `domain` | `string` | **NEW** — registrable domain from `websites[1]` |
| 13 | `phone` | `string` | **NEW** — `+1XXXXXXXXXX` (NANP) from `phones[1]` |
| 14 | `street` | `string` | **NEW** — raw `addresses[1].freeform` |

**Demoted to `schema.metadata`** (not columns): `country`, `snapshot_date`, `release_tag`, `ingested_at`, plus `schema_version=overture_places.v3`, `sort_order=region,hilbert`, `hilbert_bounds`.

### Index set — 7 BTREE + 2 BITMAP (was 5 BTREE + 2 BITMAP)

- **BTREE:** `id`, `name`, `postcode`, `locality`, `hilbert`, **`domain`**, **`phone`**
- **BITMAP:** `region`, `category`

### Projected footprint (measured per-index on the sample, scaled ×15.654 to 16,273,123 rows)

```
$ # v3_probe6_idxcost.py — per-index on-disk size, built one index at a time on the sample
# BTREE:id 26.08 · BTREE:name 19.72 · BTREE:domain 13.42 · BTREE:phone 11.34 · BTREE:postcode 6.44
# BTREE:hilbert 2.93 · BTREE:locality 2.58 · BITMAP:category 2.26 · BITMAP:region 0.02   (MiB, sample)
# scale factor to full: 15.654×
```

| Quantity | v2 SoR (measured, diagnostic) | v3 added (estimate) | Note |
|---|---:|---:|---|
| New index: `domain` BTREE | — | **~210 MiB** | 448,684 distinct/sample → ~7.0M distinct full |
| New index: `phone` BTREE | — | **~178 MiB** | 751,485 distinct/sample → ~11.8M distinct full |
| New index total | 1.340 GiB | **~388 MiB** (+28%) | justified: these ARE the deterministic keys |
| New data: `domain` col | — | **~213 MiB decoded** | avg len 16.8 |
| New data: `phone` col | — | **~171 MiB decoded** | fixed 12-char |
| New data: `street` col | — | **~274 MiB decoded** | avg len 18.2 |
| New data: `taxonomy` col | — | **~227 MiB decoded** | — |

The diagnostic warned index ≈ data size. v3 adds ~388 MiB of index on a 1.34 GiB base — a deliberate, bounded cost for the two highest-value join keys, not casual index sprawl. (`street`/`taxonomy` carry **zero** index cost.) On-disk compressed footprint will be lower than decoded; measure post-run (§7).

---

## 4. Verified primitives (proven against the live stack 2026-06-06 — do not re-derive)

- **`regexp_replace` global flag is mandatory.** DuckDB's default replaces only the FIRST match:
  ```
  regexp_replace('(808) 548-3700', '[^0-9]', '')       -> '808) 548-3700'   (WRONG — first run only)
  regexp_replace('(808) 548-3700', '[^0-9]', '', 'g')  -> '8085483700'      (correct)
  ```
  Every `regexp_replace` in `domain_sql`/`phone_sql` uses `'g'`. **This is the single most likely silent bug if the SQL is hand-edited.**
- **Phone: strip the extension BEFORE extracting digits** — else `+1 (212) 555-0199 ext 4` → `121255501994` (the `4` leaks). The shipped order strips `(?i)\s*(ext|extension|x|#)\.?\s*\d+\s*$` first, then `[^0-9]`.
- **Phone NANP validity gate** (`[2-9]\d{2}[2-9]\d{6}`) rejects vanity/junk/intl. 11/11 edge asserts pass (`v3_probe8_module.py`): `(800) BUY-CARS`→NULL, `+8801761095740`→NULL (Bangladesh), `1-866-366-3501`→`+18663663501`, `+1 (212) 555-0199 ext 4`→`+12125550199`.
- **Domain registrable + gate** — 15/15 edge asserts pass (D5). The final `regexp_full_match` gate + IPv4 rejection nulls every malformed host (no-dot, path-as-domain, IP literal, embedded scheme).
- **`ST_Hilbert(lon::DOUBLE, lat::DOUBLE, ST_Extent(ST_MakeEnvelope(-180,-90,180,90)))` → UINTEGER** builds on the sample (carried from v2; re-confirmed by the end-to-end build below).
- **Source geometry is auto-typed `GEOMETRY('OGC:CRS84')` in this release**, so `_detect_geometry_decode` resolves to `geometry` (no `ST_GeomFromWKB` needed); the helper still handles the WKB-BLOB case for older releases.
- **End-to-end: the EXACT worker `_build_sql` produces the v3 schema + coverage on the sample:**
  ```
  $ cd /Users/benjamincrane/core-x && doppler run -- /tmp/overture_diag/venv/bin/python -c "<imports reingest_v3._build_sql, runs on part-00000>"
  # geom_decode: geometry
  # schema: id,longitude,latitude,hilbert,region,locality,postcode,name,category,taxonomy,confidence,domain,phone,street
  # rows 1,039,583 · domain% 81.64 · phone% 91.83 · street% 96.61 · distinct_id==rows: True
  ```
- **Pushdown + real resolution smoke (throwaway local Lance, indices built):**
  ```
  $ # v3_probe5_e2e.py
  # domain = 'onepieceboosters.com'  -> plan: ScalarIndexQuery: ...@domain_idx(BTree)   (pushdown=True)
  #   resolves to 1 place: {name: Mhtradingcardshop, locality: Anchorage, region: AK}
  # phone = '+13312034600'           -> plan: ScalarIndexQuery: ...@phone_idx(BTree)    (pushdown=True) -> 1 place
  ```
- **Shipped code compiles + imports** (`/tmp/overture_diag/_compile_pkg`):
  ```
  $ /tmp/overture_diag/venv/bin/python -m py_compile _transform.py reingest_v3.py places.py   # all OK
  $ python -c "from pipelines.overture_maps import _transform, places, reingest_v3"           # imports OK
  #   T.SCHEMA_VERSION = overture_places.v3 · BTREE = [id,name,postcode,locality,hilbert,domain,phone]
  #   reingest_v3.PINNED_RELEASE = 2026-05-20.0 · both _build_sql contain AS domain/phone/street/taxonomy
  ```

---

## 5. Implementation — three artifacts

### 5.1 REPLACE — `pipelines/overture_maps/_transform.py`

The v2 file becomes the v3 single-source-of-truth: bumps `SCHEMA_VERSION`, adds `domain_sql`/`phone_sql`, extends the projection and the BTREE index list. (Hilbert + region-normalize are unchanged.) **Verbatim, as tested + compiled:**

```python
"""Shared transform constants for the Overture Places schema.

Imported by the one-shot migration (optimize.py), the v3 resolution-keys re-ingest
(reingest_v3.py), and the go-forward ingest (places.py) so every write path is born
in the current layout. Pure SQL fragments + the canonical index plan. No I/O.

v3 (overture_places.v3) — ADDS the deterministic + high-value resolution keys the v2
schema dropped: registrable `domain` (from websites[]), canonical `phone` (E.164/NANP
from phones[]), raw `street` (addresses[1].freeform), and richer `taxonomy`
(taxonomy.primary). `domain` and `phone` get BTREE scalar indexes — an external record
resolves to a place by domain / phone / full street, not just fuzzy name + geo.
"""

SCHEMA_VERSION = "overture_places.v3"

# ── Hilbert space-filling sort/spatial key (unchanged from v2) ────────────────
HILBERT_BOUNDS_SQL = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"
HILBERT_BOUNDS_TAG = "-180,-90,180,90"
HILBERT_EXPR_SQL = f"ST_Hilbert(longitude::DOUBLE, latitude::DOUBLE, {HILBERT_BOUNDS_SQL})"

# ── region normalization (unchanged from v2) ─────────────────────────────────
USPS_VALID = (
    "'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',"
    "'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',"
    "'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',"
    "'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'"
)
REGION_NORMALIZE_SQL = f"""CASE
  WHEN UPPER(TRIM(region)) IN ({USPS_VALID}) THEN UPPER(TRIM(region))
  WHEN UPPER(TRIM(region)) = 'CALIFORNIA'           THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'CALIF'                THEN 'CA'
  WHEN UPPER(TRIM(region)) = 'TEXAS'                THEN 'TX'
  WHEN UPPER(TRIM(region)) = 'FLORIDA'              THEN 'FL'
  WHEN UPPER(TRIM(region)) = 'NEW YORK'             THEN 'NY'
  WHEN UPPER(TRIM(region)) = 'OHIO'                 THEN 'OH'
  WHEN UPPER(TRIM(region)) = 'ARIZONA'              THEN 'AZ'
  WHEN UPPER(TRIM(region)) = 'PENNSYLVANIA'         THEN 'PA'
  WHEN UPPER(TRIM(region)) = 'VIRGINIA'             THEN 'VA'
  WHEN UPPER(TRIM(region)) = 'TENNESSEE'            THEN 'TN'
  WHEN UPPER(TRIM(region)) = 'NEVADA'               THEN 'NV'
  WHEN UPPER(TRIM(region)) = 'DELAWARE'             THEN 'DE'
  WHEN UPPER(TRIM(region)) = 'WYOMING'              THEN 'WY'
  WHEN UPPER(TRIM(region)) = 'NORTH DAKOTA'         THEN 'ND'
  WHEN UPPER(TRIM(region)) = 'DISTRICT OF COLUMBIA' THEN 'DC'
  ELSE NULL
END"""


# ── domain normalization ─────────────────────────────────────────────────────
# website string -> registrable domain (eTLD+1) or NULL. ALL regexp_replace use the
# 'g' (global) flag — DuckDB's default replaces only the FIRST match. Steps:
#   1. lower+trim  2. strip leading scheme(s) repeatedly (http://https://…)
#   3. strip userinfo (user:pass@)  4. strip path/port/query/fragment
#   5. strip leading www.  6. strip every char not [a-z0-9.-]
#   7. registrable: 3 labels iff host has >=3 labels AND penultimate is a known
#      multi-part suffix token AND last label is a 2-letter ccTLD; else last 2 labels.
#   8. FINAL GATE: keep only a clean registrable shape, and reject IPv4 literals.
def domain_sql(col: str) -> str:
    host = (
        "regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        f"lower(trim({col})), '^([a-z][a-z0-9+.-]*://)+', '', 'g'), "
        "'^[^/@]*@', '', 'g'), '[/:?#].*$', '', 'g'), "
        r"'^www\.', '', 'g'), '[^a-z0-9.-]', '', 'g')"
    )
    nlab = f"(length({host}) - length(replace({host}, '.', '')) + 1)"
    reg = (
        "CASE WHEN " + nlab + " >= 3 "
        f"AND regexp_extract({host}, '([^.]+)\\.([^.]+)$', 1) IN "
        "('co','com','org','net','gov','edu','ac','gob','go') "
        f"AND length(regexp_extract({host}, '\\.([^.]+)$', 1)) = 2 "
        f"THEN regexp_extract({host}, '([^.]+\\.[^.]+\\.[^.]+)$', 1) "
        f"ELSE regexp_extract({host}, '([^.]+\\.[^.]+)$', 1) END"
    )
    return (
        f"CASE WHEN regexp_full_match({reg}, '([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z]{{2,}}') "
        f"AND NOT regexp_full_match({host}, '[0-9]{{1,3}}(\\.[0-9]{{1,3}}){{3}}') "
        f"THEN {reg} ELSE NULL END"
    )


# ── phone normalization ──────────────────────────────────────────────────────
# phone string -> +1XXXXXXXXXX (NANP/E.164) or NULL. Extension stripped BEFORE digit
# extraction (else "ext 4" leaks a trailing 4). 'g' flag mandatory. NANP validity:
# area + exchange leading digit must be 2-9.
def phone_sql(col: str) -> str:
    digits = (
        f"regexp_replace(regexp_replace({col}, "
        "'(?i)\\s*(ext|extension|x|#)\\.?\\s*[0-9]+\\s*$', '', 'g'), "
        "'[^0-9]', '', 'g')"
    )
    ten = (
        f"CASE WHEN length({digits})=11 AND left({digits},1)='1' THEN substr({digits},2) "
        f"WHEN length({digits})=10 THEN {digits} ELSE NULL END"
    )
    return (
        f"CASE WHEN ({ten}) IS NOT NULL "
        f"AND regexp_full_match(({ten}), '[2-9][0-9]{{2}}[2-9][0-9]{{6}}') "
        f"THEN '+1' || ({ten}) ELSE NULL END"
    )


# ── index plan ───────────────────────────────────────────────────────────────
# v3 adds the two deterministic resolution keys as BTREE: domain, phone.
OPTIMIZED_BTREE_INDEXES = ["id", "name", "postcode", "locality", "hilbert", "domain", "phone"]
OPTIMIZED_BITMAP_INDEXES = ["region", "category"]


def projection_sql(src: str) -> str:
    """Per-row v3 projection. `src` exposes the flat source columns (the committed
    Lance SoR for a transform, or the ingest geo/flat CTE for an ingest). Constants
    (country/snapshot_date/release_tag/ingested_at) are demoted to schema metadata.
    domain/phone/street/taxonomy are the v3 resolution-key additions. ORDER BY clusters
    fragments by region then space-filling key."""
    return f"""SELECT
    id,
    longitude,
    latitude,
    CAST({HILBERT_EXPR_SQL} AS UINTEGER) AS hilbert,
    {REGION_NORMALIZE_SQL} AS region,
    locality,
    postcode,
    name,
    category,
    taxonomy,
    CAST(confidence AS FLOAT) AS confidence,
    domain,
    phone,
    street
FROM {src}
ORDER BY region NULLS LAST, hilbert"""
```

> **Impact on `optimize.py`:** it imports `SCHEMA_VERSION`, `OPTIMIZED_*`, `projection_sql` from this module. After this edit, `optimize.py`'s `projection_sql("src")` references `domain/phone/street/taxonomy` columns that **do not exist in the v2 SoR it reads** — so `optimize.py` would error if run against a v2 SoR. That is **correct and intended**: once v3 is the schema, the v2 in-place optimizer is obsolete (its `_verify_local` also hard-codes the 10-field v2 schema). After v3 ships, `optimize.py` is dead code for this dataset; leave it as historical record or delete in a follow-up. **Do not run `optimize.py` after editing `_transform.py`.** (This is the "downstream depends on what you added" tension, called out in §11.)

### 5.2 NEW FILE — `pipelines/overture_maps/reingest_v3.py`

The one-shot v3 re-ingest worker. Reads the Overture source pinned to `2026-05-20.0`, applies the v3 transform, builds locally, and publishes with `optimize.py`'s full safety harness (backup → LOCAL hard-verify incl. coverage + pushdown gates → publish → post-publish verify → restore-on-fail → ledger). **Verbatim, as compiled + import-checked:**

```python
"""One-shot v3 RE-INGEST of the Overture Places SoR with deterministic resolution keys.

WHY A RE-INGEST (not an in-place transform): the v2 SoR dropped websites/phones/
freeform/taxonomy at ingest — they exist ONLY in the Overture source, so they cannot be
recovered from the committed dataset (optimize.py reads the SoR and is structurally
incapable of adding them). This worker re-reads the Overture source PINNED to the SAME
release the live SoR was built from (2026-05-20.0) so v3 is a true row-superset of v2
(same release, same US filter), adds registrable `domain` + canonical `phone` + raw
`street` + `taxonomy`, BTREE-indexes the two deterministic keys (domain, phone), and
republishes to the SAME URI.

It OVERWRITES the SoR, so it carries optimize.py's full safety harness: build → LOCAL
HARD verify gate → server-side R2 backup of the current v2 → wipe+publish → post-publish
verify → restore-on-failure → ops ledger. dryrun/apply split. Re-run-safe (clean no-op
if the SoR is already v3).

    modal run pipelines/overture_maps/reingest_v3.py::dryrun   # build+verify LOCAL only, NO mutation
    modal run pipelines/overture_maps/reingest_v3.py::apply    # backup -> publish -> verify -> ledger
"""
from __future__ import annotations

import os

import modal

from pipelines.overture_maps._transform import (
    HILBERT_BOUNDS_TAG,
    OPTIMIZED_BITMAP_INDEXES,
    OPTIMIZED_BTREE_INDEXES,
    SCHEMA_VERSION,
    domain_sql,
    phone_sql,
    projection_sql,
)

# ── System-of-record (R2) ──────────────────────────────────────────────────
BUCKET = "data-sink"
DATASET_PREFIX = "active/overture_places/"
DATASET_URI = f"s3://{BUCKET}/{DATASET_PREFIX}"
SCRATCH_DIR = "/tmp/overture_v3"
LOCAL_OUT = os.path.join(SCRATCH_DIR, "out_lance")
FEED = "overture_places"

# ── Upstream (public AWS S3 — anonymous) ───────────────────────────────────
OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
OVERTURE_PLACES_GLOB = "s3://overturemaps-us-west-2/release/{rel}/theme=places/type=place/*.parquet"

# Pin to the EXACT release the live v2 SoR was built from (its schema metadata
# release_tag), so v3 is a true superset of v2 — same rows, no vintage drift.
PINNED_RELEASE = "2026-05-20.0"
# Row baseline from the 2026-06-06 diagnostic — assert no drift before publishing.
SRC_ROWS_EXPECTED = 16_273_123

# Lance fragment sizing — identical to the ingest / optimize.
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
STREAM_BATCH_ROWS = 1_048_576


class AlreadyV3(Exception):
    """Raised when the SoR is already overture_places.v3 — re-ingest is a no-op."""


image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.35",
        "psycopg[binary]>=3.2",
    )
    .run_commands(
        "python -c \"import duckdb; duckdb.connect().execute('INSTALL httpfs; INSTALL spatial;')\""
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})
    .add_local_python_source("pipelines")
)

app = modal.App("overture-maps-reingest-v3", image=image)


# ── R2 helpers (self-contained; mirror optimize.py) ──────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "aws_endpoint": endpoint,
        "aws_region": "auto",
        "aws_virtual_hosted_style_request": "false",
    }


def _lance_storage_options() -> dict[str, str]:
    return _r2_storage_options()


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["aws_endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _anon_s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client("s3", region_name=OVERTURE_REGION,
                        config=Config(signature_version=UNSIGNED))


def _list_keys(s3, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
    return keys


def _backup_r2_prefix(s3, src_prefix: str, bak_prefix: str) -> int:
    n = 0
    for key in _list_keys(s3, src_prefix):
        rel = key[len(src_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=bak_prefix + rel)
        n += 1
    return n


def _wipe_prefix(s3, prefix: str) -> None:
    batch = []
    for key in _list_keys(s3, prefix):
        batch.append({"Key": key})
        if len(batch) == 1000:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
            batch = []
    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})


def _upload_dir(s3, prefix: str, local_dir: str) -> tuple[int, int]:
    files = bytes_ = 0
    for root, _, fnames in os.walk(local_dir):
        for fn in fnames:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            files += 1
            bytes_ += os.path.getsize(lp)
    return files, bytes_


def _restore_r2_prefix(s3, bak_prefix: str, dst_prefix: str) -> int:
    _wipe_prefix(s3, dst_prefix)
    n = 0
    for key in _list_keys(s3, bak_prefix):
        rel = key[len(bak_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=dst_prefix + rel)
        n += 1
    return n


def _record_run(dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                published_files, published_bytes, write_path, status, error,
                started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.overture_places_runs
                    (feed, dataset_uri, release_tag, snapshot_date, rows_processed,
                     distinct_ids, published_files, published_bytes, write_path,
                     status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                 published_files, published_bytes, write_path, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the re-ingest
        print(f"WARN: ops.* write failed: {exc}")


def _detect_geometry_decode(con, read_glob: str) -> str:
    """Overture geometry is WKB BLOB in older releases, native GEOMETRY in newer ones.
    DESCRIBE the footer (LIMIT 0, no rows read) and pick the decode accordingly."""
    desc = con.execute(
        "DESCRIBE SELECT geometry FROM read_parquet(?) LIMIT 0", [read_glob]
    ).fetchall()
    col_type = (desc[0][1] if desc else "BLOB").upper()
    return "geometry" if "GEOMETRY" in col_type else "ST_GeomFromWKB(geometry)"


def _build_sql(geom_expr: str) -> str:
    """Overture source -> v3 schema. Anonymous read_parquet over the PINNED release;
    decode geometry once (geo CTE) + US filter pushed to scan; flatten ST_X/ST_Y +
    unpack addresses[1]/names/categories/taxonomy + normalize domain/phone/street
    (flat CTE); shared v3 projection. WKB never leaves the geo CTE. geom_expr is
    repo-controlled (probe output); only the read path binds positionally (one ?)."""
    return f"""
WITH raw AS (
    SELECT * FROM read_parquet(?)
),
geo AS (
    SELECT
        id,
        {geom_expr} AS geom,
        addresses,
        names,
        categories,
        taxonomy,
        websites,
        phones,
        confidence
    FROM raw
    WHERE addresses[1].country = 'US'
),
flat AS (
    SELECT
        nullif(trim(id), '')                     AS id,
        ST_X(geom)                               AS longitude,
        ST_Y(geom)                               AS latitude,
        nullif(trim(addresses[1].region), '')    AS region,
        nullif(trim(addresses[1].locality), '')  AS locality,
        nullif(trim(addresses[1].postcode), '')  AS postcode,
        nullif(trim(names.primary), '')          AS name,
        nullif(trim(categories.primary), '')     AS category,
        nullif(trim(taxonomy.primary), '')       AS taxonomy,
        TRY_CAST(confidence AS DOUBLE)           AS confidence,
        {domain_sql('websites[1]')}              AS domain,
        {phone_sql('phones[1]')}                 AS phone,
        nullif(trim(addresses[1].freeform), '')  AS street
    FROM geo
)
{projection_sql("flat")}
"""


# ── index build + verification ──────────────────────────────────────────────
def _build_indexes(ds) -> list[str]:
    built = []
    for col in OPTIMIZED_BTREE_INDEXES:
        ds.create_scalar_index(col, "BTREE", replace=True)
        built.append(f"BTREE:{col}")
        print(f"  BTREE  ok {col}")
    for col in OPTIMIZED_BITMAP_INDEXES:
        ds.create_scalar_index(col, "BITMAP", replace=True)
        built.append(f"BITMAP:{col}")
        print(f"  BITMAP ok {col}")
    return built


def _index_names(ds) -> set:
    out = set()
    for ix in ds.list_indices():
        cols = ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None)
        if cols:
            out.update(cols)
    return out


def _verify_local(local_path: str, expected_rows: int) -> dict:
    """HARD pre-publish gate. Raises on any failure -> SoR is never touched."""
    import lance

    ds = lance.dataset(local_path)
    rows = ds.count_rows()
    fields = {f.name: str(f.type) for f in ds.schema}
    meta = {k.decode(): v.decode() for k, v in (ds.schema.metadata or {}).items()}
    idx_cols = _index_names(ds)

    expect_fields = {
        "id": "string", "longitude": "double", "latitude": "double",
        "hilbert": "uint32", "region": "string", "locality": "string",
        "postcode": "string", "name": "string", "category": "string",
        "taxonomy": "string", "confidence": "float",
        "domain": "string", "phone": "string", "street": "string",
    }
    expect_idx = set(OPTIMIZED_BTREE_INDEXES) | set(OPTIMIZED_BITMAP_INDEXES)
    expect_meta = {"country", "release_tag", "snapshot_date", "ingested_at", "schema_version"}

    problems = []
    if rows != expected_rows:
        problems.append(f"row count {rows} != expected {expected_rows}")
    if fields != expect_fields:
        problems.append(f"schema mismatch: got {fields}")
    if not expect_idx.issubset(idx_cols):
        problems.append(f"missing indices: {expect_idx - idx_cols}")
    if {"longitude", "latitude"} & idx_cols:
        problems.append(f"stale lon/lat BTREE present: {idx_cols}")
    if not expect_meta.issubset(set(meta)):
        problems.append(f"missing metadata keys: {expect_meta - set(meta)}")
    if meta.get("schema_version") != SCHEMA_VERSION:
        problems.append(f"schema_version {meta.get('schema_version')} != {SCHEMA_VERSION}")

    # coverage gate — the whole point of v3. Floors set ~3pp under measured
    # one-file coverage (domain 81.6 / phone 91.8 / street 96.6) for full-run slack.
    cov = ds.scanner(columns=["domain", "phone", "street"]).to_table()
    n = cov.num_rows or 1
    dom_pct = 100.0 * (n - cov.column("domain").null_count) / n
    pho_pct = 100.0 * (n - cov.column("phone").null_count) / n
    st_pct = 100.0 * (n - cov.column("street").null_count) / n
    if dom_pct < 78.0:
        problems.append(f"domain coverage {dom_pct:.2f}% < 78% floor")
    if pho_pct < 88.0:
        problems.append(f"phone coverage {pho_pct:.2f}% < 88% floor")
    if st_pct < 93.0:
        problems.append(f"street coverage {st_pct:.2f}% < 93% floor")

    # pushdown smoke tests on the two new resolution keys
    for col in ("domain", "phone"):
        real = ds.scanner(columns=[col], filter=f"{col} IS NOT NULL", limit=1).to_table()
        if real.num_rows == 0:
            problems.append(f"{col} has zero non-null values")
            continue
        val = real.column(0)[0].as_py().replace("'", "''")
        plan = ds.scanner(filter=f"{col} = '{val}'", columns=["id"]).explain_plan(True)
        if "ScalarIndexQuery" not in plan or f"{col}_idx" not in plan:
            problems.append(f"{col} '=' did not use ScalarIndexQuery@{col}_idx")

    if problems:
        raise RuntimeError("LOCAL VERIFY FAILED:\n  - " + "\n  - ".join(problems))
    return {"rows": rows, "fields": fields, "metadata": meta,
            "indexed_cols": sorted(idx_cols),
            "coverage": {"domain_pct": round(dom_pct, 2), "phone_pct": round(pho_pct, 2),
                         "street_pct": round(st_pct, 2)}}


def _transform_and_build(con_threads: int = 8) -> dict:
    """Read Overture source (pinned release) -> v3 transform (sorted) -> local Lance ->
    indices -> LOCAL verify. No R2 mutation. Returns build report."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    # Idempotency guard: a v3 SoR already carries the resolution keys + schema_version.
    so = _lance_storage_options()
    try:
        cur = lance.dataset(DATASET_URI, storage_options=so)
        field_names = {f.name for f in cur.schema}
        sv = (cur.schema.metadata or {}).get(b"schema_version", b"").decode()
        if {"domain", "phone"}.issubset(field_names) and sv == SCHEMA_VERSION:
            raise AlreadyV3(sv)
    except AlreadyV3:
        raise
    except Exception as exc:  # noqa: BLE001 — a missing/unreadable SoR is fine for a rebuild
        print(f"NOTE: could not open current SoR for idempotency check ({exc}); proceeding.")

    read_glob = OVERTURE_PLACES_GLOB.format(rel=PINNED_RELEASE)
    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    metadata = {
        "country": "US",
        "release_tag": PINNED_RELEASE,
        "snapshot_date": snapshot_date,
        "ingested_at": ingested_at,
        "schema_version": SCHEMA_VERSION,
        "sort_order": "region,hilbert",
        "hilbert_bounds": HILBERT_BOUNDS_TAG,
    }
    print(f"Re-ingest pinned release {PINNED_RELEASE}; reading {read_glob}")

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    shutil.rmtree(LOCAL_OUT, ignore_errors=True)

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={con_threads};")
    con.execute("SET enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='24GB';")
    con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    con.execute("LOAD httpfs;")
    con.execute("LOAD spatial;")
    con.execute(f"SET s3_region='{OVERTURE_REGION}';")

    geom_expr = _detect_geometry_decode(con, read_glob)
    print(f"  geometry decode: ST_X/ST_Y({geom_expr})")
    sql = _build_sql(geom_expr)
    params = [read_glob]

    distinct_ids = None
    write_path = "materialize"
    try:
        table = con.execute(sql, params).to_arrow_table()
        table = table.replace_schema_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )
        out_rows = table.num_rows
        con.register("proj", table)
        distinct_ids = con.execute("SELECT count(DISTINCT id) FROM proj").fetchone()[0]
        con.unregister("proj")
        if distinct_ids != out_rows:
            raise RuntimeError(f"id no longer unique: distinct {distinct_ids} != rows {out_rows}")
        print(f"  transformed {out_rows:,} rows; distinct id = {distinct_ids:,}")
        lance.write_dataset(
            table, LOCAL_OUT, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
    except (MemoryError, duckdb.OutOfMemoryException) as exc:
        write_path = "stream"
        print(f"  materialize hit {type(exc).__name__}; streaming fallback: {exc}")
        con.close()
        con = duckdb.connect(":memory:")
        con.execute(f"PRAGMA threads={con_threads};")
        con.execute("SET enable_progress_bar=false;")
        con.execute("SET preserve_insertion_order=false;")
        con.execute("SET memory_limit='24GB';")
        con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
        con.execute("LOAD httpfs;")
        con.execute("LOAD spatial;")
        con.execute(f"SET s3_region='{OVERTURE_REGION}';")
        rdr = con.execute(sql, params).to_arrow_reader(STREAM_BATCH_ROWS)
        lance.write_dataset(
            rdr, LOCAL_OUT, schema=rdr.schema, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
        lance.dataset(LOCAL_OUT).update_schema_metadata(dict(metadata))
    finally:
        con.close()

    out_rows = lance.dataset(LOCAL_OUT).count_rows()
    # Row-superset gate: pinned to the v2 release, v3 must reproduce the v2 row count.
    if out_rows != SRC_ROWS_EXPECTED:
        raise RuntimeError(
            f"row drift: v3 produced {out_rows} != v2 baseline {SRC_ROWS_EXPECTED} "
            f"for pinned release {PINNED_RELEASE}. Re-confirm the baseline before publishing."
        )

    ds_out = lance.dataset(LOCAL_OUT)
    built = _build_indexes(ds_out)
    report = _verify_local(LOCAL_OUT, SRC_ROWS_EXPECTED)
    report.update({"built": built, "write_path": write_path,
                   "release_tag": PINNED_RELEASE, "snapshot_date": snapshot_date,
                   "distinct_ids": distinct_ids, "src_rows": out_rows})
    print(f"LOCAL build+verify OK: {report}")
    return report


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120, memory=32768, cpu=8.0, ephemeral_disk=524288,
)
def reingest_overture_places_v3(apply: bool = False) -> dict:
    """dryrun (apply=False): build+verify LOCAL only, NO mutation.
    apply=True: + R2 backup of current v2 -> publish -> post-publish verify
    (restore-on-fail) -> ledger."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        report = _transform_and_build()
    except AlreadyV3 as exc:
        msg = f"SoR is already {exc}; re-ingest is a no-op (nothing to do)."
        print(msg)
        return {"mode": "noop", "already_v3": True, "schema_version": str(exc),
                "mutated": False, "note": msg}

    if not apply:
        return {"mode": "dryrun", "mutated": False, **report}

    s3 = _s3_client()
    ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    bak_prefix = f"active/overture_places__bak_v3_{report['release_tag']}_{ts}/"
    status, error = "error", None
    published_files = published_bytes = 0
    try:
        n_bak = _backup_r2_prefix(s3, DATASET_PREFIX, bak_prefix)
        print(f"Backed up {n_bak} objects (current v2) -> s3://{BUCKET}/{bak_prefix}")

        _wipe_prefix(s3, DATASET_PREFIX)
        published_files, published_bytes = _upload_dir(s3, DATASET_PREFIX, LOCAL_OUT)
        print(f"Published {published_files} files ({published_bytes:,} B) -> {DATASET_URI}")

        # post-publish verify against R2; restore on any failure
        pub = lance.dataset(DATASET_URI, storage_options=_lance_storage_options())
        pub_rows = pub.count_rows()
        pub_idx = _index_names(pub)
        rd = pub.scanner(columns=["domain"], filter="domain IS NOT NULL", limit=1).to_table()
        dom_val = rd.column(0)[0].as_py().replace("'", "''") if rd.num_rows else None
        dom_hits = (pub.scanner(filter=f"domain = '{dom_val}'", columns=["id"]).to_table().num_rows
                    if dom_val else 0)
        ok = (pub_rows == report["src_rows"]
              and set(OPTIMIZED_BTREE_INDEXES + OPTIMIZED_BITMAP_INDEXES).issubset(pub_idx)
              and dom_val is not None and dom_hits > 0)
        if not ok:
            raise RuntimeError(
                f"POST-PUBLISH VERIFY FAILED: rows={pub_rows} idx={sorted(pub_idx)} "
                f"domain={dom_val!r} domain_hits={dom_hits}"
            )
        status = "success"
        print(f"Post-publish verify OK: rows={pub_rows:,} domain_resolves={dom_hits} idx={sorted(pub_idx)}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"FAILURE: {error} — attempting rollback from {bak_prefix}")
        try:
            n_res = _restore_r2_prefix(s3, bak_prefix, DATASET_PREFIX)
            print(f"ROLLBACK: restored {n_res} objects; SoR returned to the pre-v3 (v2) state.")
        except Exception as rexc:  # noqa: BLE001
            print(f"CRITICAL: rollback FAILED: {rexc}. Backup intact at s3://{BUCKET}/{bak_prefix}")
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, report["release_tag"], report["snapshot_date"],
                    int(report["src_rows"]), report.get("distinct_ids"),
                    published_files, published_bytes, "reingest_v3", status, error,
                    started_at, completed_at)

    return {"mode": "apply", "mutated": True, "backup_prefix": bak_prefix,
            "published_files": published_files, "published_bytes": published_bytes, **report}


@app.local_entrypoint()
def dryrun() -> None:
    import json
    print(json.dumps(reingest_overture_places_v3.remote(apply=False), indent=2, default=str))


@app.local_entrypoint()
def apply() -> None:
    import json
    print(json.dumps(reingest_overture_places_v3.remote(apply=True), indent=2, default=str))
```

> **Status:** `py_compile` OK; imports OK in a staged package; `_build_sql` runs end-to-end on the source sample producing the v3 schema + measured coverage (§4).

### 5.3 EDIT — `pipelines/overture_maps/places.py` (go-forward parity)

The monthly ingest must emit v3, else the next run reverts the SoR. The index constants already come from `T.OPTIMIZED_*` (now v3) and `schema_version` flows from `T.SCHEMA_VERSION` (now `overture_places.v3`) via `_v2_schema_metadata` — **so those need no change.** The **only** required edit is `_build_sql`: extend the `geo` CTE to carry `taxonomy, websites, phones` and the `flat` CTE to emit `taxonomy, domain, phone, street`. Replace the body of `_build_sql` with:

```python
def _build_sql(geom_expr: str) -> str:
    """100% of the transform → Overture Places v3 schema. Anonymous read_parquet
    over the resolved release → decode geometry ONCE (geo CTE) + US filter pushed
    to the scan → flatten ST_X/ST_Y + unpack addresses[1]/names/categories/taxonomy
    + normalize the resolution keys domain/phone/street (flat CTE) → shared v3
    projection (adds ``hilbert``, normalizes region, casts confidence→float32, carries
    domain/phone/street/taxonomy, sorts by region,hilbert). The 4 constant provenance
    columns are demoted to Lance schema metadata by the caller, NOT projected. The
    WKB ``geometry`` never leaves the geo CTE. ``geom_expr`` is repo-controlled
    (probe output, not user input); only the read path is bound positionally (one ?)."""
    return f"""
WITH raw AS (
    SELECT * FROM read_parquet(?)
),
geo AS (
    SELECT
        id,
        {geom_expr} AS geom,        -- decode WKB→GEOMETRY once; never persisted
        addresses,
        names,
        categories,
        taxonomy,
        websites,
        phones,
        confidence
    FROM raw
    WHERE addresses[1].country = 'US'   -- ISO 3166-1 alpha-2; predicate pushed to scan
),
flat AS (
    SELECT
        nullif(trim(id), '')                     AS id,
        ST_X(geom)                               AS longitude,   -- flattened float
        ST_Y(geom)                               AS latitude,    -- flattened float
        nullif(trim(addresses[1].region), '')    AS region,       -- raw; normalized in projection
        nullif(trim(addresses[1].locality), '')  AS locality,     -- city / town (blocking key)
        nullif(trim(addresses[1].postcode), '')  AS postcode,     -- US ZIP / ZIP+4 (blocking key)
        nullif(trim(names.primary), '')          AS name,         -- entity-resolution key
        nullif(trim(categories.primary), '')     AS category,     -- POI category slug
        nullif(trim(taxonomy.primary), '')       AS taxonomy,     -- richer category hierarchy leaf
        TRY_CAST(confidence AS DOUBLE)           AS confidence,    -- Overture 0..1 quality score
        {T.domain_sql('websites[1]')}            AS domain,        -- registrable domain (resolution key)
        {T.phone_sql('phones[1]')}               AS phone,         -- E.164/NANP phone (resolution key)
        nullif(trim(addresses[1].freeform), '')  AS street         -- full street line (resolution key)
    FROM geo
)
{T.projection_sql("flat")}
"""
```

**Also update the docstring/comment drift** (non-functional, recommended): the module header comment lists a v2 index plan and "13→10 cols"; update the "Index plan" comment block to read `BTREE: id, name, postcode, locality, hilbert, domain, phone` and `BITMAP: region, category`. The `_v2_schema_metadata` function name may be left as-is (it returns `schema_version` from `T.SCHEMA_VERSION`) or renamed to `_schema_metadata` for clarity — cosmetic.

> **Status:** the edited `places.py` `py_compile`s and imports; `_build_sql('geometry')` contains `AS domain/phone/street/taxonomy` and the `'g'`-flagged regexes (§4).

---

## 6. Execution runbook

**Phase 0 — pre-flight**
1. `cd` to the `core-x` checkout; confirm `doppler` is bound (`core-x/prd`) and Modal secrets `r2-credentials` + `hqx-postgres` exist (`modal secret list`).
2. Apply §5: replace `_transform.py`, create `reingest_v3.py`, edit `places.py::_build_sql`.
3. Compile: `python -m py_compile pipelines/overture_maps/_transform.py pipelines/overture_maps/reingest_v3.py pipelines/overture_maps/places.py`.
4. Confirm `ops.overture_places_runs` exists (it does; `places.py::initdb` is idempotent).
5. **Re-confirm the pinned release still matches the live SoR** (no out-of-band re-ingest happened since): the SoR `schema.metadata.release_tag` MUST equal `reingest_v3.PINNED_RELEASE` (`2026-05-20.0`). If a v3 has already been published, the worker no-ops; if the SoR's release differs, STOP and reconcile `PINNED_RELEASE` + `SRC_ROWS_EXPECTED` first.

**Phase 1 — dry run (NO mutation; mandatory gate)**
```
modal run pipelines/overture_maps/reingest_v3.py::dryrun
```
Reads the Overture source (pinned), builds the v3 dataset + indices on the worker's local disk, runs `_verify_local` (schema == 14 v3 fields; indices == 7 BTREE + 2 BITMAP, no lon/lat; `schema_version=overture_places.v3`; **row count == 16,273,123**; `distinct_id == rows`; **domain/phone/street coverage floors**; **domain/phone `=` → ScalarIndexQuery pushdown**), and returns the report **without touching R2**. Abort if anything is off. Inspect `coverage` (expect ~domain 81–82 / phone 91–92 / street 96–97), `write_path`.

**Phase 2 — apply (mutating; backup + publish + verify + ledger)**
```
modal run pipelines/overture_maps/reingest_v3.py::apply
```
Backs up the current v2 prefix → `active/overture_places__bak_v3_<release>_<ts>/`, wipes + publishes the v3 dataset, runs the post-publish verify against R2 (rows, full index set, **a real domain probe resolves to ≥1 place**) with auto-restore on failure, writes the ledger row (`write_path='reingest_v3'`). On success the SoR is v3 at the same URI.

**Phase 3 — independent confirmation** (read-only, against the live SoR; mirror §10)
- `lance.dataset(uri).schema` == 14 v3 fields; `schema.metadata.schema_version == overture_places.v3`; `release_tag == 2026-05-20.0`; `count_rows() == 16,273,123`; `DISTINCT(id) == rows`.
- Indices == {id,name,postcode,locality,hilbert,domain,phone} BTREE + {region,category} BITMAP; **no** longitude/latitude index.
- `domain = '<real>'` and `phone = '<real>'` plans show `ScalarIndexQuery@domain_idx(BTree)` / `@phone_idx(BTree)`; each resolves to ≥1 place.
- `modal run pipelines/overture_maps/reingest_v3.py::dryrun` again → should now **no-op** (`already_v3=True`).
- `modal run pipelines/overture_maps/places.py::show_ledger` → confirm the `reingest_v3` row, status `success`.

**Phase 4 — ship the code** — commit `_transform.py`, `reingest_v3.py`, the `places.py` edit, and this directive; PR → squash-merge → pull into the operator checkout (standard lifecycle).

**Phase 5 — backup retention** — after Phase 3 passes and the operator confirms, the v3 backup prefix (`__bak_v3_*`, ~2.2 GiB, the pre-v3 v2 dataset = the rollback target) can be deleted via `_wipe_prefix(s3, bak_prefix)` or a lifecycle rule. The older `__bak_2026-05-20.0_20260606T192125Z/` (the pre-v2 **v1** backup, present today) is orthogonal — decide its retention separately (it can no longer be reached by the v3 rollback). **Do not auto-delete inside the worker.**

---

## 7. Acceptance criteria (hard gates)

Done only when ALL hold:
- [ ] `dryrun` `_verify_local` passed: schema (14 v3 fields, correct types), indices (7 BTREE + 2 BITMAP, no lon/lat), metadata (`schema_version=overture_places.v3` + 4 demoted constants), **rows == 16,273,123**, `distinct_id == rows`, **coverage floors** (domain ≥78%, phone ≥88%, street ≥93%), **domain/phone pushdown**.
- [ ] `apply` returned `status=success`; ledger row present with `write_path='reingest_v3'`, `release_tag='2026-05-20.0'`.
- [ ] Live SoR: `count_rows() == 16,273,123`; `DISTINCT(id) == rows` (true superset of v2 — same rows).
- [ ] Live SoR schema == 14 v3 fields; `schema.metadata` carries 4 demoted constants + `schema_version=overture_places.v3`.
- [ ] Live SoR indices == {id,name,postcode,locality,hilbert,domain,phone} BTREE + {region,category} BITMAP; **no** longitude/latitude index.
- [ ] Normalization correctness: spot-assert ≥5 known domains and ≥5 known phones resolve to their canonical form (use the §4 edge cases).
- [ ] `domain = '<real>'` → `ScalarIndexQuery@domain_idx(BTree)`; `phone = '<real>'` → `ScalarIndexQuery@phone_idx(BTree)`.
- [ ] **Real resolution smoke:** pick a domain present in the SoR; `domain = '<x>'` returns its place(s) via the index (count ≥ 1, with name/locality/region).
- [ ] Measure post-run on-disk footprint (R2 `ListObjects`); confirm index growth ≈ the §3 ~388 MiB estimate (sanity, not a hard gate).
- [ ] `places.py` edit compiles; a future ingest emits v3 (review-confirmed; `_build_sql` carries the 4 new columns).

---

## 8. Rollback

Automatic: `apply` restores from the v3 backup prefix on any post-publish verification failure (wipe + server-side copy back), then re-raises. The SoR returns to the exact pre-v3 (v2) bytes.

Manual (later): `_restore_r2_prefix(s3, "active/overture_places__bak_v3_2026-05-20.0_<ts>/", "active/overture_places/")`. The backup is a byte-identical server-side copy of the pre-re-ingest **v2** dataset (data + v2 indices + manifests), so restore yields the original v2 SoR. **Code rollback** (revert the `_transform.py`/`places.py` commit) is independent and required if the data is rolled back — else the next monthly ingest re-emits v3.

---

## 9. Consumer contract — resolving an external record against the new keys

Resolve in **descending determinism**: domain → phone → (geo-blocked) street/name. The two deterministic keys are index probes; street/name are post-block refinements.

**Always push the predicate into Lance** (the diagnostic measured a 28× penalty for filtering a DuckDB-side unfiltered reader). Pass the `LanceDataset` to DuckDB (replacement scan) **or** build `ds.scanner(filter=…, columns=[…])`.

```python
import lance
ds = lance.dataset("s3://data-sink/active/overture_places/", storage_options=SO)

# 1) DOMAIN — strongest. Normalize the EXTERNAL value with the SAME rule (T.domain_sql)
#    so both sides are registrable eTLD+1, then probe the BTREE.
hits = ds.scanner(filter="domain = 'acmeplumbing.com'",
                  columns=["id","name","locality","region","phone","street"]).to_table()

# 2) PHONE — normalize the external phone to +1XXXXXXXXXX (T.phone_sql), then probe.
hits = ds.scanner(filter="phone = '+14155550199'",
                  columns=["id","name","locality","region","domain","street"]).to_table()

# 3) STREET within a geo/postal block — never a raw equality; block first (region/postcode),
#    then fuzzy-compare street downstream.
cands = ds.scanner(filter="region = 'CA' AND postcode = '94107'",
                   columns=["id","name","street","domain","phone","longitude","latitude"]).to_table()
#   -> apply an address-similarity scorer (downstream) over `street`.
```

**Critical:** apply `T.domain_sql` / `T.phone_sql` to the **external** record before the equality, or the join silently misses. The SoR stores `acmeplumbing.com` and `+14155550199`, not `http://www.AcmePlumbing.com/` or `(415) 555-0199`. Bbox / by-state / category guidance from the v2 consumer contract still applies unchanged.

**Proven** (`v3_probe5_e2e.py`): `domain = 'onepieceboosters.com'` → `ScalarIndexQuery@domain_idx(BTree)` → 1 place (Mhtradingcardshop, Anchorage AK); `phone = '+13312034600'` → `@phone_idx(BTree)` → 1 place.

---

## 10. Adversarial self-attack — how the design survives each

1. **`regexp_replace` first-match-only (silent digit/host corruption).** Proven real: `(808) 548-3700`→`808) 548-3700` without `'g'`. *Defense:* every `regexp_replace` carries `'g'`; the phone NANP gate + the coverage floor would catch a regression (a non-`'g'` build collapses phone coverage). The `'g'` requirement is called out in §4 as the top hand-edit hazard.
2. **Phone extension contamination.** `+1 (212) 555-0199 ext 4`→`121255501994` if digits are stripped before the extension. *Defense:* extension stripped first; asserted (→`+12125550199`).
3. **Domain wrong-casing / malformed hosts.** Embedded double-scheme (`http://www.http://…`), no-dot hosts, IPv4 literals, trailing unicode marks, illegal chars. *Defense:* repeated scheme strip, illegal-char strip, IPv4 rejection, and a final `regexp_full_match` gate → NULL on anything not a clean registrable domain. 15/15 edge asserts pass; 99.98% valid-rate on present websites; the rejected set is genuinely bad data (`facebook/lemuschildcare`, `n/a`, `www.com`, IP literals).
4. **Multi-part TLD (`co.uk`) handling.** *Defense:* the 3-label carve handles them; quantified prevalence is **0.0986%** of US domains, so a PSL dependency is unjustified. Residual risk: an exotic multi-part suffix not in the token list (`gov`,`co`,`com`,`net`,`org`,`edu`,`ac`,`gob`,`go`) would be truncated to 2 labels — affects <0.1%, and only the ccTLD subset of that. Accepted; documented.
5. **Underscore/illegal-char hosts produce a plausible-but-wrong domain.** `endeavour_aviation.com`→`endeavouraviation.com` (underscore stripped). *Defense:* this is lossy but rare and yields a syntactically valid domain; flagged as a known minor behavior, not a correctness failure (an underscore is not a legal hostname char). Accepted.
6. **Storage blowup.** *Defense:* measured, not guessed — +~388 MiB index (domain+phone), +~885 MiB decoded data (domain+phone+street+taxonomy, compresses on disk). `street`/`taxonomy` carry zero index. Bounded and justified; second-tier fields dropped (D6) to keep it lean.
7. **Re-ingest vintage drift.** A non-pinned re-ingest would pull a newer release with different rows → not a v2 superset. *Defense:* `PINNED_RELEASE='2026-05-20.0'` (= the live SoR's `release_tag`, proven) + a hard `out_rows == SRC_ROWS_EXPECTED` gate that aborts on any row drift before publish.
8. **"Ignore downstream" tension — now downstream depends on what we add.** v3 adds columns + indexes that consumers will join against; and `optimize.py` (which imports the shared `projection_sql`) will reference v3 columns absent from a v2 SoR. *Defense:* §5.1 explicitly states `optimize.py` is obsolete post-v3 and must NOT be run against a v2 SoR; the consumer contract (§9) pins the normalize-the-external-value requirement so downstream joins are correct by construction.
9. **Existing v1/v2 backups.** Today there is a pre-v2 **v1** backup (`__bak_2026-05-20.0_20260606T192125Z/`, 46 obj, 2.473 GiB) and the live **v2** SoR (45 obj, 2.165 GiB). The v3 worker writes a **new** prefix `__bak_v3_<release>_<ts>/` (timestamped → no collision). *Defense:* distinct prefix names; §5 Phase-5 states the v3 backup is the v2 rollback target, the old v1 backup is orthogonal and retained/cleaned separately.
   ```
   $ # v3_probe7_backups.py
   # active/overture_places/                                  45 obj  2.165 GiB  (live v2)
   # active/overture_places__bak_2026-05-20.0_20260606T192125Z/  46 obj  2.473 GiB  (pre-v2 = v1)
   ```
10. **Could the verify gate pass on a bad dataset?** Hardening: the gate checks row count == baseline AND distinct_id == rows AND exact 14-field schema AND full index set AND `schema_version` AND **coverage floors** AND **live pushdown** (`ScalarIndexQuery@{col}_idx`) AND (post-publish) **a real domain resolves to ≥1 place**. A dataset that lost the keys fails the coverage floor; one with the columns but no index fails the pushdown check; one with wrong rows fails the count. The residual hole: coverage is checked as an aggregate %, so a *systematic mis-normalization* that still produced ~81% non-null `domain` of *wrong* values would pass the floor but be semantically wrong — mitigated by the §7 spot-assert of known domain/phone values (do not skip it) and the §4 edge-assert suite that proves the normalizer itself.
11. **Multi-value fields.** websites/phones are arrays; we index only `[1]`. *Defense:* multi-value is rare (website 0.57%, max 2; phone 0.10%, max 4); a secondary value is a follow-on (a `domains LIST` column or a side table), not a v3 blocker. Documented.

---

## 11. Deploy-time risks (could NOT be fully closed in authoring — read before apply)

- **Full-run coverage vs one-file sample.** All coverage/normalization numbers are from **one** source file (`part-00000`, 1.04M US rows of 16.27M). The other 15 files are assumed representative; the `_verify_local` coverage floors (3pp under sample) are the runtime guard, but the exact full-dataset percentages are a deploy-time observation. **Risk: low** (Overture files are uniformly partitioned).
- **Worker memory at full scale.** v3 materializes 16.27M rows × 14 cols (4 new string columns) in a 32 GiB container; the streaming `to_arrow_reader` fallback is wired but **untested at full scale here** (READ-ONLY constraint — no Modal run). The two new BTREEs (domain ~7.0M distinct, phone ~11.8M distinct) train under `LANCE_BYPASS_SPILLING=true` (in-memory). **Risk: medium** — if the materialize path OOMs, the fallback engages; if index training OOMs, raise `memory` or reconsider. Watch the dryrun's `write_path` and peak memory.
- **`add_local_python_source` / Modal automount.** Carried verbatim from the working `optimize.py`/`places.py`; assumed valid for the deployed Modal version. **Risk: low** (same pattern already in production).
- **`optimize.py` becomes incompatible** with a v2 SoR after the `_transform.py` edit (it imports the now-v3 `projection_sql`). **Mitigation:** documented in §5.1 — do not run it post-edit; treat as obsolete.
- **`street` has no normalized blocking key (D4).** Address matching quality depends on a downstream normalizer not built here. **Risk: scoped out by decision**, not a defect.
- **NANP-only phone canonicalization** drops legitimately non-US numbers (~10% of raw phones are non-NANP; see the digit-length distribution — len 13 = 103,917 are mostly intl). For a **US** places SoR this is correct; if non-US resolution is later needed, store raw `phone` alongside the canonical one. **Risk: scoped by decision.**

---

## 12. Out of scope / explicitly rejected

- **Public-suffix-list dependency** — REJECTED (D5): 0.0986% multi-part-TLD prevalence in US data does not justify it; the regex carve covers it.
- **Full domain/phone LIST columns** — DEFERRED (adversary #11): multi-value <0.6%; scalar primary is the right index target.
- **`email`/`social`/`brand` columns** — DROPPED (D6): low coverage and/or weak as join keys.
- **`operating_status` column / closed-row filtering** — DROPPED as a column; rows KEPT (D7): preserves the superset for ~0.04% of rows.
- **Normalized street blocking key** — DEFERRED (D4): needs a real USPS-style normalizer; do at query time downstream.
- **`id → fixed_size_binary(16)`** — REJECTED (carried from v2 D6): plane-wide join-key contract.
- **Append/multi-vintage history** — CLOSED (operator: no continual re-ingest). Overwrite model; provenance in metadata.
- **Running `optimize.py` after v3** — FORBIDDEN (§5.1): obsolete and incompatible with the v3 schema.
