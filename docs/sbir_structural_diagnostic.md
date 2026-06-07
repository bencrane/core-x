# SBIR / STTR Award Data — Structural & Ingestion Diagnostic

Read-only, first-principles interrogation of the two **SBIR/STTR award CSVs landed in the R2 staging
tier** (`s3://data-sink/landing/sbir/`), executed to map schema, isolate resolution keys, profile
socio-economic + financial strata, and audit the abstract text payload **before** a single byte is
written to a Lance system of record. This is the pre-ingest gate: it defines the schema, the cleaning
contract, and the minted-key strategy the ingestion routine must honor.

- **Source assets interrogated (landing tier, raw transport CSV — NOT yet SoR):**
  - `award_data_no_abstract.csv` — **91,420,291 B (91.4 MB)**, 41 columns, the lean metadata matrix.
  - `award_data.csv` — **394,390,263 B (394.4 MB)**, 42 columns = the 41 lean columns **+ `Abstract`**
    (inserted at ordinal 31, after `Zip`). The full text-bearing dataset.
- **Live evidence harness (non-mutating, zero writes):** DuckDB **v1.5.2** CLI, out-of-core
  (`memory_limit=4–6 GB`, `temp_directory` on local NVMe spill), reading both CSVs with
  `read_csv(all_varchar=true, sample_size=-1, header=true)` — every value inspected as raw text so
  null sentinels, zero-padding loss, and whitespace survive inspection (a typed read silently coerces
  them away). All non-abstract profiling runs on the lean file (identical row population, 4× faster);
  abstract analysis runs exclusively on the full file. No `write_dataset`, no R2 put/delete.
- **As-of:** files landed **2026-06-07 14:53 UTC**; profiled **2026-06-07**. R2 creds via rclone `r2:`
  remote / Doppler `core-x/prd`.
- **Attestation:** every figure below is a full-scan aggregate over the committed CSV bytes
  (`sample_size=-1` — no sampling, no estimation). Distinct counts are exact `count(DISTINCT …)`, not
  HyperLogLog. The row-parity gate, the duplicate-row count, and the abstract special-char scan are
  exhaustive.

---

## 1. Headline posture

**Clinical verdict: the data is structurally sound, RFC4180-clean, and ingestion-ready — but it carries
no natural primary key and a dense layer of text/format debt that the ingest routine MUST resolve in
SQL before the Lance write. There is no parse corruption: the full text file deserializes to the exact
same row count as the lean file under strict RFC4180, proving every embedded newline/quote in the
abstracts is correctly quoted. The work is not repair; it is (1) minting a deterministic surrogate key,
(2) a disciplined defensive-cast projection, and (3) normalizing four dirty dimensions — state,
zip, duns, and the abstract placeholder layer.**

| Axis | Verdict |
|---|---|
| **Parse integrity** | ✅ **Exact row parity.** Both files = **219,502 rows**; the 394 MB file parses under strict RFC4180 (`ignore_errors=false`) with **zero errors**. Embedded newlines/quotes in abstracts are all properly quoted — no shattered records. |
| **Primary key** | ⛔ **None exists.** No single column is unique; the best composite (`Agency Tracking Number`+`Phase`+`Agency`) still collides on **2,041 excess rows** across 1,719 groups. Only **3 rows** are byte-identical. → **Mint a sha256 surrogate + per-hash ordinal** (the canonical `sba_foia` pattern). |
| **Resolution keys** | ⚠️ Present but partial + dirty. `UEI` 69.6% / `Duns` 81.5% fill (entity-level, expected — UEI post-dates DUNS). DUNS needs zero-pad (1,664 rows); UEI has 2 malformed values. **No EIN/Tax ID and no CAGE code columns exist** in this feed. |
| **Socio-economic flags** | ✅ Clean **ternary** `Y`/`N`/`U`, **100% populated, zero nulls** on all three (HUBZone, Woman-Owned, Socially & Economically Disadvantaged). Low-cardinality → BITMAP index. **No SDVOSB / veteran-owned column exists.** |
| **Financials** | ✅ `Award Amount` 99.98% fill, **100% cast-clean** (no `$`/comma debris). Right-skewed: median $148.3K, mean $374.4K, max $60.0M. 4,202 zero-dollar awards, 0 negatives. Phase I $124K avg / Phase II $931K avg. |
| **Temporal** | ⚠️ `Award Year` (1983–2026, 100% fill) is the trustworthy temporal key. The five `DATE` columns carry out-of-range garbage (`Proposal Award Date` min = **1905-07-01**) and are 30–51% sparse → validate to a plausible window. |
| **Abstract payload** | ⚠️ 99.93% raw fill, but **~29,315 are `N/A`-class placeholders** (`N/A` alone = 28,181). **Effective embeddable corpus = 190,034 (86.58%).** 24,344 carry embedded newlines (quoted, legal) → whitespace-normalize before chunking. |

**Bottom line for the ingest routine:** one DuckDB projection pass (all_varchar → defensive `TRY_CAST` /
`nullif(trim())`), a minted `sbir_surrogate_id`, four normalization rules (state, zip, duns, abstract
placeholder), then `lance.write_dataset` + scalar indices. Single standalone dataset; no joins resolved
at this stage (keys landed clean for future entity resolution).

---

## 2. Dataset inventory & parse gate

| Metric | `award_data_no_abstract.csv` | `award_data.csv` |
|---|--:|--:|
| Bytes | 91,420,291 | 394,390,263 |
| Columns | 41 | 42 (`+ Abstract` @ ordinal 31) |
| Rows (strict RFC4180, full scan) | **219,502** | **219,502** |
| Strict-parse exit | clean | **clean (0 errors)** |
| Exact byte-identical duplicate rows | **3** (219,499 distinct) | — |

> **The parity gate is the corruption verdict.** A naive line-splitter would shatter the full file on
> the 24,344 abstracts containing embedded `\n`. DuckDB's RFC4180 reader returns **219,502 — identical
> to the lean file** — proving the file is well-formed and the special characters live inside correctly
> quoted fields. The required reader is RFC4180-compliant (DuckDB / Arrow / Python `csv`); a hand-rolled
> `split('\n')` is the only thing that breaks here.

---

## 3. Profiling findings

### 3.1 Identifier extraction — the future join keys

| Field | Fill % | Populated | Distinct | Whitespace | Len (min–max) | Role |
|---|--:|--:|--:|--:|--:|---|
| `Agency Tracking Number` | 99.85 | 219,182 | 171,232 | 23 | 1–52 | Per-award business key (**not unique**: 47,950 dup overhang) |
| `Contract` | 76.26 | 167,388 | 156,783 | 91 | 1–33 | Award/contract no. (not unique, sparse) |
| `Duns` | 81.49 | 178,868 | 21,598 | 0 | 1–9 | **Entity** key — legacy; needs zero-pad |
| `UEI` | 69.61 | 152,798 | 17,157 | 0 | 11–12 | **Entity** key — SAM successor to DUNS |
| `Topic Code` | 57.42 | 126,044 | 20,460 | 62 | 1–20 | Solicitation topic |
| `Solicitation Number` | 52.46 | 115,147 | 1,582 | 27 | 1–55 | Solicitation grouping |

**Format conformance:**
- **`Duns`** — 177,204 clean 9-digit; **1,664 lost leading zeros** (1,395 @ 8-digit, 143 @ 7, 9 @ 6) and
  **117 are 1–2 digit junk** (`0`,`1`,…). Numeric upstream coercion stripped the zeros.
- **`UEI`** — 2 non-conforming: `ZVQLNPW5EKM` (11 chars, one short) and `080969063000` (12 but all-digit;
  SAM UEIs are alphanumeric and never all-numeric). The other 152,796 match `^[A-Za-z0-9]{12}$`.
- **`UEI`/`Duns` carry zero whitespace; the four free-text IDs carry 23–91 stray-whitespace values each.**

> **No `EIN`/Tax ID and no `CAGE` code columns exist in this feed.** The directive's target set of those
> two identifier classes is structurally absent from SBIR's public award export — they cannot be landed
> from this source and must be sourced via SAM.gov entity resolution (`sam_gov` pipeline) downstream,
> keyed on the `UEI`/`Duns` landed here.

**Primary-key candidate analysis (this is the load-bearing finding):**

| Candidate | Populated | Distinct | Excess (dup) rows | Verdict |
|---|--:|--:|--:|---|
| `Agency Tracking Number` | 219,182 | 171,232 | 47,950 | ⛔ not unique |
| `Contract` | 167,388 | 156,783 | 10,605 | ⛔ not unique + 24% null |
| `Contract` + `Phase` | 219,502 | 164,650 | 54,852 | ⛔ worse |
| `Agency Tracking Number` + `Phase` + `Agency` | 219,502 | 217,453 | **2,041** | ⛔ closest, still collides (1,719 groups, max group 154; only 5 involve empty ATN) |

→ **There is no natural PK.** The ingest MUST mint a deterministic surrogate, exactly as `sba_foia` does:
`sbir_surrogate_id = sha256(concat_ws(unit-sep, <all published attributes>)) || '-' || row_number()
OVER (PARTITION BY hash)`. The sha256 keeps the key idempotent across overwrite rebuilds; the ordinal
disambiguates the 3 byte-identical rows. (See §6 landing spec.)

### 3.2 Socio-economic & set-aside classifications

All three designations are stored as a **ternary single-character enum** — `Y` / `N` / `U` (U = unknown/
unspecified) — and are **100% populated with zero nulls and zero whitespace**:

| Native column | Designation | `Y` | `N` | `U` | Storage |
|---|---|--:|--:|--:|---|
| `HUBZone Owned` | HUBZone | 4,649 | 213,625 | 1,228 | `VARCHAR(1)` ternary |
| `Woman Owned` | WOSB | 21,023 | 197,678 | 801 | `VARCHAR(1)` ternary |
| `Socially and Economically Disadvantaged` | SDB / 8(a) proxy | 15,595 | 202,679 | 1,228 | `VARCHAR(1)` ternary |

**Schema implication:** preserve as native `Y/N/U` (do **not** collapse to boolean — `U` is semantically
distinct from `N`: "unknown" vs "asserted-not"). BITMAP-index each (cardinality = 3). Optionally add a
derived nullable boolean per flag (`Y`→true, `N`→false, `U`→NULL) for ergonomic filtering, but the raw
ternary is the source of truth.

> **No SDVOSB / Service-Disabled Veteran-Owned column exists** in this feed. The directive's fourth
> target designation is absent; the three present flags are the complete set. Veteran-owned status, if
> needed, must come from a SAM.gov join on `UEI`.

### 3.3 Agency allocation & financial stratification

**Award Amount distribution** (n=219,461 populated, 100% cast-clean, USD):

| min | p25 | median | mean | p75 | p95 | p99 | max | zero-$ | negative |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 0 | 80,000 | **148,309** | 374,365 | 500,000 | 1,454,610 | 2,249,993 | **59,999,818** | 4,202 | 0 |

Heavily right-skewed (mean ≈ 2.5× median); the $60M max is a legitimate large multi-phase/year award,
not an artifact. **Store as `DECIMAL(18,2)`, never float** (the source `DOUBLE` auto-detection is an
artifact of clean numerics — money belongs in fixed precision; house convention).

**By program** (`Program`, 2 values, 100% fill):

| Program | Awards | Total obligated |
|---|--:|--:|
| SBIR | 197,629 | $73.95B |
| STTR | 21,873 | $8.21B |

**By phase** (`Phase`, 2 values, 100% fill): Phase I — 151,426 (avg $124,318); Phase II — 68,076
(avg $930,517).

**By agency** (`Agency`, 13 values, 100% fill — top strata):

| Agency | Awards | Total obligated |
|---|--:|--:|
| Department of Defense | 105,821 | $41.42B |
| Health & Human Services (NIH) | 49,833 | $23.79B |
| NASA | 19,485 | $5.03B |
| Department of Energy | 16,609 | $5.90B |
| National Science Foundation | 15,544 | $3.82B |
| Agriculture | 4,091 | $0.67B |
| EPA / Commerce / Education / Transportation / DHS / NRC / Interior | 8,119 | $1.52B |

Total corpus ≈ **$82.16B across 219,502 awards, 1983–2026 (44 award years)**.

### 3.4 Text payload & abstract audit — the vectorization gate

Column: **`Abstract`** (full file only, ordinal 31).

| Metric | Value |
|---|--:|
| Raw fill | 99.93% (219,349 populated; 153 empty) |
| Char length — min / mean / median / p95 / p99 / max | 1 / **1,377** / 1,308 / 3,181 / 3,997 / **23,001** |
| Word count — mean / median / max | 198 / 191 / 3,385 |
| Token estimate (chars ÷ 4) — mean / max | ~344 / ~5,750 |
| **Embeddable (≥50 chars, non-placeholder)** | **190,034 (86.58% of all awards)** |

**Special-character scan (RFC4180-legal, naive-parser-breaking):**

| Embedded char | Abstracts | Note |
|---|--:|---|
| Newline `\n` (0x0A) | **24,344** (11.1%) | quoted — legal; **collapse before chunking** |
| Double-quote `"` | 11,955 | RFC4180-escaped `""` |
| Carriage return `\r` | 4,792 | normalize with `\n` |
| Tab `\t` | 298 | normalize to space |
| Comma `,` | 176,811 | field is quoted (expected) |
| Backslash `\` | 209 | LaTeX/path fragments |

**Placeholder / degenerate abstracts** (the real gate — 28,940 exact-match + spelling variants):

| Value | Count | | Value | Count |
|---|--:|---|---|--:|
| `N/A` | 28,181 | | `xxx` | 14 |
| `Redacted` | 377 | | `n/a` | 14 |
| `Not Available` | 226 | | `In process for public release` | 13 |
| `Not available` | 74 | | `BLANK` | 11 |
| `Redacted.` | 65 | | `Abstract` | 11 |
| `TBD` | 44 | | `null` | 10 |
| `Not avaiable.` *(sic)* | 20 | | `Not Avaialble` *(sic)* | 7 |

The variants (`Not avaiable.`, `Not Avaialble`, trailing periods, mixed case) defeat an exact-list match.
**Null any abstract whose normalized form (`lower(trim())`, strip trailing `.`) matches a redaction/
placeholder pattern OR whose trimmed length < 50 chars** — this is the embeddable gate that yields
190,034 real abstracts. The 23,001-char max and 17,691-char abstracts were verified as genuine NIH-style
research narratives (no concatenation bombs).

---

## 4. Deliverable 1 — Schema signature table (data dictionary)

42 columns, in CSV ordinal order. **Detected type** = DuckDB full-file inference; **fill %** = non-null &
non-empty over 219,502; **Lance type** = recommended landing type; **Index** = scalar index class.

| # | Column | Detected | Fill % | Distinct | → Lance type | Index | Notes |
|--:|---|---|--:|--:|---|---|---|
| 1 | `Company` | VARCHAR | 100.00 | 34,461 | string | BTREE | entity name |
| 2 | `Award Title` | VARCHAR | 100.00 | 159,289 | string | — | 84 sentinel strings |
| 3 | `Agency` | VARCHAR | 100.00 | 13 | string | BITMAP | low-card |
| 4 | `Branch` | VARCHAR | 66.02 | 30 | string | BITMAP | sub-agency |
| 5 | `Phase` | VARCHAR | 100.00 | 2 | string | BITMAP | Phase I/II |
| 6 | `Program` | VARCHAR | 100.00 | 2 | string | BITMAP | SBIR/STTR |
| 7 | `Agency Tracking Number` | VARCHAR | 99.85 | 171,232 | string | BTREE | trim; not unique |
| 8 | `Contract` | VARCHAR | 76.26 | 156,783 | string | BTREE | trim |
| 9 | `Proposal Award Date` | DATE | 51.46 | 6,332 | date32 | — | validate (1905 outlier) |
| 10 | `Contract End Date` | DATE | 49.17 | 8,384 | date32 | — | validate |
| 11 | `Solicitation Number` | VARCHAR | 52.46 | 1,582 | string | BITMAP | trim |
| 12 | `Solicitation Year` | BIGINT | 60.66 | 38 | int16 | BITMAP | — |
| 13 | `Solicitation Close Date` | DATE | 37.47 | 2,788 | date32 | — | validate |
| 14 | `Proposal Receipt Date` | DATE | 30.01 | 3,871 | date32 | — | validate |
| 15 | `Date of Notification` | DATE | 40.12 | 4,998 | date32 | — | validate |
| 16 | `Topic Code` | VARCHAR | 57.42 | 20,460 | string | BTREE | 202 sentinel strings |
| 17 | `Award Year` | BIGINT | 100.00 | 44 | int16 | BITMAP | **trustworthy temporal key** (1983–2026) |
| 18 | `Award Amount` | DOUBLE | 99.98 | 81,822 | **decimal(18,2)** | — | no float |
| 19 | `UEI` | VARCHAR | 69.61 | 17,157 | string | BTREE | validate `[A-Za-z0-9]{12}` |
| 20 | `Duns` | VARCHAR | 81.49 | 21,598 | string | BTREE | **lpad to 9** |
| 21 | `HUBZone Owned` | VARCHAR | 100.00 | 3 | string(1) | BITMAP | Y/N/U |
| 22 | `Socially and Economically Disadvantaged` | VARCHAR | 100.00 | 3 | string(1) | BITMAP | Y/N/U |
| 23 | `Woman Owned` | VARCHAR | 100.00 | 3 | string(1) | BITMAP | Y/N/U |
| 24 | `Number Employees` | BIGINT | 84.10 | 381 | int32 | — | 0–9999 (9999=sentinel); 316 > 500 |
| 25 | `Company Website` | VARCHAR | 55.84 | 12,227 | string | — | 43 sentinel strings |
| 26 | `Address1` | VARCHAR | 99.95 | 33,207 | string | — | — |
| 27 | `Address2` | VARCHAR | 17.35 | 4,150 | string | — | — |
| 28 | `City` | VARCHAR | 99.999 | 5,908 | string | — | 2 missing |
| 29 | `State` | VARCHAR | 99.999 | 55 | string(2) | BITMAP | **full names → normalize to USPS-2** |
| 30 | `Zip` | VARCHAR | 99.89 | 22,165 | string(5) | BTREE | **normalize to ZIP5** |
| 31 | `Abstract` | VARCHAR | 99.93 | — | string (large) | — | null placeholders; whitespace-normalize |
| 32 | `Contact Name` | VARCHAR | 19.65 | 4,903 | string | — | — |
| 33 | `Contact Title` | VARCHAR | 6.68 | 446 | string | — | — |
| 34 | `Contact Phone` | VARCHAR | 18.06 | 4,684 | string | — | — |
| 35 | `Contact Email` | VARCHAR | 19.53 | 4,780 | string | — | lowercase |
| 36 | `PI Name` | VARCHAR | 96.63 | 90,268 | string | BTREE | principal investigator |
| 37 | `PI Title` | VARCHAR | 41.30 | 9,462 | string | — | — |
| 38 | `PI Phone` | VARCHAR | 99.75 | 59,000 | string | — | — |
| 39 | `PI Email` | VARCHAR | 70.95 | 59,409 | string | — | lowercase |
| 40 | `RI Name` | VARCHAR | 16.72 | 4,238 | string | — | research institution (STTR) |
| 41 | `RI POC Name` | VARCHAR | 6.62 | 9,093 | string | — | — |
| 42 | `RI POC Phone` | VARCHAR | 6.82 | 7,487 | string | — | — |

*Plus 1 minted column:* `sbir_surrogate_id` — string, **BTREE (primary key)** — see §6.

---

## 5. Deliverable 2 — Structural cleaning requirements

Mandatory transformations the ingest routine must perform in the DuckDB projection (read `all_varchar=
true`, then defensive cast) **before** the Lance write. Ordered by blast radius.

1. **Mint the surrogate primary key (BLOCKING).** No natural key is unique. Compute
   `sbir_surrogate_id = sha256(concat_ws(chr(31), <all 42 published attributes, NULL→chr(30)>)) || '-' ||
   row_number() OVER (PARTITION BY <that hash>)`. Idempotent across rebuilds; disambiguates the 3
   byte-identical rows. BTREE index — this is the resolution spine.

2. **String null-sentinel normalization (every column).** Apply `nullif(trim(col), '')`. Additionally
   null the literal sentinel strings present across many columns — `lower(trim(col)) IN ('n/a','na',
   'none','null','nan','#n/a','tbd')` (e.g., `Topic Code` carries 202, `Award Title` 84,
   `Solicitation Number` 57). Empty string and these sentinels must land as SQL `NULL`, never as text.

3. **`Abstract` — placeholder nulling + whitespace normalization (vectorization gate).** (a) Null the
   abstract when `lower(trim())` (trailing `.` stripped) matches the redaction/placeholder pattern
   (`n/a`, `redacted`, `not available`/typos, `tbd`, `xxx`, `blank`, `null`, `in process for public
   release`) **or** trimmed length < 50 → leaves 190,034 embeddable. (b) For the embeddable set, collapse
   `\r\n\t` runs to single spaces (`regexp_replace(ab, '\s+', ' ', 'g')`) so 24,344 layout-newline
   abstracts don't pollute embeddings. Keep the raw abstract in a separate column if lossless retention
   is required; the normalized column is the embedding input.

4. **`State` → USPS 2-letter (BLOCKING for cross-dataset resolution).** Stored as full names
   (`California`, `Massachusetts`, …; distinct = 55 = 50 states + DC + territories/foreign). Map to
   2-letter via a name→code lookup so SBIR `state` joins the rest of the core-x plane (all of which key
   on 2-letter). Unmapped/foreign → NULL (or a `country` sidecar).

5. **`Zip` → canonical ZIP5.** Heterogeneous: 150,008 ZIP+4 (10-char), 58,143 ZIP5, 8,341 single-char
   junk, 2,757 malformed 6-char. Extract `left(regexp_replace(zip,'[^0-9]',''), 5)`; null when < 5
   digits remain. (Optionally retain ZIP+4 in a sidecar.)

6. **`Duns` → zero-pad to 9.** 1,664 values lost leading zeros. `lpad` to 9 **only** when the trimmed
   value is purely numeric and 3–9 digits; null the 117 1–2-digit junk values. UEI gets validated against
   `^[A-Za-z0-9]{12}$` (quarantine the 2 non-conforming via a `uei_valid` boolean, do not drop the row).

7. **`Award Amount` → `DECIMAL(18,2)`.** `TRY_CAST(nullif(trim(col),'') AS DECIMAL(18,2))`. Source is
   100% cast-clean (no `$`/comma debris) but money must not land as float. 4,202 legitimate $0 awards
   retained.

8. **Date validation.** `TRY_CAST(... AS DATE)`, then null values outside `[1982-01-01, current+2y]` —
   `Proposal Award Date` carries a `1905-07-01` and forward-dated entries. Prefer `Award Year` (int16,
   100% fill, 1983–2026) as the temporal partition/filter key over the sparse `DATE` columns.

9. **Integer casts + whitespace.** `Number Employees`/`Award Year`/`Solicitation Year` →
   `TRY_CAST(... AS INT/SMALLINT)` (down-typed years to int16). Flag `Number Employees = 9999` as a
   sentinel and the 316 rows > 500 (SBIR eligibility ceiling) via a derived boolean rather than mutating.
   Trim the 23–91 stray-whitespace values in the free-text ID columns.

10. **Socio-economic flags — preserve ternary.** Land `HUBZone Owned` / `Woman Owned` / `Socially and
    Economically Disadvantaged` as native `Y`/`N`/`U` `string(1)`, BITMAP-indexed. Do **not** coerce to
    boolean (loses the `U` state). Optional derived nullable booleans for ergonomics.

---

## 6. Proposed Lance landing spec (forward reference — not executed here)

Single standalone dataset, append-not-applicable (this is a wholesale snapshot feed → `mode="overwrite"`,
matching `sba_foia`).

- **URI:** `s3://data-sink/active/sbir_awards/` (env-overridable `SBIR_AWARDS_LANCE_URI`).
- **Write:** `lance.write_dataset(arrow, URI, mode="overwrite", data_storage_version="2.0",
  max_rows_per_file=1048576, storage_options=<R2>)` → 1 fragment (219,502 rows ≪ 1,048,576).
- **BTREE (high-card resolution):** `sbir_surrogate_id` (PK), `company`, `uei`, `duns`,
  `agency_tracking_number`, `contract`, `topic_code`, `pi_name`, `zip`.
- **BITMAP (low-card categorical):** `program`, `phase`, `agency`, `branch`, `state`, `award_year`,
  `solicitation_year`, `hubzone_owned`, `woman_owned`, `socially_economically_disadvantaged`.
- **Out-of-core envelope:** trivial at this scale (decoded corpus ≈ 0.5 GB). The abstract column
  dominates width; set `memory_limit` ≥ 4 GB + `temp_directory` on local NVMe for the projection, and
  `LANCE_BYPASS_SPILLING=true` is unnecessary here (row count is far below the SBA threshold that forced
  it). Idempotency via an `ops.sbir_awards_runs` ledger row on terminal state.

---

## 7. Reproduce

The full non-mutating harness is committed at `pipelines/sbir/profile.sql`:

```bash
rclone copy r2:data-sink/landing/sbir /tmp/sbir_audit          # 486 MB, ~6s
duckdb :memory: < pipelines/sbir/profile.sql                   # full-scan profile, ~30s
```
