# MSHA Data Trove — Profiling & Schema Recon (Directive 26)

Read-only architectural profile of the **20 raw MSHA (Mine Safety & Health
Administration) Open-Government-Data archives** staged in the landing zone, ahead of
DuckDB extraction + Lance materialization design. Establishes the wire format, the
entity-relationship graph, the corporate cross-walk surface to our `companies` /
resolution spine, and the GTM event-signal inventory.

- **Source:** `s3://data-sink/landing/msha/` (Cloudflare R2, `data-sink` bucket). 20 `*.zip` + a `.keep`.
- **Snapshot:** landing-zone objects written `2026-06-02`; data current through **`2026-05-28`**
  (max `Violations.VIOLATION_ISSUE_DT`); the `107(a)` report self-stamps *"Data as of 29-MAY-26"*.
- **Evidence harness (reproducible, non-mutating):** streaming `zipfile` enumeration +
  `duckdb` (v1.5.2) `read_csv` over the decompressed member streams. No archive was
  inflated whole to disk except three small identity files (`Mines`, `AddressOfRecord`,
  `ControllerOperatorHistory`) extracted to scratch for cardinality SQL.
- **Attestation:** every figure below is from `zipfile` central-directory metadata, a
  quote-aware `csv` record count, or a DuckDB `SELECT`. **Zero writes** — no extraction
  to the sink, no Lance datasets, no `ops.*` rows, no R2 puts. Per Directive 26, no
  ingestion pipeline is authored here.

---

## 0. Headline findings

1. **Uniform wire format, two booby-traps.** All 20 are single-member ZIP/DEFLATE,
   **pipe-delimited**, **header-row-present**, **CRLF**, with string fields wrapped in
   `"`. But (a) free-text fields (`DIRECTIONS_TO_MINE`, `NARRATIVE`) contain
   **unescaped interior double-quotes** (`…at the 1st "Y" go right…`), so a strict
   RFC-4180 read throws *"unterminated quote"* and silently collapses; and (b) the
   encoding is **Windows-1252, not UTF-8** (degree signs in GPS strings; ñ/ó/é in Puerto
   Rico operator names; smart-quotes/dashes). A naive `read_csv` dropped **93 %** of
   `Mines` rows. **The only correct parse is `quote='' ` + CP1252→UTF-8 transcode** (§2).
2. **`MINE_ID` is the universal spine; corporate identity is a separate, key-less
   namespace.** `MINE_ID` (7-char zero-padded VARCHAR) joins **16 of 20** files. The
   corporate actors — `CONTROLLER_ID` (ultimate parent), `OPERATOR_ID` (site operator),
   `CONTRACTOR_ID`, and `VIOLATOR_ID` (= operator **or** contractor, per
   `VIOLATOR_TYPE_CD`) — are **alpha-prefixed VARCHAR** (`L13586`, `P24632`, `C00692`,
   `M00024`) bound to the mine through **`ControllerOperatorHistory`** (an SCD with
   start/end dates).
3. **No native corporate key to our universe exists.** Across all 20 files there is
   **zero** EIN/FEIN, DUNS, UEI/CAGE, SAM registration, NAICS, or domain/website. The
   only bridge to `companies` is **normalized corporate name + state (+ mine address)**.
   This is the same name-indexed reality as the CA UCC spine.
4. **Direct `MSHA → companies` is structurally weak; bridge through
   `sos_normalized_master` instead.** Our `companies` Lance set is a **748-row curated
   GTM list anchored on `normalized_domain`** with no state/address/name-blocking column
   — MSHA has no domain, so the intersection is name-only against a tiny target. The
   correct join target is the **17.9 M-row `sos_normalized_master`** (BTREE
   `normalized_legal_name` + `zip_code`), exactly as `crosswalk_*` workers already do.
5. **High-quality, current GTM signal.** `Violations` (3.08 M rows) + `AssessedViolations`
   (3.01 M) carry **$1.81 B proposed / $1.27 B paid** penalties, **~26.5 % Significant &
   Substantial**, **89,306 withdrawal/closure Orders**, spanning **1994 → May 2026** with
   **~212 K violations + ~196 K assessments issued since 2024**. Severity flags, gravity
   points, and `mm/dd/yyyy` event dates are all first-class columns.
6. **~41 % of controllers are individuals.** Of 54,285 distinct controllers, **22,441
   (41 %) are `CONTROLLER_TYPE='PERSON'`** (sole proprietors) — structurally un-joinable
   to any B2B company table. Filter to `COMPANY` (31,844) before name resolution.

---

## 1. File inventory, format & grain

All members are `text/plain`, `|`-delimited, quoted strings, header row 1, CRLF,
DEFLATE-compressed, **one data member per archive**. `data_rows` is the quote-aware
logical record count (= physical lines − 1; **embedded newlines confirmed = 0** in every
file, so physical and logical counts are identical).

| Archive | Member | Comp. | Uncompressed | Cols | Data rows | Role |
|---|---|--:|--:|--:|--:|---|
| `Mines.zip` | `Mines.txt` | 6.9 M | 39,223,942 | 59 | **91,803** | Identity — mine master |
| `ControllerOperatorHistory.zip` | `…History.txt` | 4.3 M | 26,344,277 | 13 | **168,809** | Identity — mine↔controller↔operator SCD |
| `AddressofRecord.zip` | `AddressOfRecord.txt` | 3.4 M | 18,518,170 | 20 | **91,507** | Identity — mine mailing address |
| `Violations.zip` | `Violations.txt` | 114.1 M | 1,429,213,206 | 61 | **3,076,347** | Signal — citation/order ledger |
| `AssessedViolations.zip` | `AssessedViolations.txt` | 103.3 M | 1,318,508,733 | 58 | **3,008,799** | Signal — penalty ledger |
| `ContestedViolations.zip` | `…Violations.txt` | 28.0 M | 167,202,801 | 39 | 448,158 | Signal — litigation (contest) |
| `CivilPenaltyDocketsDecisions.zip` | `…Decisions.txt` | 13.8 M | 124,973,175 | 29 | 479,439 | Signal — docket decisions |
| `OrdersIssued.zip` | `107(a)OrdersIssued.csv` | 185.8 K | 603,695 | 13 | ~3,829 | Signal — 107(a) imminent-danger (report export) |
| `Conferences.zip` | `Conferences.txt` | 1.0 M | 16,296,028 | 7 | 161,623 | Signal — pre-penalty conference |
| `Accidents.zip` | `Accidents.txt` | 49.5 M | 226,924,525 | 57 | 273,065 | Signal — injury/fatality |
| `Inspections.zip` | `Inspections.txt` | 69.3 M | 344,007,105 | 45 | 1,147,232 | Activity — inspection events |
| `MinesProdQuarterly.zip` | `…Quarterly.txt` | 53.7 M | 257,395,968 | 13 | 2,714,840 | Firmographic — production/employment |
| `MinesProdYearly.zip` | `…Yearly.txt` | 6.7 M | 59,902,364 | 11 | 657,546 | Firmographic — production/employment |
| `ContractorProdQuarterly.zip` | `…Quarterly.txt` | 9.0 M | 125,240,648 | 12 | 1,350,534 | Firmographic — contractor activity |
| `ContractorProdYearly.zip` | `…Yearly.txt` | 3.1 M | 24,868,992 | 10 | 280,142 | Firmographic — contractor activity |
| `CoalDustSamples.zip` | `CoalDustSamples.txt` | 105.4 M | 1,008,176,282 | 30 | 2,985,614 | Compliance — respirable dust |
| `PersonalHealthSamples.zip` | `…Samples.txt` | 5.9 M | 95,556,197 | 20 | 310,908 | Compliance — IH exposure |
| `NoiseSamples.zip` | `NoiseSamples.txt` | 6.0 M | 56,409,893 | 29 | 274,645 | Compliance — noise |
| `QuartzSamples.zip` | `QuartzSamples.txt` | 5.6 M | 41,858,030 | 19 | 167,238 | Compliance — silica |
| `AreaSamples.zip` | `AreaSamples.txt` | 225.6 K | 2,267,151 | 17 | 8,368 | Compliance — area contaminant |

**Totals:** 589.4 MiB compressed → **≈ 5.38 GiB** uncompressed, **≈ 17.7 M** data rows.
Compression ratio ≈ 9–12× (highly compressible quoted text).

---

## 2. Wire format & the canonical DuckDB read

### 2.1 The two parsing hazards (both verified on `Mines.txt`)

**(a) Unescaped interior quotes.** MSHA wraps strings in `"` but does **not** double
interior quotes:

```
…|150|"¾ mile east of the 1st "Y" go right and at the 2nd "Y" go to the 4th "Y"…"|"Escambia"
```

- `read_csv(quote='"')` → `Invalid Input Error: Value with unterminated quote found` →
  the dialect sniffer fails outright, or `ignore_errors` desyncs and drops the row block.
  A strict read returned **6,304 / 91,803** Mines rows (5,950 lost per malformed line).
- `read_csv(quote='')` (quote processing **disabled**) → **exactly 91,803** rows.
  Interior quotes are preserved as literal text; the wrapping quotes are stripped
  downstream with `trim(BOTH '"' FROM col)`.

**(b) Windows-1252, not UTF-8.** Non-ASCII bytes decode cleanly only as CP1252:

| Byte | CP1252 glyph | Where |
|---|---|---|
| `0xB0` | `°` | GPS coords in `DIRECTIONS_TO_MINE` (`31°00'43.5"N 103°…`) |
| `0xF1 0xF3 0xE9` | `ñ ó é` | Puerto Rico names — `Cantera La Montaña`, `Cantera Hipódromo`, `Construcciones José Carro, S.E.`, `Añasco` |
| `0x92 0x93 0x94 0x96` | `' " " –` | smart quotes / en-dash in free text |

DuckDB's strict `encoding='latin-1'` (ISO-8859-1) **rejects** the `0x80–0x9F` CP1252
bytes (`File is not latin-1 encoded`); `encoding='utf-8'` rejects the high bytes. **The
worker must transcode CP1252→UTF-8** during the stream-to-scratch step (`open(…,
encoding='cp1252')` / `iconv -f CP1252 -t UTF-8`) **before** handing bytes to DuckDB.
Skipping this mojibakes accented PR operator names and **breaks their `name_norm`
join key** — and even silently drops ~0.2 % of rows under a lossy UTF-8 fallback.

### 2.2 Canonical read recipe (covers 19 of 20 files verbatim)

```sql
-- After the worker decompresses the member AND transcodes CP1252 → UTF-8 to /tmp:
SELECT *
FROM read_csv(
    '/tmp/<member>.txt',
    delim       = '|',
    quote       = '',          -- MUST be off: interior quotes are unescaped
    header      = true,
    all_varchar = true,        -- text in, TRY_CAST downstream (ARCHITECTURE §4)
    new_line    = '\r\n',
    strict_mode = false
);
-- Per-field normalization in the projection:
--   strings : nullif(trim(BOTH '"' FROM col), '')
--   dates   : try_strptime(trim(BOTH '"' FROM col), '%m/%d/%Y')
--   numerics: try_cast(trim(BOTH '"' FROM col) AS {INTEGER|DOUBLE})
```

**The one exception — `107(a)OrdersIssued.csv`** is an Excel *report* export, not a clean
table: line 1 `sep=|`, line 2 `107(a) Order Issued Report`, line 3 `Data as of 29-MAY-26
…`, **real header on line 4**, human-friendly column labels with spaces/`@`
(`Operator Name (Violations)`, `Controller ID @ Violations`). Read with `skip=3`. It is a
pre-filtered slice (107(a) imminent-danger only); the authoritative order universe lives
in `Violations.CIT_ORD_SAFE='Order'` (89,306 rows) — **prefer `Violations`; treat this
file as redundant.**

---

## 3. Entity-relationship model

### 3.1 The graph

```
        CORPORATE IDENTITY NAMESPACE                      PHYSICAL ASSET
        (alpha-prefixed VARCHAR ids,                      (MINE_ID, 7-char
         NO native key to our universe)                    zero-padded VARCHAR)
   ┌──────────────────────────────────────┐
   │      ControllerOperatorHistory       │   MINE_ID    ┌────────────────┐  MINE_ID(PK)  ┌──────────────────┐
   │  CONTROLLER_ID × OPERATOR_ID × MINE_ID│─────────────►│     Mines      │◄──────1:1─────│ AddressOfRecord  │
   │  + CONTROLLER_TYPE (COMPANY|PERSON)   │              │    91,803      │               │  91,507 (mailing │
   │  + start/end dates  (SCD, 168,809)    │              │  (mine master) │               │  addr, BUSINESS_ │
   └──────────────────────────────────────┘              └───────┬────────┘               │  NAME, SIC, ZIP) │
            ▲ CONTROLLER_NAME / OPERATOR_NAME                     │                         └──────────────────┘
            │ (free text → name_norm bridge, §4)         MINE_ID  │ (FK in 16 of 20 files)
            │                              ┌───────────────────────┼─────────────────────────────┐
            │                              ▼                       ▼                             ▼
   ┌────────┴─────────┐   EVENT_NO  ┌─────────────┐  EVENT_NBR ┌────────────────────┐  ASSESS_  ┌────────────────────────────┐
   │ VIOLATOR_ID      │◄────────────│ Violations  │───────────►│ AssessedViolations │  CASE_NO  │ CivilPenaltyDocketsDecisions│
   │ (=OPERATOR_ID or │ VIOLATION_NO│  3.08 M     │ VIOLATION_ │  3.01 M (penalty,  │──────────►│   479,439  (DOCKET_NO)      │
   │  CONTRACTOR_ID,  │             │ (citation/  │   _NO      │  gravity points)   │ DOCKET_NO ├────────────────────────────┤
   │  per *_TYPE_CD)  │             │  order)     │            └────────────────────┘    ▲      │ ContestedViolations 448,158 │
   └──────────────────┘             └──────┬──────┘                                      └──────│   (CITATION_NO, DOCKET_NO)  │
                                           │ EVENT_NO                                           └────────────────────────────┘
              ┌────────────────────────────┼───────────────────────────────┐
              ▼                            ▼                                ▼
        ┌───────────┐              ┌──────────────┐                ┌─────────────────────────────────────────┐
        │Inspections│ (EVENT_NO)   │  Accidents   │ (MINE_ID,      │ {Area,CoalDust,Noise,PersonalHealth,    │
        │ 1.15 M    │              │  273,065     │  CONTROLLER_ID,│  Quartz}Samples  (MINE_ID, EVENT_NO,    │
        └───────────┘              │  injury/fatal│  OPERATOR_ID,  │  CONTRACTOR_ID) — compliance/exposure   │
                                   └──────────────┘  CONTRACTOR_ID)└─────────────────────────────────────────┘

   FIRMOGRAPHIC SIZE:  MinesProd{Quarterly,Yearly}  (MINE_ID → AVG_EMPLOYEE_CNT, HOURS_WORKED, *_PRODUCTION)
                       ContractorProd{Quarterly,Yearly}  (CONTRACTOR_ID → AVG_EMPLOYEE_CNT, HOURS_WORKED)
```

### 3.2 Primary / foreign keys

| Entity | Grain / PK | Key foreign references |
|---|---|---|
| `Mines` | 1 / mine · **`MINE_ID`** | `CURRENT_CONTROLLER_ID`, `CURRENT_OPERATOR_ID` (denormalized *current* owner) |
| `AddressOfRecord` | 1 / mine · **`MINE_ID`** | `BUSINESS_NAME` (operator name, free text) |
| `ControllerOperatorHistory` | N / mine · **(`CONTROLLER_ID`,`OPERATOR_ID`,`MINE_ID`,`CONTROLLER_START_DT`)** | `MINE_ID` → `Mines`; the only place the full controller↔operator↔mine history with dates lives |
| `Violations` | 1 / violation · **`VIOLATION_NO`** (event `EVENT_NO`) | `MINE_ID`, `VIOLATOR_ID`(+`_TYPE_CD`), `CONTROLLER_ID`, `CONTRACTOR_ID`, `DOCKET_NO` |
| `AssessedViolations` | 1 / assessed violation · **(`EVENT_NBR`,`VIOLATION_NO`)** | `MINE_ID`, `VIOLATOR_ID`, `ASSESS_CASE_NO` |
| `CivilPenaltyDocketsDecisions` | N / docket·violation · `ASSESS_CASE_NO`+`VIOLATION_NO` | `DOCKET_NO`, `VIOLATOR_ID`, `MINE_ID` |
| `ContestedViolations` | 1 / contested citation · `CITATION_NO` | `MINE_ID`, `DOCKET_NO` |
| `Accidents` | 1 / accident · `DOCUMENT_NO` | `MINE_ID`, `CONTROLLER_ID`, `OPERATOR_ID`, `CONTRACTOR_ID` |
| `Inspections` | 1 / inspection · `EVENT_NO` | `MINE_ID`, `CONTROLLER_ID`, `OPERATOR_ID` |
| `*Samples` | 1 / sample | `MINE_ID`, `EVENT_NO`, `CONTRACTOR_ID` |
| `MinesProd*` | N / mine·period | `MINE_ID` |
| `ContractorProd*` | N / contractor·period | `CONTRACTOR_ID` |

### 3.3 Cross-file join-key index (which key appears where)

| Join key | # files | Files |
|---|--:|---|
| **`MINE_ID`** | 16 | Mines, AddressofRecord, ControllerOperatorHistory, Violations, AssessedViolations, ContestedViolations, CivilPenaltyDocketsDecisions, Accidents, Inspections, MinesProd{Q,Y}, {Area,CoalDust,Noise,PersonalHealth,Quartz}Samples |
| `CONTRACTOR_ID` | 7 | Accidents, AreaSamples, ContractorProd{Q,Y}, NoiseSamples, PersonalHealthSamples, Violations |
| `EVENT_NO`/`EVENT_NBR` | 6 | Violations, AssessedViolations, Inspections, AreaSamples, NoiseSamples, PersonalHealthSamples |
| `CONTROLLER_ID` | 4 | Mines\*, Accidents, ControllerOperatorHistory, Inspections, Violations |
| `VIOLATION_NO` | 4 | Violations, AssessedViolations, CivilPenaltyDocketsDecisions, NoiseSamples |
| `OPERATOR_ID` | 3 | Mines\*, Accidents, ControllerOperatorHistory, Inspections |
| `VIOLATOR_ID` | 3 | Violations, AssessedViolations, CivilPenaltyDocketsDecisions |
| `DOCKET_NO` | 3 | Violations, ContestedViolations, CivilPenaltyDocketsDecisions |
| `ASSESS_CASE_NO` | 2 | AssessedViolations, CivilPenaltyDocketsDecisions |

\* `Mines` carries the *current* controller/operator as `CURRENT_CONTROLLER_ID` /
`CURRENT_OPERATOR_ID`.

### 3.4 Identity cardinality (measured)

| Quantity | Value |
|---|--:|
| Mines (all-time) | 91,803 |
| — `Active` / `Intermittent` / other live | 6,634 / 5,648 / ~1,472 → **≈ 13,754 live** |
| — `Abandoned` / `Abandoned and Sealed` | 69,196 / 8,853 |
| Mines geocoded (`LAT`+`LONG` present) | 47,299 (**52 %**) |
| Coal / Metal-Nonmetal mines | 35,704 / 55,918 |
| States + territories | 55 |
| Distinct `CURRENT_OPERATOR_ID` (Mines) | ≈ 49,700 |
| Distinct current operator **names** | ≈ 53,303 |
| Distinct `CURRENT_CONTROLLER_ID` (Mines) | ≈ 40,965 |
| Distinct controllers (history) | **54,285** — `COMPANY` 31,844 / `PERSON` 22,441 |
| Distinct operators (history) | 67,787 (68,863 names) |
| AddressOfRecord rows (1/mine) | 91,507 — **66 %** have a full street address; 56 states |

---

## 4. The cross-walk assessment (MSHA operator/controller → our universe)

### 4.1 What joinable data exists — and what does not

| Candidate join vector | Present? | Notes |
|---|---|---|
| EIN / FEIN / Tax ID | **No** | scanned all 20 headers — absent |
| DUNS / UEI / CAGE / SAM reg. | **No** | absent |
| Domain / website / URL | **No** | absent |
| NAICS | **No** | only legacy **SIC** (`Mines.PRIMARY_SIC_CD`, `AddressOfRecord.PRIMARY_SIC_CD`) |
| **Corporate name** | **Yes** | `CONTROLLER_NAME`, `OPERATOR_NAME`, `VIOLATOR_NAME`, `CONTRACTOR_NAME`, `BUSINESS_NAME` — free text |
| **State** | **Yes** | `Mines.STATE`, `AddressOfRecord.STATE_ABBR` (+ `FIPS_STATE_CD`) |
| **Street address** | **Yes** | `AddressOfRecord` (`STREET`,`CITY`,`STATE_ABBR`,`ZIP_CD`) — mine mailing addr, 66 % populated |
| **Lat / Long** | Partial | `Mines.LATITUDE/LONGITUDE` — 52 % — but these are *mine* coords, not corporate HQ |

**Conclusion: name-indexed, exactly like the CA UCC spine.** The resolution surface is
`core.name_norm(name)` **blocked by state**, with `AddressOfRecord` street/ZIP as a
secondary geo-disambiguator. No tax/registry/domain key exists to short-circuit it.

### 4.2 Joining to `companies` directly is the wrong target

`s3://data-sink/active/companies/` (worker `pipelines/gtm/companies_people_bulk.py`) is a
**748-row curated GTM list**:

```
companies: company_id(uuid→VARCHAR, PK) | company_name | normalized_domain(anchor, BTREE)
           | company_linkedin_url | source_platform
```

It has **no state, no address, no `name_norm`/blocking column, no EIN** — its anchor is
`normalized_domain`, which **MSHA does not have**. A direct join is therefore name-only
against a 748-row, domain-curated, likely-not-mining-sector target → expect a **handful
to low-hundreds** of incidental hits. Low leverage.

### 4.3 Recommended bridge — through `sos_normalized_master`

The active sink already carries the fleet's name-resolution spine and the exact pattern
for "fuzzy name entity → our universe" (`crosswalk_hmda_gleif`,
`crosswalk_sam_usaspending`, both cite `core.name_norm`):

```
MSHA OPERATOR_NAME / CONTROLLER_NAME  (free text, CP1252)
  │  ① transcode CP1252→UTF-8   ② drop CONTROLLER_TYPE='PERSON' (41% of controllers)
  │  ③ strip "(Form:Prior Name)" lineage annotations   ④ keep state for blocking
  ▼  core.name_norm(name)         [+ legal_name_base() to absorb LLC/INC/CORP drift]
normalized_legal_name ──exact, blocked by STATE──► sos_normalized_master   (17.9 M rows;
  ▲                                                  BTREE normalized_legal_name + zip_code)
  │                                                    │
  └─ secondary: AddressOfRecord ZIP/state geo-tiebreak ▼
                                              gleif_l1_entities / pdl_companies / companies
```

`core/name_norm.py` is the single source of truth (`name_norm()` + `legal_name_base()`) —
reuse it verbatim as a DuckDB SQL literal; do **not** re-inline the rule. The CA UCC
precedent on this identical name-only problem hit **57.9 % distinct / 67.6 % appearance**
match after suffix-stripping, **93.4 %** single-entity — a reasonable yield expectation
for MSHA org names against the same spine.

### 4.4 Name-hygiene hazards (must pre-clean before `name_norm`)

1. **Persons, not companies.** 41 % of controllers (`CONTROLLER_TYPE='PERSON'`:
   `Jeremy Smith`, `Carl C Robinson Jr et al`) — filter on `COMPANY`.
2. **Lineage annotations.** `CURRENT_CONTROLLER_NAME` / `Violations.CONTROLLER_NAME`
   embed prior-name history: `Legacy Vulcan Corp (Form:Vulcan Materials Co)` — strip
   the `\s*\(Form:.*\)$` tail (it survives `name_norm`'s punctuation strip otherwise).
3. **Puerto Rico encoding.** `Cantera La Montaña` only normalizes correctly if CP1252 is
   transcoded first (§2.2) — otherwise the block key diverges.
4. **Operator vs controller grain.** `OPERATOR_ID` (site operator, 67,787) is finer than
   `CONTROLLER_ID` (parent, 54,285). Resolve **controllers** for account-level GTM;
   resolve operators for site-level. `ControllerOperatorHistory` is the rollup.

---

## 5. GTM signal evaluation

### 5.1 Severity columns (measured on the full ledgers)

| Signal | Column(s) | Magnitude |
|---|---|---|
| **Significant & Substantial** | `Violations.SIG_SUB`, `AssessedViolations.SIG_SUB_IND` (`Y/N`) | **26.4 % / 26.8 %** of all violations |
| **Withdrawal / closure orders** | `Violations.CIT_ORD_SAFE` ∈ {Citation 96.8 %, **Order 2.9 % = 89,306**, Safeguard 8,105} | the high-severity tail |
| **Imminent danger** | `SECTION_OF_ACT='107(a)'`; `OrdersIssued` report | 107(a) closure orders |
| **Proposed penalty $** | `Violations.PROPOSED_PENALTY`, `AssessedViolations.PROPOSED_PENALTY_AMT` | **Σ $1,813.0 M**; max single **$246,200** |
| **Paid penalty $** | `AssessedViolations.PAID_PROPOSED_PENALTY_AMT` | **Σ $1,266.7 M** |
| **Gravity / negligence scoring** | `AssessedViolations.{PENALTY_POINTS, GRAVITY_PERSONS_POINTS, GRAVITY_INJURY_POINTS, GRAVITY_LIKELIHOOD_POINTS, NEGLIGENCE_POINTS}`; `Violations.{LIKELIHOOD, INJ_ILLNESS, NO_AFFECTED, NEGLIGENCE}` | per-violation ordinal severity |
| **Repeat-offender history** | `AssessedViolations.{VIOLATOR_VIOLATION_CNT, VIOLATOR_REPEATED_VIOL_CNT, EXCESSIVE_HISTORY_IND}` | escalation flag |
| **Operator size (firmographic)** | `AssessedViolations.{VIOLATOR_MINE_HRS, SIZE_OF_MINE, SIZE_OF_CONTROLLING_ENTITY}`; `MinesProd*.{AVG_EMPLOYEE_CNT, HOURS_WORKED}` | employee/hours/tonnage |
| **Injury / fatality** | `Accidents.{DEGREE_INJURY, NO_INJURIES, DAYS_LOST}` | 273,065 accident records |
| **Litigation / distress** | `ContestedViolations` (448,158), `CivilPenaltyDocketsDecisions` (479,439), `Conferences` (161,623) | operator contesting penalties |

### 5.2 Temporal trigger columns

All dates are `mm/dd/yyyy` text → `try_strptime(…, '%m/%d/%Y')`.

| Event | Trigger column | Range / recency |
|---|---|---|
| Violation issued | `Violations.VIOLATION_ISSUE_DT` | 1994-09-09 → **2026-05-28**; **211,800 since 2024-01-01** |
| Penalty issued | `AssessedViolations.ISSUE_DT` | 1994-09-09 → 2026-05-05; **195,812 since 2024** |
| Final order | `AssessedViolations.FINAL_ORDER_DT` | assessment finalization |
| Case status change | `AssessedViolations.ASSESS_CASE_STATUS_DT` | delinquency / payment lifecycle |
| Contest filed | `ContestedViolations.CONTEST_DT`, `…PETITION_FILED_DT` | active litigation onset |
| Docket decision | `CivilPenaltyDocketsDecisions.DECISION_DT` | settlement / vacate |
| Accident | `Accidents.ACCIDENT_DT` | injury/fatality event |
| Inspection | `Inspections.INSPECTION_BEGIN_DT` | site-visit cadence |

**Freshness:** current to within **~5 days** of the snapshot — a viable real-time
enforcement-trigger feed (e.g. *new S&S order against a resolved operator* → GTM event).

---

## 6. Exact DuckDB read schemas

Universal recipe per §2.2 (`delim='|'`, `quote=''`, `header=true`, `all_varchar=true`,
CP1252→UTF-8 transcode). Below: the typed target projection for the five load-bearing
tables. Strings shown as their post-`trim`/`nullif` form; `→` gives the downstream
`TRY_CAST` target. All ID columns stay **VARCHAR** (leading zeros / alpha prefixes are
significant — never cast to integer).

### 6.1 `Mines` (59 cols) — keys + load-bearing subset
```
MINE_ID VARCHAR PK | CURRENT_MINE_NAME VARCHAR | COAL_METAL_IND VARCHAR('C'|'M')
CURRENT_MINE_TYPE VARCHAR | CURRENT_MINE_STATUS VARCHAR | CURRENT_STATUS_DT →DATE
CURRENT_CONTROLLER_ID VARCHAR | CURRENT_CONTROLLER_NAME VARCHAR  -- strip "(Form:…)"
CURRENT_OPERATOR_ID VARCHAR   | CURRENT_OPERATOR_NAME VARCHAR
STATE VARCHAR | FIPS_CNTY_CD VARCHAR | FIPS_CNTY_NM VARCHAR | COMPANY_TYPE VARCHAR
PRIMARY_SIC_CD VARCHAR | PRIMARY_SIC VARCHAR | ASSESS_CTRL_NO VARCHAR
NO_EMPLOYEES →INTEGER | LONGITUDE →DOUBLE | LATITUDE →DOUBLE | NEAREST_TOWN VARCHAR
-- (+ DISTRICT, OFFICE_CD, CURRENT_103I, PORTABLE_OPERATION, MINE_GAS_CATEGORY_CD, … full 59)
```

### 6.2 `ControllerOperatorHistory` (13 cols) — full
```
CONTROLLER_ID VARCHAR | CONTROLLER_NAME VARCHAR | CONTROLLER_START_DT →DATE
CONTROLLER_END_DT →DATE (NULL ⇒ current) | CONTROLLER_TYPE VARCHAR('COMPANY'|'PERSON')
COAL_METAL_IND VARCHAR | MINE_ID VARCHAR | MINE_NAME VARCHAR | MINE_STATUS VARCHAR
OPERATOR_ID VARCHAR | OPERATOR_NAME VARCHAR | OPERATOR_START_DT →DATE | OPERATOR_END_DT →DATE
```

### 6.3 `AddressOfRecord` (20 cols) — full
```
MINE_ID VARCHAR PK | MINE_NAME VARCHAR | CONTACT_TITLE VARCHAR | NEAREST_TOWN VARCHAR
BUSINESS_NAME VARCHAR | STREET VARCHAR | PO_BOX VARCHAR | CITY VARCHAR
STATE_ABBR VARCHAR | FIPS_STATE_CD VARCHAR | STATE VARCHAR | ZIP_CD VARCHAR
COUNTRY VARCHAR | PROVINCE VARCHAR | POSTAL_CD VARCHAR | MINE_TYPE_CD VARCHAR
MINE_STATUS VARCHAR | MINE_STATUS_DT →DATE | PRIMARY_SIC_CD VARCHAR | COAL_METAL_IND VARCHAR
```

### 6.4 `Violations` (61 cols) — keys + signal subset
```
EVENT_NO VARCHAR | VIOLATION_NO VARCHAR PK | INSPECTION_BEGIN_DT/END_DT →DATE
CONTROLLER_ID VARCHAR | CONTROLLER_NAME VARCHAR | VIOLATOR_ID VARCHAR
VIOLATOR_NAME VARCHAR | VIOLATOR_TYPE_CD VARCHAR('Operator'|'Contractor')
MINE_ID VARCHAR | MINE_NAME VARCHAR | MINE_TYPE VARCHAR | COAL_METAL_IND VARCHAR
CONTRACTOR_ID VARCHAR | VIOLATION_ISSUE_DT →DATE | VIOLATION_OCCUR_DT →DATE
SIG_SUB VARCHAR('Y'|'N') | SECTION_OF_ACT VARCHAR | PART_SECTION VARCHAR (CFR std)
CIT_ORD_SAFE VARCHAR('Citation'|'Order'|'Safeguard') | LIKELIHOOD VARCHAR
INJ_ILLNESS VARCHAR | NO_AFFECTED →INTEGER | NEGLIGENCE VARCHAR
PROPOSED_PENALTY →DOUBLE | AMOUNT_DUE →DOUBLE | AMOUNT_PAID →DOUBLE
FINAL_ORDER_ISSUE_DT →DATE | DOCKET_NO VARCHAR | CONTESTED_IND VARCHAR | CONTESTED_DT →DATE
```

### 6.5 `AssessedViolations` (58 cols) — keys + penalty subset
```
EVENT_NBR VARCHAR | VIOLATION_NO VARCHAR | MINE_ID VARCHAR | VIOLATOR_ID VARCHAR
VIOLATOR_NAME VARCHAR | VIOLATOR_TYPE_CD VARCHAR | COAL_METAL_IND VARCHAR
ASSESS_CASE_NO VARCHAR | PRIMARY_ACTION_CD VARCHAR | SIG_SUB_IND VARCHAR('Y'|'N')
CFR_STANDARD_CD VARCHAR | ASSESS_CASE_STATUS_CD VARCHAR | ASSESS_CASE_STATUS_DT →DATE
OCCURRENCE_DT →DATE | ISSUE_DT →DATE | FINAL_ORDER_DT →DATE
PROPOSED_PENALTY_AMT →DOUBLE | CURRENT_ASSESSMENT_AMT →DOUBLE
PAID_PROPOSED_PENALTY_AMT →DOUBLE | PENALTY_POINTS →INTEGER
GRAVITY_PERSONS_POINTS →INTEGER | GRAVITY_INJURY_POINTS →INTEGER
GRAVITY_LIKELIHOOD_POINTS →INTEGER | NEGLIGENCE_POINTS →INTEGER
VIOLATOR_VIOLATION_CNT →INTEGER | VIOLATOR_REPEATED_VIOL_CNT →INTEGER
VIOLATOR_MINE_HRS →DOUBLE | SIZE_OF_MINE VARCHAR | SIZE_OF_CONTROLLING_ENTITY VARCHAR
DELINQUENT_DT →DATE | HISTORY_START_DT/END_DT →DATE
```

The remaining 15 files parse with the identical recipe; their full headers are enumerated
in §1 / the recon log and follow the same VARCHAR-in, `TRY_CAST`-out convention.

---

## 7. Materialization implications (for the forthcoming ingest design — not built here)

- **Spine grain.** `MINE_ID` is the physical-asset BTREE anchor (universal, 16 files).
  Build a derived **operator/controller entity table** off
  `ControllerOperatorHistory` + `Mines` (current) carrying
  `normalized_legal_name = core.name_norm(name)` + `legal_name_base` + state + ZIP — that
  is the bridgeable grain, **not** the raw violation rows.
- **Two giants** (`Violations` 1.43 GB, `AssessedViolations` 1.32 GB, `CoalDustSamples`
  1.01 GB uncompressed) dominate; the rest are < 350 MB. None approaches the ~100 M-row
  Volume-staging threshold (ARCHITECTURE "Giants" rule) — direct-R2 BTREE builds suffice.
- **Index targets:** `MINE_ID` (every table), `VIOLATOR_ID`/`CONTROLLER_ID`/`OPERATOR_ID`
  (identity + enforcement), `normalized_legal_name` (the cross-walk key), `EVENT_NO`,
  `VIOLATION_NO`, `ASSESS_CASE_NO`, and the event-date columns for trigger range-scans.
- **Encoding + quote handling are correctness-critical**, not cosmetic — they gate both
  row completeness (93 % loss if wrong) and PR-operator join quality.
```
