# FEC → SAM.gov Personnel-Bridge Reconnaissance

Read-only schema diagnostic mapping the join vectors to crosswalk **FEC itemized individual
contributors** (the donor's own `name`) against the **SAM.gov entity personnel** universe — the
Points of Contact embedded in every federal registrant, plus the narrow FFATA / File-E highly-
compensated-executive tail. The objective: bridge **individual → individual** (donor ↔ POC /
officer), not donor → org, on a deterministic **Name + State + ZIP5** key passed through
`core.name_norm`.

- **Live targets (`s3://data-sink/active/`):** `fec_individual_contributions` (left),
  `sam_pocs` · `sam_master_contacts` · `ffata_exec_comp` (right personnel layers),
  `sam_master_entities` / `sam_entity_master` (geo carriers for the geo-orphaned File-E tail) —
  `pylance 7.0.0` schema read + `count_rows()` + full-set DuckDB column scans for fill / distinct
  cardinality.
- **As-of:** probe **2026-06-05**. FEC SoR = 282,923,196 rows. `sam_pocs` = 8,065,116 ·
  `sam_master_contacts` = 4,373,319 · `ffata_exec_comp` = 29,601. All schemas live-confirmed this
  probe.
- **Attestation:** every figure is a live read. **Zero data-plane mutation** — no `.lance`
  writes, no DDL, no indexes, no `ops.*` rows. Probe deps in an ephemeral `uv` env
  (`/tmp/sam_probe_meta.py`, `/tmp/sam_probe_agg.py`); Doppler-injected R2 creds (`core-x/prd`);
  `core.name_norm` imported via `PYTHONPATH` so the blocking key is **byte-identical to the fleet**.
- **Companion:** [`FEC_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md`](FEC_MSHA_ENTITY_BRIDGE_DIAGNOSTIC.md)
  fully maps the FEC left side (geo shape, sentinels, ZIP+4 split). That treatment is reused here;
  this probe focuses the **right side** on SAM *people* (not org `employer`).

---

## 0. Headline verdict

| # | Finding | Verdict |
|---|---|---|
| 1 | **Three SAM personnel layers are already materialized.** `sam_pocs` (8.07M rows, the broad layer: v2 **and** legacy cage-keyed, with `name_key`+`last_name` BTREEs), `sam_master_contacts` (4.37M, v2-only family satellite, `uei` BTREE only), `ffata_exec_comp` (29,601, File-E). No build needed. | ✅ exists |
| 2 | **Directive column guesses are WRONG — schema is LONG, not wide.** `eb_poc_first_name` / `gov_bus_poc` **do not exist as columns.** Both POC datasets are unpivoted: a `poc_type` discriminator + generic `first_name`/`last_name`. "Electronic Business POC" = a **row filter** (`WHERE poc_type='electronic_business'`), not a column. The wide `govt_bus_poc_*` names live only in the frozen field-map dictionary. | ⚠️ corrected |
| 3 | **Names are PRE-SPLIT** (`first_name`/`middle_name`/`last_name`, discrete) — the **opposite** of FEC's single `LASTNAME, FIRSTNAME` string. Reshape happens on the **SAM side**: concat to `LAST, FIRST`, then `name_norm` both sides → identical `LAST FIRST` token key. Zero name-parsing required (operator zero-alteration policy honored — SAM already delivers parts). | ✅ clean |
| 4 | **POC name fill is near-total.** `sam_pocs`: `first_name` **100.0%**, `last_name` **99.96%**, `name_key` 100%. `middle_name` sparse (24.47%). **No truncation** — POC name fields cap at 65 chars (vs FEC's 38-char employer hard-cap); names are not sliced. | ✅ dense |
| 5 | **POCs carry their OWN geo — no entity bind needed (the false-positive shield is self-contained).** `sam_pocs` row carries `state` **98.72%** + `zip5` **99.06%** directly. **`name ∧ state ∧ zip5` co-present on 7,910,584 rows (98.08%).** The directive's "bind POC to parent `physical_address_*`" fallback is **unnecessary** for the POC layers. | ✅ denormalized |
| 6 | **`ffata_exec_comp` is the exception — geo-orphaned.** No address columns at all; `officer_name` is a **single opaque `FIRST [MID] LAST` string** (not split, not `LAST,FIRST`). Geo + name-order both require work: bind `recipient_uei → sam_master_entities.physical_address_*`, and match on a **token-set / sorted-token** key (order differs from FEC). Tiny but high-value: 6,887 entities, 20,721 distinct officer names. | ⚠️ supplemental |
| 7 | **name_norm collapse is negligible here (−0.15%)**, unlike org names (−25.8% on FEC employer). Human names are already clean discrete tokens: `sam_pocs` 2,122,700 distinct `first\|last` → 2,119,414 `name_norm(LAST FIRST)`. | ✅ |
| 8 | **Addressable universe (`sam_pocs`): 2,868,249 distinct `(name_norm, state, zip5)` blocking keys** over **2,065,908 distinct geo-resolved person-names**; +20,721 File-E officer names. **No fuzzy primitive exists** — strictly deterministic `core.name_norm` + geo (consistent with the whole fleet). | ✅ deterministic |

---

## 1. Target schemas (live)

### FEC — `fec_individual_contributions` (282,923,196 rows) · LEFT
Person fields: **`name`**\* (single `LASTNAME, FIRSTNAME [MID]` string), `city`, **`state`**†,
**`zip_code`**, `employer`\*, `occupation`, `entity_tp`† (`'IND'` = individuals), `cmte_id`\*,
`transaction_amt`(dec14,2), `transaction_dt`\*, `sub_id`\*, `cycle_year`†.
(`*`=BTREE, `†`=BITMAP — 14 indices total; `name` BTREE on the **raw** string, so a `name_norm`'d
join does not ride it.) Live name samples confirm the format: `BAILEY, C.E.` · `DELMAS, SHIRLEY ANN`
· `MONTGOMERY, G.V.` (comma-delimited, middle tokens trail the first name).

### SAM personnel — RIGHT (three live layers)

| Dataset | Rows | Grain | Name fields | Per-row geo | Indices |
|---|---|---|---|---|---|
| **`sam_pocs`** *(primary)* | **8,065,116** | 1 / (entity, populated POC slot) | `first_name` `middle_name` `last_name` `full_name` `name_key` | `state` `zip5` `zip4` `city` `country` `address_line_1/2` | BTREE `uei`,`cage_code`,**`name_key`**,**`last_name`**; BITMAP `poc_type`,`source_family` |
| `sam_master_contacts` | 4,373,319 | 1 / (uei, poc_type) | `first_name` `middle_initial` `last_name` | `state_or_province` `zip_postal_code` `zip_code_4` `city` | BTREE `uei` only |
| **`ffata_exec_comp`** *(File-E tail)* | 29,601 | 1 / (recipient_uei, officer_rank 1–5) | `officer_name` (opaque `FIRST [MID] LAST`), `name_key` | **none** | BTREE `recipient_uei`,`name_key`; BITMAP `officer_rank`,`source_channel` |

`poc_type` enum (`sam_pocs`): `government_business` / `electronic_business` (the two mandatory
slots, ≈2.71M each) + `*_alt`, `past_performance`, `past_performance_alt`. (`sam_master_contacts`
uses dict-faithful labels `govt_business` / `alt_*`.) **Recommended target = `sam_pocs`**: it is the
superset (adds 3.69M legacy cage-keyed POCs the v2-only satellite omits) **and** already carries the
`name_key`+`last_name` BTREEs the reverse-name join needs; `sam_master_contacts` would force a full
scan per name probe.

### Geo carriers (only for the File-E bind, finding #6)
- `sam_master_entities` (1,541,566) — `physical_address_province_or_state`,
  `physical_address_zip_postal_code` (+`_zip_code_4`), keyed `uei` (BTREE).
- `sam_entity_master` (782,543, thin v3) — `physical_state`, `physical_zip5`, keyed `uei`.

---

## 2. The precise projection — name reshape (the headline deliverable)

`core.name_norm` is THE canonical fleet key (`UPPER → &→AND → dash→space → strip [^A-Z0-9 ] →
collapse ws`). Applied to a `LAST FIRST` token sequence on **both** sides, FEC's comma and SAM's
split both resolve to the identical blocking key.

```sql
-- ════ SAM RIGHT (sam_pocs) · pre-split parts → 'LAST, FIRST' → canonical key ════
SELECT
    p.uei, p.cage_code, p.poc_type,                       -- provenance / slot
    p.first_name, p.middle_name, p.last_name,
    -- human-readable reshape to FEC's stored shape (presentation only):
    concat(upper(trim(p.last_name)), ', ', upper(trim(p.first_name))) AS name_lastfirst,
    -- THE join key — core.name_norm over LAST FIRST [MID]; byte-identical to the FEC key below.
    -- (worker: `from core.name_norm import name_norm`; this is name_norm("concat_ws(' ', last_name, first_name, middle_name)") )
    nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(CAST(concat_ws(' ', p.last_name, p.first_name, p.middle_name) AS VARCHAR)),
        '&',' AND ','g'),'[-\x{2013}\x{2014}]+',' ','g'),
        '[^A-Z0-9 ]+','','g'),'\s+',' ','g')),'')          AS person_key,
    upper(p.state)        AS state2,                       -- USPS 2-letter (filter non-US below)
    left(p.zip5, 5)       AS zip5
FROM sam_pocs p
WHERE p.last_name IS NOT NULL
  AND p.state IS NOT NULL AND p.zip5 IS NOT NULL
  AND p.country = 'USA';                                  -- drop foreign POCs (e.g. state='ROMA')

-- ════ FEC LEFT · split single 'LAST, FIRST [MID]' string → SAME key ════
WITH fec AS (
  SELECT sub_id, name, state, zip_code,
         trim(split_part(name, ',', 1))                       AS last_raw,
         trim(regexp_replace(name, '^[^,]*,', ''))            AS rest_raw   -- first [+ middle]
  FROM fec_individual_contributions
  WHERE entity_tp = 'IND' AND name IS NOT NULL
    AND state IS NOT NULL AND zip_code IS NOT NULL
)
SELECT sub_id,
       name_norm("concat_ws(' ', last_raw, rest_raw)")        AS person_key,  -- LAST FIRST [MID]
       upper(state) AS state2, left(zip_code, 5) AS zip5
FROM fec;
```

**Matching-key controls (deterministic, no fuzzy macro exists in the fleet):**
- **Primary** — exact `person_key` (`LAST FIRST [MID]`) ∧ `state2` ∧ `zip5`.
- **Initial-vs-full asymmetry** — FEC frequently stores initials (`BAILEY, C.E.`) where SAM has the
  full given name. A `(last_norm, left(first_norm,1), state2, zip5)` **surname+first-initial** key is
  the recall fallback; promote to full-first only where both sides carry it.
- **Middle token** — `middle_name` fills only 24% on SAM and is inconsistent on FEC; block on
  `LAST FIRST`, treat the middle token as a confirmatory tiebreaker, never a required equality.

---

## 3. Join path — geo traversal

```sql
-- Path A · sam_pocs → geo is ON THE ROW (zero join — the false-positive shield is self-contained)
--   state2 = upper(state), zip5 = left(zip5,5) come straight off the POC row (§2). Bind directly:
SELECT s.sub_id, s.person_key, p.uei, p.cage_code, p.poc_type,
       s.transaction_amt, s.transaction_dt
FROM   fec_left s                                            -- §2 FEC projection
JOIN   sam_pocs_left p                                       -- §2 SAM projection
  ON   p.person_key = s.person_key
 AND   p.state2     = s.state2                               -- strong discriminant (corp HQ state)
 AND   p.zip5       = s.zip5;                                -- confirmatory (see caveat §4)

-- Path B · ffata_exec_comp → GEO-ORPHANED; must bind to the entity for State/ZIP
SELECT f.recipient_uei, f.officer_name, f.officer_rank, f.officer_amount,
       e.physical_address_province_or_state                 AS state2,
       left(e.physical_address_zip_postal_code, 5)          AS zip5
FROM   ffata_exec_comp f
JOIN   sam_master_entities e ON e.uei = f.recipient_uei      -- uei BTREE both sides
WHERE  name_norm("f.officer_name") <> 'NA';                 -- drop the 'NA' sentinel
-- officer_name is FIRST [MID] LAST (not LAST,FIRST) → join FEC on a SORTED-TOKEN set key:
--   list_aggregate(list_sort(string_split(person_key,' ')),'string_agg',' ')  on BOTH sides.
```

---

## 4. Matching topology & data anomalies

| Axis | FEC (left) | SAM POC (right) | FFATA File-E (right) | Bridge action |
|---|---|---|---|---|
| Name storage | single `LAST, FIRST [MID]` | **split** first/mid/last | single `FIRST [MID] LAST` | `name_norm`; POC reshape→`LAST FIRST`; FFATA→sorted-token |
| Name fill | `name` ~99.98% | first 100% / last 99.96% | officer 100% | — |
| Truncation | `employer` 38c (n/a here) | **none** (65c cap) | none | no prefix-pass needed |
| State | `state` USPS 2-letter | `state` 98.72% | via entity bind | exact `state2` |
| ZIP | `zip_code` 45% z5 / 55% z9 | `zip5` 99.06% (native 5) | via entity bind | `left(zip_code,5)` ↔ `zip5` |
| Geo locus | **donor home** addr | **POC corp** addr | entity HQ addr | see ZIP caveat |
| Match engine | — | — | — | **deterministic only — no fuzzy macro** |

- **ZIP locus mismatch (the real precision risk).** FEC ZIP is the donor's **home/personal**
  address; the SAM POC ZIP is the **corporate** address block. For an executive who is both a donor
  and a registrant POC, `state2` usually agrees (HQ state ≈ home state) but `zip5` often will not.
  Treat **State as the primary discriminant, ZIP5 as a confirmatory boost** — do not hard-require
  ZIP5 equality or recall collapses. (Contrast the FEC→MSHA org bridge, where both sides are the
  same corporate address and ZIP5 is a hard gate.)
- **Foreign POCs.** `sam_pocs` carries non-US registrants (e.g. `state='ROMA'`, null `zip5`); they
  never match US FEC donors. Filter `country='USA'` pre-block (§2).
- **FFATA sentinels.** `officer_name` includes junk (`'NA'`) and double-spaced names
  (`DARIN  CABRAL`) — `name_norm` collapses the whitespace; the `NA` row is dropped explicitly.
- **`uei` vs `cage_code` keying on `sam_pocs`.** `uei` fills 54.22% (v2 only); `cage_code` 95.51%
  (legacy tail carries CAGE, no UEI). Carry **both** as the entity provenance key.

---

## 5. Appendix — live evidence (probe 2026-06-05)

- **`sam_pocs` fill (full scan, 8,065,116 rows):** first_name 100.0% · last_name 99.96% ·
  name_key 100% · middle_name 24.47% · city 100.0% · **state 98.72%** · **zip5 99.06%** · zip4 31.61%
  · uei 54.22% · cage_code 95.51%. **name ∧ state ∧ zip5 = 7,910,584 (98.08%).**
- **`sam_pocs` poc_type (rows / distinct name_key):** government_business 2,708,531 / 1,720,674 ·
  electronic_business 2,708,523 / 1,728,162 · government_business_alt 814,641 · electronic_business_alt
  808,543 · past_performance 599,271 · past_performance_alt 425,607. source_family: v2 4,372,870
  (1,540,966 uei) · legacy_v1 3,692,246 (0 uei, 1,167,568 cage).
- **`sam_pocs` distinct-person universe:** name_key(`FIRST MID LAST`) 2,430,172 · last_name 446,899 ·
  `first\|last` 2,122,700 · **`name_norm(LAST FIRST)` 2,119,414** (−0.15% collapse). Geo-eligible
  distinct persons **2,065,908**; **distinct `(name_norm,state,zip5)` = 2,868,249 (addressable).**
- **`sam_pocs` reshape samples:** `MINTZ`/`MARCUS`→`MINTZ, MARCUS`→`MINTZ MARCUS` (SC|29118) ·
  `MCCOY`/`SHAWN`→`MCCOY SHAWN` (VA|22201) · `SCHNUGG`/`CAROL`→`SCHNUGG CAROL` (CA|92122).
- **`sam_master_contacts` (4,373,319):** first 100% · last 100.0% · state_or_province 98.52% ·
  zip_postal_code 98.97% · distinct uei 1,540,966 · distinct `name_norm(LAST FIRST)` 1,528,779.
- **`ffata_exec_comp` (29,601):** distinct recipient_uei 6,887 · distinct name_key 20,721 ·
  officer_name 100% · ranks 1–5 = 6,887/6,239/5,713/5,442/5,320. Samples: `NAOMI L TINSLEY` ·
  `TIMOTHY B WARD` · `DARIN  CABRAL` · `'NA'` (sentinel).
- **FEC left name format (entity_tp='IND'):** `GAGNE, ROBERT` (MS|39525) · `ALFORD, RALPH M.`
  (VA|22031) · `LITTIG, M. JAMES` (VA|22192) — confirms single-string `LAST, FIRST [MID]`.
- **Harness:** `/tmp/sam_probe_meta.py` (active-prefix listing + pylance schema/count/indices) +
  `/tmp/sam_probe_agg.py` (pushdown fills + full-set distinct + canonical `core.name_norm` import +
  bounded FEC sample). pylance 7.0.0 / duckdb 1.5.x / pyarrow ≥17; ephemeral `uv` env; Doppler
  `core-x/prd`. **Zero mutation.**
