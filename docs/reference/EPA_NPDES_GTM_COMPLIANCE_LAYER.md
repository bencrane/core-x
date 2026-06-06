# EPA NPDES — GTM Compliance Layer

The query/consumption layer over the 422 M-row NPDES discharge trove. Turns raw discharge events
into an **entity-resolvable, index-fast compliance graph** so a GTM agent (or any downstream
consumer) answers *"active high-violation dischargers in TX, resolved to a company"* by an indexed
lookup (~100 ms) instead of an 18 s full scan.

Built by `pipelines/ingest_epa/materialize_epa_gtm.py` (datasets) and
`pipelines/ingest_epa/materialize_epa_history.py` (the raw-DMR indices). All datasets live under
`s3://data-sink/active/`, Lance `data_storage_version="2.1"`. Verified live **2026-06-06**.

---

## 0. The four datasets

| Dataset | Grain | Rows | What it is |
|---|---|--:|---|
| `epa_npdes_dmrs` | one DMR form-value | **422,447,436** | the raw discharge-event trove (FY1982–FY2026) |
| `epa_permits` | permit-version | **1,686,705** | **the bridge**: `EXTERNAL_PERMIT_NMBR → REGISTRY_ID + PERMIT_NAME + geo` |
| `epa_permit_compliance` | one **reporting permit** | **156,014** | per-permit compliance resume (DMR rolled up + entity attached) |
| `epa_permit_parameter_compliance` | permit × **parameter** | **1,884,617** | per-pollutant resume — pollutant-specific targeting |
| `epa_entity_compliance` | one **entity** (`REGISTRY_ID`) | **142,933** | per-company NPDES footprint (the GTM-actionable grain) |

Only **156,014** of the 1.69 M master permits ever submitted a DMR — that reporting set (resolving
to **142,933 distinct entities**) is the GTM-relevant universe, and it is **100.0% entity-resolved**
(156,004 / 156,014 permits carry a `REGISTRY_ID`).

---

## 1. The resolution chain (why this layer exists)

`epa_npdes_dmrs` is event data — it carries **no name, domain, REGISTRY_ID, or UEI**; its only
handle is `EXTERNAL_PERMIT_NMBR`. The chain that makes a discharge event a targetable company:

```
epa_npdes_dmrs.EXTERNAL_PERMIT_NMBR
   └─(epa_permits)→ REGISTRY_ID + PERMIT_NAME + normalized_legal_name + FAC_STATE/lat-long
        └─(name_norm)→ companies / people  ·  epa_to_sos_bridge (REGISTRY_ID↔SoS)  ·  epa_facilities
```

`normalized_legal_name` on `epa_permits` / `epa_permit_compliance` / `epa_entity_compliance` is the
**byte-identical `core.name_norm` blocking key** every resolution spine uses, so these tables join
to `companies`, `people`, `sos_normalized_master`, PPP, etc. with no drift.

---

## 2. Indices & query performance (measured, warm, over-WAN — faster in-region)

The raw DMR table's indices push down through DuckDB to sub-second; the compliance summaries make
the GTM analytics instant. Same analytic, two worlds:

| Query | On raw `epa_npdes_dmrs` | On the compliance layer |
|---|--:|--:|
| "active high-violation dischargers in TX" | ~18 s (scan 422 M + permits join) | **105 ms** (`epa_permit_compliance`) |
| "company's full NPDES compliance profile" | not possible (no entity key) | **96 ms** (`epa_entity_compliance`, BTREE `REGISTRY_ID`) |
| "compliance profile by company name" | not possible (no name) | **88 ms** (BTREE `normalized_legal_name`) |
| "top 20 worst entities nationally" | ~18 s | **198 ms** |

### Index inventory

- **`epa_npdes_dmrs`** — BTREE `EXTERNAL_PERMIT_NMBR`, `MONITORING_PERIOD_END_DATE`, `PARAMETER_CODE`;
  BITMAP `FISCAL_YEAR`, `VIOLATION_CODE`, `NODI_CODE`. (Permit/time retrieval + violation/parameter
  drill-down all push down through DuckDB.)
- **`epa_permits`** — BTREE `REGISTRY_ID`, `EXTERNAL_PERMIT_NMBR`, `NPDES_ID`, `normalized_legal_name`;
  BITMAP `FAC_STATE_CODE`, `PERMIT_STATUS_CODE`, `MAJOR_MINOR_STATUS_FLAG`, `PERMIT_TYPE_CODE`.
- **`epa_permit_compliance`** — BTREE `EXTERNAL_PERMIT_NMBR`, `REGISTRY_ID`, `normalized_legal_name`;
  BITMAP `FAC_STATE`, `PERMIT_STATUS_CODE`, `has_violations`, `is_active`, `violation_tier`, `entity_resolved`.
- **`epa_permit_parameter_compliance`** — BTREE `EXTERNAL_PERMIT_NMBR`, `REGISTRY_ID`, `normalized_legal_name`;
  BITMAP `PARAMETER_CODE`, `FAC_STATE`, `has_violations`, `has_exceedances`, `is_active`. (Pollutant ∧ geo ∧
  exceedance bitmap-AND.)
- **`epa_entity_compliance`** — BTREE `REGISTRY_ID`, `normalized_legal_name`;
  BITMAP `FAC_STATE`, `has_violations`, `is_active`, `violation_tier`.

The BITMAP set is chosen for **bitmap-AND composition** — `FAC_STATE='TX' ∧ violation_tier='high' ∧
is_active` intersects three bitmaps with no scan.

---

## 3. Schemas

### `epa_permit_compliance` (per reporting permit) — the workhorse

| Column | Role |
|---|---|
| `EXTERNAL_PERMIT_NMBR` | NPDES permit id — join key to `epa_npdes_dmrs` · BTREE |
| `REGISTRY_ID` | universal entity hub (from the bridge) · BTREE |
| `PERMIT_NAME`, `normalized_legal_name` | permittee legal name + the join-to-companies key · BTREE(norm) |
| `FAC_STATE`, `FAC_CITY`, `FAC_ZIP`, `FAC_COUNTY_CODE`, `FAC_LATITUDE`, `FAC_LONGITUDE` | facility geo |
| `PERMIT_TYPE_CODE`, `PERMIT_STATUS_CODE`, `MAJOR_MINOR_STATUS_FLAG`, `FACILITY_TYPE_INDICATOR` | permit meta |
| `n_dmr_rows`, `n_measured`, `n_distinct_parameters` | reporting volume (distincts are HLL-approx) |
| `first_period`, `last_period`, `first_fy`, `last_fy` | activity span |
| `n_violations`, `n_distinct_violation_codes`, `last_violation_period` | violation history |
| `n_exceedances`, `max_exceedence_pct`, `n_nodi` | exceedance / no-data signal |
| `has_violations`, `is_active` (last_fy ≥ 2024), `entity_resolved`, `violation_tier` (none/low/medium/high) | GTM flags · BITMAP |

### `epa_entity_compliance` (per `REGISTRY_ID`) — the targeting grain

`REGISTRY_ID`, `entity_name`, `normalized_legal_name`, `FAC_STATE/CITY/ZIP/LAT/LONG`, `n_permits`,
`n_dmr_rows`, `n_violations`, `n_exceedances`, `max_exceedence_pct`, `first/last_period`,
`first/last_fy`, `last_violation_period`, `has_violations`, `is_active`, `violation_tier`.

### `epa_permits` (per permit-version) — the bridge

`REGISTRY_ID`, `PERMIT_NAME`, `normalized_legal_name`, `EXTERNAL_PERMIT_NMBR`, `NPDES_ID`,
`VERSION_NMBR`, `ACTIVITY_ID`, permit meta + lifecycle dates + design/actual flow, and the
facility address/geo (`FAC_*`).

---

## 4. Query cookbook (DuckDB over Lance — the fundamental path)

```sql
-- A) Targeting: active, high-severity dischargers in a state, with name + geo + contact-ready key
SELECT EXTERNAL_PERMIT_NMBR, REGISTRY_ID, PERMIT_NAME, normalized_legal_name,
       FAC_CITY, FAC_LATITUDE, FAC_LONGITUDE, n_violations, max_exceedence_pct, last_violation_period
FROM epa_permit_compliance
WHERE FAC_STATE = 'TX' AND violation_tier = 'high' AND is_active
ORDER BY max_exceedence_pct DESC NULLS LAST;

-- B) Company compliance profile (point lookup; resolve a name first via name_norm or the bridge)
SELECT * FROM epa_entity_compliance WHERE normalized_legal_name = 'ACME MANUFACTURING';

-- C) Join EPA non-compliance to the GTM company graph (the payoff)
SELECT c.company_id, c.company_name, e.n_violations, e.violation_tier, e.FAC_STATE
FROM epa_entity_compliance e
JOIN companies c ON c.normalized_legal_name = e.normalized_legal_name
WHERE e.is_active AND e.violation_tier IN ('high','medium');

-- D) Drill from a flagged permit into the raw events (raw DMR indices push the filters down)
SELECT MONITORING_PERIOD_END_DATE, PARAMETER_CODE, DMR_VALUE_NMBR, VIOLATION_CODE, EXCEEDENCE_PCT
FROM epa_npdes_dmrs
WHERE EXTERNAL_PERMIT_NMBR = 'TX0123456' AND VIOLATION_CODE IS NOT NULL
ORDER BY MONITORING_PERIOD_END_DATE DESC;

-- E) Pollutant-specific targeting: active dischargers of a parameter, over limit, in a state
--    (PARAMETER_CODE ∧ FAC_STATE ∧ has_exceedances ∧ is_active — pure bitmap-AND, ~150 ms)
SELECT PERMIT_NAME, REGISTRY_ID, normalized_legal_name, FAC_STATE, n_violations, max_exceedence_pct
FROM epa_permit_parameter_compliance
WHERE PARAMETER_CODE = '00556'   -- Oil & Grease (00400 pH, 00530 TSS, 00610 Ammonia, 01092 Zinc, …)
  AND FAC_STATE = 'TX' AND has_exceedances AND is_active
ORDER BY max_exceedence_pct DESC NULLS LAST;
```

All four datasets are committed under `active/` and are **auto-discovered** by any consumer that
lists the sink (e.g. the gtm-mcp dynamic registry) — they are immediately namable in raw-SQL with no
registration step. A typed point-lookup tool (the guaranteed pure-Lance sub-100 ms path) can be added
per consumer if/when desired; it is not required for access.

---

## 5. Rebuild / refresh

```
modal run pipelines/ingest_epa/materialize_epa_gtm.py::all       # permits → permit_compliance → entity_compliance
modal run pipelines/ingest_epa/materialize_epa_history.py::reindex   # raw DMR indices (R2-safe local round-trip)
```

`epa_permits` depends on `npdes_downloads.zip` (landing); the summaries depend on `epa_npdes_dmrs`
+ `epa_permits`. Each build is overwrite (idempotent), self-verifying, and records a terminal
`ops.epa_ingest_runs` row (`feed='epa_gtm_layer'`). Net-new datasets index direct-to-R2 (safe at
~1–1.7 M rows); the 422 M-row DMR table indexes via the R2-safe local round-trip.
