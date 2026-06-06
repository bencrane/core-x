# SAM Entity Daily Delta — Ingest + Reconciliation Build Plan

**Status:** Proposed (not started) · **Authored:** 2026-06-06 · **Owner repo:** `core-x`
**One-liner:** Add a daily SAM.gov entity-registration delta feed (its own append-only Lance table) and reconcile it into the existing `sam_master_entities` mirror — a two-track ingest (monthly bulk + daily delta) mirroring the USASpending pattern.

> This document is self-contained. An executing agent should be able to run it end-to-end without prior conversation context. Where a fact is **VERIFIED** it is marked; where it must be confirmed before building it is in **§Phase 0 (confirm-first)**. Do not skip Phase 0 — several "obvious" assumptions were already falsified during scoping (see §3).

---

## 1. Business driver & goal

**Driver:** A key client segment is **surety bond companies**. The product promise is to reach **newly-registered, relevant federal entities the week they register — preceding their first contract win.** That is a *recency* signal on new SAM registrants.

**Gap today:** The SAM entity master is rebuilt **only from the monthly bulk extract**, so the new-registrant signal is stale by up to ~30 days — exactly the window where it has value.

**Goal:**
1. Stand up a **daily SAM entity delta ingest** landing to **its own Lance table** (append-only).
2. **Phase-1 deliverable (revenue-capable on its own):** a query/feed over that daily table filtered to *new + relevant* registrants → surety GTM.
3. **Phase-2:** reconcile the daily delta into `sam_master_entities` (and derived `sam_normalized_entities`, `sam_master_contacts`) so the "all companies" mirror is fresh too.

Non-goal: replacing the monthly bulk. Monthly stays as the full re-baseline.

---

## 2. Current state (what already exists)

- **Raw monthly SoR:** `s3://data-sink/active/entity_registrations/` (Lance, append-only, ~19.3M rows across snapshots). Latest snapshot label **`20260503`** (May 3 2026 monthly). **VERIFIED** via `ops.sam_master_runs`.
- **Ingest (monthly, manual-drop):** `pipelines/sam_gov/entity_registrations_bulk.py` — Modal app `sam-gov-entity-pipelines`. **Does NOT download from SAM**; it parses ZIP extracts already dropped into R2 landing under `landing/entity_registrations_raw_public-historical/` (`SAM_PUBLIC_MONTHLY_*_MODIFIED`) and `landing/entity_registrations_raw_public-v2/` (`SAM_PUBLIC_UTF-8_MONTHLY_V2_*`). It is a **bounded backfill, not a feed** (no cron). **VERIFIED.**
- **Mirror (master):** `pipelines/sam_gov/sam_master.py` → `build_sam_master` (Modal app `sam-gov-master-pipelines`). Produces 3 Lance datasets under `s3://data-sink/active/`:
  - `sam_master_entities/` — **1 row per UEI** (all statuses; `is_active` is a *column*, not a filter), latest-row-per-UEI across all snapshots. ~1,541,566 rows. BTREE on `uei, primary_naics, cage_code`.
  - `sam_master_contacts/` — POCs unpivoted (~4,373,319). BTREE `uei`.
  - `sam_master_domains/` — website→domain index (~709,546). BTREE `normalized_domain, uei`.
  - Write mode = **`overwrite`** (full rebuild). Uniqueness **hard-gated**: `distinct_uei == entities_rows` aborts the write on any dup. Rollback guard restores prior versions on failure.
- **Derived:** `sam_normalized_entities.py` (normalized names, 1:1 with entities), `sam_pocs.py` (human-contact layer; derived from the monthly raw).
- **Orchestration plumbing:** `core/modal_dispatcher.py` (universal dispatcher; Trigger POSTs `{app_name, function_name, kwargs, trigger_callback_url}`, dispatcher `fn.spawn(...)`s, worker POSTs terminal state back to the waitpoint). Ops ledgers live in HQX Postgres as `ops.<feed>_runs`.
- **Auto-refresh status:** the master's daily auto-refresh orchestrator `src/trigger/sam_spine_refresh.ts` is **CRON-DISABLED** (PR #233 — removed the declarative cron; runtime schedule deactivated `active=false`; **merged to `main` but NOT yet deployed** to Trigger, so the deployed bundle still carries the cron, held inert only by the runtime override). This plan's daily delta is the intended replacement freshness source; the master refresh should be **event-driven (ledger-gated), not a daily 128GB cold-start.**
- **Template to clone:** the USASpending two-track already exists — `usaspending-daily-delta` Trigger schedule + `ops.usaspending_award_search_api_landing_runs` (daily API landing) alongside the bulk. Mirror its structure.

---

## 3. VERIFIED facts about the SAM data surface (with sources)

> Sources: [GSA Open — SAM Entity Extracts API](https://open.gsa.gov/api/sam-entity-extracts-api/), [SAM.gov Data Services](https://sam.gov/data-services), [data.gov catalog entry](https://catalog.data.gov/dataset/system-for-award-management-sam-public-extract-entity-registration). Verified live 2026-06-06.

1. **Daily entity extracts exist.** Processed **Tuesday–Saturday** (after 7 AM ET). **Incremental** — "new/updated/deactivated/expired entities since the previous day's file." New registrations ARE included. **VERIFIED.**
2. **Monthly extract** = full snapshot of **all active + entities expired in the last 6 months**, processed **first Sunday** of each month. (So the upstream feed itself prunes entities expired >6 months; our master only retains the long tail because the raw SoR accumulates historical snapshots.) **VERIFIED.**
3. **Format:** ZIP'd **positional pipe-delimited** files (NOT CSV). Public variant naming `SAM_PUBLIC_UTF-8_..._V2_YYYYMMDD.ZIP`; daily sibling expected `SAM_PUBLIC_..._DAILY_V2_YYYYMMDD.ZIP`. Same family as the monthly → **the existing `entity_registrations_bulk.py` width-based parser should parse it** (confirm exact public daily filename in Phase 0).
4. **Access channels — verified live:**
   | Channel | Status | Limit |
   |---|---|---|
   | Open no-key download `www.sam.gov/SAM/extractfiledownload?...` | **DEAD** — legacy portal, 301→302→`error.jsf`. data.gov links are **stale**. | — |
   | `https://api.sam.gov/data-services/v1/extracts` (key-gated) | **LIVE** | Our key role `SI-NONFED` = **10 requests/day**. Roles: non-fed+role = 1,000/day; fed-system = 10,000/day. |
   | SFTP (GSA: "SAM hosts the extract files on our SFTP server") | Exists; creds/setup **unknown** | Likely *not* under the API's 10/day cap (separate channel) — **must verify**. |
   | Per-entity API `entity-information/v3/entities` | Live; also rate-limited | NOT the bulk-delta path. Ignore for this build. |
5. **API request shape (VERIFIED):** `GET https://api.sam.gov/data-services/v1/extracts?api_key=<KEY>&fileType=ENTITY&sensitivity=PUBLIC&frequency=DAILY&date=MM/DD/YYYY` (monthly uses `frequency=MONTHLY&date=MM/YYYY`). **No list/enumerate endpoint** exists, and **no daily retention window is documented** — you cannot discover which dailies are available except by requesting specific dates (each request costs quota).
6. **Our key is currently rate-limited out** (429, quota resets at next UTC midnight). Confirmed role = `SI-NONFED`.
7. **Date math (as of 2026-06-06, Sat):** last ingested monthly = `20260503`. Next monthly drops **2026-06-07** (first Sunday). Gap = ~34 days ≈ **~24 daily files** (Tue–Sat only).

**Secrets (Doppler `core-x/prd`):** `SAM_API_KEY`, `R2_ENDPOINT`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, `HQX_DB_URL_POOLED`, `MODAL_DISPATCHER_URL`/`MODAL_KEY`/`MODAL_SECRET`. Run workers/scripts under `doppler run -p core-x -c prd -- ...`.

---

## 4. Architecture decisions (locked during scoping)

1. **Two-track, physically separate tables.** Daily delta lands in its **own** append-only Lance table; the monthly bulk SoR (`entity_registrations`) is never mutated by the daily path. (Blast-radius containment: a bad daily pull can only dirty the daily table.)
2. **Daily table = raw SoR shape**, parallel to `entity_registrations` (`uei, extract_label, source_file, pipe_fields, format_family` + a `pull_date`). Append-only. Rationale: faithful SoR + reuse of the existing parser **and** `build_sql` projection. (Alternative — store pre-projected — is acceptable but duplicates projection logic; prefer raw.)
3. **The master is the reconciled current-truth = latest-row-per-UEI over (monthly ∪ daily).** Two materialization paths, both consistent (latest-`last_update_date` wins):
   - **Periodic full re-baseline:** `build_sam_master` overwrite-rebuild, reading **both** raw sources (union). Deterministic, drift-free. Run on each monthly + as a backstop.
   - **Daily fast path:** `merge_insert` (upsert on `uei`) of the day's *projected* delta into `sam_master_entities`. Cheap; no full re-read.
4. **Reconciliation mechanism = `merge_insert`, NOT raw append** (raw append would duplicate UEIs and fail the uniqueness gate). Daily table append-only; the *fold into the master* is the upsert.
5. **Cadence vs. window are distinct:** run the job **daily** (frequency), each run pulling a **rolling ~4-day lookback window** (absorbs SAM's 1–2 day publish lag + the Sun/Mon no-daily gap), dedup on `(uei, last_update_date)` keeping latest. Do **not** run "every 4 days."
6. **Phase 1 ships surety value from the daily table alone** (filter new+relevant); reconciliation into the master is Phase 2.
7. **Provenance:** add a `source` (`bulk`|`daily`) and `valid_as_of` column to `sam_master_entities` so merged rows are auditable and a re-baseline can supersede correctly.

---

## 5. The binding constraint & unblock levers (decide before backfill)

**The 10 requests/day API cap is the only real blocker.** Everything else (format, parser, merge) is solved.

- **Forward daily feed (1 file/day):** fits the 10/day cap even on the current key. **No blocker.**
- **Backfill (~24 files):** infeasible at 10/day. Requires one of:
  - **(A) SAM key role upgrade → 1,000/day** (add a role to the SAM.gov account). Unblocks API backfill + gives headroom for retries/weekend catch-up.
  - **(B) SFTP channel** — file-based, likely not under the API cap. Needs SAM SFTP credentials + setup.
- **Gap-close without backfill:** ingest the **2026-06-07 monthly** (full snapshot = 1 API/SFTP call) to close the May→June gap deterministically; start the daily feed forward from there. Accept loss of *day-level* registration grain for the gap (acceptable for a go-forward motion). **This is the recommended default unless day-grain gap history is explicitly required.**

---

## 6. Phased execution plan

### Phase 0 — Confirm-first (cheap, do before building)
- [ ] After UTC-midnight quota reset, make **one** API call: `fileType=ENTITY&sensitivity=PUBLIC&frequency=DAILY&date=<recent Tue–Sat>`. Record: HTTP status, **exact public daily filename**, file size, and that it is a ZIP of positional pipe-delimited data.
- [ ] Confirm the daily file's internal **width/layout matches the monthly v2 (142-field)** so `entity_registrations_bulk.py`'s width-classifier + `build_sql` apply unchanged. If layout differs, note deltas.
- [ ] Decide **SFTP vs role-upgrade** for backfill (or "monthly-only gap-close"). Check whether SAM SFTP credentials exist/are obtainable. Record decision.
- [ ] Decide **gap-close strategy**: June 7 monthly (default) vs daily backfill.
- [ ] (Optional, costs 1 quota call) probe an older date (e.g. 21 days back) to empirically test daily **retention**.
- [ ] Define **"new"** (trigger on `registrationDate` vs `activationDate`) and **"relevant"** (the NAICS/PSC set surety underwrites). These define the Phase-1 feed and must come from the operator.

### Phase 1 — Daily delta ingest → own table (ships surety value)
- [ ] New worker `pipelines/sam_gov/sam_entity_daily.py` (Modal app — either extend `sam-gov-entity-pipelines` or a new `sam-gov-entity-daily-pipelines`). Pattern after `entity_registrations_bulk.py` + the USASpending daily-delta worker.
  - **Acquire:** for each date in the rolling window (default last 4 days, Tue–Sat), GET the daily ZIP via `data-services/v1/extracts` (or SFTP if chosen). Stream to `/tmp`. Respect the rate limit (≤ window-size calls/run).
  - **Parse:** reuse the `entity_registrations_bulk.py` transform — unzip → transcode (UTF-8/cp1252) → DuckDB `read_csv` whole-line on `\x1f` → split on `|` → emit `uei, extract_label, source_file, pipe_fields, format_family`.
  - **Land:** **append** to `s3://data-sink/active/sam_entity_daily/` (Lance, `mode="append"`), tagging each row `source='daily'`, `pull_date`, `extract_label` (= the daily file date). Dedup the rolling window on `(uei, last_update_date)` before append (or rely on the master merge to resolve — but keep the table clean).
  - **Ledger:** write `ops.sam_entity_daily_runs` (mirror the `ops.sam_master_runs` shape: feed, dataset_uri, label, rows, status, error, timestamps).
- [ ] Trigger task `src/trigger/sam_entity_daily.ts`: scheduled task, **daily cron**, dispatched through the universal dispatcher with a waitpoint callback. Schedule for Tue–Sat firing (or fire daily and no-op when no new daily file). Clone `sam_opps_bulk.ts` / the USASpending delta task for structure.
- [ ] **Surety feed (the Phase-1 product):** a projected view/materialization over `sam_entity_daily` — apply `build_sql` projection, filter to **new registrations** (per Phase-0 "new" definition, within window) **∩ relevant NAICS/PSC**. Expose for GTM consumption. **Revenue-capable here, before any master reconciliation.**

### Phase 2 — Reconcile into the master mirror
- [ ] Add a daily-merge entrypoint (in `sam_master.py` or a sibling) that:
  - Projects the day's `sam_entity_daily` rows to the `sam_master_entities` schema via `build_sql`.
  - `merge_insert` into `sam_master_entities`:
    ```python
    (lance.dataset(entities_uri, storage_options=so)
        .merge_insert(on="uei")
        .when_matched_update_all(condition="source.last_update_date >= target.last_update_date")
        .when_not_matched_insert_all()
        .execute(projected_daily_arrow))   # verify source/target qualifier syntax for installed pylance
    ```
    Stamp `source='daily'`, `valid_as_of`. Preserve the uniqueness invariant (1 row/UEI).
  - Re-derive affected UEIs into `sam_normalized_entities` and `sam_master_contacts` (merge_insert into those too, keyed on uei).
- [ ] **Monthly re-baseline coupling:** when the monthly bulk lands, run the full `build_sam_master` overwrite reading **(monthly ∪ daily)**, then a **catch-up merge** of daily deltas dated after the monthly's cut — so a re-baseline never transiently loses recent daily freshness. Both sources are retained, so the master is always reconstructable.
- [ ] **Maintenance cadence (merge path only):** schedule periodic `dataset.optimize.compact_files()` + index re-optimize (e.g. weekly) on `sam_master_entities` to clear merge tombstones/fragmentation and keep BTREE quality. (The monthly overwrite resets this for free.)
- [ ] **Master refresh trigger:** make the re-baseline **event-driven** off the ingest ledger (cheap label check), NOT a daily 128GB cold-start. (Replaces the disabled `sam-spine-refresh` cron defect.)

### Phase 3 — Backfill & hardening (gated on §5 decision)
- [ ] If role-upgrade/SFTP unblocked: backfill the daily sequence since the last monthly into `sam_entity_daily`, then re-merge into the master (or just rely on the monthly re-baseline).
- [ ] Port the gate discipline from `sam_master.py` (row floors, ±25% delta guards, write-integrity + index-presence post-write gates, rollback guard) to the daily worker and the merge step.
- [ ] Alerts via `core/ops_alert.alert(...)` on any non-skip failure.

---

## 7. Verification / success criteria (gates an executor must pass)

- [ ] **Phase 0:** a real daily ZIP is retrievable, filename + layout recorded, parser confirmed.
- [ ] **Phase 1:** `sam_entity_daily` populates each run; `ops.sam_entity_daily_runs` records it; the new+relevant feed returns plausible non-zero counts; re-running the same window is idempotent (no growth from re-pull).
- [ ] **Phase 2:** after a daily merge, `sam_master_entities` still satisfies **`count_rows() == count_distinct(uei)`** (no dups); a UEI present in the daily shows the **daily's `last_update_date`** post-merge (latest-wins proven); indices intact; new UEIs from the daily are present.
- [ ] **Re-baseline:** monthly overwrite (reading union) + catch-up merge leaves **no gap** vs the pre-rebaseline state for daily-only UEIs.
- [ ] **Idempotency:** ledger-guarded; a retried run does not double-append or double-count.

---

## 8. Risks & guardrails

- **Rate limit (10/day)** — the binding constraint. Do NOT design backfill around the API at 10/day; resolve via role-upgrade or SFTP first. Forward feed is safe.
- **No list endpoint / unknown retention** — never assume a historical daily is fetchable; the monthly is the deterministic gap-close.
- **Stale public docs** — the data.gov open-download links are dead; do not reintroduce them. API or SFTP only.
- **Do not pollute the deterministic master** — keep `sam_entity_daily` as a durable raw SoR so the master is always reconstructable from (bulk ∪ daily). The master is materialized, not authoritative.
- **Blast radius** — daily failures must not touch `entity_registrations` or the monthly baseline; merge under a rollback guard.
- **Uniqueness** — the master's `distinct_uei == rows` gate is sacred; merge_insert must preserve 1-row-per-UEI.
- **SAM publish lag / weekend gap** — the rolling 4-day window covers it; verify it actually catches a Monday-run (no Sun/Mon daily).

---

## 9. Open decisions for the operator (block exit from Phase 0)

1. **Backfill channel:** role-upgrade (→1,000/day) vs SFTP vs monthly-only gap-close (default).
2. **"New" trigger:** `registrationDate` (submission) vs `activationDate` (biddable). Defines the surety feed timing.
3. **"Relevant":** the NAICS/PSC set surety underwrites (the GTM filter).
4. **Day-grain gap history:** needed (→ backfill) or not (→ June-7 monthly closes the gap)?

---

## 10. Reference appendix

- **API:** `GET https://api.sam.gov/data-services/v1/extracts?api_key=$SAM_API_KEY&fileType=ENTITY&sensitivity=PUBLIC&frequency=DAILY&date=MM/DD/YYYY` · monthly `frequency=MONTHLY&date=MM/YYYY` · no list endpoint.
- **Key files:** `pipelines/sam_gov/entity_registrations_bulk.py` (parser to reuse), `pipelines/sam_gov/sam_master.py` (`build_sam_master`, merge target, gate/rollback patterns), `pipelines/sam_gov/sam_normalized_entities.py`, `pipelines/sam_gov/sam_pocs.py`, `core/modal_dispatcher.py`, `src/trigger/sam_opps_bulk.ts` (Trigger task pattern), `src/trigger/sam_spine_refresh.ts` (disabled — context only).
- **Datasets (R2, `s3://data-sink/active/`):** `entity_registrations/` (monthly SoR), `sam_entity_daily/` (NEW), `sam_master_entities/`, `sam_master_contacts/`, `sam_master_domains/`, `sam_normalized_entities/`.
- **Ledgers (HQX Postgres `ops.`):** `sam_master_runs`, `sam_normalized_entities_runs`, `sam_entity_daily_runs` (NEW). Connect via `HQX_DB_URL_POOLED`.
- **Secrets:** Doppler `core-x/prd` (see §3).
- **Sources:** [GSA SAM Entity Extracts API](https://open.gsa.gov/api/sam-entity-extracts-api/) · [SAM.gov Data Services](https://sam.gov/data-services) · [data.gov catalog](https://catalog.data.gov/dataset/system-for-award-management-sam-public-extract-entity-registration).
