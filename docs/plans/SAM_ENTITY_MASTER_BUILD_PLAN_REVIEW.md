# SAM Entity Master — Build Plan, Adversarial Design Review

Reviewer stance: hostile. Goal is to find what is wrong, risky, over-engineered, or
under-specified. Plan under review: `docs/plans/SAM_ENTITY_MASTER_BUILD_PLAN.md`. Live
consumer graph and gateway behavior were independently re-verified against the checked-in
code (not taken from the plan's self-description).

---

## 1. Verdict

**Ship with changes — but two of the six "locked" decisions must be reopened first, and
Phase 4 is not a plan, it is a placeholder.** The mirror/satellite topology is sound, the
field authority is real work done well, and the v2-only spine is defensible *as a Phase 1*.
The single biggest risk is **Decision 5 ("active" as a view) crossed with the gateway's
auto-discovery**: the plan's entire enforcement story — "consumers allowlist `sam_master_*`,
deny `raw_*`" — describes a control layer that **does not exist in `apps/gtm_mcp`**, which
auto-registers every R2 prefix it finds (`apps/gtm_mcp/src/database.py:240-256`). Renaming
`entity_registrations` does not seal raw; it just renames the dataset the MCP already serves
and lets `sam_master_entities` appear beside it with zero access control. The plan asserts a
boundary it never builds. Second-biggest risk: discarding the temporal stack at the mirror
(Decision 2) is irreversible per-build and directly contradicts the stated "audience off
these companies" goal. Everything else is fixable in-plan.

---

## 2. Critical findings (severity-ordered)

### BLOCKERS

#### B1 — The "seal raw / allowlist `sam_master_*`" enforcement does not exist; the rename is cosmetic and risks a silent gateway break
**Concern.** §4 and Phase 4 (`plan:72`, `plan:164-169`) rest on "consumers allowlist
`sam_master_*`, deny `raw_*`" and "enforce zero app reads of raw (gateways allowlist
`sam_master_*`, deny `raw_*`)." That mechanism is not in the codebase. `apps/gtm_mcp/src/database.py:240-256`
discovers datasets by walking R2 and registering **any** prefix containing a `_versions`
marker, keyed by its path. There is no allow/deny filter anywhere in that file. Consequence
of Phase 4 as written:
- Renaming `active/entity_registrations/` → `active/raw_sam_entity_registrations/` does **not**
  hide it from the MCP — it re-exposes it under a new name (`describe_dataset`, the audience
  SQL surface, and the discovery manifest all pick it up automatically).
- `sam_master_entities/` will auto-appear in the same gateway with no gating.
- The `catalyst_api` gateway is unaffected (it reads `firmographics_blitz` + `award_search` +
  `contractor_award_summary` only — verified `apps/catalyst_api/src/config.py:51-57`,
  `lance_store.py`), so the "seal" buys nothing there either.

**Why it matters.** The plan claims a tier boundary as a *shipped guarantee* ("Success = …
never touching raw"). It is a naming convention with no teeth. Worse, the rename is a
**destructive prefix move on the 19.3M-row system of record** with three live readers hard-coded
to the old path — for an enforcement outcome the code can't deliver.

**Recommendation.** Decouple. (a) Do **not** rename raw in this build. Land `sam_master_*`
alongside `entity_registrations` and repoint the three consumers first. (b) If a boundary is
actually wanted, it must be **built**: add an explicit allow/deny set to
`apps/gtm_mcp/src/database.py` discovery (drop names matching `raw_*` from the registry, or
gate them behind a flag), and only *then*, in a separate follow-up, relocate raw under a
`source/` prefix (the plan's own "optional stronger form," `plan:168`) so the boundary is a
path the discovery walker doesn't descend. Naming alone is not enforcement and must not be
sold as such.

#### B2 — Phase 4 is dangerously under-specified for a destructive, multi-consumer cutover
**Concern.** Phase 4 (`plan:164-169`) bundles, in one paragraph: rename the SoR prefix,
repoint `crosswalk_sam_usaspending`, repoint `sam_fmcsa_domain_spine`, **replace** `sam_pocs`
with a rebuild of `sam_master_contacts`, and add review-lint enforcement. Each is a separate
risk surface; there is no ordering, no per-consumer parity proof beyond one line in §7, no
rollback, and no statement of what happens to the *existing* `sam_pocs` dataset and its
downstream (`GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md` documents `sam_pocs` as a live
profile source with 8.06M rows and four BTREE indexes incl. `name_key`/`last_name` that
`sam_master_contacts` per §5c does **not** reproduce — no `name_key`, no `full_name`, no
`last_name` index).

**Why it matters.** `sam_master_contacts` (§5c) is **not** a drop-in for `sam_pocs`:
- §5c drops `name_key`/`full_name` (the SoR's documented reverse-name lookup,
  `sam_pocs.py:80`, used by the profile schema) and the `last_name` BTREE.
- §5c is **v2-only**; `sam_pocs` deliberately retains the **legacy cage-keyed POC tail**
  (`sam_pocs.py:30-34`, "ALL distinct legacy cages retained (max spine)") which
  `crosswalk_sam_usaspending` bridges via `cage_code`. Repointing the crosswalk's POC
  consumer to a v2-only contacts set silently drops the defense tail the crosswalk exists to
  reclaim.

**Recommendation.** Split Phase 4 into its own plan with: (1) a frozen list of every consumer
(see B7 — the plan's "3 consumers" is incomplete), (2) per-consumer before/after parity
capture (row counts, distinct keys, a fixed UEI sample) gated *before* old-path read removal,
(3) explicit decision on `name_key`/`last_name`/`full_name` and the legacy POC tail — either
`sam_master_contacts` carries them (then it is no longer a "faithful minimal mirror") or
`sam_pocs` stays and is **not** superseded. Do not rename raw in the same step (B1).

### HIGH

#### B3 — Decision 2 (no temporal history) is the wrong default for the stated goal and is irreversible per build
**Concern.** §3 / Decision 2 (`plan:51`, `plan:122-127` analog) collapses the 26-snapshot
stack to latest-row-per-UEI with "**No temporal history.**" The operator's stated intent
(quoted in the mandate) is to "build an audience off these companies independent of active
SAM status." First-seen date, registration-status transitions, lapse/reactivation, and
tenure are *exactly* the audience-segmentation primitives ("registered <12mo," "lapsed since
2022," "continuously active 5yr"), and the snapshot stack already contains them. The mirror
throws them away at the one place they're cheap to compute.

**Why it matters.** "Rebuild it later from raw" is the plan's escape hatch, but raw is the
26-snapshot stack — once consumers are repointed and (per Phase 4) raw is sealed/renamed and
the legacy half deferred, reconstructing temporal features means re-deriving them from the
same stack the master was meant to abstract. You don't lose the *ability*, but you entrench
"latest-only" as the served grain and every audience query that needs tenure re-scans 19.3M
rows — the precise pathology the master exists to kill.

**Recommendation.** Keep the golden-row grain, but in the **same single scan** (you're already
paying for it — Decision 5/§5.6) compute and attach a small set of cross-snapshot scalars to
`sam_master_entities`: `first_seen_label`, `last_seen_label`, `snapshot_count`,
`first_registration_date` (min across history), and a boolean `ever_inactive`. This is a few
window aggregates over the data already in the scan, adds five scalar columns, and converts
"the snapshot stack IS a time series" from a discarded asset into an indexed one. A full
SCD-2 history table is correctly out of scope; five derived columns are not.

#### B4 — Decision 4 (faithful `~`-delimited `*_string` columns, no parsing) re-creates the divergence the master exists to eliminate
**Concern.** §5a/§5b (`plan:81-116`) keep `naics_code_string`, `psc_code_string`,
`bus_type_string`, `sba_business_types_string`, `disaster_response_string`,
`naics_exception_string` as **raw `~`-delimited verbatim strings**, declaring array-parsing
"a downstream **mart** concern, never the mirror." But the *entire stated objective* (§1,
`plan:12-23`) is that "every consumer answers any firmographic question … never touching raw"
and that NAICS is no longer "trapped in a positional array." Shipping `naics_code_string =
'541511Y~541512N~…'` trades a positional-array parse for a delimiter-and-flag parse. Every
cohort consumer must now re-implement "split on `~`, strip the trailing Y/N flag, filter
blanks" — which is *the exact logic the superseded `sam_entity_master.py:131-137` already
wrote once*. The plan deletes a working `naics_codes ARRAY` projection and replaces it with a
string every consumer must re-parse. That is a regression dressed as fidelity.

**Why it matters.** "Faithful mirror" and "kill dedup/parse divergence" are in direct tension
and the plan resolves it the wrong way. The master's whole reason to exist is to centralize
the parse. A mirror that centralizes nothing but column names is a renamed raw scan with a
dedup.

**Recommendation.** Resolve the tension explicitly: keep the verbatim `*_string` columns for
provenance **and** ship the parsed forms in the same dataset — `primary_naics` (already
planned), plus `naics_codes LIST<VARCHAR>` (flag-stripped) and `psc_codes LIST<VARCHAR>`,
reusing the proven `naics_array`/`psc_array` builders from the existing
`sam_entity_master.py:131-137` you're otherwise deleting. The mirror then satisfies its own
success criterion (a consumer never parses), and the verbatim string remains for auditors.
This is not "a mart" — it is the one transform that justifies the dataset.

#### B5 — Single multi-output worker has no publish atomicity → split-vintage system of record on partial failure
**Concern.** Decision 3 / Phase 2 (`plan:53-54`, `plan:156-159`): one worker emits three
datasets via three independent `lance.write_dataset(..., mode="overwrite")` calls in one
process. If dataset 1 (`sam_master_entities`) overwrites successfully and dataset 2/3 throws
(OOM on the wide `pipe_fields` unnest, R2 5xx, BTREE multipart trip), you have a freshly
overwritten entities dataset and stale contacts/domains — a split-vintage SoR with no
transaction wrapping the three. The existing single-output workers (`crosswalk`, `sam_pocs`)
overwrite-in-place too, but each is *one* dataset, so a failure leaves the prior version
intact; the multi-output case loses that property across datasets.

**Why it matters.** "All at the same snapshot vintage" (Decision 5) is asserted as a property
but nothing enforces it across a partial failure. Overwrite-in-place + no cross-dataset
rollback = the consumer can read entities@vintage_N joined to contacts@vintage_N-1.

**Recommendation.** Lance overwrite is versioned — exploit it. Capture each dataset's
`version` before write; on any failure in the trio, `dataset(uri, version=v_before).restore()`
all already-written members back to their pre-run version (the pattern already exists in this
repo: `crosswalk_sam_usaspending.py:576` does exactly this for its integrity gate). Record all
three target versions in one `ops.sam_master_runs` row so a split is detectable. Alternatively
(simpler, recommended): compute all three Arrow tables **first** (all in-memory before any
write), then write the three back-to-back with a restore-all wrapper — fail before any write
if any projection fails. Atomicity across three overwrites must be designed, not assumed.

#### B6 — Validation gates are too weak for a system-of-record overwrite (the existing worker's floor is already a liability)
**Concern.** §7 gates are floors + a single-UEI spot check (KIPPER) + "index manifest carries
3 BTREEs." Missing, for a dataset that **overwrites** the served surface:
- **No uniqueness assertion on `uei`.** The grain is "1/uei" but nothing asserts
  `count(*) == count(distinct uei)` post-dedup. A QUALIFY bug or a duplicate snapshot silently
  ships a non-unique spine.
- **No null-rate guards** on load-bearing columns (`legal_business_name`, `primary_naics`,
  `cage_code`). A layout drift that nulls a column passes a row-count floor cleanly.
- **No schema-drift guard.** SAM appends fields across extract vintages (the whole
  v1→v2 widening is why this codebase classifies by width, `entity_registrations_bulk.py:194-262`).
  If a future extract shifts positions, the frozen field-map projection reads the wrong column
  and every gate except an exact-position assertion passes.
- The existing `sam_entity_master.py:269` ships a **500,000** floor against a ~782k–1.54M
  population — a 35–67% data-loss event passes. The plan's ≥1.4M floor (`plan:153`) is better
  but still a blunt instrument.

**Why it matters.** Every gate here guards quantity, none guards *correctness of the
projection*, which is the actual failure mode of a positional-array mirror.

**Recommendation.** Add to the publish gate, hard-fail: (1) `count(*) == count(distinct uei)`;
(2) per-column non-null floors for the handful of must-have columns, expressed as ratios not
absolutes; (3) a **position-integrity assertion** tied to the Phase-0 frozen map — re-run the
"5 live invariants" (`plan:145`, `plan:180`) against the freshly built dataset, not just the
field-map artifact, so a silent SAM re-sequencing is caught at build time; (4) a delta guard
vs. the prior published version (`abs(new_rows - old_rows) / old_rows < 0.25`) so the next
build can't quietly halve the registry. The §7 cutover-parity gate (`plan:186`) is the one
genuinely good gate — keep it and make it blocking.

#### B7 — The consumer inventory is incomplete; "3 consumers" understates blast radius
**Concern.** Phase 4 names three consumers. Verified against the tree, the readers of the
renamed/repointed surfaces are broader, and at least one auto-discovers:
- `apps/gtm_mcp` auto-registers `entity_registrations` today and will auto-register both the
  renamed raw and the new masters (`database.py:240-256`) — **not** in the plan's consumer
  list, and the rename changes its exposed dataset name with no code change and no notice.
- `sam_pocs` is itself consumed as a documented profile source
  (`GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md:279, 369-372`), so "repoint `sam_pocs` → rebuild
  of `sam_master_contacts`" has its **own** downstream that the plan doesn't trace.
- Confirmed **non**-consumers (the plan is correct to omit, state so to bound the work):
  `contractor_award_summary` derives `primary_naics`/`primary_psc` from `award_search` via
  `mode()` keyed on `recipient_uei` (`contractor_award_summary.py:195-196`), **not** from
  SAM — so the mandate's framing that Phase 4 "discards freshly-shipped #119/#120" is
  **overstated**; #119/#120 are independent of this rename. `firmographics_blitz` carries its
  own `uei` from PDL/Clay, not from `entity_registrations`. `federal_spine_index_campaign`
  does **not** list `entity_registrations` in its DATASETS map
  (`federal_spine_index_campaign.py:59-68`), so the rename does not break the index campaign.

**Why it matters.** A rename-the-SoR operation must enumerate every reader. The plan's count
is low by at least the gateway, and the `contractor_award_summary` "discard" framing is a
non-issue that inflates the perceived stakes while the real silent break (gateway
auto-discovery) goes unlisted.

**Recommendation.** Replace "3 consumers" with the verified inventory above. Explicitly mark
`contractor_award_summary`, `firmographics_blitz`, `federal_spine_index_campaign` as
out-of-blast-radius (with the one-line reason each) so the cutover scope is bounded and the
#119/#120 anxiety is retired.

### MEDIUM

#### B8 — "Complete 1:1 mirror, 50+ columns" carries dead government-only fields → YAGNI
**Concern.** §5b deliberately keeps `entity_eft_indicator(3)`, `dodaac(5)`,
`d_b_open_data_flag(24)`, `credit_card_usage(38)`, `correspondence_flag(39)` (`plan:110-111`).
The plan's own §2 (`plan:38-40`) states the public extract redacts banking/EFT/TIN/IGT — these
flags are the residue of redacted government-only machinery and the GOVCON audit (which
profiled the same data) never surfaces one of them as a profile field. Mirroring them is
columns-for-symmetry, not for use.

**Why it matters.** Minor, but it's the tell that "faithful 1:1" is being treated as the goal
rather than "serve every field a consumer needs." It also inflates the schema a consumer must
reason about and the `describe_dataset` output the gateway exposes.

**Recommendation.** Keep them only if they are non-null for a non-trivial fraction of v2 rows
(cheap to check in the Phase-1 dry-run — add a null-rate column to the dry-run output). Drop
the all-null/near-all-null ones. "Mirror every field that carries signal" is the right rule;
"mirror every field" is not.

#### B9 — Domains satellite ignores the coverage + single-vintage caveat the prior audit already flagged
**Concern.** §5d builds `sam_master_domains` from `entity_url` (pos 27) via the canonical
normalizer. The GOVCON audit (`GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md:240-249`) measured
this exact build: **~46% URL coverage, 45.6% yield a valid domain, 98.8% unique**, with an
explicit caveat that the 45.6% was a **single-vintage (`2020_NOV`) sample** to be re-confirmed
against the latest extract before any SLA. The plan inherits the build but not the caveat — no
coverage floor, no "re-confirm vs latest label" gate, no handling of the 1.2% multi-UEI
corporate-family case the audit calls out.

**Why it matters.** A domains dataset that silently covers <half the registry and was sized on
a 6-year-old vintage will be consumed as if complete. The audit did the homework; the plan
dropped it.

**Recommendation.** Add a coverage metric to `ops.sam_master_runs` (rows-with-domain /
distinct-uei) and a floor gate (e.g. ≥40%); re-measure against the *current* extract, not
2020_NOV. Decide the multi-UEI-per-domain grain explicitly (the plan says `1/(domain,uei)`,
which is correct, but the consumer needs to know a domain can map to N UEIs — document it).

#### B10 — `sam_master_*` as a namespace overloads "master" and won't scale cleanly across sources
**Concern.** §4 (`plan:71`) locks Tier-1 = `<source>_master_<grain>`, grouping the served
layer under `sam_master_*`. "Master" is doing two jobs: the golden-record *grain* (1-row-per-
entity) and a *served-tier namespace* that also holds `sam_master_contacts` (which is **not** a
golden record — it's a 1-to-6 satellite) and `sam_master_domains` (a many-to-many bridge). A
domains bridge is conceptually a `bridge_*`/`crosswalk_*` (the plan even preserves that
convention for cross-source products, `plan:73`) — calling it `sam_master_domains` muddies the
one distinction the repo otherwise keeps clean.

**Why it matters.** Naming is cheap to get right now and expensive to migrate later
(B1 shows renames are destructive prefix moves on the SoR). When `usaspending_master_*` or
`fmcsa_master_*` arrive, "master" will mean "served tier" in some names and "golden grain" in
others.

**Recommendation.** Reserve `*_master` for the golden-record grain only (`sam_master_entities`
— good). Name the satellites for what they are: `sam_contacts` (or `sam_entity_contacts`) and
`sam_domain_uei` (the GOVCON audit's own name for this exact dataset,
`GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md:342`, which would also unify naming with the build
that doc already specced). Don't overload "master" as a namespace.

### LOW / NITS

- **B11 — Trigger control plane for a ~2×/year feed is borderline ceremony, but consistent.**
  Phase 3 (`plan:160-162`) wires a Trigger task + dispatcher for a rebuild that fires manually
  or on new-extract landing. For a twice-yearly cadence this is heavier than `modal run`. It's
  *justified* only because ARCHITECTURE.md mandates one control-plane pattern and consistency
  beats a bespoke path — accept it, but the plan should say the cadence is event-driven (new
  extract), not scheduled, so no one adds a cron later.
- **B12 — Build cost/memory on 11.6M wide-array rows is unstated.** The scan reads `pipe_fields`
  (142-wide list) for the v2 universe and does a high-cardinality `uei` BTREE sort plus a 6×
  POC unnest. The existing worker pins 32 GiB / `LANCE_BYPASS_SPILLING` / 24 GB DuckDB for the
  *active-only* (~780k) cut; the new scope is ~1.54M deduped from a larger scan **plus** two
  more output datasets in the same process. The plan says nothing about memory envelope,
  Lance fragment sizing (`MAX_ROWS_PER_FILE`/`MAX_BYTES_PER_FILE`), or whether the BTREE on
  ~1.54M `uei` needs the Volume-staged giant path (it does not — well under the ~100M
  threshold, ARCHITECTURE.md:135 — but the plan should state it so no one over-provisions onto
  spot). Add the envelope to Phase 1.
- **B13 — `registration_expiration_date` (§5b) vs the existing `expiration_date`.** The plan's
  faithful-name slug yields `registration_expiration_date(9)` where the existing master and
  crosswalk use `expiration_date`. Harmless if consumers are repointed, but it's a concrete
  example of why "faithful names" silently breaks anything not in the repoint list — the
  column a consumer joined on yesterday vanishes. Document the rename map; don't assume.
- **B14 — `sam_extract_code(6)` is kept but its provenance twin `sam_extract_label` is the
  real partition key.** Minor: ensure the "active view" query in §4 (`plan:68`) uses
  `sam_extract_label`, which is the provenance column actually written (§5b last line), not
  the dictionary `sam_extract_code`. Easy to conflate; name it once, precisely.

---

## 3. Decision-by-decision verdict (the six locked items)

| # | Decision | Verdict | Reasoning |
|---|---|---|---|
| 1 | **v2-only scope** (defer 476k legacy) | **ENDORSE as Phase 1, REJECT the "bait-and-switch" framing** | Correct to ship v2 first — every entity has a UEI, no identity resolution, clean spine. The operator's "go broad" intent is satisfied by the *audience* breadth (1.54M all-time v2 UEIs, not just the 876k active), **provided B3 (temporal columns) lands** so "independent of active SAM status" is real. The legacy 476k are all last-seen ≤2020 (6+ yr stale) — low audience value, high cost (no official dict, reverse-engineered layout, unsolved CAGE↔UEI). Deferring is right. But "rebuild later" (Phase 5) is a **real trap** as written (no 120-field dict, identity unsolved) — so do not *promise* it; label it "speculative, may never be feasible" and stop treating it as a guaranteed follow-up. The honest position: v2 is the product; legacy is a maybe. |
| 2 | **No temporal history** (latest-row-per-uei) | **CHANGE** | Wrong default for an audience product (B3). The grain is fine; the *total* discard of the time series is the mistake. Attach ~5 cross-snapshot scalars (first_seen, last_seen, snapshot_count, first_registration_date, ever_inactive) in the same scan. Full SCD-2 stays out of scope. |
| 3 | **One multi-output worker, single scan** | **ENDORSE topology, CHANGE execution** | One scan emitting three datasets is the right efficiency call. But it ships without cross-dataset publish atomicity (B5) → split-vintage SoR on partial failure. Materialize all three Arrow tables before any write, wrap the three overwrites in a restore-all-on-failure guard, record all three versions in one ops row. |
| 4 | **Faithful naming, verbatim `~`-strings, no array parsing** | **CHANGE** | Faithful column *names* — fine. Keeping `*_string` columns **verbatim and unparsed** — reject (B4): it re-creates the parse-divergence the master exists to kill and deletes a working array projection. Ship verbatim strings *and* parsed `naics_codes`/`psc_codes` LISTs in the same dataset. The mirror must centralize the one transform that justifies it. |
| 5 | **"Active" as a view, not a dataset** | **ENDORSE the no-dataset call, REJECT the "centralizes" claim** | Correct *not* to fork an active-only dataset — that's the dedup-divergence trap in a new costume. But Lance/R2 has no view layer, so this centralizes nothing by itself: every consumer re-types `WHERE sam_extract_label='<latest>' AND registration_status='A'` (filter-divergence replaces dedup-divergence). Mitigate: (a) ship the `is_active` boolean as a materialized column in `sam_master_entities` so the filter is `WHERE is_active` (one definition, computed once at build), and (b) put the canonical active predicate in the gateway as a named helper, the way `core.name_norm` centralizes the blocking key. A documented SQL string in a plan is not centralization. |
| 6 | **Field authority = validated 142-field map, zero reverse-engineering** | **ENDORSE** | The one unambiguous win. Parsing + validating the official Feb-2025 layout (5/5 live invariants) is exactly right, and freezing it as a committed artifact + unit test (Phase 0) is the correct way to make the projection auditable and drift-detectable. Strengthen only by re-running the invariants against the *built dataset* too (B6), not just the map. |

---

## 4. Concrete go-forward

The sequence I would actually execute (changes vs. the plan are marked **▲**):

1. **Phase 0 — Freeze the field map.** Ship as written (`pipelines/sam_gov/reference/sam_v2_public_field_map.py` + `.json` + the 5-invariant unit test). This is good; do it first, unchanged.

2. **▲ Reopen Decisions 2, 4, 5 before writing the worker.** Three small, decisive amendments:
   - D2 → add `first_seen_label`, `last_seen_label`, `snapshot_count`, `first_registration_date`, `ever_inactive` to `sam_master_entities` (computed in the same scan).
   - D4 → add parsed `naics_codes LIST`, `psc_codes LIST` (reuse `sam_entity_master.py:131-137` before deleting that file) alongside the verbatim `*_string` columns.
   - D5 → add materialized `is_active` boolean; do **not** fork an active dataset.

3. **Phase 1+2 — Build `sam_master_entities` + `sam_contacts` + `sam_domain_uei`** (▲ rename the two satellites per B10) in one multi-output worker, single scan.
   - ▲ Publish atomicity: materialize all three Arrow tables, then write with a restore-all-on-failure guard (B5).
   - ▲ Decide `name_key`/`full_name`/`last_name` + legacy POC tail for the contacts dataset **now** (B2) — either it carries them (and is no longer "minimal mirror") or `sam_pocs` is NOT superseded.
   - `--dry-run` first, ▲ with per-column null-rates and coverage metrics in the output (B8, B9).
   - ▲ Strengthen gates (B6): uniqueness on `uei`, non-null ratios, position-integrity re-assert vs frozen map, ±25% delta vs prior version, domain-coverage floor.

4. **Phase 3 — Control plane.** Trigger task + dispatcher as written (B11). ▲ State the cadence is event-driven (new extract landing), not scheduled.

5. **▲ STOP. Do not seal/rename raw in this build (B1).** Land the masters *alongside* `entity_registrations`. Ship Phases 0–3 and verify in production before touching the SoR prefix or any consumer.

6. **▲ Phase 4 becomes its own plan (B2, B7).** Scoped separately, after Phases 0–3 are proven:
   - Frozen, verified consumer inventory (crosswalk, sam_fmcsa_domain_spine, sam_pocs-downstream, **gtm_mcp auto-discovery**) — explicitly excluding contractor_award_summary / firmographics_blitz / federal_spine_index_campaign with the one-line reason each.
   - Per-consumer before/after parity, gated before old-path removal (the §7 cutover gate, made blocking).
   - **Build the gateway allow/deny** in `apps/gtm_mcp/src/database.py` — the "seal" the plan asserts but never implements. Only then relocate raw under `source/` (path-tier, the plan's "stronger form") so the boundary is real, not a naming convention.

7. **Phase 5 (legacy) — relabel "speculative."** Do not carry it as a committed follow-up; the 120-field dict and CAGE↔UEI identity are unsolved and may never be worth it. v2 is the product.

**One-line bottom line:** the data work (field authority, mirror+satellite topology, single-scan build) is right; the *systems* work around it (publish atomicity, validation correctness, and especially the imaginary "seal raw" enforcement) is where this plan is thin — fix those three and split Phase 4 out, and it ships.
