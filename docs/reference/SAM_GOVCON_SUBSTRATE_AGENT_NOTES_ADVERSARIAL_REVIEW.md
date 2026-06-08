# Adversarial Review — `SAM_GOVCON_SUBSTRATE_AGENT_NOTES.md`

First-principles, evidence-driven audit of the operating notes. Every verdict below
is backed by a command that was actually run (or a `file:line` that was read) plus the
observed output. Performed read-only against the live R2 lake, the local
`/tmp/bridge_diag/bridge.duckdb` + `winners.parquet`, the landed manifest, and live
read-only GETs to the SAM.gov frontend (browser headers, paced). No mutations, no git,
no PDF downloads. `api.sam.gov` gateway was deliberately NOT hit (quota preservation).

Audit date: 2026-06-07. Datasets/artifacts as they stood at that time.

---

## Verdict summary

| # | Claim (abbrev.) | Verdict | One-line basis |
|---|---|---|---|
| C1 | Award-grain Sol# fill **17.4 %** → 82.6 % carry no Sol# | **Confirmed** | Measured 17.36 % award-grain over 1,229,191 distinct awards |
| C2 | "~82.6 % structurally unreachable / hard ceiling" (vs PIID path) | **Confirmed (with scope caveat)** | PIID/award_number path adds only +0.35 pp reachability; the ceiling is real |
| C3 | Solnum→notice resolution **33–34 %** | **Confirmed** | Bridge 33.2 % (full pop); live 500-sample 34.4 % |
| C4 | Substrate yield on resolved solnums **85–90 %** | **Confirmed (range honest; headline optimistic)** | Full manifest = 85.21 %; the 89.5 % was a 172-solnum sample |
| C5 | End-to-end award→substrate **~5.3 %** | **Overstated (mildly)** | 5.35 % uses the 89.5 % sample; full-pop realizes ~4.9 % |
| C6 | Rank on `base_type` not `notice_type` is **decision-critical**, mis-demotes **~8,400** awarded solicitations | **Overstated / Wrong numbers** | nt-ranking changes only 5,738 winners; only **268** lose a host-tier; the 11,957 vs 3,554 figures match nothing in the artifact |
| C7 | `notice_type` flips to "Award Notice" once awarded (inflates Award count) | **Confirmed (direction); Wrong specific counts** | Direction true (nt Award 40,540 > bt 24,293 in join); the cited 11,957/3,554 are unreproducible |
| C8 | One solnum → many notices; "one observed had **51** siblings" | **Confirmed (understated)** | Real max is **8,365** siblings (47QSMD20R0001); 51 wildly understates it |
| C9 | `size_bytes` corrupt for ≥10 MB: `((true-1) mod 10M)+1`; "declared 5 MB may be a real 45 MB file" | **Wrong for this pipeline's data** | Manifest holds 4,830 rows >10 MB, max 249 MB; live Range probe: true Content-Length == declared exactly |
| C10 | `api.sam.gov` SI-NONFED caps **~5–10 req/day**, 429 "900804" | **Unverified (not tested; quota-preserved)** | Plausible & corroborated by repo recall scripts; not independently re-confirmed |
| C11 | Frontend `sgs/v1/search` open; `_id` = notice_id; `type.value`; `page.totalElements` | **Confirmed** | Live: 200, `_id`=32-hex, `type.value` populated |
| C12 | hal+json: strict `Accept: application/json` → **406**; permissive → 200 | **Confirmed** | Live: strict=406, permissive=200 on the same resources URL |
| C13 | `opps/v2/opportunities/search` frontend auth-gated (401) | **Confirmed** | Live: 401 |
| C14 | No `setsid` binary on macOS; use `os.setsid()` | **Confirmed** | `which setsid` → not found; Darwin 25.5.0 arm64 |
| C15 | macOS objc fork crash; daemonize BEFORE threaded imports | **Confirmed (mechanism sound)** | Documented Cocoa fork-safety behavior; scripts fork before `import duckdb,requests`; harvest ran to completion |
| C16 | Resumable JSONL checkpoint makes resume-kills harmless (~5 kills) | **Confirmed** | Two-run resume in logs (todo 49,248 → 43,415 → done); 49,248 ckpt lines |
| C17 | Pace 0.12 s, ~5.5 req/s, 0.09 % error over 49 K calls | **Confirmed** | Logs: 5.4–5.5/s; 43 err / 49,248 = 0.087 % |
| C18 | DuckDB register/table same-name alias = silent empty (0 rows) | **Confirmed (reproduced)** | `register('m'); CREATE TABLE m AS SELECT * FROM m` → 0 rows; distinct names → 5 |
| C19 | Materializing 2.88 M archived rows into DuckDB STALLS | **Plausible but not reproduced; partially self-contradicted** | Committed pipeline materializes the full 2.84 M set (narrow cols) without stalling |
| C20 | Scalar BTREE pushdown only via `scanner(filter=)`, not registered relation | **Plausible; not the path the committed pipeline uses** | 90-day pipeline uses column-projection-only materialization; filter pushdown lives in `sam_play1` |
| C21 | ~2 % attachments `private`; ~16 % null-mime/zero-size | **Confirmed** | Manifest: 1.96 % private; 15.83 % null-mime (== zero-size, same rows) |
| C22 | Offline join reproduces live hit rate "within 2/500" | **Confirmed (aggregate); masks 22 per-row diffs** | Live 172 vs offline 170 (net 2); but 12 only-live + 10 only-offline |
| C23 | Manifest landed: 155,183 rows, 22 cols, 5 BTREE indices | **Confirmed** | Live read-back of the Lance dataset |

Net: most operational/runtime notes (C11–C18, C21) are solid and high-value. The two
load-bearing analytical claims the mandate flagged are the weakest: **C6 (base_type
ranking impact) is materially overstated**, and **C9 (size_bytes corruption) is simply
not true for this pipeline's data** — the single most important correction.

---

## Per-claim detail

### C1 — Award-grain Sol# fill 17.4 % / 82.6 % no-Sol# — Confirmed
Note (§11): "Award-grain Sol# fill **17.4 %**". Diagnostic: "82.6 % of awards have no Sol#".
Ran `/tmp/audit5.py` (per-award aggregation over `usaspending_api_fresh/contract_prime_txn`):
```
distinct_awards = 1,229,191
awards_with_sol = 213,372
award_grain_sol_fill_pct = 17.36
```
17.36 % ≈ 17.4 %; complement 82.64 %. **Confirmed.** This matches the committed
`step1_source_profile.py` design and the diagnostic's `17.36 %`.

### C2 — "Structural ceiling" vs the PIID/award_number path — Confirmed, with scope caveat
The mandate's central challenge: is 82.6 % truly *structural*, or just an artifact of
the solnum bridge while `sam_play1_target_select.py` uses `award_number=PIID OR
solicitation_number`? Ran `/tmp/audit5.py` computing per-award reachability via BOTH
the normalized-solnum bridge and the `award_id_piid → SAM.award_number` bridge against
active∪archived:
```
awards               = 1,229,191
via_sol              = 102,484  (8.34 %)
via_piid             =  42,281
via_either           = 106,860  (8.69 %)
piid_only_incremental=   4,376  (+0.35 pp)
unreachable          = 91.31 %
```
The PIID path rescues only **4,376 additional awards (+0.35 pp)** beyond the solnum
bridge. The ceiling is genuinely structural, not an artifact of the bridge key choice —
this **vindicates** the notes against the mandate's skepticism.

Caveat on framing: the "82.6 %" is specifically the **Stage-1 no-Sol# gap at the
distinct-award grain**. The numbers above are computed at distinct-award grain and show
~8.7 % *award-grain* reachability end-to-end (lower than 17.4 % because not every named
solnum resolves to a SAM notice). The notes/diagnostic keep these grains separate
correctly, but a future reader must not conflate "17.4 % carry a Sol#" with "17.4 %
reach substrate." (Also note `sam_play1` operates on a *different, NAICS-scoped UEI
footprint* universe — `usaspending/transaction_search_fpds`, not the 90-day API-fresh
feed — so its PIID join is not a drop-in alternative to this bridge.)

### C3 — Resolution 33–34 % — Confirmed
Note (§11): "Solnum → notice_id resolution **33–34 %**". `/tmp/audit1.py` on
`bridge.duckdb`:
```
fpds_distinct_solnorm     = 148,359
joined_distinct_solnorm   =  49,248
resolution_rate_pct       = 33.2
```
Live corroboration (`/tmp/audit_offline_live.py`, 500-solnum sample): **172/500 = 34.4 %**
live frontend hit rate. Both inside the stated band. **Confirmed.**

### C4 — Substrate yield 85–90 % — Confirmed (range honest, headline optimistic)
Note (§11): "Substrate yield on resolved solnums **85–90 %**". Two measurements exist:
- Diagnostic (`PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md:47`): **154/172 = 89.53 %**
  on a 172-solnum sample.
- Full landed manifest (`/tmp/audit6.py`): **41,963 / 49,248 = 85.21 %**.

The "85–90 %" range honestly brackets both; that is good practice. But be aware the
*upper* edge is a 172-sample point estimate and the *realized full-population* value sits
at the *lower* edge (85.2 %). **Confirmed**, with the note that 89.5 % is small-sample.

### C5 — End-to-end ~5.3 % — Overstated (mildly)
Note (§11): "End-to-end award → downloadable substrate **~5.3 %**". The diagnostic
derives `0.1736 × 0.344 × 0.8953 = 0.0535` — using the **89.5 % sample yield** and the
**34.4 % sample resolution**. Substituting the **full-population** figures
(33.2 % resolution, 85.21 % yield): `0.1736 × 0.332 × 0.8521 = 0.0491` ≈ **4.9 %**
(`/tmp/audit6.py` reported `end_to_end_estimate_pct = 4.91`). The "~5.3 %" is the
optimistic-sample composition; the realized number is ~4.9 %. The "~" softens it, but a
future planner sizing downstream cost should use **~4.9–5.0 %**, not 5.3 %.

### C6 — "Rank on base_type, decision-critical, mis-demotes ~8,400" — Overstated / wrong numbers
This is the mandate's headline test. Note (§5): "Ranking on `notice_type` mis-demotes
**~8,400** awarded solicitations (exactly the notices you want)." Re-derived winners
BOTH ways over the real `joined` table (`/tmp/audit2.py`, `/tmp/audit3.py`):
```
winners (either ranking)            = 49,248
winner notice_id differs (bt vs nt) =  5,738   # total selections that change at all
of those, nt-winner is still a real solicitation host:
  Combined Synopsis/Solicitation    =  2,335
  Solicitation                      =  2,031   (≈ 4,366 still host-tier)
MATERIAL harm (bt picks rank<=3 host, nt demotes to non-host) =   268
nt-winner lands on Award Notice base_type                     =    15
```
So switching to `notice_type` ranking changes the *selected notice* for **5,738**
solnums, not "~8,400," and the **actually-harmful** demotions (host → non-host) number
**268** — two orders of magnitude below the claim. ~76 % of the 5,738 changes still land
on a genuine Combined-Synopsis/Solicitation host. The direction is correct and base_type
*is* the better key, but "decision-critical" + "~8,400 mis-demoted" overstates the
blast radius. **Verdict: the recommendation (rank on base_type) is sound; the
justification's magnitude is overstated and the ~8,400 figure is wrong against the
artifact.** Recommend restating as: "ranking on notice_type changes ~5.7 K winner
selections and demotes ~270 solnums from a document-host tier to a non-host tier; prefer
base_type."

### C7 — notice_type flips to "Award Notice" when awarded — Confirmed (direction); wrong counts
Note (§5): "(inflated count 11,957 vs `base_type` 3,554)." The directional claim holds:
in `joined`, `notice_type='Award Notice'` = 40,540 vs `base_type='Award Notice'` = 24,293
(`/tmp/audit2.py`); on the winners set, 12,271 vs 7,063 (`/tmp/audit4.py`).
**No measurement in `bridge.duckdb` reproduces 11,957 or 3,554.** Candidates checked:
joined-rows (40,540/24,293), distinct-notices (40,540/24,293), winners (12,271/7,063).
The specific pair is unreproducible — likely from an earlier draft run on a different
slice. **Direction Confirmed; the cited numbers are Wrong / stale.**

### C8 — Multiplicity, "one observed had 51 siblings" — Confirmed but understated
Note (§5): "one observed had 51 siblings." `/tmp/audit4.py` over `joined`:
```
max_siblings = 8,365   (sol_norm 47QSMD20R0001 — a GSA government-wide vehicle)
next: 440, 205, 203, 165
```
Multiplicity is very real — the note is directionally right and the min-rank dedup is
the correct response — but "51" dramatically understates the tail. A future agent should
expect solnums with **thousands** of sibling notices (IDV/BPA umbrellas) and ensure the
`row_number()` dedup + spill config can handle them. Recommend replacing "51" with
"thousands for government-wide vehicles (observed max 8,365)."

### C9 — size_bytes corruption `((true-1) mod 10M)+1` — Wrong for this pipeline's data
Note (§8): "`size_bytes` is a LOWER BOUND, corrupt for files ≥10 MB: SAM returns
`((true_size - 1) mod 10_000_000) + 1` … declared 5 MB may be a real 45 MB file."
This is inherited verbatim from `sam_attachment_manifest.py:29-41`, which claims the
**identical endpoint and field** (`.../resources` → `opportunityAttachmentList[].
attachments[].size`).

Tested two ways:
1. `/tmp/audit_size2.py`: the landed manifest contains **4,830 rows with size_bytes
   > 10 MB, 113 rows > 100 MB, max = 249,199,712**. The corruption formula's output is
   mathematically bounded to `[1, 10,000,000]`. A corrupted field **cannot** emit 249 MB.
2. `/tmp/audit_size.py`: live HTTP **Range** probe (`bytes=0-0`, reads `Content-Range`
   total, no full download) on two declared-≥10 MB public rows:
   ```
   declared 249,199,712 → true Content-Length 249,199,712  (HTTP 206, exact match)
   declared 240,130,303 → true Content-Length 240,130,303  (HTTP 206, exact match)
   declared   9,671,259 → true 9,671,259   declared 410,069 → true 410,069
   ```

The `size` field returned by this pipeline's `/resources` endpoint is the **true size**,
not the corrupted lower bound. Either the prior-art claim was wrong, or SAM changed the
field, or the corruption only ever affected a different field/endpoint version. Either
way the note **as written is false for this manifest's data**. The conservative *advice*
("don't use size_bytes as a hard storage cap; enforce real size at fetch") remains
prudent defense-in-depth, but the **stated mechanism and the "5 MB may be 45 MB"
example are not true here** and should be removed or rewritten as: "size_bytes from
`/resources` has matched true Content-Length in all spot-checks (incl. ≥100 MB files) as
of 2026-06; the prior `((true-1) mod 10M)+1` corruption seen in
`sam_attachment_manifest.py` did NOT reproduce — still verify Content-Length at fetch."

### C10 — api.sam.gov ~5–10 req/day cap — Unverified (quota-preserved)
Note (§3). NOT independently tested: the mandate caps gateway calls at 1–2 and the quota
is near-exhausted; spending a probe to confirm a throttle message is poor use of the
budget. Corroborating evidence (non-live): `step5b_recall.py` / `step2c_validate.py`
both bound themselves to ≤8 gateway calls citing the daily quota, and the "900804 /
Message throttled out" code is the documented SI-NONFED throttle. **Plausible and
internally consistent; left Unverified by design.**

### C11–C13 — Frontend endpoint contract — Confirmed
`/tmp/audit_shape.py` + `/tmp/audit_frontend.py` (live, paced, browser headers):
```
sgs/v1/search                 → 200, _embedded.results present, page.totalElements present
result._id                    → "4b0645d73d3a4bd8a86eb6312ff0d3fc" (32-hex notice_id)
result.type.value             → "Combined Synopsis/Solicitation"
result.solicitationNumber     → present
resources, Accept application/json            → 406
resources, Accept application/json,text/plain,*/* → 200   (hal+json quirk)
opps/v2/opportunities/search (frontend)       → 401   (auth-gated)
```
All **Confirmed** exactly as the notes describe.

### C14 — No setsid binary on macOS — Confirmed
`which setsid` → "setsid not found"; `uname -srm` → "Darwin 25.5.0 arm64". The note's
prescription to use `os.setsid()` instead of a `setsid` binary is correct.

### C15 — objc fork crash; daemonize before threaded imports — Confirmed (mechanism sound)
The crash signature (`+[NSNumber initialize] may have been in progress in another
thread when fork() was called`) is the well-documented macOS Cocoa fork-safety abort:
forking a process that has already initialized Objective-C runtime state on multiple
threads is unsafe and the runtime aborts the child. The mitigation in `harvest.py:16-36`
and `sink_manifest.py:13-32` — `_daemonize()` (double-fork + `os.setsid()`) runs FIRST,
and `import duckdb, requests` happens AFTER the fork (line 36, with an explicit
`# noqa: E402`) — is the correct ordering: fork while still single-threaded, import
threaded libs only in the daemon child. The harvest then ran to completion across a
resume (see C16), which is consistent with the fix working. The diagnosis is sound.
(`OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` backstop is mentioned in the note but is NOT
present in any committed `pipelines/sam_gov/` file — it was an in-session launch env, not
codified.)

### C16 — Resumable checkpoint — Confirmed
`harvest.log` shows run 1 reaching ~5,833/49,248 then stopping. `harvest_resume.log`
shows run 2 starting with `todo=43,415` (i.e., it skipped the ~5,833 already done) and
finishing with `HARVEST_DONE`, `processed=43,415`. `harvest_ckpt.jsonl` has exactly
**49,248** lines (`wc -l`), status `{ok: 41,963, empty: 7,242, err: 43}`. Resume
demonstrably worked. **Confirmed.** (The "~5 resume-kills" exact count is not separately
provable from these two logs, but the resumability mechanism is proven.)

### C17 — Pace / rate / error-rate — Confirmed
Logs show steady **5.4–5.5 req/s** at the 0.12 s pace. Error total **43 / 49,248 =
0.087 %** ≈ the claimed "0.09 % over 49 K calls." HTTP breakdown: `{200: 49,205,
404: 42, 400: 1}`. **Confirmed.**

### C18 — DuckDB register/alias collision = silent empty — Confirmed (reproduced)
`/tmp/audit_register.py` (no R2 needed, tiny pyarrow table):
```
register('m', reader); CREATE TABLE m AS SELECT * FROM m   → count = 0     (silent!)
register('msrc', reader); CREATE TABLE m AS SELECT * FROM msrc → count = 5  (correct)
```
**Reproduced exactly.** This is the single most valuable footgun note — it passes
null-checks trivially (0 nulls in 0 rows). The companion advice ("always assert rowcount
> 0") is well-earned. Note: `verify.py` itself correctly uses the `msrc → m` pattern
(line 35), but the committed `sam_opps_attachment_manifest_90day_winners.py:362-363`
*also* uses `msrc → m` — good, the lesson was internalized.

### C19 — 2.88 M archived materialization STALLS — Plausible but not reproduced; self-contradicted
`/tmp/audit_arch.py`: archived = **2,839,948 rows** (≈ 2.84 M, not 2.88 M; the 2.88 M is
closer to active+archived combined = 2,917,631, or a slightly stale count). The stall
was not reproduced (doing so would require materializing the wide set, costly and risky).
Tension worth flagging: the **committed** pipeline (`build_bridge`, lines 158-173)
materializes the FULL active∪archived set into DuckDB via `scanner(columns=use)` —
exactly the "materialize the archived set into DuckDB" the §6 note warns stalls — and it
evidently does NOT stall (the bridge built `winners.parquet`). The reconciliation:
**narrow column projection** (8 cols) is what makes it tolerable; the stall §6 describes
was presumably a *wide* / unprojected materialization or an Arrow-streaming edge. As
written, §6 ("Don't materialize the 2.88 M archived set … it STALLS") over-generalizes;
the real lesson is "don't materialize it *wide* — project to the few needed columns
first," which the committed code already does. Recommend tightening the wording.

### C20 — BTREE pushdown only via scanner(filter=) — Plausible; not the committed path
The 90-day pipeline does NOT use `scanner(filter=)` at all — it projects columns and
materializes, then joins in DuckDB (no predicate pushdown). Actual `scanner(filter=...)`
pushdown is used in `sam_play1_target_select.py:172-174` (NAICS IN-list pushed into the
scan). The §6 claim that registered-relation + SQL `WHERE` reads near-full columns over
the WAN is consistent with Lance's architecture (DuckDB cannot push a SQL predicate back
through an Arrow reader into the Lance scanner), but this specific pipeline sidesteps it
via column projection rather than predicate pushdown. The note is **architecturally
plausible and a correct general caution**, but a reader should know the canonical pipeline
relies on *projection*, and the scalar-BTREE-pushdown win is realized elsewhere
(`sam_play1`). Not independently benchmarked here.

### C21 — ~2 % private, ~16 % null-mime/zero-size — Confirmed
`/tmp/audit6.py` over the landed manifest:
```
private        = 3,037 / 155,183 = 1.96 %
null_mime      = 15.83 %   zero_size = 15.83 %   (identical row set — same placeholders)
null_size      = 0.00 %
```
**Confirmed.** Useful refinement for the note: null-mime and zero-size are the *same*
rows (not two independent ~16 % populations) — a single placeholder/link-resource class.

### C22 — Offline join reproduces live within 2/500 — Confirmed (aggregate); masks per-row diffs
`/tmp/audit_offline_live.py` compared the live 500-sample (`translate.json`) against the
offline normalized-solnum membership in SAM active∪archived:
```
live_exact_hits = 172   offline_hits = 170   → net |Δ| = 2   ("within 2/500" ✓)
agree_both_hit  = 160   only_live = 12   only_offline = 10   (22 per-row disagreements)
```
The headline "hit *rate* within 2/500" is **Confirmed** (172 vs 170). But the near-equal
totals are a near-cancellation of 22 individual differences — the offline join is not a
strict superset/subset of the live result (keyword search recall vs exact normalized
match diverge in both directions). For a count-level corroboration this is fine; for
per-solnum fidelity, expect ~4 % set churn vs the live frontend.

### C23 — Manifest landed shape — Confirmed
`/tmp/audit6.py`: `count_rows() = 155,183`, 22 columns, 5 BTREE indices
(`notice_id_idx, sol_norm_idx, contract_award_unique_key_idx,
solicitation_identifier_idx, resource_id_idx`). Matches `verify.py`'s hard-coded
`rowcount==155183` / `cols==22` assertions. `live_drift.json`: 15/15 exact resource_id
set match, VERDICT PASS — parsing/landing fidelity independently corroborated.

---

## Methods & reproducibility

All scripts run via the mandated wrapper:
```
doppler run --project core-x --config prd -- \
  uv run --quiet --with pylance --with pyarrow --with 'duckdb>=1.5,<2' --with requests \
  python /tmp/<script>.py
```
R2 `storage_options`: `{aws_access_key_id: $R2_ACCESS_KEY_ID, aws_secret_access_key:
$R2_SECRET_ACCESS_KEY, endpoint: $R2_ENDPOINT, region: 'auto'}`.

Audit scripts written (all read-only):
- `/tmp/audit1.py` — bridge.duckdb table sizes + resolution rate (C3).
- `/tmp/audit2.py` — base_type vs notice_type winner divergence; type distributions (C6, C7).
- `/tmp/audit3.py` — material-harm quantification (host→non-host demotion) (C6).
- `/tmp/audit4.py` — winners/joined Award-Notice counts; sibling multiplicity (C7, C8).
- `/tmp/audit5.py` — award-grain solnum fill + PIID-path incremental reachability (C1, C2).
- `/tmp/audit6.py` — landed manifest funnel reconciliation + access/mime/size dists (C4, C5, C21, C23).
- `/tmp/audit_size.py` — live Range probe true-size vs declared size_bytes (C9).
- `/tmp/audit_size2.py` — size_bytes >10 MB count proving uncorrupted (C9).
- `/tmp/audit_register.py` — DuckDB register/alias collision reproduction (C18).
- `/tmp/audit_frontend.py` — hal+json 406, open sgs/v1, 401 v2 (C11–C13).
- `/tmp/audit_shape.py` — search response shape `_id`/`type.value` (C11).
- `/tmp/audit_offline_live.py` — offline-join vs live-500 hit comparison (C22).
- `/tmp/audit_arch.py` — active/archived row counts (C19).

Shell checks: `which setsid`, `uname -srm`, `wc -l harvest_ckpt.jsonl`, checkpoint
status counter; `cat harvest.log` / `harvest_resume.log`; greps over
`pipelines/sam_gov/*.py` and the companion docs.

Files read: `sam_opps_attachment_manifest_90day_winners.py`,
`sam_attachment_manifest.py`, `sam_play1_target_select.py`, all `/tmp/bridge_diag/*.py`,
`PRIME_AWARD_ATTACHMENT_BRIDGE_DIAGNOSTIC.md`.

Live SAM.gov GETs issued: ~10 total, all paced ≥0.3 s, browser headers — frontend
`sgs/v1/search` (3), `/resources` (4), `/v2/.../search` (1), download Range `bytes=0-0`
(4). No `api.sam.gov` gateway calls. No bytes downloaded beyond a single Range byte.

---

## What the notes get RIGHT (be fair)

- The **entire frontend access contract** (§3) is precisely correct: open `sgs/v1/search`,
  `_id` as notice_id, `type.value`, the hal+json 406 quirk, the auth-gated v2 endpoint.
  This is the highest-value, most reusable section and every probe confirmed it.
- The **register/alias silent-empty footgun** (§6) is real, reproducible, and exactly the
  kind of trap that wastes an iteration — the "assert rowcount > 0" rule is gold.
- **Daemonization + resumable checkpoint** (§2): the macOS fork mechanism is correctly
  diagnosed, the `os.setsid()` (no binary) point is correct, and resumability is proven in
  the logs. Strong, hard-won operational knowledge.
- **base_type > notice_type for ranking** (§5) — the *recommendation* is correct even
  though its magnitude is overstated; base_type is the right key for document-host identity.
- **Offline-join-not-live-search** (§4) — the architectural call is right and the
  count-level corroboration holds (172 vs 170).
- The **funnel ranges** (§11) are honestly stated as *ranges*, and the "filter, not a
  leak" framing is the correct read — the structural ceiling (C2) is genuinely structural.
- **Pacing / error-rate** numbers (§3) match the logs almost exactly.
- The **2 % private / 16 % placeholder** consumer caveats (§8) are accurate.

## What the notes MISS or should add

- **C9 is the dangerous one:** the inherited size_bytes-corruption claim is false for this
  pipeline's `/resources` data (verified to 249 MB). Leaving it as stated will cause a
  future agent to under-budget storage or mis-trust true sizes. Rewrite to "verified
  uncorrupted as of 2026-06; still confirm Content-Length at fetch."
- **C6 magnitude:** replace "~8,400 mis-demoted / decision-critical" with the measured
  "~5.7 K winner selections change, ~270 lose a host tier." Keep the recommendation.
- **C7 numbers:** drop or correct the unreproducible "11,957 vs 3,554."
- **C8 multiplicity tail:** "51 siblings" understates reality by ~160×; cite the real max
  (8,365, a government-wide vehicle) so agents size dedup/spill correctly.
- **The committed pipeline does NOT daemonize.** §2's daemonization lived only in the
  throwaway `/tmp/harvest.py`; `sam_opps_attachment_manifest_90day_winners.py` has no
  `_daemonize`. A future agent running the *committed* `harvest` stage for a multi-hour
  sweep gets none of the resume protection. Either port `_daemonize` into the committed
  pipeline or have the note say explicitly "the committed pipeline must be launched
  detached yourself."
- **End-to-end should read ~4.9 %**, not 5.3 %, for full-population sizing (C5).
- **§6 stall wording over-generalizes:** the committed bridge *does* materialize the full
  2.84 M archived set (narrow columns) without stalling. The real rule is "project to the
  needed columns before materializing," not "never materialize."
- **null-mime ≡ zero-size** are the same rows, not two independent populations (C21).
- Minor: archived is **2.84 M**, not 2.88 M (2.88 M ≈ combined). The hierarchy label
  "Modification" in §5 prose maps to SQL `MODIFICATION/AMENDMENT/CANCEL` — harmless but
  worth aligning so a reader greps the right token.

---

## Overall assessment

**Safe and useful for a future agent on the operational/runtime axis; requires two
corrections before its analytical claims should be trusted verbatim.**

The runtime knowledge (frontend contract, DuckDB footguns, daemonization, pacing,
checkpointing, schema gotchas) is excellent, reproducible, and would save a successor
real iterations — this is exactly what operating notes should be. The analytical /
funnel section is mostly sound (resolution, fill, yield, structural ceiling all
verified), but two load-bearing claims do not survive scrutiny:

1. **size_bytes corruption (§8)** — false for this pipeline's data; correct before relying on it.
2. **base_type-ranking impact (§5)** — recommendation right, magnitude overstated ~30×;
   correct the numbers.

With those two fixes (plus the smaller adjustments above), the document is trustworthy.
As-is, an agent who takes §8's size claim and §5's "~8,400" at face value would
mis-plan. The single most important correction: **strike the size_bytes
`((true-1) mod 10M)+1` corruption claim — measured true Content-Length equals declared
size up to 249 MB.**
