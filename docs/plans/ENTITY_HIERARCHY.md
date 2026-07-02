# Federal Entity Hierarchy — `uei → immediate_parent → ultimate_parent`

**Status:** LANDED (2026-07-02). Worker `pipelines/resolution/entity_hierarchy.py`.
**Dataset:** `s3://data-sink/active/entity_hierarchy/` (Lance v2.1, overwrite-snapshot).
**Ops ledger:** `ops.entity_hierarchy_runs`.

The corporate-family spine for the federal record. One row per child UEI, carrying the
**immediate** parent (the raw reported edge) and the **ultimate** parent (top of family,
derived by cycle-safe transitive closure). This is the §3.2 deliverable of the GTM identity
refactors — the complement to company dedup: dedup collapses one company logged twice;
hierarchy relates *distinct* subsidiaries under one parent. Parent/subsidiary is
**authoritative in the federal record** and is NOT inferred from domain/LinkedIn.

---

## 1. Grain & schema

One row per child UEI that carries a parent edge in any authoritative source. **148,766 rows**
(2026-07-02 snapshot), strictly 1 row/uei.

| Column | Type | Index | Meaning |
|---|---|---|---|
| `uei` | string | BTREE | the child entity (PK) |
| `immediate_parent_uei` | string | BTREE | raw reported immediate parent; **null** when only an ultimate is known (subaward-only children) |
| `immediate_parent_name` | string | | legal name of the immediate parent (best across sources) |
| `ultimate_parent_uei` | string | BTREE | top-of-family, ALWAYS populated (closure root, or FSRS ultimate) |
| `ultimate_parent_name` | string | | legal name of the ultimate parent |
| `hierarchy_depth` | int32 | | hops child→ultimate (1 direct … 4 observed); **null** for subaward-only |
| `in_cycle` | bool | | immediate chain forms a cycle; ultimate canonicalized to `min(uei)` in the cycle |
| `parent_source` | string | BITMAP | provenance of the edge: `recipient_lookup` \| `subaward_search` \| `govcon_active_awards` |
| `snapshot_date` | date | | recompute stamp |

**Non-child roots get no row.** Downstream rolls a UEI to its family top via
`coalesce(entity_hierarchy.ultimate_parent_uei, uei)`.

---

## 2. Why both columns (the grain decision — resolved by live analysis)

The handoff left "immediate vs ultimate" as an open decision. It is resolved by measuring the
data, not by choosing:

- `recipient_lookup.parent_uei` is the **immediate** parent, not the ultimate. 1,194 parents
  are themselves children; 4,296 edges are ≥2-hop chains. If it were the ultimate, no parent
  could have its own parent.
- So the immediate edge is the authoritative **atom**, and the ultimate is **derived** by
  transitive closure. Storing both dominates either single choice: immediate preserves the
  reported structure; ultimate powers federal-$ roll-up to the top of the family.
- Depth distribution (snapshot): depth-1 **78,352** · depth-2 **4,085** · depth-3 **42** ·
  depth-4 **1** · subaward-only (no immediate) **66,286**.
- Worked example: `iSIGHT SECURITY → … → MANDIANT` (depth 4); `LINEAR TECHNOLOGY → ANALOG
  DEVICES` (depth 3); `RSC ACQUISITIONS → BERKSHIRE HATHAWAY` (depth 3). Akima's children roll
  up *through* Akima to **NANA REGIONAL CORPORATION** — the immediate edge shows Akima, the
  ultimate shows NANA.

---

## 3. Sources (authoritative, in-SoR only)

All parent linkage in the SoR is USAspending-derived. Coverage (distinct child edges):

| Source | URI | Edge | Children | Role |
|---|---|---|---|---|
| `recipient_lookup` | `…/usaspending/recipient_lookup/` | `uei → parent_uei` (IMMEDIATE) | 82,383 | **primary**; recipient dimension, 0 parent-instability (functional graph) |
| `subaward_search` | `…/usaspending/subaward_search/` | `(sub_)awardee_uei → (sub_)ultimate_parent_uei` (ULTIMATE) | 83,808 | adds subawardees never seen as primes; immediate unknown |
| `govcon_active_awards` | `…/active/govcon_active_awards/` | `recipient_uei → recipient_parent_uei` (immediate) | 6,858 | low-precedence fill (506 net-new) |

**Union = 148,766 distinct children** (recipient_lookup alone would be 82,383 — a 44% coverage
loss). Precedence for the immediate edge: `recipient_lookup` > `govcon_active_awards`; subaward
supplies only the ultimate for children absent from the immediate graph.

### SAM `entity_registrations` is deliberately NOT a source
The SAM v2 **public** monthly extract in the SoR carries **no parent UEI** — only the four
EVS-source flags (`{immediate,ultimate,hq,domestic}_parent_evs_source`), and even those are not
projected into the 18-column `entity_registrations` dataset. The actual parent-entity UEIs live
in the API-gated hierarchy block, not the flat file. The handoff's "SAM registration →
ultimate-parent fields" is therefore not realizable from the in-SoR SAM data.

---

## 4. Build (compute plane)

`recipient_lookup ∪ govcon` immediate edges → **cycle-safe transitive closure** →
fold in `subaward` ultimates for children with no immediate edge. 100% DuckDB, bounded
(`memory_limit`, disk spill), reading committed Lance only. Output → `lance.write_dataset`
(R2, v2.1, **overwrite** snapshot) → BTREE/BITMAP indexes on R2 → one `ops.*` row.

**Closure:** recursive walk up the functional immediate-graph to a terminal (a node that is not
itself a child). Depth-capped at 45 (real max = 4); any origin that fails to terminate is a
genuine **cycle** — flagged `in_cycle=true` and canonicalized to `min(uei)` in the walk
(deterministic). 409 UEIs are in cycles.

**Determinism:** every collapse (`sub`, `govcon`, name map) breaks frequency ties with
`row_number() … ORDER BY cnt DESC, <key> ASC`, so a rebuild on identical inputs is
byte-reproducible.

### Two correctness invariants enforced at build time (fail-closed)
1. **Grain:** `rows == distinct uei` and `ultimate_parent_uei` is never null.
2. **Completeness:** `rows == |imm.child ∪ sub.child|` — catches any silent edge-drop.

> **Lesson encoded in the worker.** A Lance `to_reader()` Arrow stream is **single-use**.
> The subaward prime/sub UNION-ALL originally referenced one registered reader twice, silently
> under-reading the second leg (subaward children 43k instead of 84k). The fix: drain the reader
> into a table once, then reference the table. The completeness gate exists to catch exactly
> this class of bug.

---

## 5. Downstream usage

- **Federal-$ roll-up to ultimate parent** (the ICP signal): join award $ on `uei`, group by
  `coalesce(h.ultimate_parent_uei, uei)`. **For corporate ownership, filter
  `parent_source='recipient_lookup'`** — `subaward_search` ultimates include FSRS grant
  *pass-through* (e.g. state Departments of Education → sub-recipients), which is legitimate
  linkage but not corporate ownership. The `parent_source` BITMAP exists to make this cut.
- **Family enumeration:** `SELECT uei FROM entity_hierarchy WHERE ultimate_parent_uei = :X`.
- **Company-dedup validation (§3.1):** the authoritative family structure to check a name-gated
  collapse against — a dedup rule that merges two UEIs sharing an ultimate parent but with
  distinct awards is fusing real subsidiaries.

---

## 6. Rebuild / rollback

```bash
doppler run -p core-x -c prd -- uv run --no-project \
  --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with 'psycopg[binary]>=3.2' \
  python3 pipelines/resolution/entity_hierarchy.py <init_ops|build|reindex|verify>
```

`reindex` rebuilds indexes in place (recovers an interrupted index pass without re-materializing).
Rollback = `lance.dataset(uri, version=<prior>).restore()` — the recompute is overwrite-snapshot,
so the prior version is retained by Lance as the anchor.

---

## 7. Known limitations

- **Name coverage floor:** ~10.9k immediate and ~14.3k ultimate parent UEIs have no legal name
  in any USAspending award/recipient name field. The **UEIs (resolution keys) are 100%
  populated**; names are best-effort. SAM `legal_business_name` (888k v2 UEIs) is a future name
  source if display coverage must rise.
- **subaward-only children have `hierarchy_depth = NULL`** (only an FSRS ultimate is known, no
  immediate chain to measure).
- **Cadence:** materialized on demand today. Wiring a control-plane schedule (Trigger.dev) is a
  follow-up; the sources refresh independently.
