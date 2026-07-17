# Sidecar gap report — 2026-07-17 · military installations overlay

- **Date:** 2026-07-17
- **Serving artifact at compile:** `query_sidecar_20260717T020529Z` (instance `srv-d97gbf57vvec73c5r2a0`)
- **Session topic:** capital-provider intake instrument — payment regimes, territory
  clustering of collection members' active awards, military-installation overlay for the
  Territory Map viewer tab (operator directive: universe-wide, not construction-only).

## Entry 1 — where do a collection's member awards sit relative to military installations?

1. **Intent:** overlay US military installations (name, branch, state, lat/lon) on member
   work territories, for any market collection, during intake calls.
2. **Why not the sidecar:** `missing table` — no installations reference exists in the
   serving set. Landed this session as `s3://data-sink/active/military_installations_lance`
   (DoD MIRTA points, 831 rows; BTREE on state_code).
3. **What I ran instead:** direct pylance read of the Lance dataset (all columns), filtered
   `country='USA' AND operational_status='act'` in python, baked into the viewer JSON.
4. **Cost:** seconds (831 rows) — cost is not the issue; servability is: any future
   sidecar query joining installations to PoP centroids/state cuts cannot run warm.
5. **Recurrence:** recurring — operator states the overlay is relevant across all
   market collections; territory/installation proximity questions are a standing shape
   for the provider-intake program.

## Footer — ranking

Single entry; operator-directed promotion ("take care of the installation overlay …
sidecar gap cycle, do that as well", 2026-07-16 session). Structural gate satisfied by
directive; cost trivial (831 rows, one generic copy step).

---

## Disposition (build cycle 2026-07-17, artifact `query_sidecar_20260717T030649Z`)

| Entry | Verdict | Shipped |
|---|---|---|
| 1 · installations overlay | **Promote** (operator-directed; structural, trivial cost) | `military_installations` — Tier D generic copy of `military_installations_lance`, sorted `state_code`, 831 rows, exact parity |

**Build scope block (adjacency sweep):**
- From demand: the full landed schema (site/feature names + description, component,
  state, country, operational status, joint-base flag, lat/lon) — every column a
  territory/overlay consumer plausibly asks for ships in the one build.
- Adjacency riders: none needed — the dataset IS the reference; no build-time joins,
  so no join-side sweep surface.
- Parked (structural-gated): `isFirrmaSite`/`isCui` source flags — compliance
  attributes with no foreseeable GTM question; not landed to Lance. A zip3/centroid →
  nearest-installation crosswalk considered and parked: the proximity shape runs warm
  at query time (measured below), no derived table justified.

**Measured before → after:** before = not answerable on serving (pylance read of the
Lance dataset, seconds + creds required). After: full-table read 0.9 ms; ~40km
proximity shape around a point (San Diego test: 18 sites) 7.8 ms on serving.

**Guide updated in this PR:** catalog row, header count 96 → 97 manifest-tracked
(serving shows 99 incl. the UCC pair from #1179's cycle), §4 proximity pattern.
