# Equipment-yard presentation — query-backed narrative deck

Market-wide → state → one yard's zip. Every deck section is a named, parameterized
query in `queries.py` against the query-sidecar; nothing hardcoded. Acts 1–2 are
zip-independent; Act 3 takes `(zip, radius)`.

## Regenerate for a prospect

```bash
doppler run -p core-x -c prd -- \
  python3 demos/equipment_yard/queries.py snapshot --zip 79925 --state TX --radius 50
```

Writes `snapshot_<zip>.json` (artifact-pinned via `require_artifact` — a mid-run
artifact swap 409s instead of mixing snapshots).

## Present

```bash
cd demos/equipment_yard && python3 -m http.server 8756
# open http://localhost:8756/deck.html   (↑/↓ or scroll; hover any mark)
```

`deck.html` currently fetches `snapshot_79925.json`; point it at another zip's
snapshot by changing the fetch (or symlink). Sections: title → national bucket
portrait → states (TX) → TX counties (EL PASO) → local headline → radar map →
expiry clock → plain-English work cards → holders + fresh-money tables → close.

## Query registry (one per section)

| name | act | mart(s) |
|---|---|---|
| national_buckets | 1 | combo_award_active_state ⋈ naics_psc_equipment_needs |
| national_states | 1 | txn_events_combo ⋈ needs (fy ≥ 2024, PoP state) |
| state_counties | 2 | txn_events_combo_by_geo ⋈ needs |
| local_buckets / local_awards | 3 | gtm_open_awards (haversine ≤ r) ⋈ needs, ⋈ naics_psc_deliverable |
| local_expiry / local_subout | 3 | same local CTE |
| local_fresh_money | 3 | local UEI set (≥$100k local equipment award) ⋈ gtm_txn_events_slim ⋈ needs, 90d |
| local_work_language | 3 | local ⋈ needs ⋈ deliverable |

Known residuals: the combo equipment verdicts classify some medical-logistics
combos in scope (e.g. 325412×6505); the fresh-money seed floor (`total_obligation
>= 100000` on the local equipment award) suppresses most of it — curate the final
firm list per prospect.
