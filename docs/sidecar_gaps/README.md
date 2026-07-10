# Sidecar gap reports — demand capture for the query-sidecar

Any agent that answers a data question WITHOUT the query-sidecar (Lance scan, pylance
probe, catalyst endpoint, DuckDB-over-Lance, degraded answer) writes a gap entry here.
This directory is the demand ledger that drives what gets promoted into the artifact —
the manifest grows only where demand is proven.

- **File convention:** `SIDECAR_GAP_REPORT_<YYYY-MM-DD>-<short-slug>.md` (one per session/topic).
- **Entry schema (5 fields, exact):** Intent · Why-not-the-sidecar (`missing table` /
  `missing column(s)` / `wrong grain` / `missing sort` / `freshness required` /
  `didn't know it was there`, + specific dataset/columns) · What-I-ran-instead (exact
  query + only the columns actually needed) · Cost (wall time, rows scanned vs returned) ·
  Recurrence (one-off vs recurring, honestly).
- **Header:** date, sidecar artifact stamp from /healthz, session topic.
  **Footer:** gaps ranked by recurrence × cost. Demand only — no proposed solutions.
- **Lifecycle:** the build cycle (`/sidecar-gaps` Mode 2) gates each entry —
  promote (manifest edit + parity-gated rebuild) / routing fix (guide/skill) /
  correctly-on-Lance (freshness/coverage) — appends a **Disposition** section, and moves
  the file to `processed/`.
- **Adjacency sweep (mandatory when a build fires):** a rebuild is a committed fixed
  cost; before it runs, every join/projection in scope is swept for the columns a
  consumer will ask for in the same session (attribution, type codes, names, sibling
  columns), and the next 2–3 analyst questions are simulated against the post-build
  artifact. Demand evidence gates STRUCTURAL growth (tables/grains/sort copies);
  column-grain adds riding a paid-for build ship opportunistically. One build per
  cycle — "that column needs another rebuild" is a failed cycle.

Schema authority: docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md §Gap reporting.
