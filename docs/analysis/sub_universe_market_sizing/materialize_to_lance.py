#!/usr/bin/env python3
"""Materialize the frozen market-sizing results (results.json) into a queryable
Lance dataset in the SoR, section-tagged. Re-runnable: reads results.json (which
market_sizing.py produces) and snapshot-overwrites the dataset.

  SoR  s3://data-sink/active/gtm_sub_universe_market_sizing/
       (1 row per result record; section-tagged; BTREE section; JSON payload)

    doppler run -p core-x -c prd -- /Users/benjamincrane/core-x/.venv/bin/python \
      docs/analysis/sub_universe_market_sizing/materialize_to_lance.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402
from apps.catalyst_api.src import config  # noqa: E402

OUT = "s3://data-sink/active/gtm_sub_universe_market_sizing/"


def main() -> int:
    R = json.loads((HERE / "results.json").read_text())
    rows: list[dict] = []
    # meta row (sources + versions + definitions) so the dataset is self-describing
    rows.append({"section": "_meta", "idx": 0, "label": "sources+versions+definitions",
                 "payload": json.dumps({"sources": R["sources"], "definitions": R["definitions"],
                                        "target_set": R["target_set"]})})
    for label, recs in [("thresholds", R["thresholds"]),
                        ("sector_composition_5M", R["sector_composition_5M"]["sectors"]),
                        ("cut_the_top", R["cut_the_top"]["rows"]),
                        ("widest_subs", R["widest_subs"])]:
        for i, rec in enumerate(recs):
            rows.append({"section": label, "idx": i,
                         "label": str(rec.get("name") or rec.get("threshold")
                                      or rec.get("remove_top") or rec.get("naics2") or i),
                         "payload": json.dumps(rec)})
    rows.append({"section": "breadth_distribution_5M", "idx": 0, "label": "quantiles",
                 "payload": json.dumps(R["breadth_distribution_5M"])})

    tbl = pa.Table.from_pylist(rows, schema=pa.schema([
        ("section", pa.string()), ("idx", pa.int64()),
        ("label", pa.string()), ("payload", pa.large_string())]))
    ds = write_indexed_dataset(tbl.to_reader(), OUT, [("section", "BTREE")],
                               storage_options=config.r2_storage_options())
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows()}  "
          f"sections={sorted(set(r['section'] for r in rows))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
