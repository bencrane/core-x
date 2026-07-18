#!/usr/bin/env python3
"""READ-ONLY provenance probe — extractor-tag distribution in govcon_award_requirements.

Settles the audit's decisive open question: are the live labor_category rows pure
regex-lane, or regex+LLM blended? And did the LLM lane actually land the
equipment_capability / past_performance types the regex lane provably cannot emit?
Resolved by counting rows per (requirement_type, extractor-family) directly off R2.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/award_requirements_provenance_probe.py > /tmp/req_prov.json
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import duckdb
import lance

REQ = "s3://data-sink/active/govcon_award_requirements/"


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def rows(con, q):
    cur = con.execute(q)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    ds = lance.dataset(REQ, storage_options=so)
    con.register("req", ds)

    out: dict = {"as_of": dt.datetime.now(dt.timezone.utc).date().isoformat(), "req_uri": REQ}
    out["columns"] = [f.name for f in ds.schema]
    out["total_rows"] = con.execute("SELECT count(*) FROM req").fetchone()[0]

    has_extractor = "extractor" in out["columns"]
    out["has_extractor_column"] = has_extractor
    # extractor family = text before ':' (regex / llm) when present
    fam = "split_part(extractor, ':', 1)" if has_extractor else "'(no_extractor_col)'"

    # requirement_type x validated
    out["by_requirement_type"] = rows(con, """
        SELECT requirement_type,
               count(*) AS rows,
               count(*) FILTER (WHERE validated) AS validated_rows,
               count(DISTINCT requirement_value) AS distinct_values
        FROM req GROUP BY 1 ORDER BY rows DESC""")

    if has_extractor:
        out["by_extractor"] = rows(con, f"""
            SELECT extractor, count(*) AS rows,
                   count(*) FILTER (WHERE validated) AS validated_rows
            FROM req GROUP BY 1 ORDER BY rows DESC""")
        out["by_extractor_family"] = rows(con, f"""
            SELECT {fam} AS extractor_family, count(*) AS rows,
                   count(*) FILTER (WHERE validated) AS validated_rows
            FROM req GROUP BY 1 ORDER BY rows DESC""")
        # the crux: requirement_type x extractor_family
        out["type_x_extractor_family"] = rows(con, f"""
            SELECT requirement_type, {fam} AS extractor_family,
                   count(*) AS rows,
                   count(*) FILTER (WHERE validated) AS validated_rows
            FROM req GROUP BY 1,2 ORDER BY requirement_type, rows DESC""")
        # labor_category specifically
        out["labor_category_by_extractor"] = rows(con, f"""
            SELECT extractor, count(*) AS rows,
                   count(DISTINCT requirement_value) AS distinct_values,
                   count(*) FILTER (WHERE validated) AS validated_rows
            FROM req WHERE requirement_type='labor_category'
            GROUP BY 1 ORDER BY rows DESC""")
        # the two types the regex lane allegedly cannot produce
        out["llm_only_types_by_extractor"] = rows(con, f"""
            SELECT requirement_type, {fam} AS extractor_family, count(*) AS rows
            FROM req WHERE requirement_type IN ('equipment_capability','past_performance')
            GROUP BY 1,2 ORDER BY 1,3 DESC""")
    else:
        # fall back: still characterize the two contested types
        out["llm_only_types_present"] = rows(con, """
            SELECT requirement_type, count(*) AS rows,
                   count(*) FILTER (WHERE validated) AS validated_rows
            FROM req WHERE requirement_type IN ('equipment_capability','past_performance')
            GROUP BY 1 ORDER BY rows DESC""")

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    print("DONE", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
