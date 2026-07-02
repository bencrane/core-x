"""Step 7 — VERIFY: read the materialized SoR datasets back from R2 and check integrity.

Counts, provenance (must be 100% the in-session Opus model_id), reconciliation (profiles <->
categories both directions; non-play == placeholder rows), on-disk enum validity, distributions,
enrichment coverage, and index presence. Read-only.

Run under: doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.verify_datasets
"""
from __future__ import annotations

import duckdb
import pyarrow as pa

from ._util import load_module

M = load_module()


def main() -> None:
    prof_ds, cat_ds = M._ds(M.PROFILE_URI), M._ds(M.CATEGORIES_URI)
    p, c = prof_ds.to_table(), cat_ds.to_table()
    sca_codes, _, soc_codes, _, _, _ = M._vocabularies()
    con = duckdb.connect()
    con.register("p", p)
    con.register("c", c)
    con.register("soc", pa.table({"soc_code": soc_codes}))
    con.register("sca", pa.table({"sca_code": sca_codes}))
    q = lambda s: con.execute(s).fetchall()      # noqa: E731
    one = lambda s: con.execute(s).fetchone()[0]  # noqa: E731

    print("=== COUNTS ===")
    print("profiles rows          ", one("select count(*) from p"))
    print("profiles distinct combo", one("select count(*) from (select distinct naics_code,psc_code from p)"))
    print("categories rows        ", one("select count(*) from c"))
    print(f"\n=== PROVENANCE (must be 100% {M.AGENT_MODEL_ID}) ===")
    print("profile model_id  ", q("select model_id,count(*) from p group by 1"))
    print("category model_id ", q("select model_id,count(*) from c group by 1"))
    print("prompt_version    ", q("select prompt_version,count(*) from p group by 1"))
    print("\n=== RECONCILIATION ===")
    print("profile combos missing from categories",
          one("select count(*) from (select naics_code,psc_code from p except select naics_code,psc_code from c)"))
    print("category combos missing from profiles ",
          one("select count(*) from (select naics_code,psc_code from c except select naics_code,psc_code from p)"))
    np_ = one("select count(*) from p where not is_labor_play")
    ph = one("select count(*) from c where soc_code is null")
    print(f"non-play profiles {np_}  == placeholder cat rows (null soc) {ph}  -> {np_ == ph}")
    print("play combos with >=1 real category",
          one("select count(*) from (select distinct naics_code,psc_code from c where soc_code is not null)"))
    print("\n=== ON-DISK ENUM VALIDITY ===")
    print("category rows w/ soc OUT of vocab",
          one("select count(*) from c where soc_code is not null and soc_code not in (select soc_code from soc)"))
    print("category rows w/ sca OUT of vocab",
          one("select count(*) from c where sca_code is not null and sca_code not in (select sca_code from sca)"))
    print("play profiles with 0 real categories",
          one("""select count(*) from p where is_labor_play and (naics_code,psc_code) not in
                 (select naics_code,psc_code from c where soc_code is not null)"""))
    print("\n=== DISTRIBUTIONS ===")
    print("is_labor_play  ", q("select is_labor_play,count(*) from p group by 1"))
    print("top_confidence ", q("select top_confidence,count(*) from p group by 1 order by 2 desc"))
    print("role_class     ", q("select role_class,count(*) from c where role_class is not null group by 1"))
    print("confidence     ", q("select confidence,count(*) from c where confidence is not null group by 1"))
    print("off_pattern    ", q("select off_pattern,count(*) from c where off_pattern is not null group by 1"))
    print("resolution_lvl ", q("select resolution_level,count(*) from p group by 1 order by 2 desc"))
    print("wage/growth enrichment present:",
          "a_median non-null", one("select count(*) from c where a_median is not null"),
          "| ep_growth non-null", one("select count(*) from c where ep_growth_2024_2034_pct is not null"))
    print("\n=== INDEXES ===")
    print("profile   ", [(i.get('name'), i.get('type')) for i in prof_ds.list_indices()])
    print("category  ", [(i.get('name'), i.get('type')) for i in cat_ds.list_indices()])


if __name__ == "__main__":
    main()
