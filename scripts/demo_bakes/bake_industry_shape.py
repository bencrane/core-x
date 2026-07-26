"""Bake — INDUSTRY_SHAPE sector rollup for the 'shape of the $2.4T' card (as-run 2026-07-26).

Active book (combo_award_active_state) by NAICS6 -> KLEMS industry (same mapping as
bake_cost_structure.py, incl. PATCH + public-admin residual) -> 8 sectors:
  Manufacturing (all 3xx KLEMS codes incl. 311FT/313TT/315AL/3361MV/3364OT)   [accent]
  Professional, scientific & technical (5411,5415,5412OP,55)
  Construction (23)                                                            [accent]
  Facilities, administrative & waste (561,562)
  Transportation & warehousing (481..487OS,493)
  Information & software (513,PUB,512)
  Health, education & social (61,621,622HO,624)
  Everything else (residual incl. public admin)
Parity gate: sector dollars sum EXACTLY to the book total. Output values go into
gc-hq-new apps/platform-app/src/map/rehearsal.ts INDUSTRY_SHAPE (keep provenance comment).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_industry_shape.py
(mapping code: import from bake_cost_structure or replicate its to_pc()/PATCH)
"""
MFG={"321","327","331","332","333","334","335","3361MV","3364OT","337","339","311FT","313TT","315AL","322","323","324","325","326"}
GROUPS=[("Manufacturing (incl. aerospace & defense)",MFG,True),
 ("Professional, scientific & technical services",{"5411","5415","5412OP","55"},False),
 ("Construction",{"23"},True),
 ("Facilities, administrative & waste services",{"561","562"},False),
 ("Transportation & warehousing",{"481","482","483","484","485","486","487OS","493"},False),
 ("Information & software",{"513","PUB","512"},False),
 ("Health, education & social services",{"61","621","622HO","624"},False)]
if __name__=="__main__":
    raise SystemExit("Doc-bearing skeleton: weights come from bake_cost_structure's mapping "
                     "(mapped[pc] dollars); group per GROUPS, residual = Everything else.")
