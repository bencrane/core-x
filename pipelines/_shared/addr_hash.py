"""Canonical address-hash SQL — the md5 join key for the geocode crosswalk.

``addr_hash_sql()`` is the LEFT JOIN key between ``geocode_xwalk`` (address → lat/lon)
and every map builder that reads coordinates back from it. The danger it guards: if two
call sites emit even a byte-different normalization, the coordinate join silently yields
all-NULL coords — a blank map, no error, no exception. That is why this expression lives
in ONE place and every builder imports it; the join key cannot drift if there is only one
definition.

Importers (all three MUST resolve to this single definition):
  • pipelines/usaspending/geocode_xwalk.py        — writes the crosswalk (addr_hash, coords)
  • pipelines/serving/materialize_winners_map.py  — reads coords for the winners map
  • pipelines/serving/materialize_company_map.py  — reads coords for the company map

The byte-identity + fixture-hash parity guarantee is pinned by
pipelines/_shared/tests/test_addr_hash_parity.py.

Pure stdlib (string builders only) — safe to import anywhere; no DuckDB/Lance needed.
The args are SQL column references (or expressions) spliced verbatim into the emitted
DuckDB SQL, so callers pass column names like ``"street"`` or ``"e.physical_address_line_1"``.
"""
from __future__ import annotations


def addr_hash_sql(street: str, city: str, state: str, zip_: str) -> str:
    """Canonical addr_hash expression. The ONLY definition of the crosswalk join key —
    do not copy; import it. Normalization: street is whitespace-collapsed + uppercased,
    city/state are trimmed + uppercased, zip is reduced to its first 5 digits, all joined
    by ``|`` and md5'd."""
    return ("md5("
            f"upper(regexp_replace(trim(coalesce({street},'')),'\\s+',' ','g'))||'|'||"
            f"upper(trim(coalesce({city},'')))||'|'||"
            f"upper(trim(coalesce({state},'')))||'|'||"
            f"substr(regexp_replace(coalesce({zip_},''),'[^0-9]','','g'),1,5))")


def _zip5_sql(zip_: str) -> str:
    """First 5 digits of a zip column (digits-only) — byte-identical to the zip5 segment
    inside ``addr_hash_sql`` so the crosswalk's stored ``zip5`` and the hash agree."""
    return f"substr(regexp_replace(coalesce({zip_},''),'[^0-9]','','g'),1,5)"
