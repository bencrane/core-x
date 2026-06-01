"""Single source of truth for the SAM.gov <-> PDL identity-resolution spine.

Pure strings / pure-Python builders — NO heavy imports (no modal / duckdb /
lance). Imported by BOTH the Modal worker (``sam_pdl_spine.py``) and any local
runner, so the transform logic can never drift between them.

Join vector: the normalized web domain. SAM ``entity_url`` (the entity's
corporate URL) <-> PDL ``domain`` (source column ``website``). Normalization +
structural validity are the approved reconnaissance macros (``norm_host`` /
``is_domain``); ``is_platform`` adds the directive's structural-noise stoplist;
a PDL fan-out cap removes aggregator / link-in-bio / franchise domains that PDL
maps to thousands of distinct companies (e.g. ``sites.google.com`` -> 14,714 PDL
rows) — pure cross-product noise, not a resolution signal.

WIDTH-DRIVEN SAM EXTRACTION (critical correctness invariant)
------------------------------------------------------------
The SAM worker's ``_classify()`` labels each extract ``v2`` / ``legacy_v1`` by a
FILENAME token (``_V2_``). Many 142-field v2-LAYOUT files lack that token, so
they are mislabeled ``legacy_v1`` and ``FIELD_MAP["legacy_v1"]`` misprojects
them — the real UEI (f[1]) lands in ``duns``, a date (f[11]) lands in
``legal_business_name``, and ``uei`` is forced NULL. So ``format_family`` and the
flat ``uei`` / ``legal_business_name`` columns are NOT trustworthy. The physical
record WIDTH (``field_count`` = ``len(pipe_fields)``) IS: 120 = legacy layout,
142 = v2 layout. This module derives every SAM field width-aware from the raw
``pipe_fields`` array, making the bridge correct regardless of the upstream
mislabeling. (Verified field positions, live data.)

Key reality this module encodes:
  * Only the 142/v2 layout carries ``uei``; the 120/legacy layout carries
    ``duns`` (often redacted to 'No longer available' -> NULL). The bridge
    carries BOTH identifiers and admits any row with ``uei OR duns`` — keying on
    ``uei`` alone would forfeit the entire DUNS-era corpus.
"""

from __future__ import annotations

# Verified pipe-array positions (1-indexed) per record WIDTH. (None = absent in
# that layout.) Confirmed against live rows: 120/legacy vs 142/v2.
#   field                : (pos@width120, pos@width142)
SAM_FIELD_POS = {
    "uei":                 (None, 1),
    "duns":                (1, 3),
    "registration_status": (5, 6),
    "legal_business_name": (11, 12),
    "entity_url":          (25, 27),
}

# Directive structural-noise stoplist — matched as the exact host OR any
# subdomain of it (``foo.business.site``, ``m.facebook.com`` both dropped).
PLATFORM_STOPLIST = [
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "business.site", "wixsite.com", "wordpress.com",
]

# Final bridge schema (directive's 6 columns + duns for DUNS-era coverage).
BRIDGE_COLUMNS = [
    "uei", "duns", "legal_business_name", "registration_status",
    "pdl_company_id", "company_name", "normalized_domain",
]
# BTREE scalar indexes on the load-bearing resolution keys. uei + pdl_company_id
# are the directive's keys; duns is the DUNS-era lookup key.
INDEX_COLUMNS = ["uei", "pdl_company_id", "duns"]

DEFAULT_FANOUT_CAP = 25

_STOP_IN = ",".join(f"'{d}'" for d in PLATFORM_STOPLIST)
_STOP_RE = "|".join(d.replace(".", r"\.") for d in PLATFORM_STOPLIST)

# norm_host: lowercase, strip scheme, strip leading www., host-only (cut
# path/query/fragment), strip :port, strip stray dots.  is_domain: structural
# gate (dotted host + alpha TLD>=2) + junk-placeholder blocklist.  is_platform:
# the directive stoplist (exact host or any subdomain thereof).
MACROS = rf"""
CREATE OR REPLACE MACRO norm_host(u) AS (
  regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(lower(trim(u)), '^[a-z][a-z0-9+.\-]*://', ''),
        '^www[0-9]*\.', ''),
      '[/?#].*$', ''),
    ':[0-9]+$', ''),
  '^\.+|\.+$', '', 'g')
);

CREATE OR REPLACE MACRO is_domain(h) AS (
  h IS NOT NULL AND length(h) BETWEEN 4 AND 253
  AND regexp_matches(h, '^[a-z0-9]([a-z0-9\-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9\-]*[a-z0-9])?)*\.[a-z]{{2,}}$')
  AND h NOT IN (
    'none.com','none.none','example.com','example.org','example.net',
    'test.com','test.org','domain.com','mydomain.com','company.com',
    'yourcompany.com','yourdomain.com','website.com','yourwebsite.com',
    'email.com','noemail.com','no.com','na.com','nan.com','tbd.com','tba.com',
    'xxx.com','xx.com','abc.com','asdf.com','sample.com','notavailable.com',
    'unknown.com','pending.com','n-a.com','none.org','noweb.com','nowebsite.com'
  )
);

CREATE OR REPLACE MACRO is_platform(h) AS (
  h IN ({_STOP_IN})
  OR regexp_matches(h, '\.({_STOP_RE})$')
);
"""


def _wcase(field: str, arr: str = "pipe_fields", n: str = "field_count") -> str:
    """Width-aware element pick: CASE on field_count selecting the right
    1-indexed position from the raw pipe array for this field."""
    p120, p142 = SAM_FIELD_POS[field]
    branches = []
    if p120 is not None:
        branches.append(f"WHEN {n} = 120 THEN {arr}[{p120}]")
    if p142 is not None:
        branches.append(f"WHEN {n} = 142 THEN {arr}[{p142}]")
    return "CASE " + " ".join(branches) + " END"


def sam_normalize_sql(view: str) -> str:
    """SAM side: derive identifiers + normalized domain WIDTH-AWARE from the raw
    pipe array (robust to the format_family mislabeling), then drop invalid /
    platform domains and rows lacking BOTH federal identifiers. ``view`` must
    expose ``pipe_fields`` (list<string>) and ``field_count`` (int)."""
    uei = f"nullif(trim({_wcase('uei')}), '')"
    duns = f"nullif(nullif(trim({_wcase('duns')}), ''), 'No longer available')"
    name = f"nullif(trim({_wcase('legal_business_name')}), '')"
    status = f"nullif(trim({_wcase('registration_status')}), '')"
    url = _wcase("entity_url")
    return f"""
SELECT * FROM (
  SELECT
    {uei}    AS uei,
    {duns}   AS duns,
    {name}   AS legal_business_name,
    {status} AS registration_status,
    norm_host({url}) AS nd
  FROM {view}
)
WHERE (uei IS NOT NULL OR duns IS NOT NULL)
  AND is_domain(nd) AND NOT is_platform(nd)
"""


def pdl_normalize_sql(view: str) -> str:
    """PDL side: project id + name + normalized domain, drop invalid/platform."""
    return f"""
SELECT * FROM (
  SELECT pdl_company_id, company_name, norm_host(domain) AS nd
  FROM {view}
)
WHERE pdl_company_id IS NOT NULL AND is_domain(nd) AND NOT is_platform(nd)
"""


def bridge_select_sql(sam_v: str, pdl_v: str, pdl_fan: str,
                      fanout_cap: int = DEFAULT_FANOUT_CAP,
                      distinct: bool = True) -> str:
    """Inner join on the normalized domain. DISTINCT preserves every distinct
    entity<->company linkage while dropping byte-identical monthly-snapshot
    duplicates. ``fanout_cap`` drops domains PDL maps to > cap companies
    (0/None disables the cap -> literal directive behaviour)."""
    dedup = "DISTINCT" if distinct else ""
    cap = f"AND f.pdl_n <= {fanout_cap}" if fanout_cap and fanout_cap > 0 else ""
    return f"""
SELECT {dedup}
  s.uei,
  s.duns,
  s.legal_business_name,
  s.registration_status,
  p.pdl_company_id,
  p.company_name,
  s.nd AS normalized_domain
FROM {sam_v} s
JOIN {pdl_v} p   ON p.nd = s.nd
JOIN {pdl_fan} f ON f.nd = s.nd
WHERE TRUE {cap}
"""
