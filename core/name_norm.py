"""Canonical cross-spine blocking-key SQL builders — THE single source of truth.

Pure DuckDB-SQL *string* builders for the fleet's entity-resolution blocking keys.
This module imports **nothing** (no ``modal`` / ``lance`` / ``duckdb`` / ``pyarrow``),
so it is safe to import at module-load time in every pipeline AND cheap to ship into
each Modal image. Each separate Modal app packages it explicitly:

    image = modal.Image.debian_slim(...).pip_install(...).add_local_python_source("core.name_norm")

``add_local_python_source`` resolves this file locally (the repo root is on ``sys.path``
when ``modal run``/``modal deploy`` is invoked from there) and copies it to
``/root/core/name_norm.py`` inside the container, which is on the container ``PYTHONPATH``
— so ``from core.name_norm import name_norm`` resolves identically locally and remotely.
``core/`` needs no ``__init__.py`` (implicit namespace package, Python 3.3+).

WHY THIS EXISTS. The blocking-key rule was copy-pasted across seven workers, each
self-documenting as "byte-identical to sos_normalized". A change had to be hand-propagated
to every copy, and a missed copy silently breaks the cross-layer exact-join against
``sos_normalized_master``'s BTREE blocking key. Sourcing the rule from HERE means it has
exactly one definition and cannot drift. Every worker whose ``normalized_legal_name`` must
exact-join the SoS spine imports from this module:

    pipelines/sos_normalized/normalize.py            (the master spine)
    pipelines/fl_federal_tax_liens/ingest.py
    pipelines/osha/osha_sniper.py
    pipelines/resolution/recon_ca_ucc_sos.py
    pipelines/resolution/crosswalk_hmda_gleif.py
    pipelines/resolution/crosswalk_sam_usaspending.py   (passes an EXPRESSION, not a column)
    pipelines/resolution/credit_spine_normalize_index.py (PPP / 7(a) / 504 credit spines)

``\\s`` / ``\\x{..}`` in this Python source emit ``\\s`` / ``\\x{..}`` verbatim in the
generated SQL. Change the rule HERE and every spine/bridge moves together — do NOT
re-inline a copy "to tweak it".
"""

from __future__ import annotations


def name_norm(expr: str) -> str:
    """Canonical blocking-key name normalization (a DuckDB SQL expression).

    UPPER → ``&`` → `` AND `` (literal, replaced PRE-strip so the conjunction survives as
    a token instead of being dropped) → dash / en-dash / em-dash → `` `` (so "BRAND - STORE"
    and "BRAND-STORE" both block as two tokens, never "BRANDSTORE") → strip every remaining
    non-``[A-Z0-9 space]`` char (punctuation, quotes, accents) → collapse whitespace runs →
    trim. NULL if emptied.

    ``expr`` is interpolated verbatim, so it may be a bare column name (``"entity_name"``,
    ``"d.debtor_name"``) OR any scalar SQL expression (``"sam_legal_name"``, a nested call) —
    one builder serves both the historical ``_name_norm(col)`` and ``_norm_sql(expr)`` shapes.
    THE canonical rule: change it here and every spine/bridge moves together.
    """
    return (
        "nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "upper(CAST(" + expr + " AS VARCHAR)),"
        " '&', ' AND ', 'g'),"
        " '[-\\x{2013}\\x{2014}]+', ' ', 'g'),"
        " '[^A-Z0-9 ]+', '', 'g'),"
        " '\\s+', ' ', 'g')), '')"
    )


def legal_name_base(name_norm_expr: str) -> str:
    """Suffix-stripped base of a ``name_norm``'d expression (the cross-layer resolution key
    that recovers "PACIFIC TRUCKING" vs "PACIFIC TRUCKING LLC" drift).

    Peel one OR MORE trailing corporate designators (LLC/INC/CORP/CO/LTD/PLC) off the end.
    The ``$`` anchor means a designator strips only as a whole trailing token: "ACME CO LLC"
    → "ACME", while "TACO COMPANY" / "ACME CORPORATION" keep their tail (COMPANY/CORPORATION
    aren't in the set, and a partial CO/CORP can't reach end-of-string). NULL if emptied.

    Pass a ``name_norm`` output — a bare column alias of one (``"normalized_legal_name"``)
    or ``name_norm(...)`` itself — never a raw name.
    """
    return (
        "nullif(trim(regexp_replace(" + name_norm_expr + ","
        " '( (LLC|INC|CORP|CO|LTD|PLC))+$', '', 'g')), '')"
    )
