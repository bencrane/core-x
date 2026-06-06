"""Canonical cross-spine web-identity blocking-key SQL builders — THE single source of truth.

Pure DuckDB-SQL *string* builders for the fleet's domain / LinkedIn blocking keys, the web-identity
sibling of ``core/name_norm.py``. This module imports **nothing** (no ``modal`` / ``lance`` /
``duckdb`` / ``pyarrow``), so it is safe to import at module-load time in every pipeline AND cheap to
ship into each Modal image:

    image = modal.Image.debian_slim(...).add_local_python_source("core.web_norm")

``add_local_python_source`` resolves this file locally (the repo root is on ``sys.path`` when
``modal run``/``modal deploy`` is invoked from there) and copies it to ``/root/core/web_norm.py``
inside the container, so ``from core.web_norm import normalized_domain`` resolves identically locally
and remotely. ``core/`` needs no ``__init__.py`` (implicit namespace package).

WHY THIS EXISTS. The domain-host normalization rule previously lived only as a private
``_norm_host_sql`` inside ``pipelines/resolution/sam_fmcsa_domain_spine.py`` and was about to be
copy-pasted onto PDL. A cross-spine blocking key copied per-pipeline drifts and silently breaks the
exact-join the moment one copy changes. Sourcing the rule from HERE means it has exactly one
definition. Every spine whose ``normalized_domain`` must exact-join another spine's imports from this
module — change the rule HERE and every spine/bridge moves together. Do NOT re-inline a copy.

``\\.`` / ``\\x{..}`` in this Python source emit ``\\.`` / ``\\x{..}`` verbatim in the generated SQL.
"""

from __future__ import annotations


def _bare_host(expr: str) -> str:
    """``expr`` (URL / website string) → bare host, as a DuckDB SQL expression (no validity gate).

    lower+trim → strip scheme (``http(s)://``) → strip userinfo (``user[:pw]@``) → strip leading
    ``www.`` → drop path/port/query/fragment → strip leading/trailing dots. Userinfo is stripped
    BEFORE the path/port cut so ``bob:pw@host.com`` is not truncated at the ``:`` in ``bob:pw``;
    ``'^[^/@]*@'`` only fires when the ``@`` precedes any ``/``, so it cannot eat a path segment like
    ``medium.com/@handle``.
    """
    return (
        "trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "lower(trim(CAST(" + expr + " AS VARCHAR))),"
        " '^https?://', ''), '^[^/@]*@', ''),"
        " '^www\\.', ''), '[/:?#].*$', ''), '.')"
    )


def normalized_domain(host_expr: str) -> str:
    """Canonical bare host gated to a plausible registrable domain; NULL otherwise.

    Pass a column/expression that already holds ``_bare_host(...)`` output (so the regex chain runs
    once). Stores the TRUE host — webmail/social/marketplace filtering is ``is_generic_domain`` (NOT a
    NULL here): the substrate keeps audit value, the consumer filters. Hosts are stored **as-is (raw),
    not punycoded** — a unicode SLD with an ASCII TLD survives verbatim; a full-unicode host is NULLed
    by the ASCII TLD gate. Any consumer that punycodes must store raw-unicode on its side too.
    """
    h = host_expr
    return (
        "nullif(CASE WHEN " + h + " LIKE '%.%' "
        "AND length(" + h + ") BETWEEN 4 AND 253 "
        "AND " + h + " NOT LIKE '% %' "
        "AND regexp_matches(" + h + ", '\\.[a-z]{2,}$') "
        "THEN " + h + " END, '')"
    )


def linkedin_slug(expr: str) -> str:
    """``expr`` (LinkedIn URL) → bare company/school slug, lowercased; NULL if absent.

    Lowercase the INPUT so scheme/host/path case all fold BEFORE the anchor match — do NOT lower the
    output, or an uppercase host (``LINKEDIN.COM`` / ``/COMPANY/``) never matches the anchor and the
    key silently NULLs. Anchored on ``linkedin.com/(company|school)/<slug>`` so scheme/www/trailing
    slash fold away; ``/in/<person>`` profiles return NULL (this is a company substrate).
    """
    return (
        "nullif(regexp_extract(lower(CAST(" + expr + " AS VARCHAR)),"
        " 'linkedin\\.com/(?:company|school)/([^/?#]+)', 1), '')"
    )


# Generic / shared-host classifier set — webmail + social + link-in-bio + video + B2B-marketplace.
# Superset of ``sam_fmcsa_domain_spine.py``'s CONSUMER_BLOCK; the §10 reconciliation folds that
# pipeline's blocklist into this one canonical set. A host here must NOT be used as a 1:1 join key.
_GENERIC_DOMAINS = (
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "protonmail.com", "proton.me", "ymail.com", "gmx.com", "mail.com", "me.com",
    "facebook.com", "web.facebook.com", "m.facebook.com", "fb.com", "instagram.com", "twitter.com",
    "x.com", "youtube.com", "youtu.be", "linkedin.com", "linktr.ee", "medium.com", "wordpress.com",
    "blogspot.com", "sites.google.com", "g.page", "behance.net", "wa.me", "t.me", "calendly.com",
    "indiamart.com", "yelp.com", "etsy.com", "amazon.com", "ebay.com", "tiktok.com", "pinterest.com",
    "github.io", "wixsite.com", "weebly.com", "godaddysites.com",
)


def is_generic_domain(host_expr: str) -> str:
    """1 when the normalized host is a shared webmail/social/marketplace host that must NOT be a 1:1
    join key; 0 when a real company host; NULL when the host itself is NULL.

    Pass the GATED ``normalized_domain`` value (not the bare host). Stored + BITMAP-indexed, so every
    consumer filters ``WHERE NOT is_generic_domain`` instead of re-deriving a blocklist.
    """
    items = "(" + ",".join("'" + s.replace("'", "''") + "'" for s in _GENERIC_DOMAINS) + ")"
    return (
        "CASE WHEN " + host_expr + " IS NULL THEN NULL "
        "WHEN " + host_expr + " IN " + items + " THEN true ELSE false END"
    )
