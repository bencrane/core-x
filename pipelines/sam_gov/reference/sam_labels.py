"""Canonical SAM extract-label sort key — single source for the SAM feed workers.

SAM ships two label *formats* for the same logical snapshot — a numeric ``YYYYMMDD``
(e.g. ``20260503``) and an alpha ``YYYY_MMM`` (e.g. ``2026_MAY``). A raw-string ``max``
or equality over mixed forms is wrong (``'_'`` > any digit, so ``'2026_MAY'`` sorts
above ``'20260601'``). ``snap_key_sql`` normalizes both forms to a comparable BIGINT so
``max``/equality over the key is correct.

Imported (lazily, inside the functions that build SQL) by ``sam_master`` and
``sam_normalized_entities`` and mounted into their images via
``add_local_python_source("pipelines.sam_gov.reference.sam_labels")``. ``sam_pocs`` keeps
an inline copy this cycle; consolidating it here is a tracked follow-up.
"""
from __future__ import annotations


def snap_key_sql(col: str = "extract_label") -> str:
    """DuckDB expression: SAM extract label (``YYYYMMDD`` or ``YYYY_MMM``) → sortable BIGINT.

    ``YYYYMMDD`` → itself as BIGINT; ``YYYY_MMM`` → ``YYYY`` || month-number || ``00``.
    Use under ``ORDER BY``/``max``/equality to compare labels format-agnostically.
    """
    months = ("WHEN 'JAN' THEN '01' WHEN 'FEB' THEN '02' WHEN 'MAR' THEN '03' WHEN 'APR' THEN '04' "
              "WHEN 'MAY' THEN '05' WHEN 'JUN' THEN '06' WHEN 'JUL' THEN '07' WHEN 'AUG' THEN '08' "
              "WHEN 'SEP' THEN '09' WHEN 'OCT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEC' THEN '12'")
    return (f"CASE WHEN regexp_matches({col}, '^[0-9]{{8}}$') THEN CAST({col} AS BIGINT) "
            f"ELSE CAST(substr({col},1,4) || CASE upper(substr({col},6,3)) {months} ELSE '00' END "
            f"|| '00' AS BIGINT) END")
