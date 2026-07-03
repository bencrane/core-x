"""NAF payband-range parser → dataset ``naf_nf_payband_ranges`` (schedule_type=PBS, ``*-NF.pdf``).

Pure text→rows. No network, no file I/O. Never raises on odd input — returns the rows it can
extract (possibly ``[]``) and lets the caller count empties.

Source layout (``pdftotext -layout``)::

    Pay Schedule #    009-16-A                                     Issue Date: 21 Feb 1997
    SUBJECT:    NAF Pay Ranges for the Sacramento, CA Wage Area
                                        PAY RANGES
                            MINIMUM                          MAXIMUM
        NF LEVELS      PER YEAR     PER HOUR          PER YEAR      PER HOUR
         * 1           10,440       ( 5.00)            22,110       (10.59)
           2           13,130       ( 6.29)            30,910       (14.81)
         ...
                                                   Effective Date : 01 Mar 1997

One output row per NF level::

    {wage_area, schedule_number:int, nf_level:int,
     min_annual:float, min_hourly:float, max_annual:float, max_hourly:float,
     effective_date, source_pdf_filename}

Robustness notes:
  * Level marker may be prefixed/suffixed with ``*`` / ``**`` ("* 1", "1  *", "** 1") — stripped.
  * A level may carry only a min pair (max shown as a bare ``*`` footnote ref) or no pairs at all
    (both sides ``*``, or a "Please see DoDI ..." sentence). Missing numeric cells → ``None``.
  * Hourly values are parenthesized, often with a leading space: ``( 5.00)``. Commas stripped.
  * effective_date is the real date, not the "The first day of the first pay period" header
    boilerplate: the date in the "beginning on or after <date>" clause (possibly wrapped onto
    the next line) or the inline "Effective Date : <date>" value. Both the spelled
    "DD Mon YYYY" and the numeric slash "DD/MM/YYYY" forms are recognised.
  * schedule_number is the schedule component of the version (``009-16-A`` → 9), taken from
    ``meta['version']`` when numeric-leading, else from the ``Pay Schedule #`` line, else the
    filename stem.
"""

from __future__ import annotations

import re

# A parenthesized hourly value, e.g. "( 5.00)" or "(12.94)".
_HOURLY = re.compile(r"\(\s*([0-9][0-9.,]*)\s*\)")
# An annual value followed by its parenthesized hourly: "10,440   ( 5.00)".
_PAIR = re.compile(r"([0-9][0-9,]*)\s*\(\s*([0-9][0-9.,]*)\s*\)")
# Leading NF-level cell: optional asterisks, the level digit, optional trailing asterisks.
_LEVEL = re.compile(r"^\s*\**\s*([1-9])\b\s*\**\s*")

_MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
# A spelled month date ("01 Mar 1997") or a numeric slash date ("01/01/2003", day-first DD/MM/YYYY
# as printed by the older schedules). Either form is the actual effective date.
_DATE = re.compile(
    rf"\b(\d{{1,2}}\s+(?:{_MONTHS})[a-z]*\.?\s+\d{{4}}|\d{{1,2}}/\d{{1,2}}/\d{{2,4}})\b",
    re.IGNORECASE,
)

# Header boilerplate the effective date is phrased against — never a valid date value. The real
# date follows in the "beginning on or after <date>" clause (often wrapped onto the next line).
_BOILERPLATE = "The first day of the first pay period"
# The clause anchor. Tolerates the source-typo "beginining", and a bare "on or after" (the
# leading word wraps onto the previous line, dropping it from this block). Anchoring on
# "on or after" keeps the match scoped to the effective-date clause, not stray dates elsewhere.
_BEGINNING = re.compile(r"(?:begin\w*\s+)?on\s+or\s+after\s+", re.IGNORECASE)


def _to_float(tok: str | None) -> float | None:
    if tok is None:
        return None
    tok = tok.replace(",", "").strip()
    if not tok:
        return None
    try:
        return float(tok)
    except ValueError:
        return None


def _schedule_number(meta: dict, pdf_text: str) -> int | None:
    """Schedule component (the ``009`` in ``009-16-A``) as int, or None."""
    candidates: list[str] = []

    ver = meta.get("version")
    if ver:
        candidates.append(str(ver))

    m = re.search(r"Pay\s+Schedule\s*#\s*([0-9][0-9A-Za-z\-]*)", pdf_text)
    if m:
        candidates.append(m.group(1))

    fn = meta.get("filename")
    if fn:
        # 002-009-16-A-NF.pdf -> strip area prefix + -NF.pdf suffix, keep schedule-version stem.
        stem = re.sub(r"-NF\.pdf$", "", str(fn), flags=re.IGNORECASE)
        parts = stem.split("-")
        # Area dir is the leading 3-digit group; the schedule number is the next group.
        if len(parts) >= 2 and re.fullmatch(r"\d{3}", parts[0]):
            candidates.append("-".join(parts[1:]))
        else:
            candidates.append(stem)

    for c in candidates:
        m = re.match(r"\s*0*(\d+)", c)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                continue
    return None


def _wage_area(meta: dict, pdf_text: str) -> str | None:
    """3-digit NAF/wage area. Prefer meta, else the ``AC - 002`` header, else filename prefix."""
    for k in ("wage_area", "naf_area"):
        v = meta.get(k)
        if v:
            return str(v)

    m = re.search(r"\bAC\s*[-–]?\s*(\d{2,4})\b", pdf_text)
    if m:
        return m.group(1)

    fn = meta.get("filename")
    if fn:
        m = re.match(r"(\d{3})-", str(fn))
        if m:
            return m.group(1)
    return None


def _effective_date(pdf_text: str) -> str | None:
    """Real effective date, never the 'first day of the first pay period' header boilerplate.

    The date is either an inline ``Effective Date : <date>`` value, or — when the label is phrased
    against the boilerplate — the date in the ``beginning on or after <date>`` clause that follows
    (frequently wrapped onto the next line). Both spelled (``01 Mar 1997``) and numeric slash
    (``01/01/2003``) date forms are recognised.
    """
    lines = pdf_text.splitlines()

    def _date_after_beginning(text: str) -> str | None:
        """Date immediately following the 'beginning on or after' clause in a text block."""
        b = _BEGINNING.search(text)
        if not b:
            return None
        m = _DATE.search(text, b.end())
        return m.group(1) if m else None

    for i, ln in enumerate(lines):
        if re.search(r"Effective\s+Date", ln, re.IGNORECASE):
            # Text on the 'Effective Date' line itself, after the label/colon.
            tail = re.sub(r".*Effective\s+Date\s*:?\s*", "", ln, flags=re.IGNORECASE).strip()
            # A concrete date on this line wins outright.
            m = _DATE.search(tail)
            if m:
                return m.group(1)
            # Otherwise stitch the continuation lines and pull the first date from the block.
            # _DATE now also matches the numeric-slash form ("01/01/2003") in which the real
            # 'beginning on or after' date is printed when the label reads the boilerplate.
            block = tail
            for cont in lines[i + 1 : i + 5]:
                block += " " + cont.strip()
            m = _DATE.search(block)
            if m:
                return m.group(1)
            # The label read the boilerplate (or an empty tail) with no recoverable date in the
            # block — the real date is absent (truncated continuation). NEVER store the boilerplate.
            if not tail or _BOILERPLATE.lower() in tail.lower():
                return None
            return tail
    # Fallback: the date in the 'beginning on or after' clause anywhere in the document.
    return _date_after_beginning(pdf_text)


def _parse_level_row(line: str) -> dict | None:
    """Parse a single NF-level table row. Returns None if the line isn't a level row."""
    m = _LEVEL.match(line)
    if not m:
        return None
    level = int(m.group(1))

    pairs = _PAIR.findall(line)  # list of (annual, hourly) in left-to-right order
    min_annual = min_hourly = max_annual = max_hourly = None
    if len(pairs) >= 1:
        min_annual = _to_float(pairs[0][0])
        min_hourly = _to_float(pairs[0][1])
    if len(pairs) >= 2:
        max_annual = _to_float(pairs[1][0])
        max_hourly = _to_float(pairs[1][1])

    # A line matching only the leading digit but carrying no numeric pair (e.g. footnote-only
    # NF-6 rows like "6  *  *" or "6  Please see DoDI ...") is still a valid level with nulls.
    return {
        "nf_level": level,
        "min_annual": min_annual,
        "min_hourly": min_hourly,
        "max_annual": max_annual,
        "max_hourly": max_hourly,
    }


def parse(pdf_text: str, meta: dict) -> list[dict]:
    if not isinstance(pdf_text, str) or not pdf_text.strip():
        return []
    meta = meta or {}

    try:
        wage_area = _wage_area(meta, pdf_text)
        schedule_number = _schedule_number(meta, pdf_text)
        effective_date = _effective_date(pdf_text)
        source_pdf_filename = meta.get("filename")

        # Bound the scan to the PAY RANGES table so stray digit lines (dates, addresses,
        # footnotes with a leading number) can't masquerade as level rows.
        lines = pdf_text.splitlines()
        start = 0
        for i, ln in enumerate(lines):
            if re.search(r"NF\s+LEVELS", ln, re.IGNORECASE):
                start = i + 1
                break
        else:
            for i, ln in enumerate(lines):
                if re.search(r"PAY\s+RANGES", ln, re.IGNORECASE):
                    start = i + 1
                    break

        rows: list[dict] = []
        seen: set[int] = set()
        blank_run = 0
        for ln in lines[start:]:
            if not ln.strip():
                blank_run += 1
                # The table block ends at a run of blanks before the signature; stop once we
                # already have rows to avoid picking up trailing numeric noise.
                if rows and blank_run >= 2:
                    break
                continue
            blank_run = 0
            # Stop at the signature / effective-date block that follows the table.
            if re.search(r"Effective\s+Date|Order\s+Date|Wage and Salary", ln, re.IGNORECASE):
                if rows:
                    break
                continue
            row = _parse_level_row(ln)
            if row is None:
                continue
            if row["nf_level"] in seen:
                continue
            seen.add(row["nf_level"])
            row.update(
                {
                    "wage_area": wage_area,
                    "schedule_number": schedule_number,
                    "effective_date": effective_date,
                    "source_pdf_filename": source_pdf_filename,
                }
            )
            rows.append(row)

        # Emit in level order with a stable key set.
        rows.sort(key=lambda r: r["nf_level"])
        return [
            {
                "wage_area": r["wage_area"],
                "schedule_number": r["schedule_number"],
                "nf_level": r["nf_level"],
                "min_annual": r["min_annual"],
                "min_hourly": r["min_hourly"],
                "max_annual": r["max_annual"],
                "max_hourly": r["max_hourly"],
                "effective_date": r["effective_date"],
                "source_pdf_filename": r["source_pdf_filename"],
            }
            for r in rows
        ]
    except Exception:  # noqa: BLE001 — a bad PDF must never crash the batch.
        return []
