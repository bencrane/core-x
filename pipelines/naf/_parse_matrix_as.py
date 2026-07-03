"""Parser for NAF AS/PS priced-matrix wage schedules → dataset ``naf_wage_rates``.

Source PDFs are DoD/DCPAS NAF *Regular Wage Rate Schedules* (schedule_type=AS).
Each carries two side-by-side priced sub-tables:

    AS   AS-Rates          PS   PS-Rates
    Grade 1  2 3 4  5      Grade 1  2 3 4  5
     1 5.23 5.45 5.67 ...   1* 4.75 4.95 5.15 ...

pdftotext ``-layout`` preserves columns. Column spacing is NOT reliable: some
schedules render the two sub-tables single-spaced with a wide gap between them,
others render intra-table cells wide-spaced (grades 1-4 wide, 5-7 tight is common
within a single PDF). So splitting on runs of >=2 spaces fragments wide-spaced
rows and drops them. Instead each data row is parsed spacing-independently by its
NUMERIC TOKEN COUNT: a valid AS/PS data row whitespace-tokenizes to exactly 12
tokens ``[grade, AS r1..r5, grade, PS r1..r5]`` where the grade repeats. A trailing
``*`` on either grade is a footnote marker (Federal minimum-wage adjustment) and is
stripped from the int.

Output: one row per (series, grade, step).

Pure text -> rows. Never raises on odd input: returns the rows it can extract
(possibly ``[]``) so the caller can count empties.
"""

from __future__ import annotations

import re

_STEP_COUNT = 5
# A valid AS/PS data row tokenizes to exactly 12 whitespace-separated tokens:
# [grade, AS r1..r5, grade, PS r1..r5]. The grade (identical in both halves)
# leads each 6-token half.
_ROW_TOKEN_COUNT = 2 * (1 + _STEP_COUNT)  # 12

# Rates are dotted decimals (e.g. 5.23, 10.41). Grade is 1-3 digits with an
# optional trailing '*' footnote marker.
_RATE_RE = re.compile(r"^\d{1,3}\.\d{2}$")
_GRADE_RE = re.compile(r"^(?P<grade>\d{1,3})(?P<foot>\*?)$")

_DATE_RES = {
    "effective_date": re.compile(r"Effective\s+Date:\s*(.+?)\s*$", re.IGNORECASE),
    "issue_date": re.compile(r"Issue\s+Date:\s*(.+?)\s*$", re.IGNORECASE),
}
_SUBJECT_RE = re.compile(r"^\s*SUBJECT:\s*(.+?)\s*$", re.IGNORECASE)


def _extract_scalar(pattern: re.Pattern, text_lines: list[str]) -> str | None:
    for line in text_lines:
        m = pattern.search(line)
        if m:
            val = m.group(1).strip()
            if val:
                return val
    return None


def _parse_half(tokens: list[str]) -> tuple[int, bool, list[float]] | None:
    """Parse one sub-table half (6 tokens: grade + 5 rates) -> (grade, footnoted,
    [5 rates]) or None if the tokens do not form a valid grade+rates half."""
    if len(tokens) != 1 + _STEP_COUNT:
        return None
    gm = _GRADE_RE.match(tokens[0])
    if not gm:
        return None
    try:
        grade = int(gm.group("grade"))
    except ValueError:  # pragma: no cover - regex guarantees digits
        return None
    footnoted = gm.group("foot") == "*"
    rates: list[float] = []
    for tok in tokens[1:]:
        if not _RATE_RE.match(tok):
            return None
        try:
            rates.append(float(tok))
        except (TypeError, ValueError):  # pragma: no cover - regex guarantees float
            return None
    return grade, footnoted, rates


def parse(pdf_text: str, meta: dict) -> list[dict]:
    if not isinstance(pdf_text, str) or not pdf_text.strip():
        return []

    lines = pdf_text.splitlines()

    subject = _extract_scalar(_SUBJECT_RE, lines)
    effective_date = _extract_scalar(_DATE_RES["effective_date"], lines)
    issue_date = _extract_scalar(_DATE_RES["issue_date"], lines)

    try:
        schedule_number = int(str(meta.get("version")).strip())
    except (TypeError, ValueError):
        schedule_number = None

    wage_area = meta.get("wage_area")
    source_pdf_filename = meta.get("filename")

    # series is positional: first sub-table = AS, second = PS.
    series_order = ("AS", "PS")

    rows: list[dict] = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            continue
        # Tokenize on any whitespace run (spacing-independent). A valid AS/PS data
        # row is exactly 12 tokens: [grade, AS r1..r5, grade, PS r1..r5]. This is
        # immune to how wide the intra-table or inter-table gaps render.
        tokens = line.split()
        if len(tokens) != _ROW_TOKEN_COUNT:
            continue
        half = 1 + _STEP_COUNT  # 6
        parsed_blocks = [
            _parse_half(tokens[i * half : (i + 1) * half])
            for i in range(len(series_order))
        ]
        # Every half must parse to a grade+5-rate block, and the repeated grade
        # must match across halves (guards against coincidental 12-token lines).
        if not all(parsed_blocks):
            continue
        if parsed_blocks[0][0] != parsed_blocks[1][0]:  # type: ignore[index]
            continue
        for series, block in zip(series_order, parsed_blocks):
            grade, footnoted, rates = block  # type: ignore[misc]
            for step_idx, rate in enumerate(rates, start=1):
                rows.append(
                    {
                        "wage_area": wage_area,
                        "schedule_number": schedule_number,
                        "series": series,
                        "grade": grade,
                        "step": step_idx,
                        "hourly_rate": rate,
                        "rate_type": "regular",
                        "schedule_family": "AS",
                        "subject": subject,
                        "effective_date": effective_date,
                        "issue_date": issue_date,
                        "footnote": footnoted,
                        "source_pdf_filename": source_pdf_filename,
                    }
                )
    return rows
