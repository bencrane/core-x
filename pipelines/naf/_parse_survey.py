"""Parser — NAF *Pay Report* survey-summary PDF (schedule_type ``PBPR``, filename ``…-PR.pdf``).

Target dataset: ``naf_nf_payband_survey`` — one row per survey job under each NF pay level.

  SOURCE LAYOUT  DoD DCPAS NAF ``NAF PAY SYSTEM / PAY REPORT``. A prose preamble, then a
                 ``SURVEY SUMMARY BY PAY LEVEL`` table. The table is broken into NF-level
                 sections (``NF - 1``, ``NF - 2``, …); under each, one line per survey job:

                     SURVEY JOB TITLE        MATCHES     AVERAGE     HIGH      LOW
                     SALES ASSOCIATE            78         5.62      7.50      4.75

                 The table closes with a right-block footer::

                     WAGE AREA : #055 Yuma, Arizona
                     ORDER DATE: October 3, 1997
                     SURVEY    : #013/015-20

  FORMAT FAMILIES  Two layouts occur and both are handled by trailing-column detection:
                   * CLASSIC (pre-~2024): left-aligned titles, ``NF - 1`` (ASCII hyphen).
                   * NEW (~2024+): whole table centre-indented, header reads
                     ``SURVEY JOB TITLE``, titles right-aligned (leading whitespace varies),
                     NF markers mix ASCII hyphen and EN-DASH (``NF – 2``, U+2013).

  ROBUSTNESS  Pure text->rows. Never raises: on any odd/empty PDF returns the rows it can
              extract (possibly ``[]``). A survey row is recognised purely by its four
              trailing numeric columns (matches int, then three ``NN.NN`` floats), so
              indentation, right-alignment, and multi-word titles are all tolerated. A stray
              OCR leading dot on ``matches`` (seen: ``.31``) is stripped. Column splitting is
              whitespace/position based — never comma based.

  OUTPUT ROW  {wage_area, schedule_number:int, nf_level:int, survey_job_title:str,
               matches:int, average:float, high:float, low:float,
               effective_date:str, issue_date:str, source_pdf_filename:str}
"""
from __future__ import annotations

import re

# NF-level section header, e.g. "NF - 1", "NF – 2", "NF—3" (ASCII hyphen / en / em dash).
_NF_RX = re.compile(r"^\s*NF\s*[-‐‑‒–—―]\s*(\d+)\s*$")

# Start of the survey table; anything before it is preamble/prose.
_TABLE_START_RX = re.compile(r"SURVEY\s+SUMMARY\s+BY\s+PAY\s+LEVEL", re.I)

# Footer / column-header noise lines that never carry a job row.
_STOP_RX = re.compile(r"WAGE\s+AREA\b", re.I)
_HEADER_NOISE_RX = re.compile(
    r"^\s*(SURVEY\b|JOB\s+TITLE\b|WEIGHTED\b|MATCHES\b|AVERAGE\b|RANGE\b|HIGH\b|LOW\b)",
    re.I,
)

# A survey job row: <title text> <matches> <average> <high> <low>.
#   matches : integer, tolerating a stray leading "." OCR artifact (".31" -> 31).
#   average/high/low : NN.NN floats (dollars, two decimals; allow 1-2 decimals defensively).
_ROW_RX = re.compile(
    r"^(?P<title>.*?\S)\s+"
    r"\.?(?P<matches>\d{1,6})\s+"
    r"(?P<average>\d{1,4}\.\d{1,2})\s+"
    r"(?P<high>\d{1,4}\.\d{1,2})\s+"
    r"(?P<low>\d{1,4}\.\d{1,2})\s*$"
)

# Footer field lines.
_WAGE_AREA_RX = re.compile(r"WAGE\s+AREA\b\s*:?\s*#?\s*(\d+)", re.I)
_ORDER_DATE_RX = re.compile(r"ORDER\s+DATE\b\s*:?\s*(.+?)\s*$", re.I)
# SURVEY : #012/011-18A  or  SURVEY : 014/013-22  ->  middle group is schedule_number.
_SURVEY_RX = re.compile(r"SURVEY\b\s*:?\s*#?\s*\d+\s*/\s*(\d+)\s*-\s*\d+", re.I)

# Filename: {area}-{schedule}-{version}[-{variant}]-PR.pdf  ->  schedule_number = 2nd field.
_FILENAME_SCHED_RX = re.compile(r"^\s*[0-9A-Za-z]+-(\d+)-", re.I)


def _to_int(s: str):
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _to_float(s: str):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _schedule_number(meta: dict, footer_sched):
    """Prefer the filename's second field; fall back to the SURVEY footer group."""
    fn = (meta or {}).get("filename") or ""
    m = _FILENAME_SCHED_RX.match(fn)
    if m:
        n = _to_int(m.group(1))
        if n is not None:
            return n
    return footer_sched


def parse(pdf_text: str, meta: dict) -> list[dict]:
    if not pdf_text or not isinstance(pdf_text, str):
        return []
    meta = meta or {}
    src = meta.get("filename")

    lines = pdf_text.splitlines()

    # --- footer scan (order date + schedule number + wage area), whole doc, robust to layout ---
    footer_wage_area = None
    footer_sched = None
    order_date = None
    for ln in lines:
        if footer_wage_area is None:
            m = _WAGE_AREA_RX.search(ln)
            if m:
                footer_wage_area = m.group(1)
        if order_date is None:
            m = _ORDER_DATE_RX.search(ln)
            if m:
                order_date = m.group(1).strip() or None
        if footer_sched is None:
            m = _SURVEY_RX.search(ln)
            if m:
                footer_sched = _to_int(m.group(1))

    wage_area = meta.get("wage_area") or footer_wage_area
    schedule_number = _schedule_number(meta, footer_sched)
    # order date is both the effective and issue date as printed on the pay report.
    effective_date = order_date
    issue_date = order_date

    # --- locate the survey table body ---
    start = None
    for i, ln in enumerate(lines):
        if _TABLE_START_RX.search(ln):
            start = i + 1
            break
    if start is None:
        return []

    rows: list[dict] = []
    cur_nf = None
    for ln in lines[start:]:
        if _STOP_RX.search(ln):
            break  # reached the footer block; no more job rows

        nf_m = _NF_RX.match(ln)
        if nf_m:
            cur_nf = _to_int(nf_m.group(1))
            continue

        if not ln.strip():
            continue
        if _HEADER_NOISE_RX.match(ln):
            continue

        row_m = _ROW_RX.match(ln)
        if not row_m:
            continue

        title = re.sub(r"\s+", " ", row_m.group("title")).strip()
        # A real job title has at least one alphabetic char (guards against stray numeric noise).
        if not title or not re.search(r"[A-Za-z]", title):
            continue

        matches = _to_int(row_m.group("matches"))
        average = _to_float(row_m.group("average"))
        high = _to_float(row_m.group("high"))
        low = _to_float(row_m.group("low"))
        if matches is None or average is None or high is None or low is None:
            continue

        rows.append({
            "wage_area": wage_area,
            "schedule_number": schedule_number,
            "nf_level": cur_nf,
            "survey_job_title": title,
            "matches": matches,
            "average": average,
            "high": high,
            "low": low,
            "effective_date": effective_date,
            "issue_date": issue_date,
            "source_pdf_filename": src,
        })

    return rows
