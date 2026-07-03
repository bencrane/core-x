"""NAF NA/NL/NS priced-matrix parser — Phase-2 (schedule_type CT + Special).

Pure text -> rows. NO network / file I/O. Never raises on a bad/odd PDF: on any
structural failure it returns whatever rows it could extract (possibly ``[]``),
letting the caller count empties.

Handles two schedule families that share the SAME three-side-by-side sub-table
body (NA non-supervisory, NL leader, NS supervisory — grades 1-15 x 5 steps —
plus NS-only grades 16-19 x 5 steps):

    CT       "NAF (Regular) Wage Rate Schedules"       rate_type='regular'
    Special  "NAF Schedule of Special Rates"           rate_type='special'

pdftotext -layout preserves columns, so a normal grade row is the grade number
followed by exactly 15 bare ``xx.xx`` rates: [NA s1..s5][NL s1..s5][NS s1..s5].
We split by TOKEN (whitespace), which is robust to the layout variants where
adjacent columns collide into a single run of space-separated numbers (seen in
older 1996/2009 schedules, e.g. ``8.11 8.44 8.76``). The grade number may carry a
minimum-wage footnote marker (``1*`` / ``1 *``) which is stripped.

NS-16..19 rows are right-offset with only 5 numbers and a ``NS-16`` label; in the
minimum-wage ("MW") Special variant a footnote sentence is prepended to those
lines, so we key off the ``NS-1[6-9]`` label and take the LAST 5 numeric tokens.

Output: one row per (series, grade, step).
"""
from __future__ import annotations

import re

# A hourly rate token: 1-3 integer digits + 2 decimals (e.g. 4.75, 16.23, 148.12).
_RATE = re.compile(r"\d{1,3}\.\d{2}")
# NS-only supervisory extension grades (16-19), label anywhere on the line.
_NS_HIGH = re.compile(r"NS[-\s]?(1[6-9])\b")
# A normal grade row: optional leading spaces, grade 1-15, then (possibly glued or
# space-separated) an optional MW marker '*' and the run of rate tokens. Older
# schedules render the whole row as '1*13.50 14.06 ...' (grade glued to a starred
# first rate) or '1   *12.15 ...' (grade, spaces, starred rate); both must match.
_GRADE_ROW = re.compile(r"^\s*(\d{1,2})\s*\*?\s*(\d{1,3}\.\d{2}.*)$")

_SERIES = ("NA", "NL", "NS")


def _clean_date(s: str) -> str | None:
    s = s.strip()
    return s or None


def _extract_meta_fields(text: str) -> dict:
    """Pull SUBJECT / Wage Schedule # / Issue / Effective dates from header text."""
    subject = None
    sched_no = None
    issue_date = None
    effective_date = None

    lines = text.splitlines()
    for i, ln in enumerate(lines):
        # SUBJECT line — everything after 'SUBJECT:' up to a trailing 'Wage Schedule #'
        if subject is None:
            m = re.search(r"SUBJECT:\s*(.+)", ln)
            if m:
                subj = m.group(1)
                # On Special PDFs the 'Wage Schedule #NNN' sits at the right of the
                # SUBJECT line; split it off so subject stays clean.
                sm = re.search(r"Wage\s*Schedule\s*#\s*(\d+)", subj)
                if sm:
                    sched_no = sm.group(1)
                    subj = subj[: sm.start()]
                subject = re.sub(r"\s{2,}", " ", subj).strip()

        # Wage Schedule # on its own line (CT layout: 'Wage Schedule #: 045' or '# 011')
        if sched_no is None:
            sm = re.search(r"Wage\s*Schedule\s*#\s*:?\s*(\d+)", ln)
            if sm:
                sched_no = sm.group(1)

        if issue_date is None:
            im = re.search(r"Issue\s*Date:\s*(.+)", ln)
            if im:
                issue_date = _clean_date(im.group(1))

        if effective_date is None:
            em = re.search(r"Effective\s*Date:\s*(.+)", ln)
            if em:
                eff = em.group(1).strip()
                # Effective date can wrap onto the next 1-2 lines (Special MW variant:
                # 'The first day of the first applicable pay period / beginning on or
                # after 30 January 2022.'). Stitch continuation lines that carry no
                # other labelled field.
                for cont in lines[i + 1 : i + 3]:
                    c = cont.strip()
                    if not c:
                        break
                    if re.search(r"(Issue|Order|Effective)\s*Date:", c):
                        break
                    # The date-wrap continuation is right-column text. In -layout output
                    # it can share a physical line with the left-column signature block
                    # ('Chief', 'Wage and Salary Division'); strip that signature prefix
                    # and keep only the trailing (right-column) remainder.
                    c = re.sub(
                        r"^.*?(?:Chief|Wage\s*and\s*Salary\s*Division|Division)\s{2,}",
                        "",
                        c,
                    ).strip()
                    # If the whole line WAS just signature text (no right-column wrap),
                    # nothing remains -> stop.
                    if not c or re.fullmatch(
                        r"(?:Chief|Wage\s*and\s*Salary\s*Division|Division)\.?", c
                    ):
                        break
                    eff = f"{eff} {c}"
                    if c.endswith("."):
                        break
                effective_date = _clean_date(eff)

    return {
        "subject": subject,
        "sched_no": sched_no,
        "issue_date": issue_date,
        "effective_date": effective_date,
    }


def _rows_from_grade_line(grade: int, rates: list[float]) -> list[tuple[str, int, int, float]]:
    """Map the 15 rates of a normal grade row onto (series, grade, step, rate)."""
    out: list[tuple[str, int, int, float]] = []
    if len(rates) < 15:
        return out
    # Guard against trailing noise: use exactly the first 15 tokens.
    r = rates[:15]
    for si, series in enumerate(_SERIES):
        block = r[si * 5 : si * 5 + 5]
        for step, val in enumerate(block, start=1):
            out.append((series, grade, step, val))
    return out


def parse(pdf_text: str, meta: dict) -> list[dict]:
    if not pdf_text:
        return []

    schedule_type = (meta or {}).get("schedule_type") or ""
    is_special = schedule_type.lower().startswith("special")
    family = "special" if is_special else "CT"
    rate_type = "special" if is_special else "regular"

    try:
        fields = _extract_meta_fields(pdf_text)
    except Exception:
        fields = {"subject": None, "sched_no": None, "issue_date": None, "effective_date": None}

    # schedule_number: CT -> int(meta['version']); Special -> parsed '#', else None.
    schedule_number = None
    if is_special:
        if fields.get("sched_no"):
            try:
                schedule_number = int(fields["sched_no"])
            except (TypeError, ValueError):
                schedule_number = None
    else:
        try:
            schedule_number = int((meta or {}).get("version"))
        except (TypeError, ValueError):
            # Fall back to a schedule number scraped from the PDF header if version
            # is unusable — better than dropping the field entirely.
            if fields.get("sched_no"):
                try:
                    schedule_number = int(fields["sched_no"])
                except (TypeError, ValueError):
                    schedule_number = None

    wage_area = (meta or {}).get("wage_area")
    source_pdf_filename = (meta or {}).get("filename")

    base = {
        "wage_area": wage_area,
        "schedule_number": schedule_number,
        "rate_type": rate_type,
        "schedule_family": family,
        "subject": fields.get("subject"),
        "effective_date": fields.get("effective_date"),
        "issue_date": fields.get("issue_date"),
        "source_pdf_filename": source_pdf_filename,
    }

    rows: list[dict] = []
    seen: set[tuple[str, int, int]] = set()

    def _emit(series: str, grade: int, step: int, rate: float) -> None:
        key = (series, grade, step)
        if key in seen:
            return
        seen.add(key)
        r = dict(base)
        r.update({"series": series, "grade": grade, "step": step, "hourly_rate": rate})
        rows.append(r)

    lines = pdf_text.splitlines()
    n = len(lines)
    idx = 0
    while idx < n:
        ln = lines[idx]

        # --- NS-16..19 extension rows (only 5 numbers, label anywhere on line) ---
        nm = _NS_HIGH.search(ln)
        if nm:
            grade = int(nm.group(1))
            # Take numeric tokens AFTER the NS-1x label; footnote text may precede it.
            tail = ln[nm.end():]
            nums = _RATE.findall(tail)
            # Fallback: some layouts put the label at the very end w/ nums before it —
            # then use the LAST 5 numbers on the whole line.
            if len(nums) < 5:
                nums = _RATE.findall(ln)[-5:]
            if len(nums) >= 5:
                block = [float(x) for x in nums[:5]]
                for step, val in enumerate(block, start=1):
                    _emit("NS", grade, step, val)
            idx += 1
            continue

        # --- Normal grade rows 1-15 (grade number + 15 rates) ---
        gm = _GRADE_ROW.match(ln)
        if not gm:
            idx += 1
            continue
        try:
            grade = int(gm.group(1))
        except ValueError:
            idx += 1
            continue
        if not (1 <= grade <= 15):
            idx += 1
            continue
        nums = _RATE.findall(gm.group(2))
        # Line-wrap stitching: some schedules (e.g. 048-035-CT) split a grade row
        # across two physical pdftotext lines — the grade + first NA rate on one
        # line, the remaining rates on the continuation line(s). When this grade row
        # carries FEWER than 15 rate tokens, pull in following continuation lines
        # until exactly 15 are gathered. A continuation line must NOT itself start a
        # new grade row and must NOT be an NS-16..19 extension row (either would mean
        # we've over-run into the next record — stop and leave this row short).
        consumed = idx
        if len(nums) < 15:
            look = idx + 1
            while len(nums) < 15 and look < n:
                nxt = lines[look]
                if not nxt.strip():
                    look += 1
                    continue
                if _NS_HIGH.search(nxt):
                    break
                if _GRADE_ROW.match(nxt):
                    break
                more = _RATE.findall(nxt)
                if not more:
                    look += 1
                    continue
                nums.extend(more)
                consumed = look
                look += 1
        if len(nums) < 15:
            # Still not a full priced row (stray line) — skip robustly.
            idx += 1
            continue
        rates = [float(x) for x in nums]
        for series, g, step, val in _rows_from_grade_line(grade, rates):
            _emit(series, g, step, val)
        idx = consumed + 1

    return rows
