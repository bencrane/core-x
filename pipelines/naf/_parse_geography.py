"""Parser for NAF wage-area *geography* schedules (schedule_type=RSB, filenames ``*-ScheduleBack.pdf``).

Feeds the ``naf_wage_area_geography`` dataset, which drives a county -> FIPS crosswalk.
Pure text -> rows: no network, no file I/O, never raises. On a bad/odd PDF it returns
whatever rows it can extract (possibly ``[]``); the caller counts empties.

pdftotext ``-layout`` output preserves column positions, so extraction is whitespace /
column driven, never comma-splitting.

Two row kinds are emitted into a single list, discriminated by ``row_kind``:

  installation: an ``Agency`` + ``Installation`` pairing landed above the county table
                e.g. ``Army & Air Force Exchange Service`` / ``COLUMBUS AIR FORCE BASE``
  county:       a ``County and State`` -> ``Applicable Schedule`` crosswalk row
                e.g. ``Lowndes County, MS   AC 004 RUS   004``  ->  county='Lowndes County',
                state='MS', applicable_schedule='004'

Layout families handled (all observed vintages of ``-ScheduleBack.pdf``):

  * "definition" format (the common case, ~all modern files)::

        NAF Wage Area Definition for Lowndes, MS - Area Number : 004 ,
                            Schedule Number : 030
           Army & Air Force Exchange Service
                            COLUMBUS AIR FORCE BASE          AC    004   RUS
           Department of the Air Force
                            COLUMBUS AIR FORCE BASE          AC    004   RUS
        Pay schedules also apply to all installations ...
        County and State                 Applicable Schedule - Crafts and Trades   NPS
        Lowndes County, MS                              AC    004     RUS     004
        Tuscaloosa County, AL                           AC    004     RUS     004

    The "Area Number :" / "Schedule Number :" pair may be split across lines in
    several ways (comma leading or trailing, area number wrapped onto the next
    line). All variants are stitched from the header block.

  * "columnar" format (some 04x-vintage files)::

        NAF Wage Area Definition for Lowndes, Mississippi
           Area Number: 004, Schedule Number: 043
         Agency                 Installation                       CT             NPS
         Army & Air Force ...    COLUMBUS AIR FORCE BASE        AC 004 (RUS)    004
        County and State                                          CT            NPS
        Lowndes, MS                                            AC 004 (RUS)     004

  * "FWS" format (old files, e.g. 056)::

        SANTA CLARA, CALIFORNIA FWS WAGE AREA
        DEPARTMENT OF THE NAVY                       AC - 056 (SF)
        Moffett Federal Airfield
        ...
        GENERAL SCHEDULE LOCALITY PAY AREA: SAN FRANCISCO   AC - 056 (SF)
        California:
             Alameda
             Contra Costa

    Here the applicable-schedule column is absent (counties are listed indented
    under a "State:" heading); county rows carry the area's own schedule number.
"""

from __future__ import annotations

import re

# ── header extraction ───────────────────────────────────────────────────────────────
# Area / schedule numbers can wrap across lines in half a dozen ways; the robust move is
# to collapse the header region to a single string and regex the two labelled numbers.
_AREA_RE = re.compile(r"Area\s+Number\s*:\s*(\d{2,4})", re.IGNORECASE)
_SCHED_RE = re.compile(r"Schedule\s+Number\s*:\s*(\d{1,4})", re.IGNORECASE)
# FWS-family fallback: the area number lives only in the ``AC – 056 (SF)`` marker.
_AC_MARK_RE = re.compile(r"\bAC\s*[‐‑‒–—\-]+\s*(\d{2,4})\s*\(")

# A county-table row. Leading county+state, an arbitrary AC/schedule middle, then the
# NPS column (the "applicable schedule" — a 2-4 digit number, optionally followed by a
# single grade/NPS letter). Anchored on the ``, ST`` state token to avoid the AC middle.
_STATE = r"[A-Z]{2}"
# county name = anything up to the ", ST" that is NOT itself the AC schedule block.
_COUNTY_ROW_RE = re.compile(
    r"^\s*(?P<county>[A-Za-z][A-Za-z0-9 .,'()/&\-‐–*]*?),\s*"
    r"(?P<state>" + _STATE + r")\b"
    r"(?P<mid>.*?)"
    r"(?P<appl>\d{2,4})(?:\s+(?:[A-Z](?:\s*/\s*[A-Z])*|[-‐‑‒–—]))?(?:\s*[*‡†])?\s*$"
)

# Stateless applicable-area rows: territories / atolls / islands with no ", ST" code
# (e.g. "American Samoa   AC 306 RUS   106", "Johnston Atoll", "Midway Islands"). These
# still carry an ``AC {sched} {suffix}`` middle and a trailing NPS applicable schedule.
# Kept tight — an ``AC``/``NF`` marker with 2+ column gaps on either side — so footnote
# prose and page footers do not match.
_STATELESS_ROW_RE = re.compile(
    r"^\s*(?P<county>[A-Za-z][A-Za-z .'()/&\-]*[A-Za-z])\s{2,}"
    r"(?P<mid>(?:AC|NF)\b[A-Z0-9 ()‐‑‒–—\-]*?)\s{2,}"
    r"(?P<appl>\d{2,4})(?:\s+(?:[A-Z](?:\s*/\s*[A-Z])*|[-‐‑‒–—]))?(?:\s*[*‡†])?\s*$"
)

# Agency header lines in the "definition" / "FWS" families. AAFES + the service branches
# and their exchange/NEXCOM variants, plus the DoD combat-support / defense agencies that
# open their own installation block in multi-agency RSB PDFs. Matched case-insensitively,
# whole-line. These render as a left-aligned org name with NO ``AC <area>`` schedule column
# (that column is what distinguishes an indented installation row from an agency header);
# the call site additionally gates on the absence of an ``AC`` token. Attribution follows
# the most recent header, so every ``Defense ...``/``National Security Agency`` block that
# used to be swallowed as an installation under the prior agency now starts a fresh block.
_AGENCY_RE = re.compile(
    r"^\s*("
    r"Army\s*&\s*Air\s*Force\s+Exchange\s+Service"
    r"|United\s+States\s+Marine\s+Corps(?:\s+Exchange)?"
    r"|U\.?\s*S\.?\s+Marine\s+Corps(?:\s+Exchange)?"
    r"|Marine\s+Corps(?:\s+Exchange)?"
    r"|Department\s+of\s+the\s+(?:Army|Navy|Air\s+Force)(?:\s*-\s*Nexcom)?"
    r"|Department\s+of\s+Defense[A-Za-z ]*"
    r"|Defense\s+Logistics\s+Agency"
    r"|Defense\s+Commissary\s+Agency"
    r"|Defense\s+Finance\s+and\s+Accounting\s+Service"
    r"|Defense\s+Information\s+Systems\s+Agency"
    r"|Defense\s+Contract\s+(?:Management|Audit)\s+Agency"
    r"|Defense\s+Threat\s+Reduction\s+Agency"
    r"|Defense\s+Intelligence\s+Agency"
    r"|Defense\s+Advanced\s+Research\s+Projects\s+Agency"
    r"|Defense\s+Security\s+Cooperation\s+Agency"
    r"|Missile\s+Defense\s+Agency"
    r"|National\s+Security\s+Agency"
    r"|National\s+Geospatial[‐‑‒–—\-\s]*Intelligence\s+Agency"
    r"|National\s+Guard\s+Bureau"
    r"|Coast\s+Guard[A-Za-z ]*"
    r"|DEPARTMENT\s+OF\s+THE\s+(?:ARMY|NAVY|AIR\s+FORCE)"
    r")\s*$",
    re.IGNORECASE,
)

# FWS-family agency header carries the ``AC - nnn (xx)`` marker on the same line.
_FWS_AGENCY_RE = re.compile(
    r"^\s*(?P<agency>[A-Z][A-Z .&\-]+?)\s{2,}AC\s*[‐‑‒–—\-]+\s*\d{2,4}",
)

# Lines that are structural, not data — never treat as installation/county rows.
_NOISE_RE = re.compile(
    r"^\s*("
    r"NAF\s+Wage\s+Area\s+Definition"
    r"|Area\s+Number"
    r"|Schedule\s+Number"
    r"|County\s+and\s+State"
    r"|Applicable\s+Schedule"
    r"|Agency\b"
    r"|Pay\s+schedules?\s+also\s+apply"
    r"|These\s+schedules\s+also\s+apply"
    r"|GENERAL\s+SCHEDULE\s+LOCALITY\s+PAY\s+AREA"
    r")",
    re.IGNORECASE,
)


# Trailing qualifier that some rows tack onto the geography name, e.g.
# "ENIWETOK ATOLL - NF ONLY", "Manatee County - NF ONLY", "Grenada County, MS - NF ONLY".
# Also strips a bare trailing "AC" token that can leak in on stateless matches.
_NAME_QUALIFIER_RE = re.compile(
    r"\s*[‐‑‒–—\-]\s*NF\s*ONLY.*$|\s*[‐‑‒–—\-]\s*NF.*$|\s+AC\s*$",
    re.IGNORECASE,
)


def _clean_county(name: str) -> str:
    """Normalise a county / geography name: collapse whitespace, drop trailing NF-ONLY
    and stray AC-token qualifiers so the county->FIPS join key stays clean."""
    name = re.sub(r"\s+", " ", name).strip()
    name = _NAME_QUALIFIER_RE.sub("", name).strip()
    # Drop a dangling trailing punctuation left by the qualifier strip.
    name = re.sub(r"[\s,;:.\-‐‑‒–—]+$", "", name).strip()
    return name


def _extract_area_sched(text: str, meta: dict) -> tuple[str | None, int | None]:
    """Best-effort (wage_area, schedule_number:int) from the header, falling back to meta."""
    head = text[:2000]
    m_area = _AREA_RE.search(head)
    m_sched = _SCHED_RE.search(head)

    wage_area = meta.get("wage_area")
    if wage_area is None:
        if m_area:
            wage_area = m_area.group(1)
        else:
            m_ac = _AC_MARK_RE.search(head)  # FWS family
            wage_area = m_ac.group(1) if m_ac else meta.get("naf_area")

    sched: int | None = None
    if m_sched:
        try:
            sched = int(m_sched.group(1))
        except (TypeError, ValueError):
            sched = None
    if sched is None:
        v = meta.get("version")
        try:
            sched = int(str(v).lstrip("0") or "0") if v not in (None, "") else None
        except (TypeError, ValueError):
            sched = None
    return wage_area, sched


# The trailing schedule column on an indented installation row. It is always set off
# from the name by a run of whitespace (observed min gap = 3 spaces) and opens with an
# ``AC`` token followed by either a schedule number (``AC 101 RUS``, ``AC 019 (AQ)``) or
# the non-appropriated-fund marker (``AC NF ONLY``). Requiring the leading whitespace gap
# keeps a name-internal " AC " token (none observed, but cheap insurance) intact, and the
# ``NF`` alternation catches the ``AC NF ONLY`` column that a digits-only cut used to miss
# — leaving 261 rows like ``CAMP JOHNSON NF ONLY  ...  AC NF ONLY`` with the column glued
# onto the name. A legitimate name-suffix ``NF ONLY`` / ``- NF ONLY`` / ``(NF ONLY)`` sits
# *before* this gap and is preserved.
_SCHED_COL_RE = re.compile(
    r"\s{2,}\bAC\b[\s.]*[‐‑‒–—\-]?\s*(?:\(?\d{2,4}|NF\b)"
)


def _split_installation_line(line: str) -> str | None:
    """From an indented installation row, return the installation name (drop the AC tail)."""
    # Installation rows carry a trailing schedule column: ``AC   nnn   xxx``,
    # ``AC nnn (xxx)``, or ``AC   NF   ONLY``. Cut at the whitespace-set-off ``AC`` column.
    m = _SCHED_COL_RE.search(line)
    name = line[: m.start()] if m else line
    name = name.strip()
    # Defence in depth: if the column marker was not set off by the usual whitespace gap
    # (odd spacing), scrub a still-glued trailing ``AC NF ONLY`` / ``AC <area>`` /
    # ``AC <digits> <suffix>`` artifact. Only fires when the ``AC`` carries an actual
    # schedule token (NF ONLY or digits); a legitimate name-suffix ``NF ONLY`` with no
    # leading ``AC`` column is left untouched, and no bare ``AC`` is ever truncated.
    name = re.sub(
        r"\s+AC\b[\s.]*[‐‑‒–—\-]?\s*(?:NF\s+ONLY|\(?\d{2,4}(?:[A-Z0-9 ()‐‑‒–—\-]*?))\s*$",
        "",
        name,
    ).strip()
    # An installation name has letters; guard against stray column artifacts.
    if len(name) < 2 or not re.search(r"[A-Za-z]", name):
        return None
    return name


# FWS "State:" heading that opens a county list, e.g. "California:".
_FWS_STATE_HDR_RE = re.compile(r"^\s*([A-Z][A-Za-z .]+?)\s*:\s*$")

# Full US state / territory names -> USPS code, for FWS county blocks that head the
# list with a spelled-out state name rather than a 2-letter code.
_STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "puerto rico": "PR", "guam": "GU", "virgin islands": "VI",
}


def _parse_fws(lines: list[str], wage_area, sched, src) -> list[dict]:
    """FWS-family layout: plain-name installations + a locality-pay county list."""
    rows: list[dict] = []

    # Split the document at the county-list marker.
    loc_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"GENERAL\s+SCHEDULE\s+LOCALITY\s+PAY\s+AREA", ln, re.IGNORECASE):
            loc_idx = i
            break
    inst_end = loc_idx if loc_idx is not None else len(lines)

    # Installations: under each "DEPARTMENT OF THE X   AC – nnn (xx)" header, the plain
    # (unmarked) indented/left-aligned names until a blank line, the next agency, or the
    # "These schedules also apply" boilerplate.
    current_agency: str | None = None
    for ln in lines[:inst_end]:
        s = ln.strip()
        if not s:
            current_agency = None
            continue
        if re.search(r"(These|Pay)\s+schedules?\s+also\s+apply", s, re.IGNORECASE):
            current_agency = None
            continue
        if re.search(r"FWS\s+WAGE\s+AREA", s, re.IGNORECASE):
            continue
        fws_hdr = _FWS_AGENCY_RE.match(ln)
        if fws_hdr:
            current_agency = re.sub(r"\s+", " ", fws_hdr.group("agency").strip())
            continue
        if current_agency is not None and re.search(r"[A-Za-z]", s):
            rows.append(
                {
                    "wage_area": wage_area,
                    "schedule_number": sched,
                    "row_kind": "installation",
                    "agency": current_agency,
                    "installation": re.sub(r"\s+", " ", s),
                    "county": None,
                    "state": None,
                    "applicable_schedule": None,
                    "source_pdf_filename": src,
                }
            )

    # Counties: after the locality-pay marker, a "State:" heading sets the state, then
    # each indented line is a county name. No per-county schedule column exists, so the
    # applicable schedule is the area's own schedule number (as printed).
    appl = str(wage_area) if wage_area is not None else None
    if loc_idx is not None:
        cur_state: str | None = None
        for ln in lines[loc_idx + 1 :]:
            s = ln.strip()
            if not s:
                continue
            if re.search(r"GENERAL\s+SCHEDULE\s+LOCALITY\s+PAY\s+AREA", s, re.IGNORECASE):
                continue
            hdr = _FWS_STATE_HDR_RE.match(ln)
            if hdr:
                cand = hdr.group(1).strip()
                code = _STATE_NAME_TO_CODE.get(cand.lower())
                if code:
                    cur_state = code
                elif re.fullmatch(r"[A-Z]{2}", cand):
                    cur_state = cand
                continue
            if cur_state is None:
                continue
            county = re.sub(r"\s+", " ", s)
            if not re.search(r"[A-Za-z]", county):
                continue
            rows.append(
                {
                    "wage_area": wage_area,
                    "schedule_number": sched,
                    "row_kind": "county",
                    "agency": None,
                    "installation": None,
                    "county": county,
                    "state": cur_state,
                    "applicable_schedule": appl,
                    "source_pdf_filename": src,
                }
            )
    return rows


def parse(pdf_text: str, meta: dict) -> list[dict]:
    """text -> rows. Never raises: returns the rows extractable, else ``[]``."""
    try:
        return _parse(pdf_text, meta)
    except Exception:  # robustness contract: a bad/odd PDF must not blow up the batch.
        return []


def _parse(pdf_text: str, meta: dict) -> list[dict]:
    if not pdf_text or not isinstance(pdf_text, str):
        return []

    src = meta.get("filename")
    wage_area, sched = _extract_area_sched(pdf_text, meta)
    rows: list[dict] = []

    lines = pdf_text.splitlines()

    # ── FWS-family route ─────────────────────────────────────────────────────────────
    # Old files: "{name} FWS WAGE AREA" header, DEPARTMENT-OF headers + plain-name
    # installations, a "GENERAL SCHEDULE LOCALITY PAY AREA" block with a "State:" line
    # and indented county names (no per-county AC/schedule column). Route only when the
    # modern "NAF Wage Area Definition" header is absent.
    if re.search(r"FWS\s+WAGE\s+AREA", pdf_text, re.IGNORECASE) and not re.search(
        r"NAF\s+Wage\s+Area\s+Definition", pdf_text, re.IGNORECASE
    ):
        return _parse_fws(lines, wage_area, sched, src)

    # Locate the county table header ("County and State ... Applicable Schedule").
    county_hdr_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"County\s+and\s+State", ln, re.IGNORECASE) and re.search(
            r"Applicable\s+Schedule", ln, re.IGNORECASE
        ):
            county_hdr_idx = i
            break
    # Fallback: a lone "County and State" header (rare / wrapped).
    if county_hdr_idx is None:
        for i, ln in enumerate(lines):
            if re.search(r"County\s+and\s+State", ln, re.IGNORECASE):
                county_hdr_idx = i
                break

    # ── installation rows: everything from the header block down to the county table ──
    inst_region_end = county_hdr_idx if county_hdr_idx is not None else len(lines)
    current_agency: str | None = None
    for ln in lines[:inst_region_end]:
        if not ln.strip():
            continue
        if _NOISE_RE.search(ln):
            # a "County and State"/"Applicable" header slipped in — stop agency context.
            continue

        # Columnar ("Agency  Installation  CT  NPS") family: agency+installation on one
        # line. Detect by an ``AC nnn (xxx)`` marker with a *lettered* agency starting
        # near column 0 on the left. The definition family instead deeply indents the
        # installation with only whitespace to its left, so require the agency group to
        # begin with a letter within the first few columns (guards against a whitespace-
        # only "agency" match on an indented installation row).
        col_m = re.match(
            r"^ {0,4}(?P<agency>[A-Za-z][A-Za-z0-9 .,'()/&\-]+?)\s{2,}"
            r"(?P<inst>[A-Z0-9][A-Z0-9 .,'()/&\-]+?)\s{2,}"
            r"AC\s+\d{2,4}",
            ln,
        )
        agency_hdr = _AGENCY_RE.match(ln)
        fws_hdr = _FWS_AGENCY_RE.match(ln)

        if agency_hdr and not re.search(r"\bAC\b", ln):
            current_agency = re.sub(r"\s+", " ", agency_hdr.group(1).strip())
            continue
        if fws_hdr:
            # FWS agency header + it also implies subsequent indented installations.
            current_agency = re.sub(r"\s+", " ", fws_hdr.group("agency").strip())
            continue
        if col_m and not agency_hdr:
            agency = re.sub(r"\s+", " ", col_m.group("agency").strip())
            inst = re.sub(r"\s+", " ", col_m.group("inst").strip())
            if inst and agency and not _NOISE_RE.search(agency):
                rows.append(
                    {
                        "wage_area": wage_area,
                        "schedule_number": sched,
                        "row_kind": "installation",
                        "agency": agency,
                        "installation": inst,
                        "county": None,
                        "state": None,
                        "applicable_schedule": None,
                        "source_pdf_filename": src,
                    }
                )
            continue

        # Indented installation row under a current agency header.
        if current_agency is not None:
            # An installation row is indented and carries an AC marker OR (FWS) is a
            # plain indented name. Require either an AC marker or clear indentation.
            has_ac = bool(re.search(r"\bAC\b", ln))
            indented = ln[:1].isspace() or ln.startswith("   ")
            if has_ac or indented:
                inst = _split_installation_line(ln)
                if inst and not _NOISE_RE.search(inst):
                    rows.append(
                        {
                            "wage_area": wage_area,
                            "schedule_number": sched,
                            "row_kind": "installation",
                            "agency": current_agency,
                            "installation": inst,
                            "county": None,
                            "state": None,
                            "applicable_schedule": None,
                            "source_pdf_filename": src,
                        }
                    )

    # ── county rows ──────────────────────────────────────────────────────────────────
    if county_hdr_idx is not None:
        for ln in lines[county_hdr_idx + 1 :]:
            if not ln.strip():
                continue
            if _NOISE_RE.search(ln):
                continue
            # The trailing page marker is a lone right-aligned area number — skip.
            if re.match(r"^\s*\d{2,4}\s*$", ln):
                continue
            # Skip footnote / legislative prose (long sentences, lowercase-heavy).
            if re.match(r"^\s*[*‡]", ln):
                continue
            m = _COUNTY_ROW_RE.match(ln)
            if not m:
                # Stateless applicable-area row (territory/atoll/island, no ", ST").
                sm = _STATELESS_ROW_RE.match(ln)
                if sm and (
                    "AC" in sm.group("mid") or "NF" in sm.group("mid")
                ):
                    county = _clean_county(sm.group("county"))
                    if county and re.search(r"[A-Za-z]", county):
                        rows.append(
                            {
                                "wage_area": wage_area,
                                "schedule_number": sched,
                                "row_kind": "county",
                                "agency": None,
                                "installation": None,
                                "county": county,
                                "state": None,
                                "applicable_schedule": sm.group("appl").strip(),
                                "source_pdf_filename": src,
                            }
                        )
                continue
            county = _clean_county(m.group("county"))
            state = m.group("state").strip()
            appl = m.group("appl").strip()
            # Guard: a real county row has an AC marker in the middle, distinguishing it
            # from footnote sentences that merely happen to contain ", XX".
            if "AC" not in m.group("mid") and "NF" not in m.group("mid"):
                # allow FWS-style (no AC), but only if the line is short & title-like.
                if len(ln) > 90 or re.search(r"[a-z]{4,}\s+[a-z]{4,}\s+[a-z]{4,}", ln):
                    continue
            if not county or not re.search(r"[A-Za-z]", county):
                continue
            rows.append(
                {
                    "wage_area": wage_area,
                    "schedule_number": sched,
                    "row_kind": "county",
                    "agency": None,
                    "installation": None,
                    "county": county,
                    "state": state,
                    "applicable_schedule": appl,
                    "source_pdf_filename": src,
                }
            )

    return rows
