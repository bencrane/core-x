"""SAM.gov 90-day attachment — Phase-1 DETERMINISTIC requirement extraction (the regex lane).

Implements build-plan PHASE 1 (docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md) in the
spec §17 fixed-name module (docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md — Phase 5
structured extraction artifact; the LLM lane lands here later as Phase 2 of the build plan).

WHAT IT DOES (one pass per resource over the three READ-ONLY chunk sinks):
  * INPUT = all resources in govcon_scope_vectors_90day / govcon_pricing_90day /
    govcon_unknown_90day (ALL unknown docs — the lexicon gate applies to the LLM lane only).
  * Re-assembles each resource's text in `chunk_ix` order via OVERLAP-AWARE suffix→prefix matching
    (the chunker whitespace-snaps and .strip()s, so the overlap is ~CHUNK_OVERLAP, never exactly
    CHUNK_OVERLAP; fixed-width stripping corrupts text). A char-offset→chunk_id interval map built
    during reassembly is how regex spans populate `source_chunk_ids`/`evidence_quote`.
  * Pricing lane: the doc-level `cells` grid is read ONCE per resource (chunk_ix=0 filtered scan)
    and scanned as a second evidence region; cells-derived evidence validates against `cells`, not
    `text` (plan Phase 1); cells matches attribute to the resource's first chunk_id.
  * Runs the VERSIONED pattern library (families: clearance, certification, standard, set_aside,
    bonding_insurance, staffing, labor_category, pop, license). Rows write `confidence=1.0`
    (float32), `validated=true`, extractor = `regex:<family>@v1` (matches the plan's
    `extractor LIKE 'regex:%'` idempotency predicate).
  * NEGATION handling is conservative (precision over recall — this feeds live outreach): any
    negation cue in a window around a match SUPPRESSES the match and counts it ("clearance not
    required" must never produce a clearance-required row; when in doubt, drop and count).
  * REDACTION AT WRITE: for any resource whose chunks carry non-empty `content_marking` (the single
    egress enforcement signal), the verbatim free-text fields `evidence_quote` AND
    `requirement_detail` are written NULL (plan wording); `place_of_performance_text`/
    `place_of_performance` (also doc-verbatim free text) are NULLed under the same rule
    (anti-pattern #10: verbatim text from marked docs never reaches serving). Structured/enum
    fields stay.
  * Same pass emits govcon_labor_demand_90day (spec §3.6 exact): one row per
    (resource, labor_category_norm); `demand_id = <resource_id>:<n>` with n assigned
    DETERMINISTICALLY by rank over (labor_category_norm, first chunk_ix) — never a write-order
    ordinal. Also computes `lexicon_hit_fullbody` over the reassembled body for the ledger.

IDEMPOTENCY (plan §5 / anti-pattern #3 — scoped delete-before-merge, the regex lane's own lane):
  * Per flush batch, BEFORE merging: delete("resource_id IN (...) AND extractor LIKE 'regex:%'")
    on the requirements sink and delete("resource_id IN (...)") on govcon_labor_demand_90day
    (plan Phase-1 verbatim — the labor sink delete is unscoped because demand_id is a
    deterministic rank, and stale ranks from ANY prior pass must die before the new ranks land).
    Extractor evolution shifts value_norm (→ new requirement_id) and chunking changes shift
    demand_id; content-hash merge alone would strand stale orphans.
  * requirement_id = sha256(resource_id|requirement_type|value_norm)[:24] — content hash, never
    ordinal. merge_insert on requirement_id / demand_id after the scoped delete.
  * Failed resources are ledgered regex_state='failed' WITHOUT deleting their previously-merged
    rows (a transient failure never destroys a prior good extraction).
  * JSONL checkpoint per resource written ONLY AFTER its batch's deletes+merges+ledger are durable;
    resume = ledger regex_state terminal ∪ checkpoint (re-select non-terminal).
  * Ledger govcon_requirements_extract_ledger_90day merges on resource_id PRESERVING the LLM-lane
    columns (llm_state/batch_id/model/prompt_hash/n_requirements_llm/validation_pass_rate) — the
    regex lane writes batch_id NULL and llm_state='pending' only on first insert.

SAFETY RAILS: chunk sinks are READ-ONLY (never written, never re-marked); the `embedding` column is
NEVER in any column list; every sink write runs under its SinkCommitLease (D3) and is preceded by
govcon_gtm_schemas.assert_schema (frozen-schema drift detector, anti-pattern #1).

Run (corpus pass is hours-scale: daemonize/background it; resumable):
    doppler run -- python pipelines/sam_gov/sam_labor_demand_extract_90day.py \
        --phase extract --resume --daemon
    ... --phase extract --resource-ids RID1,RID2          # explicit slice (smoke)
    ... --phase extract --max-resources 50                # capped slice (smoke)
    ... --phase index                                     # after Phase-1 merges settle (plan):
                                                          # BTREE/BITMAP campaign, NO requirement_id
                                                          # index until Phase-2 merges complete
"""
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    CHUNK_OVERLAP, LABOR_LEXICON_RX, MAX_CHUNKS_PER_FILE, PRICING_URI, SCOPE_URI, UNKNOWN_URI,
    SinkCommitLease, _daemonize, _dataset_exists, _r2_storage_options,
)
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    EXTRACT_LEDGER_URI, LABOR_DEMAND_URI, REQUIREMENTS_URI,
    assert_schema, extract_ledger_schema, labor_demand_schema, requirements_schema,
)

FEED = "sam_labor_demand_extract_90day"
REGEX_LANE_VERSION = "v1"
LABOR_DEMAND_EXTRACTOR = f"regex:labor_demand@{REGEX_LANE_VERSION}"   # §3.6 sink has no version col

CKPT_PATH = os.environ.get("GOVCON_REGEX_CKPT", "/tmp/govcon_regex_extract_ckpt.jsonl")
LOG_PATH = os.environ.get("GOVCON_REGEX_LOG", "/tmp/govcon_regex_extract.log")
REPORT_PATH = os.environ.get("GOVCON_REGEX_REPORT", "/tmp/govcon_regex_extract_report.json")
SCAN_BATCH_ROWS = int(os.environ.get("GOVCON_REGEX_SCAN_BATCH", "4096"))
FLUSH_RESOURCES = int(os.environ.get("GOVCON_REGEX_FLUSH_RESOURCES", "200"))

# ── Extraction tunables (deterministic; bump REGEX_LANE_VERSION on ANY behavior change) ──────────
MIN_OVERLAP = 16          # smallest suffix→prefix run accepted as chunker overlap (anti-false-join)
NEG_BEFORE = 100          # negation window chars before a match
NEG_AFTER = 60            # negation window chars after a match
MANDATORY_WINDOW = 120    # "shall/must/required" detection window (both sides)
CONTEXT_WINDOW = 100      # context-gate window (set-aside / labor demand cues)
HEADCOUNT_LOOKBACK = 30   # chars before a labor term scanned for an adjacent count
EVIDENCE_MAX = 300        # plan: evidence_quote <= 300 verbatim
DETAIL_MAX = 200

# Chunk-sink read columns — the embedding column is NEVER read (hard rule).
CHUNK_COLS = ["chunk_id", "resource_id", "chunk_ix", "text", "content_marking",
              "notice_id", "solicitation_number", "naics_code", "contract_award_unique_key"]

_SINKS = (("scope", SCOPE_URI), ("pricing", PRICING_URI), ("unknown", UNKNOWN_URI))

# ════════════════════════════════════════════════════════════════ reassembly (overlap-aware)
def _overlap_len(tail: str, nxt: str) -> int:
    """Largest k with MIN_OVERLAP <= k <= len(tail) such that tail[-k:] == nxt[:k]. `tail` is the
    accumulated body's last CHUNK_OVERLAP chars. Probes the largest k first (leftmost occurrence of
    nxt's MIN_OVERLAP-char prefix inside tail), verifying the full run — the chunker's overlap is
    ~CHUNK_OVERLAP but never exact (whitespace snap + .strip()), so the run length is discovered,
    never assumed."""
    kmax = min(len(tail), len(nxt))
    if kmax < MIN_OVERLAP:
        return 0
    probe = nxt[:MIN_OVERLAP]
    idx = tail.find(probe)
    while idx != -1:
        k = len(tail) - idx
        if k <= len(nxt) and nxt[:k] == tail[idx:]:
            return k
        idx = tail.find(probe, idx + 1)
    return 0


def reassemble_with_offsets(rows: list[tuple[int, str, str]]) -> tuple[str, list[tuple[int, int, str]]]:
    """rows = [(chunk_ix, chunk_id, text)] — re-assemble in chunk_ix order with overlap-aware
    suffix→prefix matching. Returns (body, intervals) where intervals = [(start, end, chunk_id)] in
    body coordinates and body[start:end] == that chunk's text verbatim (the offset→chunk_id map the
    plan requires for span attribution). Chunks with no detected overlap are joined with '\\n'."""
    parts: list[str] = []
    intervals: list[tuple[int, int, str]] = []
    total = 0
    tail = ""                                     # invariant: tail == body[-CHUNK_OVERLAP:]
    for _ix, cid, text in sorted(rows, key=lambda r: r[0]):
        t = text or ""
        if total == 0:
            appended = t
            start = 0
        else:
            k = _overlap_len(tail, t)
            if k > 0:
                appended = t[k:]
                start = total - k
            else:
                appended = "\n" + t
                start = total + 1
        parts.append(appended)
        total += len(appended)
        intervals.append((start, start + len(t), cid))
        tail = (tail + appended)[-CHUNK_OVERLAP:]
    return "".join(parts), intervals


def chunks_for_span(intervals: list[tuple[int, int, str]], starts: list[int],
                    s: int, e: int) -> list[str]:
    """All chunk_ids whose [start,end) interval intersects span [s,e) — overlap regions mean a span
    can live in two adjacent chunks (boundary-straddling evidence, anti-pattern #8)."""
    out: list[str] = []
    i = bisect.bisect_right(starts, s) - 1
    while i > 0 and intervals[i - 1][1] > s:
        i -= 1
    i = max(i, 0)
    for j in range(i, len(intervals)):
        cs, ce, cid = intervals[j]
        if cs >= e:
            break
        if ce > s:
            out.append(cid)
    return out


# ════════════════════════════════════════════════════════════════ normalization (unit-tested)
_CLEARANCE_RANK = {"PUBLIC_TRUST": 0, "CONFIDENTIAL": 1, "SECRET": 2, "TOP_SECRET": 3, "TS_SCI": 4}


def norm_clearance(s: str | None) -> str | None:
    """Enum-locked clearance levels (plan: `clearance_level` enum-locked)."""
    if not s:
        return None
    t = re.sub(r"[\s/–—-]+", " ", s.lower()).strip()
    if "sci" in t:
        return "TS_SCI"
    if "top secret" in t:
        return "TOP_SECRET"
    if t == "secret":
        return "SECRET"
    if t == "confidential":
        return "CONFIDENTIAL"
    if "public trust" in t:
        return "PUBLIC_TRUST"
    return None


def norm_dollars(s: str) -> str | None:
    """'1,000,000' / '1,000,000.00' -> '1000000' (integer dollars, deterministic)."""
    t = s.replace(",", "").strip().split(".")[0]
    if not t.isdigit():
        return None
    return str(int(t))


_MONTH_NUM = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
              "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def parse_date(s: str) -> dt.date | None:
    """Deterministic date parse over the formats the PoP pattern can capture; None on anything
    else (precision over recall — a PoP row requires BOTH dates to parse)."""
    t = s.strip().rstrip(".,")
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$", t)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        yy = yy + 2000 if yy < 100 else yy
        try:
            return dt.date(yy, mm, dd)
        except ValueError:
            return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", t)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.match(r"^([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})$", t)
    if m:
        mn = _MONTH_NUM.get(m.group(1)[:3].lower())
        if mn is None:
            return None
        try:
            return dt.date(int(m.group(3)), mn, int(m.group(2)))
        except ValueError:
            return None
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\.?,?\s*(\d{4})$", t)
    if m:
        mn = _MONTH_NUM.get(m.group(2)[:3].lower())
        if mn is None:
            return None
        try:
            return dt.date(int(m.group(3)), mn, int(m.group(1)))
        except ValueError:
            return None
    return None


def requirement_id(resource_id: str, requirement_type: str, value_norm: str) -> str:
    """Plan-exact content hash: sha256(resource_id|requirement_type|value_norm)[:24]."""
    return hashlib.sha256(
        f"{resource_id}|{requirement_type}|{value_norm}".encode()).hexdigest()[:24]


# ════════════════════════════════════════════════════════════════ negation / modality windows
# Conservative by design (hard rule 5): any cue in the window suppresses; suppressions are counted.
# 'no' is excluded when it is "No." (document numbering) or a requirement-strengthening idiom
# ("no later/less/fewer/more/earlier than") — everything else drops the match.
NEGATION_RX = re.compile(
    r"\b(?:not|never|without|waived?|waiver|unless|except|nor)\b"
    r"|\bno\b(?!\.|\s+(?:later|less|fewer|more|earlier))"
    r"|\bin\s+lieu\s+of\b|n['’]t\b", re.IGNORECASE)
MANDATORY_RX = re.compile(r"\b(?:shall|must|will|required?|requires|mandatory)\b", re.IGNORECASE)


def is_negated(text: str, s: int, e: int) -> bool:
    before = text[max(0, s - NEG_BEFORE):s]
    after = text[e:e + NEG_AFTER]
    return bool(NEGATION_RX.search(before)) or bool(NEGATION_RX.search(after))


def is_mandatory(text: str, s: int, e: int) -> bool:
    window = text[max(0, s - MANDATORY_WINDOW):min(len(text), e + MANDATORY_WINDOW)]
    return bool(MANDATORY_RX.search(window))


# ════════════════════════════════════════════════════════════════ pattern library (regex:v1)
# Each pattern: (family, requirement_type, compiled rx, handler, optional context_rx).
# handler(match, region_text) -> dict(value_norm=..., [clearance_level|headcount|pop_start|pop_end|
# wage_floor|labor_category]) or None to DROP (counted). Bare acronyms are case-sensitive UPPERCASE
# (the EAR/CUI full-body lesson); spelled-out phrases are case-insensitive.
_CLR_LEVEL = r"top\s+secret(?:\s*[/–-]\s*sci|\s+sci)?|ts\s*/\s*sci|secret|confidential|public\s+trust"

_h_clearance = lambda m, t: (  # noqa: E731
    {"value_norm": f"clearance:{lvl.lower()}", "clearance_level": lvl}
    if (lvl := norm_clearance(m.group("level"))) else None)


def _h_facility(m, t):
    lvl = norm_clearance(m.groupdict().get("level"))
    vn = f"facility_clearance:{lvl.lower()}" if lvl else "facility_clearance"
    return {"value_norm": vn, "clearance_level": lvl}


def _h_iso(m, t):
    return {"value_norm": f"iso_{m.group('num')}"}


def _h_mil_std(m, t):
    return {"value_norm": f"mil-std-{m.group('num').lower()}"}


def _h_nist(m, t):
    return {"value_norm": f"nist-800-{m.group('sub')}"}


def _h_ufc(m, t):
    return {"value_norm": "ufc-" + "-".join(re.findall(r"\d+|[A-Z]$", m.group("num")))}


def _h_astm(m, t):
    return {"value_norm": f"astm-{m.group('ltr').lower()}{m.group('num')}"}


def _h_wd(m, t):
    num = m.groupdict().get("num")
    return {"value_norm": f"wd:{num.replace(' ', '-')}"} if num else {"value_norm": "wage_determination"}


def _h_bond(m, t):
    kind = m.group("kind").lower()
    pct = m.groupdict().get("pct")
    return {"value_norm": f"{kind}_bond" + (f":{int(pct)}pct" if pct else "")}


def _h_insurance(m, t):
    kind = re.sub(r"\s+", "_", re.sub(r"[^a-z ]", "", m.group("kind").lower()).strip())
    am = re.search(r"\$\s?(\d[\d,]*(?:\.\d+)?)", t[m.end():m.end() + 60])
    amount = norm_dollars(am.group(1)) if am else None
    return {"value_norm": f"insurance:{kind}" + (f":{amount}" if amount else "")}


def _h_fte(m, t):
    n = int(m.group("n"))
    if not 1 <= n <= 5000:
        return None
    noun = m.group("noun").lower()
    key = "fte" if ("fte" in noun or "full" in noun) else "headcount"
    return {"value_norm": f"{key}:{n}", "headcount": n}


def _h_pop(m, t):
    d1, d2 = parse_date(m.group("d1")), parse_date(m.group("d2"))
    if d1 is None or d2 is None or d2 < d1:
        return None
    return {"value_norm": f"period_of_performance:{d1.isoformat()}:{d2.isoformat()}",
            "pop_start": d1, "pop_end": d2}


_PE_NORM = {"engineer": "professional_engineer", "professional engineer": "professional_engineer",
            "professional_engineer": "professional_engineer"}


def _h_license_trade(m, t):
    trade = re.sub(r"\s+", " ", m.group("trade").lower())
    trade = _PE_NORM.get(trade, trade).replace(" ", "_")
    return {"value_norm": f"license:{trade}"}


def _h_license_kind(m, t):
    kind = re.sub(r"\s+", " ", m.group("kind").lower()).rstrip("s").replace("'", "")
    kind = {"contractor": "contractor", "electrical": "electrical", "plumbing": "plumbing",
            "professional engineer": "professional_engineer",
            "professional engineering": "professional_engineer"}.get(kind)
    if kind is None:
        return None
    return {"value_norm": f"license:{kind}"}


def _h_license_state(m, t):
    st = re.sub(r"\s+", "_", m.group("state").strip().lower())
    return {"value_norm": f"license:business:{st}"}


def _make_labor_handler(canon: str):
    def h(m, t):
        s = m.start()
        look = t[max(0, s - HEADCOUNT_LOOKBACK):s]
        hm = re.search(r"(?:(\d{1,4})\s*|\(\s*(\d{1,4})\s*\)\s*)$", look)
        n = int(hm.group(1) or hm.group(2)) if hm else None
        if n is not None and not 1 <= n <= 5000:
            n = None
        ls = t.rfind("\n", 0, s) + 1
        le = t.find("\n", m.end())
        le = len(t) if le == -1 else le
        wm = re.search(r"\$\s?(\d{1,3}\.\d{2})\b", t[ls:le])
        wage = float(wm.group(1)) if wm else None
        return {"value_norm": canon, "headcount": n, "wage_floor": wage, "labor_category": canon}
    return h


# Curated labor-category lexicon (precision over recall; demand-cue gated). canonical -> body regex.
LABOR_TERMS: dict[str, str] = {
    "electrician": r"electricians?",
    "plumber": r"plumbers?",
    "pipefitter": r"pipe\s?fitters?",
    "carpenter": r"carpenters?",
    "welder": r"welders?",
    "painter": r"painters?",
    "roofer": r"roofers?",
    "mason": r"masons?",
    "glazier": r"glaziers?",
    "millwright": r"millwrights?",
    "hvac_technician": r"hvac\s+(?:technicians?|mechanics?)",
    "sheet_metal_worker": r"sheet\s+metal\s+(?:workers?|mechanics?)",
    "crane_operator": r"crane\s+operators?",
    "heavy_equipment_operator": r"heavy\s+equipment\s+operators?",
    "equipment_operator": r"equipment\s+operators?",
    "general_laborer": r"general\s+laborers?",
    "security_guard": r"security\s+(?:guards?|officers?)",
    "custodian": r"custodians?",
    "janitor": r"janitors?",
    "registered_nurse": r"registered\s+nurses?",
    "licensed_practical_nurse": r"licensed\s+practical\s+nurses?",
    "medical_assistant": r"medical\s+assistants?",
    "project_manager": r"project\s+managers?",
    "program_manager": r"program\s+managers?",
    "site_superintendent": r"site\s+superintendents?",
    "quality_control_manager": r"quality\s+control\s+managers?",
    "safety_officer": r"safety\s+officers?",
    "surveyor": r"surveyors?",
    "locksmith": r"locksmiths?",
    "pest_control_technician": r"pest\s+control\s+(?:technicians?|operators?)",
    "food_service_worker": r"food\s+service\s+workers?",
    "interpreter": r"interpreters?",
    "translator": r"translators?",
    "instructor": r"instructors?",
    "truck_driver": r"truck\s+drivers?",
    "dispatcher": r"dispatchers?",
}

SET_ASIDE_CTX_RX = re.compile(r"set[- ]?aside|reserved\s+for|restricted\s+to", re.IGNORECASE)
DEMAND_CUE_RX = re.compile(
    r"\b(?:shall|must|will|provided?|furnish(?:ed)?|employ(?:ed)?|assign(?:ed)?|maintain(?:ed)?|"
    r"required?|qualified|certified|licensed|labor\s+categor\w*|ftes?|personnel|positions?|"
    r"staff(?:ing)?|workforce|perform(?:ed)?)\b", re.IGNORECASE)

_MONTHS_S = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
             r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_DATE_S = (r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}"
           rf"|{_MONTHS_S}\.?\s+\d{{1,2}},?\s+\d{{4}}"
           rf"|\d{{1,2}}\s+{_MONTHS_S}\.?,?\s*\d{{4}})")


class Pattern:
    __slots__ = ("family", "rtype", "rx", "handler", "context_rx")

    def __init__(self, family, rtype, rx, handler, context_rx=None):
        self.family, self.rtype, self.rx = family, rtype, rx
        self.handler, self.context_rx = handler, context_rx


def _build_patterns() -> list[Pattern]:
    I = re.IGNORECASE  # noqa: E741
    pats: list[Pattern] = [
        # ── clearance (personnel + facility, levels) ─────────────────────────────────────────
        Pattern("clearance", "clearance",
                re.compile(rf"(?:active\s+)?(?P<level>{_CLR_LEVEL})\s+(?:security\s+)?clearance", I),
                _h_clearance),
        Pattern("clearance", "clearance",
                re.compile(rf"clearance\s+(?:at\s+the\s+)?(?P<level>{_CLR_LEVEL})\s+level", I),
                _h_clearance),
        Pattern("clearance", "clearance",
                re.compile(rf"(?:(?P<level>{_CLR_LEVEL})\s+)?facility\s+(?:security\s+)?clearance", I),
                _h_facility),
        Pattern("clearance", "clearance",
                re.compile(r"\bFCL\b"),                       # bare acronym: uppercase-only
                lambda m, t: {"value_norm": "facility_clearance"}),
        # ── certification (CMMC levels / ISO family / AS9100) ────────────────────────────────
        Pattern("certification", "certification",
                re.compile(r"\bCMMC\b[\s,]*(?:2\.0\s*)?"
                           r"(?i:(?:maturity\s+)?(?:level|lvl|l))\s*[-: ]?\s*(?P<lvl>[1-3])\b"),
                lambda m, t: {"value_norm": f"cmmc_l{m.group('lvl')}"}),
        Pattern("certification", "certification",
                re.compile(r"\bCMMC\b"),
                lambda m, t: {"value_norm": "cmmc"}),
        Pattern("certification", "certification",
                re.compile(r"\bISO\s*(?:/\s*IEC)?\s*[- ]?\s*"
                           r"(?P<num>9000|9001|9100|13485|14001|17020|17025|20000|27001|45001)\b"
                           r"(?:\s*:\s*\d{4})?"),
                _h_iso),
        Pattern("certification", "certification",
                re.compile(r"\bAS\s?-?9100[A-D]?\b"),
                lambda m, t: {"value_norm": "as9100"}),
        # ── standard compliance (MIL-STD / NIST / UFC / ASTM / EM 385 / SCA / Davis-Bacon / WD) ──
        Pattern("standard", "standard_compliance",
                re.compile(r"\bMIL[- ]?STD[- ]?(?P<num>\d{3,5}[A-Z]?)\b"), _h_mil_std),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bNIST\b(?i:\s*(?:sp|special\s+publication)?)\s*[- ]?\s*"
                           r"800[-–\s]*(?P<sub>\d{2,3})\b"), _h_nist),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bUFC\s*[- ]?\s*(?P<num>\d{1,2}\s*-\s*\d{3}\s*-\s*\d{2}[A-Z]?)\b"),
                _h_ufc),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bASTM\s*[- ]?(?P<ltr>[A-Z])\s?(?P<num>\d{2,4})\b"), _h_astm),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bEM\s*[- ]?385[- ]1[- ]1\b"),
                lambda m, t: {"value_norm": "em-385-1-1"}),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bservice\s+contract\s+act\b|\bservice\s+contract\s+labor\s+standards\b", I),
                lambda m, t: {"value_norm": "sca"}),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bdavis[- ]bacon\b", I),
                lambda m, t: {"value_norm": "davis_bacon"}),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bwage\s+determination\b(?:\s*(?:no\.?|number|#))?\s*[:#]?\s*"
                           r"(?P<num>\d{4}[- ]\d{4})?", I), _h_wd),
        Pattern("standard", "standard_compliance",
                re.compile(r"\bWD\s*(?:No\.?|#)?\s*[:#]?\s*(?P<num>\d{4}[- ]\d{4})\b"), _h_wd),
        # ── set-asides (doc-stated eligibility constraint -> vehicle_constraint) ─────────────
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\bSDVOSB\b|(?i:service[- ]disabled\s+veteran[- ]owned)"),
                lambda m, t: {"value_norm": "set_aside:sdvosb"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\bVOSB\b|(?i:\bveteran[- ]owned\s+small\s+business\b)"),
                lambda m, t: {"value_norm": "set_aside:vosb"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\bEDWOSB\b"),
                lambda m, t: {"value_norm": "set_aside:edwosb"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\bWOSB\b|(?i:\bwom[ae]n[- ]owned\s+small\s+business\b)"),
                lambda m, t: {"value_norm": "set_aside:wosb"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\b8\s?\(\s?a\s?\)"),
                lambda m, t: {"value_norm": "set_aside:8a"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\bHUBZone\b", I),
                lambda m, t: {"value_norm": "set_aside:hubzone"}, SET_ASIDE_CTX_RX),
        Pattern("set_aside", "vehicle_constraint",
                re.compile(r"\b(?:total|100\s?%)\s+small\s+business\s+set[- ]?aside\b", I),
                lambda m, t: {"value_norm": "set_aside:small_business"}),
        # ── bonding / insurance thresholds ───────────────────────────────────────────────────
        Pattern("bonding_insurance", "insurance_bonding",
                re.compile(r"(?:(?P<pct>\d{1,3})\s*(?:%|percent)\s+)?(?P<kind>performance|payment|bid)"
                           r"\s+bonds?\b", I), _h_bond),
        Pattern("bonding_insurance", "insurance_bonding",
                re.compile(r"\bbid\s+guarantee\b", I),
                lambda m, t: {"value_norm": "bid_bond"}),
        Pattern("bonding_insurance", "insurance_bonding",
                re.compile(r"(?P<kind>general\s+liability|workers'?\s+compensation|"
                           r"professional\s+liability|builder'?s\s+risk|automobile\s+liability|"
                           r"errors\s+and\s+omissions)\s+(?:insurance|coverage)\b", I), _h_insurance),
        # ── staffing (FTE / headcount) ───────────────────────────────────────────────────────
        Pattern("staffing", "staffing_constraint",
                re.compile(r"\b(?P<n>\d{1,4})\s*(?:\(\s*\d{1,4}\s*\))?\s+"
                           r"(?P<noun>full[- ]time\s+equivalents?|FTEs?)\b", I), _h_fte),
        Pattern("staffing", "staffing_constraint",
                re.compile(r"\b(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
                           r"fifteen|twenty|thirty|forty|fifty)\s*\(\s*(?P<n>\d{1,4})\s*\)\s+"
                           r"(?P<noun>full[- ]time\s+equivalents?|FTEs?|personnel|employees|positions?)\b",
                           I), _h_fte),
        Pattern("staffing", "staffing_constraint",
                re.compile(r"\bminimum\s+of\s+(?P<n>\d{1,4})\s+"
                           r"(?P<noun>personnel|employees|staff|workers)\b", I), _h_fte),
        # ── period of performance (both dates must parse) ────────────────────────────────────
        Pattern("pop", "deliverable",
                re.compile(rf"period\s+of\s+performance[\s\S]{{0,80}}?(?P<d1>{_DATE_S})\s*"
                           rf"(?:through|thru|to|until|[-–—])\s*(?P<d2>{_DATE_S})", I), _h_pop),
        # ── state / trade licenses ───────────────────────────────────────────────────────────
        Pattern("license", "license",
                re.compile(r"\blicen[cs]ed\s+(?P<trade>electrician|plumber|contractor|"
                           r"professional\s+engineer|engineer)\b", I), _h_license_trade),
        Pattern("license", "license",
                re.compile(r"\b(?P<kind>contractor'?s?|electrical|plumbing|"
                           r"professional\s+engineer(?:ing)?)\s+licen[cs]e\b", I), _h_license_kind),
        Pattern("license", "license",
                re.compile(r"licen[cs]ed?\s+(?:to\s+do\s+business\s+)?in\s+the\s+[Ss]tate\s+of\s+"
                           r"(?P<state>[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"), _h_license_state),
    ]
    # ── labor categories (lexicon, demand-cue gated) ──────────────────────────────────────────
    for canon, body_rx in LABOR_TERMS.items():
        pats.append(Pattern("labor_category", "labor_category",
                            re.compile(rf"\b(?:{body_rx})\b", I),
                            _make_labor_handler(canon), DEMAND_CUE_RX))
    return pats


PATTERNS: list[Pattern] = _build_patterns()
PATTERN_FAMILY_COUNTS: dict[str, int] = {}
for _p in PATTERNS:
    PATTERN_FAMILY_COUNTS[_p.family] = PATTERN_FAMILY_COUNTS.get(_p.family, 0) + 1

# Doc-level auxiliary (NOT a requirement family — fills labor-demand/place columns only).
PLACE_RX = re.compile(r"place\s+of\s+performance\s*(?:\([^)\n]{0,40}\))?\s*"
                      r"(?:is|shall\s+be|will\s+be)?\s*[:\-–]\s*(?P<place>[^\n;|]{3,80})",
                      re.IGNORECASE)


# ════════════════════════════════════════════════════════════════ extraction core (pure)
def extract_from_text(text: str, counters: dict | None = None) -> list[dict]:
    """Run the full pattern library over one region. Returns match dicts sorted by (start, end);
    negation-suppressed / context-dropped / invalid-value matches are counted into `counters`
    ({'negation': {family: n}, 'no_context': {...}, 'invalid': {...}}) and DROPPED."""
    if counters is None:
        counters = {}
    neg = counters.setdefault("negation", {})
    noctx = counters.setdefault("no_context", {})
    inval = counters.setdefault("invalid", {})
    out: list[dict] = []
    if not text:
        return out
    for p in PATTERNS:
        for m in p.rx.finditer(text):
            s, e = m.start(), m.end()
            if p.context_rx is not None:
                window = text[max(0, s - CONTEXT_WINDOW):min(len(text), e + CONTEXT_WINDOW)]
                if not p.context_rx.search(window):
                    noctx[p.family] = noctx.get(p.family, 0) + 1
                    continue
            if is_negated(text, s, e):
                neg[p.family] = neg.get(p.family, 0) + 1
                continue
            d = p.handler(m, text)
            if d is None or not d.get("value_norm"):
                inval[p.family] = inval.get(p.family, 0) + 1
                continue
            out.append({"family": p.family, "requirement_type": p.rtype, "start": s, "end": e,
                        "raw": m.group(0), "mandatory": is_mandatory(text, s, e), **d})
    out.sort(key=lambda r: (r["start"], r["end"]))
    return _post_rules(_drop_contained_labor(out))


def _drop_contained_labor(matches: list[dict]) -> list[dict]:
    """Within one region, drop a labor_category match whose span is strictly contained in another
    labor_category match's span ('equipment operator' inside 'heavy equipment operator')."""
    labor = [m for m in matches if m["family"] == "labor_category"]
    drop: set[int] = set()
    for a in labor:
        for b in labor:
            if a is b:
                continue
            if b["start"] <= a["start"] and a["end"] <= b["end"] and \
                    (b["end"] - b["start"]) > (a["end"] - a["start"]):
                drop.add(id(a))
    return [m for m in matches if id(m) not in drop]


def _post_rules(matches: list[dict]) -> list[dict]:
    """Deterministic family post-rules: leveled CMMC supersedes the bare 'cmmc' value."""
    if any(m["value_norm"].startswith("cmmc_l") for m in matches):
        matches = [m for m in matches if m["value_norm"] != "cmmc"]
    return matches


def make_quote(region_text: str, s: int, e: int) -> str:
    """<=300-char verbatim evidence window around [s,e). Deterministic."""
    if e - s >= EVIDENCE_MAX:
        return region_text[s:s + EVIDENCE_MAX]
    qs = max(0, s - 80)
    qe = min(len(region_text), max(e + 80, qs + EVIDENCE_MAX))
    qe = min(qe, qs + EVIDENCE_MAX)
    return region_text[qs:qe]


def collapse_matches(matches: list[dict]) -> dict[tuple[str, str], dict]:
    """One entry per (requirement_type, value_norm). Representative = first occurrence in region
    order (text before cells) then offset order; headcount=max, first non-null clearance/pop/wage,
    mandatory=OR, spans accumulated. Deterministic given the sorted input."""
    out: dict[tuple[str, str], dict] = {}
    for m in matches:
        key = (m["requirement_type"], m["value_norm"])
        cur = out.get(key)
        if cur is None:
            out[key] = {
                "family": m["family"], "requirement_type": m["requirement_type"],
                "value_norm": m["value_norm"], "raw": m["raw"], "mandatory": bool(m["mandatory"]),
                "headcount": m.get("headcount"), "clearance_level": m.get("clearance_level"),
                "pop_start": m.get("pop_start"), "pop_end": m.get("pop_end"),
                "wage_floor": m.get("wage_floor"), "labor_category": m.get("labor_category"),
                "first_region": m["region"], "first_start": m["start"], "first_end": m["end"],
                "spans": [(m["region"], m["start"], m["end"])],
            }
        else:
            cur["mandatory"] = cur["mandatory"] or bool(m["mandatory"])
            if m.get("headcount") is not None:
                cur["headcount"] = max(cur["headcount"] or 0, m["headcount"])
            for f in ("clearance_level", "pop_start", "pop_end", "wage_floor"):
                if cur.get(f) is None and m.get(f) is not None:
                    cur[f] = m[f]
            cur["spans"].append((m["region"], m["start"], m["end"]))
    return out


def process_resource_payload(resource_id: str, chunk_rows: list[dict], cells: str | None,
                             run_id: str, now: dt.datetime,
                             counters: dict | None = None) -> dict:
    """PURE core: one resource's chunk rows (+ optional pricing cells grid) -> requirement rows,
    labor-demand rows, and the ledger update. No I/O. chunk_rows carry the CHUNK_COLS fields."""
    rows = sorted(chunk_rows, key=lambda r: r["chunk_ix"])
    meta = rows[0]
    marked = any(r.get("content_marking") for r in rows)
    marking_union = sorted({c for r in rows for c in (r.get("content_marking") or [])})
    n_chunks = len(rows)
    coverage_truncated = n_chunks >= MAX_CHUNKS_PER_FILE
    ix_by_chunk = {r["chunk_id"]: r["chunk_ix"] for r in rows}
    first_chunk_id = rows[0]["chunk_id"]

    body, intervals = reassemble_with_offsets(
        [(r["chunk_ix"], r["chunk_id"], r["text"]) for r in rows])
    starts = [iv[0] for iv in intervals]
    if counters is None:
        counters = {}

    matches = [dict(m, region="text") for m in extract_from_text(body, counters)]
    if cells:
        matches += [dict(m, region="cells") for m in extract_from_text(cells, counters)]
    matches = _post_rules(_drop_contained_labor(matches))
    collapsed = collapse_matches(matches)

    # doc-level auxiliaries
    pm = PLACE_RX.search(body) or (PLACE_RX.search(cells) if cells else None)
    doc_place = re.sub(r"\s+", " ", pm.group("place")).strip() if pm else None
    if doc_place is not None and not re.search(r"[A-Za-z]", doc_place):
        doc_place = None
    pop_entries = sorted([c for c in collapsed.values() if c["family"] == "pop"],
                         key=lambda c: (c["first_region"] != "text", c["first_start"]))
    doc_pop = pop_entries[0] if pop_entries else None
    clr_levels = [c["clearance_level"] for c in collapsed.values()
                  if c["family"] == "clearance" and c["clearance_level"]]
    doc_clearance_max = max(clr_levels, key=lambda v: _CLEARANCE_RANK[v]) if clr_levels else None
    lexicon_hit = bool(LABOR_LEXICON_RX.search(body)) or bool(cells and LABOR_LEXICON_RX.search(cells))

    def span_chunks(spans: list[tuple[str, int, int]]) -> list[str]:
        ids: set[str] = set()
        for region, s, e in spans:
            if region == "text":
                ids.update(chunks_for_span(intervals, starts, s, e))
            else:                                     # cells grid rides the first chunk row
                ids.add(first_chunk_id)
        return sorted(ids, key=lambda cid: ix_by_chunk.get(cid, 1 << 30))

    req_rows: list[dict] = []
    for (rtype, value_norm), c in sorted(collapsed.items()):
        chunk_ids = span_chunks(c["spans"])
        region_text = body if c["first_region"] == "text" else (cells or "")
        quote = make_quote(region_text, c["first_start"], c["first_end"])
        req_rows.append({
            "requirement_id": requirement_id(resource_id, rtype, value_norm),
            "resource_id": resource_id,
            "notice_id": meta.get("notice_id"),
            "solicitation_number": meta.get("solicitation_number"),
            "naics_code": meta.get("naics_code"),
            "contract_award_unique_key": meta.get("contract_award_unique_key"),
            "requirement_type": rtype,
            "requirement_value": value_norm,
            "requirement_detail": None if marked else c["raw"][:DETAIL_MAX],
            "mandatory": c["mandatory"],
            "headcount": c["headcount"],
            "clearance_level": c["clearance_level"],
            "pop_start": c["pop_start"], "pop_end": c["pop_end"],
            "place_of_performance_text": (None if marked else doc_place) if c["family"] == "pop" else None,
            "wage_floor": c["wage_floor"],
            "source_chunk_ids": chunk_ids,
            "evidence_quote": None if marked else quote,
            "validated": True, "marked_resource": marked, "coverage_truncated": coverage_truncated,
            "extractor": f"regex:{c['family']}@{REGEX_LANE_VERSION}",
            "extractor_version": REGEX_LANE_VERSION,
            "confidence": 1.0,
            "run_id": run_id, "created_at": now,
        })

    # labor demand (§3.6): one row per labor_category_norm; demand_id = deterministic rank over
    # (labor_category_norm, first chunk_ix) — NOT write order.
    labor_entries = []
    for c in collapsed.values():
        if c["family"] != "labor_category":
            continue
        chunk_ids = span_chunks(c["spans"])
        first_ix = ix_by_chunk.get(chunk_ids[0], 0) if chunk_ids else 0
        labor_entries.append((c["labor_category"], first_ix, c, chunk_ids))
    labor_entries.sort(key=lambda x: (x[0], x[1]))
    labor_rows: list[dict] = []
    for n, (canon, _first_ix, c, chunk_ids) in enumerate(labor_entries):
        labor_rows.append({
            "demand_id": f"{resource_id}:{n}",
            "resource_id": resource_id,
            "contract_award_unique_key": meta.get("contract_award_unique_key"),
            "notice_id": meta.get("notice_id"),
            "solicitation_number": meta.get("solicitation_number"),
            "naics_code": meta.get("naics_code"),
            "labor_category": canon,
            "headcount": c["headcount"],
            "clearance_level": doc_clearance_max,
            "pop_start": doc_pop["pop_start"] if doc_pop else None,
            "pop_end": doc_pop["pop_end"] if doc_pop else None,
            "place_of_performance": None if marked else doc_place,
            "wage_floor": c["wage_floor"],
            "source_chunk_ids": chunk_ids,
            "extractor": LABOR_DEMAND_EXTRACTOR,
            "confidence": 1.0,
            "run_id": run_id, "created_at": now,
        })

    return {
        "resource_id": resource_id, "state": "done",
        "req_rows": req_rows, "labor_rows": labor_rows,
        "marking_full_body": marking_union, "marked": marked,
        "lexicon_hit_fullbody": lexicon_hit, "n_chunks": n_chunks,
        "counters": counters,
    }


# ════════════════════════════════════════════════════════════════ predicates / ledger merge
def _quote_sql(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def in_predicate(column: str, values: list[str]) -> str:
    return f"{column} IN ({','.join(_quote_sql(v) for v in values)})"


def requirements_delete_predicate(resource_ids: list[str]) -> str:
    """Plan Phase-1 verbatim: scoped to the regex lane so LLM-lane rows are never touched."""
    return f"{in_predicate('resource_id', resource_ids)} AND extractor LIKE 'regex:%'"


def labor_delete_predicate(resource_ids: list[str]) -> str:
    """Plan Phase-1 verbatim: unscoped per-resource delete on govcon_labor_demand_90day."""
    return in_predicate("resource_id", resource_ids)


def _merge_stats(res) -> dict:
    out = {}
    for k in ("num_inserted_rows", "num_updated_rows", "num_deleted_rows"):
        v = res.get(k) if isinstance(res, dict) else getattr(res, k, None)
        if v is not None:
            out[k] = int(v)
    return out


def merge_ledger(so: dict, uri: str, updates: list[dict], run_id: str, now: dt.datetime) -> dict:
    """Merge regex-lane ledger updates on resource_id, PRESERVING LLM-lane columns from any existing
    row (llm_state/batch_id/model/prompt_hash/n_requirements_llm/validation_pass_rate). New rows get
    llm_state='pending', batch_id NULL (regex lane never sets batch_id — plan §5)."""
    import lance
    import pyarrow as pa
    assert_schema(uri, extract_ledger_schema(), so)
    ds = lance.dataset(uri, storage_options=so)
    ids = sorted({u["resource_id"] for u in updates})
    prev = {r["resource_id"]: r
            for r in ds.to_table(filter=in_predicate("resource_id", ids)).to_pylist()}
    rows = []
    for u in updates:
        p = prev.get(u["resource_id"], {})
        rows.append({
            "resource_id": u["resource_id"],
            "regex_state": u["state"],
            "llm_state": p.get("llm_state") or "pending",
            "batch_id": p.get("batch_id"),
            "marking_full_body": (u["marking_full_body"] if u["marking_full_body"] is not None
                                  else p.get("marking_full_body")),
            "lexicon_hit_fullbody": (u["lexicon_hit_fullbody"] if u["lexicon_hit_fullbody"] is not None
                                     else p.get("lexicon_hit_fullbody")),
            "n_requirements_regex": len(u.get("req_rows") or []),
            "n_requirements_llm": p.get("n_requirements_llm"),
            "validation_pass_rate": p.get("validation_pass_rate"),
            "model": p.get("model"), "prompt_hash": p.get("prompt_hash"),
            "extractor_version": REGEX_LANE_VERSION,
            "run_id": run_id, "completed_at": now,
        })
    tbl = pa.Table.from_pylist(rows, schema=extract_ledger_schema())
    res = (ds.merge_insert("resource_id").when_matched_update_all()
           .when_not_matched_insert_all().execute(tbl))
    return _merge_stats(res)


# ════════════════════════════════════════════════════════════════ batch writer (D3, leases held)
class _BatchWriter:
    """Accumulates per-resource results; every FLUSH_RESOURCES (or at finalize) commits one batch:
    scoped delete-before-merge on both sinks, merge_insert, ledger merge — ONLY THEN the per-resource
    checkpoint lines (a checkpoint never references rows that are not durable). The three sink leases
    are acquired by the caller and held for the whole run."""

    def __init__(self, so: dict, args, run_id: str, ckpt_path: str):
        self.so, self.args, self.run_id = so, args, run_id
        self.ckpt = open(ckpt_path, "a", buffering=1)
        self.pending: list[dict] = []
        self.totals = {"resources_done": 0, "resources_failed": 0, "req_rows": 0, "labor_rows": 0,
                       "by_family": {}, "suppressed": {"negation": {}, "no_context": {}, "invalid": {}},
                       "marked_resources": 0, "write_stats": []}

    def add(self, result: dict) -> None:
        self.pending.append(result)
        if len(self.pending) >= FLUSH_RESOURCES:
            self.flush()

    def _bump_totals(self, r: dict) -> None:
        if r["state"] == "done":
            self.totals["resources_done"] += 1
            self.totals["req_rows"] += len(r["req_rows"])
            self.totals["labor_rows"] += len(r["labor_rows"])
            if r.get("marked"):
                self.totals["marked_resources"] += 1
            for row in r["req_rows"]:
                fam = row["extractor"].split(":", 1)[1].split("@", 1)[0]
                self.totals["by_family"][fam] = self.totals["by_family"].get(fam, 0) + 1
            for kind, fams in (r.get("counters") or {}).items():
                slot = self.totals["suppressed"].setdefault(kind, {})
                for fam, n in fams.items():
                    slot[fam] = slot.get(fam, 0) + n
        else:
            self.totals["resources_failed"] += 1

    def flush(self) -> None:
        if not self.pending:
            return
        import lance
        import pyarrow as pa
        now = dt.datetime.now(dt.timezone.utc)
        done = [r for r in self.pending if r["state"] == "done"]
        stats: dict = {}
        if done:
            ids = [r["resource_id"] for r in done]
            req_rows = [row for r in done for row in r["req_rows"]]
            labor_rows = [row for r in done for row in r["labor_rows"]]
            # requirements: scoped delete-before-merge (regex lane only), then merge on requirement_id
            assert_schema(self.args.requirements_uri, requirements_schema(), self.so)
            ds = lance.dataset(self.args.requirements_uri, storage_options=self.so)
            ds.delete(requirements_delete_predicate(ids))
            if req_rows:
                tbl = pa.Table.from_pylist(req_rows, schema=requirements_schema())
                res = (ds.merge_insert("requirement_id").when_matched_update_all()
                       .when_not_matched_insert_all().execute(tbl))
                stats["requirements"] = _merge_stats(res)
            # labor demand: unscoped per-resource delete (plan verbatim), merge on demand_id
            assert_schema(self.args.labor_uri, labor_demand_schema(), self.so)
            ds = lance.dataset(self.args.labor_uri, storage_options=self.so)
            ds.delete(labor_delete_predicate(ids))
            if labor_rows:
                tbl = pa.Table.from_pylist(labor_rows, schema=labor_demand_schema())
                res = (ds.merge_insert("demand_id").when_matched_update_all()
                       .when_not_matched_insert_all().execute(tbl))
                stats["labor_demand"] = _merge_stats(res)
        # ledger (done + failed), preserving LLM-lane columns
        stats["ledger"] = merge_ledger(self.so, self.args.ledger_uri, self.pending, self.run_id, now)
        # ONLY NOW the checkpoints
        for r in self.pending:
            self._bump_totals(r)
            self.ckpt.write(json.dumps({
                "resource_id": r["resource_id"], "state": r["state"],
                "n_req": len(r.get("req_rows") or []), "n_demand": len(r.get("labor_rows") or []),
                "suppressed": {k: sum(v.values()) for k, v in (r.get("counters") or {}).items()},
            }) + "\n")
        self.totals["write_stats"].append(stats)
        print(f"flush: {len(self.pending)} resources "
              f"(req={sum(len(r.get('req_rows') or []) for r in self.pending)}, "
              f"labor={sum(len(r.get('labor_rows') or []) for r in self.pending)}) {stats}", flush=True)
        self.pending.clear()

    def finalize(self) -> dict:
        self.flush()
        self.ckpt.flush()
        self.ckpt.close()
        return self.totals


# ════════════════════════════════════════════════════════════════ worklist / resume
def _load_ckpt_done(ckpt_path: str) -> set[str]:
    done: dict[str, str] = {}
    if os.path.exists(ckpt_path):
        with open(ckpt_path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                    done[r["resource_id"]] = r.get("state", "")
                except Exception:  # noqa: BLE001
                    continue
    return {rid for rid, st in done.items() if st in ("done", "quarantined")}


def _ledger_done(so: dict, uri: str) -> set[str]:
    """Resources whose regex lane is terminal (done/quarantined). failed re-attempts (plan §5)."""
    import lance
    if not _dataset_exists(uri, so):
        return set()
    t = lance.dataset(uri, storage_options=so).to_table(columns=["resource_id", "regex_state"])
    return {rid for rid, st in zip(t.column("resource_id").to_pylist(),
                                   t.column("regex_state").to_pylist())
            if st in ("done", "quarantined")}


def _pricing_cells_map(ds, so: dict, resource_ids: list[str] | None) -> dict[str, str]:
    """Doc-level cells grid, fetched ONCE per resource via chunk_ix=0 filtered scan (the grid is
    duplicated on every chunk row — never read the full column)."""
    flt = "chunk_ix = 0"
    if resource_ids is not None:
        flt += f" AND {in_predicate('resource_id', resource_ids)}"
    out: dict[str, str] = {}
    for b in ds.to_batches(columns=["resource_id", "cells"], batch_size=4096, filter=flt):
        for rid, c in zip(b.column("resource_id").to_pylist(), b.column("cells").to_pylist()):
            if c:
                out[rid] = c
    return out


# ════════════════════════════════════════════════════════════════ phase: extract
def phase_extract(args, so: dict, run_id: str) -> dict:
    import lance

    sinks = [(n, u) for n, u in (("scope", args.scope_uri), ("pricing", args.pricing_uri),
                                 ("unknown", args.unknown_uri)) if n in args.sinks]
    # Frozen-schema asserts BEFORE anything else (anti-pattern #1: assert on open, every writer).
    assert_schema(args.requirements_uri, requirements_schema(), so)
    assert_schema(args.labor_uri, labor_demand_schema(), so)
    assert_schema(args.ledger_uri, extract_ledger_schema(), so)

    done: set[str] = set()
    if args.resume:
        done = _ledger_done(so, args.ledger_uri) | _load_ckpt_done(args.ckpt)
        print(f"resume: {len(done):,} resources already terminal (ledger ∪ checkpoint)", flush=True)

    explicit_ids = None
    if args.resource_ids:
        explicit_ids = [r for r in args.resource_ids if r not in done] or None
        if explicit_ids is None:
            print("resume: every requested resource already terminal — nothing to do", flush=True)
            return {"resources_done": 0, "resources_failed": 0}

    # Single-committer leases (D3) for all three WRITTEN datasets, held for the whole run.
    leases: list[SinkCommitLease] = []
    try:
        for uri in (args.requirements_uri, args.labor_uri, args.ledger_uri):
            leases.append(SinkCommitLease(uri, holder=f"regex_extract:{run_id}",
                                          ttl_s=24 * 3600).acquire())
    except Exception:
        for lease in leases:
            lease.release()
        raise

    writer = _BatchWriter(so, args, run_id, args.ckpt)
    try:
        for sink_name, uri in sinks:
            t0 = time.time()
            ds = lance.dataset(uri, storage_options=so)   # version-pinned for both passes

            ids_filter = explicit_ids
            if ids_filter is None and args.max_resources:
                seen: list[str] = []
                seen_set: set[str] = set()
                for b in ds.to_batches(columns=["resource_id"], batch_size=65536):
                    for rid in b.column("resource_id").to_pylist():
                        if rid not in seen_set and rid not in done:
                            seen_set.add(rid)
                            seen.append(rid)
                    if len(seen) >= args.max_resources:
                        break
                ids_filter = seen[:args.max_resources]
                if not ids_filter:
                    print(f"extract {sink_name}: nothing to do (max-resources)", flush=True)
                    continue

            def _process(rid: str, rows: list[dict], cells: str | None) -> None:
                try:
                    writer.add(process_resource_payload(rid, rows, cells, run_id,
                                                        dt.datetime.now(dt.timezone.utc)))
                except Exception as exc:  # noqa: BLE001
                    print(f"extract {sink_name}: FAILED {rid}: {exc}", flush=True)
                    writer.add({"resource_id": rid, "state": "failed", "req_rows": [],
                                "labor_rows": [], "marking_full_body": None, "marked": None,
                                "lexicon_hit_fullbody": None, "n_chunks": None, "counters": None})

            if ids_filter is not None:
                # filtered slice mode (smoke / explicit ids): one filtered read per sink
                flt = in_predicate("resource_id", ids_filter)
                t = ds.to_table(columns=CHUNK_COLS, filter=flt)
                by_rid: dict[str, list[dict]] = {}
                for r in t.to_pylist():
                    by_rid.setdefault(r["resource_id"], []).append(r)
                if not by_rid:
                    continue
                cells_map = (_pricing_cells_map(ds, so, sorted(by_rid))
                             if sink_name == "pricing" else {})
                for rid in sorted(by_rid):
                    _process(rid, by_rid[rid], cells_map.get(rid))
                print(f"extract {sink_name}: slice {len(by_rid)} resources "
                      f"({time.time()-t0:.0f}s)", flush=True)
                continue

            # full-stream mode: pass A counts, pass B buffer + flush-on-complete
            expected: dict[str, int] = {}
            for b in ds.to_batches(columns=["resource_id"], batch_size=65536):
                for rid in b.column("resource_id").to_pylist():
                    expected[rid] = expected.get(rid, 0) + 1
            print(f"extract {sink_name}: {ds.count_rows():,} chunks / {len(expected):,} resources "
                  f"(counts in {time.time()-t0:.0f}s)", flush=True)
            cells_map = _pricing_cells_map(ds, so, None) if sink_name == "pricing" else {}

            buf: dict[str, list[dict]] = {}
            seen_n: dict[str, int] = {}
            rows_seen = n_proc = n_skip = 0
            for b in ds.to_batches(columns=CHUNK_COLS, batch_size=SCAN_BATCH_ROWS):
                batch_rows = b.to_pylist()
                rows_seen += len(batch_rows)
                for r in batch_rows:
                    rid = r["resource_id"]
                    seen_n[rid] = seen_n.get(rid, 0) + 1
                    if rid in done:
                        if seen_n[rid] == expected[rid]:
                            n_skip += 1
                        continue
                    buf.setdefault(rid, []).append(r)
                    if seen_n[rid] == expected[rid]:
                        _process(rid, buf.pop(rid), cells_map.get(rid))
                        n_proc += 1
                if rows_seen % (SCAN_BATCH_ROWS * 64) < SCAN_BATCH_ROWS:
                    print(f"extract {sink_name}: rows {rows_seen:,}/{ds.count_rows():,} "
                          f"processed {n_proc:,} skipped {n_skip:,} buffered {len(buf):,} "
                          f"({time.time()-t0:.0f}s)", flush=True)
            if buf:
                raise RuntimeError(f"extract {sink_name}: {len(buf)} resources incomplete at "
                                   f"stream end (e.g. {list(buf)[:3]}) — counts/stream diverged")
            print(f"extract {sink_name}: DONE {n_proc:,} processed (+{n_skip:,} resumed) "
                  f"in {time.time()-t0:.0f}s", flush=True)
    finally:
        totals = writer.finalize()
        for lease in leases:
            lease.release()
    return totals


# ════════════════════════════════════════════════════════════════ phase: index (post-merge-settle)
def phase_index(args, so: dict) -> dict:
    """Plan Phase 1: BTREE(resource_id, contract_award_unique_key) + BITMAP(requirement_type,
    clearance_level, mandatory, validated) on requirements; spec §3.6 indices on labor demand.
    requirement_id stays UNINDEXED until Phase-2 merges complete (#3177)."""
    import lance
    out = {}
    plans = [
        (args.requirements_uri, requirements_schema(),
         [("resource_id", "BTREE"), ("contract_award_unique_key", "BTREE"),
          ("requirement_type", "BITMAP"), ("clearance_level", "BITMAP"),
          ("mandatory", "BITMAP"), ("validated", "BITMAP")]),
        (args.labor_uri, labor_demand_schema(),
         [("resource_id", "BTREE"), ("contract_award_unique_key", "BTREE"),
          ("naics_code", "BITMAP"), ("clearance_level", "BITMAP")]),
    ]
    for uri, schema, specs in plans:
        assert_schema(uri, schema, so)
        ds = lance.dataset(uri, storage_options=so)
        built = []
        for col, ix_type in specs:
            ds.create_scalar_index(col, index_type=ix_type, replace=True)
            built.append(f"{ix_type}({col})")
        out[uri] = built
        print(f"index {uri}: {', '.join(built)}", flush=True)
    return out


# ════════════════════════════════════════════════════════════════ main
def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="GovCon Phase-1 regex requirement extraction (build plan PHASE 1; spec §17).")
    p.add_argument("--phase", default="extract", choices=["extract", "index"])
    p.add_argument("--sinks", default="scope,pricing,unknown",
                   help="comma list of chunk sinks to read (read-only)")
    p.add_argument("--resource-ids", default=None,
                   help="comma list — explicit slice mode (smoke)")
    p.add_argument("--max-resources", type=int, default=0,
                   help="cap distinct resources per sink (smoke)")
    p.add_argument("--resume", action="store_true",
                   help="skip resources already terminal in ledger/checkpoint")
    p.add_argument("--run-id", default=None)
    p.add_argument("--daemon", action="store_true", help="double-fork + setsid; log to --log")
    p.add_argument("--ckpt", default=CKPT_PATH)
    p.add_argument("--log", default=LOG_PATH)
    p.add_argument("--report-out", default=REPORT_PATH)
    p.add_argument("--scope-uri", default=SCOPE_URI)
    p.add_argument("--pricing-uri", default=PRICING_URI)
    p.add_argument("--unknown-uri", default=UNKNOWN_URI)
    p.add_argument("--requirements-uri", default=REQUIREMENTS_URI)
    p.add_argument("--labor-uri", default=LABOR_DEMAND_URI)
    p.add_argument("--ledger-uri", default=EXTRACT_LEDGER_URI)
    args = p.parse_args(argv)
    args.sinks = {s.strip() for s in args.sinks.split(",") if s.strip()}
    args.resource_ids = ([s.strip() for s in args.resource_ids.split(",") if s.strip()]
                         if args.resource_ids else None)
    run_id = args.run_id or f"regex_extract_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"

    if args.daemon:
        _daemonize(args.log)

    so = _r2_storage_options() if args.requirements_uri.startswith("s3://") else {}
    report: dict = {"run_id": run_id, "extractor_version": REGEX_LANE_VERSION,
                    "pattern_families": PATTERN_FAMILY_COUNTS,
                    "started_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.phase == "extract":
        report["extract"] = phase_extract(args, so, run_id)
    else:
        report["index"] = phase_index(args, so)
    report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    with open(args.report_out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    summary = {k: v for k, v in report.get("extract", {}).items() if k != "write_stats"}
    print("RESULT: " + json.dumps(summary or report.get("index", {}), default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
