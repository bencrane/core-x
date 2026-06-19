"""SAM.gov 90-day attachment — Phase-1 DETERMINISTIC requirement extraction (the regex lane).

Implements build-plan PHASE 1 (docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md) in the
spec §17 fixed-name module (docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md — Phase 5
structured extraction artifact; the LLM lane lands here later as Phase 2 of the build plan).

WHAT IT DOES (one pass per resource over the three READ-ONLY chunk sinks):
  * INPUT = all resources in govcon_scope_vectors / govcon_pricing /
    govcon_unknown (ALL unknown docs — the lexicon gate applies to the LLM lane only).
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
  * Same pass emits govcon_labor_demand (spec §3.6 exact): one row per
    (resource, labor_category_norm); `demand_id = <resource_id>:<n>` with n assigned
    DETERMINISTICALLY by rank over (labor_category_norm, first chunk_ix) — never a write-order
    ordinal. Also computes `lexicon_hit_fullbody` over the reassembled body for the ledger.

IDEMPOTENCY (plan §5 / anti-pattern #3 — scoped delete-before-merge, the regex lane's own lane):
  * Per flush batch, BEFORE merging: delete("resource_id IN (...) AND extractor LIKE 'regex:%'")
    on the requirements sink and delete("resource_id IN (...)") on govcon_labor_demand
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
  * Ledger govcon_requirements_extract_ledger merges on resource_id PRESERVING the LLM-lane
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

LLM LANE (build-plan PHASE 2 — engine-agnostic select/stage → extract → validate/land; operator
decision A: marked resources bracketed out, recorded as llm_state='excluded_marked'):
    ... --phase bracket                                   # record decision A in the ledger (live
                                                          #   chunk content_marking; idempotent)
    ... --phase select --pilot 4 --manifest-out /tmp/m.json   # stage task files (seeded sample)
    ... --phase select                                    # stage the FULL pending worklist
    ... --phase census                                    # token census, no staging (JSON report)
    <opaque extraction step: agents read tasks/, write results/<rid>.result.json>
    ... --phase ingest                                    # validate + land + ledger (run gate 98%)
    ... --phase reset-llm --resource-ids RID1             # reverse an LLM landing (lane hygiene)
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
    CHUNK_OVERLAP, LABOR_LEXICON_RX, MAX_CHUNKS_PER_FILE, PRICING_URI, SCOPE_HDR_RX, SCOPE_URI,
    UNKNOWN_URI, SinkCommitLease, _daemonize, _dataset_exists, _r2_storage_options,
)
from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    DOC_SCOPE_URI, EXTRACT_LEDGER_URI, LABOR_DEMAND_URI, REQUIREMENTS_URI,
    assert_schema, doc_scope_schema, extract_ledger_schema, labor_demand_schema,
    requirements_schema,
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


# ╔═══════════════════════════════════════════════════════════════════════════════════════════════
# ║ LLM LANE — build-plan PHASE 2 (engine-agnostic: deterministic select/stage → opaque extraction
# ║ step producing result JSON files → deterministic validate/land). OPERATOR DECISION A (binding):
# ║ resources with ANY non-empty chunk content_marking are BRACKETED OUT of all LLM processing —
# ║ recorded in the ledger as llm_state='excluded_marked' (reversible, derived live), and marked
# ║ text NEVER enters any staged task file (hard-asserted at staging). ZERO marginal spend: the
# ║ pilot engine is session agents reading staged task files; the harness never calls an API.
# ╚═══════════════════════════════════════════════════════════════════════════════════════════════
LLM_PROMPT_VERSION = "v1"
LLM_ENGINE_DEFAULT = "session-fable"
LLM_ARTIFACT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "reference", "govcon_llm_lane_v1")
LLM_STAGING_DIR = os.environ.get("GOVCON_LLM_STAGING_DIR", "/tmp/govcon_llm_stage")
LLM_BRACKET_REPORT = os.environ.get("GOVCON_LLM_BRACKET_REPORT", "/tmp/govcon_llm_bracket_report.json")
LLM_CENSUS_REPORT = os.environ.get("GOVCON_LLM_CENSUS_REPORT", "/tmp/govcon_llm_census.json")
LLM_INGEST_REPORT = os.environ.get("GOVCON_LLM_INGEST_REPORT", "/tmp/govcon_llm_ingest_report.json")

# Selection policy (deterministic; stated in the build report):
#   token estimate = ceil(chars / 4)  (the chars/4 convention; chunk text is ASCII-heavy English)
#   per-doc HARD budget = 8,000 tokens (≈32,000 chars ≈ 26 full chunks). Rationale: a 10-doc agent
#   batch carries ≤80K tokens of document text — inside a 200K context with instructions + output.
#   Docs whose ELIGIBLE selection exceeds the budget are truncated by priority tier and flagged
#   coverage_truncated=true (map-reduce over multiple windows is the future upgrade for that tail;
#   census reports its exact size).
CHARS_PER_TOKEN = 4
DOC_TOKEN_BUDGET = int(os.environ.get("GOVCON_LLM_DOC_TOKEN_BUDGET", "8000"))
N_OPENING_CHUNKS = int(os.environ.get("GOVCON_LLM_OPENING_CHUNKS", "6"))
LLM_SLICE_FETCH_MAX = 500          # ≤ this many ids per sink → filtered reads; else full stream
LLM_FETCH_SLICE = 200              # ids per filtered read
LLM_INGEST_FLUSH = 200             # resources per ingest commit batch
LLM_MIN_PASS_RATE = 0.98           # plan Phase-2 run gate (≥98% or nothing lands)

# Validator-strength confidence (hard rule 1: LLM rows always < 1.0):
LLM_CONF_RAW = 0.90                # evidence_quote matched cited text verbatim (raw substring)
LLM_CONF_NORM = 0.80               # matched only after whitespace normalization
LLM_DOC_CONFIDENCE = 0.80          # doc-grain outputs (summary/tags) — synthesis, not quote-checked

EXCLUDED_MARKED = "excluded_marked"
EXCLUDED_OUT_OF_SCOPE = "excluded_out_of_scope"
# Bracket rewrites ONLY the three pre-flight states; every other llm_state is a lane event it must
# never erase (done/quarantined = results; submitted/results_fetched = batch in flight; failed/
# truncated/marked_local_only = explicit lane outcomes with their own resume semantics).
LLM_BRACKET_MANAGED = {"pending", EXCLUDED_MARKED, EXCLUDED_OUT_OF_SCOPE}

_LEDGER_PRESERVE_SENTINEL = object()


def load_llm_artifacts() -> tuple[str, dict, dict, str]:
    """(prompt_template_text, output_schema, vocabulary, prompt_hash). prompt_hash = sha256 over the
    raw bytes of template + vocabulary + schema IN THAT ORDER (mandate E) — any artifact edit
    changes the hash, and the hash rides every stored row's ledger entry."""
    with open(os.path.join(LLM_ARTIFACT_DIR, "prompt_template.md"), "rb") as f:
        t_bytes = f.read()
    with open(os.path.join(LLM_ARTIFACT_DIR, "vocabulary.json"), "rb") as f:
        v_bytes = f.read()
    with open(os.path.join(LLM_ARTIFACT_DIR, "output_schema.json"), "rb") as f:
        s_bytes = f.read()
    phash = hashlib.sha256(t_bytes + v_bytes + s_bytes).hexdigest()
    return (t_bytes.decode("utf-8"), json.loads(s_bytes), json.loads(v_bytes), phash)


def llm_extractor_tag(engine: str) -> str:
    return f"llm:{engine}@{LLM_PROMPT_VERSION}"


def llm_delete_predicate(resource_ids: list[str]) -> str:
    """Hard rule 2: LLM-lane idempotency = scoped delete-before-merge — the resource's prior
    llm:% rows die, regex:% rows are NEVER touched (mirror of requirements_delete_predicate)."""
    return f"{in_predicate('resource_id', resource_ids)} AND extractor LIKE 'llm:%'"


def estimate_tokens(text: str) -> int:
    """chars/4, rounded up — the stated census/budget convention. Deterministic."""
    return (len(text) + CHARS_PER_TOKEN - 1) // CHARS_PER_TOKEN


def select_chunks(chunk_rows: list[dict], budget_tokens: int = DOC_TOKEN_BUDGET) -> dict:
    """TARGETED READING selection, deterministic (mandate B). Priority tiers per chunk:
      0 = opening chunks (chunk_ix < N_OPENING_CHUNKS — title page / section 1 / doc framing)
      1 = scope-header-bearing chunks (SCOPE_HDR_RX on the chunk text)
      2 = labor-lexicon-hit chunks (LABOR_LEXICON_RX on the chunk text)
    Eligible = chunks with any tier. Fill order = (tier, chunk_ix); a chunk that does not fit the
    remaining budget is SKIPPED (counted) and filling continues — hard budget, never exceeded.
    Returns selected rows in chunk_ix order plus the census stats."""
    rows = sorted(chunk_rows, key=lambda r: r["chunk_ix"])
    eligible: list[tuple[int, int, dict]] = []
    total_tokens = 0
    for r in rows:
        text = r.get("text") or ""
        total_tokens += estimate_tokens(text)
        if r["chunk_ix"] < N_OPENING_CHUNKS:
            tier = 0
        elif SCOPE_HDR_RX.search(text):
            tier = 1
        elif LABOR_LEXICON_RX.search(text):
            tier = 2
        else:
            continue
        eligible.append((tier, r["chunk_ix"], r))
    eligible.sort(key=lambda t: (t[0], t[1]))
    eligible_tokens = sum(estimate_tokens(r.get("text") or "") for _, _, r in eligible)
    selected: list[dict] = []
    sel_tokens = 0
    for _tier, _ix, r in eligible:
        tk = estimate_tokens(r.get("text") or "")
        if sel_tokens + tk > budget_tokens:
            continue
        selected.append(r)
        sel_tokens += tk
    selected.sort(key=lambda r: r["chunk_ix"])
    return {
        "selected": selected,
        "n_chunks_total": len(rows), "n_chunks_eligible": len(eligible),
        "n_chunks_selected": len(selected),
        "est_tokens_total": total_tokens, "est_tokens_eligible": eligible_tokens,
        "est_tokens_selected": sel_tokens,
        "coverage_truncated": len(selected) < len(eligible),
        "over_budget": eligible_tokens > budget_tokens,
    }


def bracket_target(current_llm_state: str | None, in_input: bool, marked: bool) -> str | None:
    """Pure bracket-state law (operator decision A; unit-tested). Returns the state the ledger row
    must carry, or None when the row must not be touched (already correct, or carries a lane event
    bracket must never erase)."""
    if current_llm_state not in LLM_BRACKET_MANAGED and current_llm_state is not None:
        return None
    if not in_input:
        want = EXCLUDED_OUT_OF_SCOPE
    elif marked:
        want = EXCLUDED_MARKED
    else:
        want = "pending"
    return None if want == current_llm_state else want


# ── sink maps / worklist resolution ───────────────────────────────────────────────────────────────
def compute_sink_maps(so: dict, args) -> tuple[dict[str, dict[str, int]], set[str]]:
    """Per sink: {resource_id: n_chunks} (one resource_id column scan), plus the LIVE marked set
    (resources with ANY chunk whose content_marking is non-empty — array_length pushdown, the
    single egress enforcement signal). Resources are document-disjoint across sinks (plan §1)."""
    import lance
    sink_counts: dict[str, dict[str, int]] = {}
    marked: set[str] = set()
    for name, uri in (("scope", args.scope_uri), ("pricing", args.pricing_uri),
                      ("unknown", args.unknown_uri)):
        ds = lance.dataset(uri, storage_options=so)
        counts: dict[str, int] = {}
        for b in ds.to_batches(columns=["resource_id"], batch_size=131072):
            for rid in b.column("resource_id").to_pylist():
                counts[rid] = counts.get(rid, 0) + 1
        for b in ds.to_batches(columns=["resource_id"], batch_size=131072,
                               filter="array_length(content_marking) > 0"):
            marked.update(b.column("resource_id").to_pylist())
        sink_counts[name] = counts
    return sink_counts, marked


def compute_input_set(sink_counts: dict[str, dict[str, int]], ledger_rows: list[dict]) -> set[str]:
    """Operator decision A input set: scope-sink ALL ∪ pricing ALL ∪ (unknown ∩ (lexicon_hit_fullbody
    ∪ regex-hit residue)). The residue term is n_requirements_regex > 0 over any sink — a no-op for
    scope/pricing (already in unconditionally), the measured lexicon-miss regex-hit unknowns."""
    lex_or_hit = {r["resource_id"] for r in ledger_rows
                  if r.get("lexicon_hit_fullbody") or (r.get("n_requirements_regex") or 0) > 0}
    return (set(sink_counts["scope"]) | set(sink_counts["pricing"])
            | (set(sink_counts["unknown"]) & lex_or_hit))


def _ledger_full(so: dict, uri: str) -> list[dict]:
    import lance
    assert_schema(uri, extract_ledger_schema(), so)
    return lance.dataset(uri, storage_options=so).to_table().to_pylist()


def merge_ledger_llm(so: dict, uri: str, updates: list[dict], run_id: str,
                     now: dt.datetime) -> dict:
    """LLM-lane ledger merge (mirror of merge_ledger, opposite lane): regex-lane columns
    (regex_state, n_requirements_regex, marking_full_body, lexicon_hit_fullbody, extractor_version)
    are ALWAYS preserved from the existing row; LLM-lane columns are taken from the update when the
    key is present (explicit None = clear) and preserved otherwise. extractor_version stays the
    regex lane's — LLM versioning rides model + prompt_hash (plan-gap resolution). A missing ledger
    row is a hard error: P1 ledgered every resource."""
    import lance
    import pyarrow as pa
    assert_schema(uri, extract_ledger_schema(), so)
    ds = lance.dataset(uri, storage_options=so)
    ids = sorted({u["resource_id"] for u in updates})
    prev: dict[str, dict] = {}
    for i in range(0, len(ids), LLM_FETCH_SLICE):
        t = ds.to_table(filter=in_predicate("resource_id", ids[i:i + LLM_FETCH_SLICE]))
        prev.update({r["resource_id"]: r for r in t.to_pylist()})
    missing = [u["resource_id"] for u in updates if u["resource_id"] not in prev]
    if missing:
        raise RuntimeError(f"merge_ledger_llm: {len(missing)} resources have no ledger row "
                           f"(e.g. {missing[:3]}) — the P1 ledger is the worklist substrate")
    rows = []
    for u in updates:
        p = prev[u["resource_id"]]

        def pick(key, _u=u, _p=p):
            return _u[key] if key in _u else _p.get(key)
        rows.append({
            "resource_id": u["resource_id"],
            "regex_state": p["regex_state"],
            "llm_state": u["llm_state"],
            "batch_id": pick("batch_id"),
            "marking_full_body": p.get("marking_full_body"),
            "lexicon_hit_fullbody": p.get("lexicon_hit_fullbody"),
            "n_requirements_regex": p.get("n_requirements_regex"),
            "n_requirements_llm": pick("n_requirements_llm"),
            "validation_pass_rate": pick("validation_pass_rate"),
            "model": pick("model"), "prompt_hash": pick("prompt_hash"),
            "extractor_version": p.get("extractor_version"),
            "run_id": run_id, "completed_at": now,
        })
    tbl = pa.Table.from_pylist(rows, schema=extract_ledger_schema())
    res = (ds.merge_insert("resource_id").when_matched_update_all()
           .when_not_matched_insert_all().execute(tbl))
    return _merge_stats(res)


# ════════════════════════════════════════════════════════════════ phase: bracket (decision A)
def _marking_gate_ok(report: "dict | None", *, require_after: "str | None" = None) -> None:
    """CUI EGRESS GATE (pure). The full-body marking pass (Phase F) MUST have reconciled before any
    LLM phase stages a task file — an in-session agent reading a not-yet-promoted marked chunk is
    egress. Raises unless the marking report is present, `reconcile_overall == PASS`, and (when a
    freshness bound is supplied) the report `completed_at` is at/after it. None report => raise.
    The live-`content_marking` asserts downstream (bracket/_pending_worklist/select/build_task_payload)
    are the belt; this is the suspenders that make 'F ran' a precondition, not a hope."""
    if report is None:
        raise RuntimeError("CUI gate: marking report absent — run Phase F (sam_marking_fullbody_90day "
                           "scan->writeback->reconcile) to reconcile_overall=PASS before any LLM phase.")
    overall = report.get("reconcile_overall")
    if overall != "PASS":
        raise RuntimeError(f"CUI gate: marking reconcile_overall={overall!r} (need 'PASS') — refusing "
                           f"to stage LLM work until the full-body marking pass reconciles.")
    if require_after is not None:
        ca = report.get("completed_at")
        if ca is None or str(ca) < str(require_after):
            raise RuntimeError(f"CUI gate: marking report stale — completed_at={ca!r} is older than "
                               f"--require-marking-after={require_after!r}; re-run Phase F after the "
                               f"latest chunk write.")


def _assert_marking_complete(args) -> None:
    """Read the marking report from --marking-report and apply the pure CUI gate."""
    report = None
    if args.marking_report and os.path.exists(args.marking_report):
        with open(args.marking_report) as fh:
            report = json.load(fh)
    _marking_gate_ok(report, require_after=getattr(args, "require_marking_after", None))


def _scope_pending(pending: "set[str]", allow: "set[str] | None") -> "set[str]":
    """STRUCTURAL H2 SCOPING (pure). When an allow-list is supplied (--resource-ids/-file), KEEP only
    the pending resources that belong to it — making H2 scope structural, not incidental on the rest of
    the corpus being terminal. The bracket pass is whole-corpus, so unrelated prod resources can be
    `pending`; scoping leaves those for prod rather than account-burning them here. Parent-aware:
    expanded-zip inner ids `<rid>::<inner>` qualify when their parent rid is in the allow-list (those
    synthetic ids are never in $IDS themselves). None => no scoping (all pending)."""
    if not allow:
        return set(pending)
    return {rid for rid in pending if rid.split("::", 1)[0] in allow}


def phase_llm_bracket(args, so: dict, run_id: str) -> dict:
    """Record operator decision A in the ledger. Derives the marked set LIVE from chunk
    content_marking; partitions every non-terminal ledger row into pending / excluded_marked /
    excluded_out_of_scope (the three states tile the pre-flight ledger exactly — coverage
    accounting stays honest and reversible). Idempotent: re-running merges only drifted rows."""
    _assert_marking_complete(args)                     # CUI egress gate: Phase F must have reconciled
    ledger_rows = _ledger_full(so, args.ledger_uri)
    sink_counts, marked = compute_sink_maps(so, args)
    input_set = compute_input_set(sink_counts, ledger_rows)
    updates, state_counts = [], {}
    for r in ledger_rows:
        rid = r["resource_id"]
        tgt = bracket_target(r.get("llm_state"), rid in input_set, rid in marked)
        final = tgt or r.get("llm_state")
        state_counts[final] = state_counts.get(final, 0) + 1
        if tgt is not None:
            updates.append({"resource_id": rid, "llm_state": tgt})
    stats = {}
    if updates:
        lease = SinkCommitLease(args.ledger_uri, holder=f"llm_bracket:{run_id}",
                                ttl_s=2 * 3600).acquire()
        try:
            stats = merge_ledger_llm(so, args.ledger_uri, updates, run_id,
                                     dt.datetime.now(dt.timezone.utc))
        finally:
            lease.release()
    out = {
        "ledger_rows": len(ledger_rows),
        "sink_resources": {k: len(v) for k, v in sink_counts.items()},
        "marked_resources_live": len(marked),
        "input_set": len(input_set),
        "marked_in_input": len(input_set & marked),
        "rows_changed": len(updates),
        "llm_state_after": state_counts,
        "merge": stats,
    }
    # the partition must tile the ledger — hard assert, not a hope
    n_pre = sum(state_counts.get(s, 0) for s in LLM_BRACKET_MANAGED)
    n_other = len(ledger_rows) - n_pre
    out["non_bracket_states"] = n_other
    print(f"bracket: {json.dumps(out, default=str)}", flush=True)
    return out


# ════════════════════════════════════════════════════════════════ phase: select / census (shared)
def _fetch_resources_sliced(ds, ids: list[str], cols: list[str]):
    """Small-worklist fetch: chunked filtered reads, yields (rid, rows) per complete resource."""
    for i in range(0, len(ids), LLM_FETCH_SLICE):
        t = ds.to_table(columns=cols, filter=in_predicate("resource_id", ids[i:i + LLM_FETCH_SLICE]))
        by_rid: dict[str, list[dict]] = {}
        for r in t.to_pylist():
            by_rid.setdefault(r["resource_id"], []).append(r)
        for rid in sorted(by_rid):
            yield rid, by_rid[rid]


def _stream_resources(ds, want: dict[str, int], cols: list[str]):
    """Full-worklist fetch: single natural-order stream, buffer per resource, yield on completion
    (the phase_extract pattern). `want` = {resource_id: expected_chunk_count} from compute_sink_maps."""
    buf: dict[str, list[dict]] = {}
    seen: dict[str, int] = {}
    for b in ds.to_batches(columns=cols, batch_size=SCAN_BATCH_ROWS):
        for r in b.to_pylist():
            rid = r["resource_id"]
            exp = want.get(rid)
            if exp is None:
                continue
            seen[rid] = seen.get(rid, 0) + 1
            buf.setdefault(rid, []).append(r)
            if seen[rid] == exp:
                yield rid, buf.pop(rid)
    if buf:
        raise RuntimeError(f"_stream_resources: {len(buf)} resources incomplete at stream end "
                           f"(e.g. {list(buf)[:3]}) — counts/stream diverged")


def _iter_worklist(so: dict, args, worklist_by_sink: dict[str, list[str]],
                   sink_counts: dict[str, dict[str, int]]):
    """Yields (sink, rid, chunk_rows) over the worklist, slice-fetch for small per-sink lists,
    stream for large ones."""
    import lance
    for sink, uri in (("scope", args.scope_uri), ("pricing", args.pricing_uri),
                      ("unknown", args.unknown_uri)):
        ids = worklist_by_sink.get(sink) or []
        if not ids:
            continue
        ds = lance.dataset(uri, storage_options=so)
        if len(ids) <= LLM_SLICE_FETCH_MAX:
            it = _fetch_resources_sliced(ds, sorted(ids), CHUNK_COLS)
        else:
            it = _stream_resources(ds, {r: sink_counts[sink][r] for r in ids}, CHUNK_COLS)
        for rid, rows in it:
            yield sink, rid, rows


def _size_bucket(n_chunks: int) -> str:
    return "small<=25" if n_chunks <= 25 else ("medium26-200" if n_chunks <= 200 else "large>200")


def stratified_pilot(candidates: list[tuple[str, str, int]], n: int, seed: int) -> list[str]:
    """SEEDED stratified sample across (sink × size-bucket) strata (mandate B). candidates =
    [(resource_id, sink, n_chunks)]. Proportional allocation by largest remainder with ≥1 per
    non-empty stratum (while n allows); within-stratum draw via a per-stratum seeded RNG over the
    sorted id list. Fully deterministic for (candidates, n, seed)."""
    import random
    strata: dict[tuple[str, str], list[str]] = {}
    for rid, sink, nch in candidates:
        strata.setdefault((sink, _size_bucket(nch)), []).append(rid)
    names = sorted(strata)
    total = sum(len(strata[k]) for k in names)
    n = min(n, total)
    quotas = {k: (n * len(strata[k])) / total for k in names}
    alloc = {k: int(quotas[k]) for k in names}
    if n >= len(names):
        for k in names:
            alloc[k] = max(alloc[k], 1)
    while sum(alloc.values()) > n:                    # min-1 overshoot: trim largest allocations
        k = max(names, key=lambda k: (alloc[k], k))
        alloc[k] -= 1
    rema = sorted(names, key=lambda k: (-(quotas[k] - int(quotas[k])), k))
    i = 0
    while sum(alloc.values()) < n:
        k = rema[i % len(rema)]
        if alloc[k] < len(strata[k]):
            alloc[k] += 1
        i += 1
    out: list[str] = []
    for k in names:
        pool = sorted(strata[k])
        take = min(alloc[k], len(pool))
        rng = random.Random(f"{seed}|{k[0]}|{k[1]}")
        out.extend(rng.sample(pool, take))
    return sorted(out)


def build_task_payload(resource_id: str, sink: str, chunk_rows: list[dict], sel: dict,
                       template: str, schema: dict, vocab: dict, phash: str,
                       engine: str, result_path: str) -> dict:
    """Self-contained task file (mandate B): doc metadata, selected chunk texts + ids, controlled
    vocabulary v1, full output schema, extraction instructions, exact result path. HARD-ASSERTS
    decision A on every chunk row of the resource — a marked chunk anywhere in the doc kills
    staging outright (defense in depth behind the ledger bracket)."""
    for r in chunk_rows:
        if r.get("content_marking"):
            raise RuntimeError(
                f"DECISION-A VIOLATION: resource {resource_id} carries marked chunk "
                f"{r.get('chunk_id')} ({r.get('content_marking')}) — must be excluded_marked, "
                f"never staged. Re-run --phase bracket.")
    meta = sorted(chunk_rows, key=lambda r: r["chunk_ix"])[0]
    return {
        "task_version": LLM_PROMPT_VERSION,
        "resource_id": resource_id,
        "sink": sink,
        "engine": engine,
        "prompt_version": LLM_PROMPT_VERSION,
        "prompt_hash": phash,
        "doc_meta": {
            "notice_id": meta.get("notice_id"),
            "solicitation_number": meta.get("solicitation_number"),
            "naics_code": meta.get("naics_code"),
            "contract_award_unique_key": meta.get("contract_award_unique_key"),
            "n_chunks_total": sel["n_chunks_total"],
            "n_chunks_selected": sel["n_chunks_selected"],
            "est_tokens_selected": sel["est_tokens_selected"],
            "coverage_truncated": sel["coverage_truncated"],
        },
        "instructions": template,
        "vocabulary": vocab,
        "output_schema": schema,
        "result_path": result_path,
        "chunks": [{"chunk_id": r["chunk_id"], "chunk_ix": r["chunk_ix"], "text": r["text"]}
                   for r in sel["selected"]],
    }


def _pending_worklist(so: dict, args, sink_counts: dict[str, dict[str, int]],
                      marked: set[str]) -> dict[str, list[str]]:
    """Worklist = ledger llm_state='pending' AFTER bracket (mandate B), split by sink. HARD-ASSERTS
    zero marked resources in the worklist — a marked pending row means the bracket is stale."""
    ledger_rows = _ledger_full(so, args.ledger_uri)
    pending = {r["resource_id"] for r in ledger_rows if r.get("llm_state") == "pending"}
    # STRUCTURAL SCOPING: when an allow-list is supplied, grind ONLY in-scope pending; the whole-corpus
    # bracket can leave unrelated prod resources `pending` — those stay for prod, never account-burned here.
    allow = set(args.resource_ids) if getattr(args, "resource_ids", None) else None
    if allow is not None:
        kept = _scope_pending(pending, allow)
        excluded = len(pending) - len(kept)
        if excluded:
            print(f"H2 scope: {excluded} pending ids outside the allow-list left for prod; "
                  f"grinding {len(kept)} in-scope (of {len(pending)} total pending).", flush=True)
        pending = kept
    stale = pending & marked
    if stale:
        raise RuntimeError(f"{len(stale)} marked resources still llm_state='pending' "
                           f"(e.g. {sorted(stale)[:3]}) — run --phase bracket first (decision A).")
    out: dict[str, list[str]] = {}
    for sink, counts in sink_counts.items():
        ids = sorted(pending & set(counts))
        if ids:
            out[sink] = ids
    n_sunk = sum(len(v) for v in out.values())
    if n_sunk != len(pending):
        raise RuntimeError(f"worklist/sink mismatch: {len(pending)} pending vs {n_sunk} resolved "
                           f"to sinks — ledger and chunk sinks diverged")
    return out


def phase_llm_select(args, so: dict, run_id: str) -> dict:
    _assert_marking_complete(args)                     # CUI egress gate: Phase F must have reconciled
    template, schema, vocab, phash = load_llm_artifacts()
    sink_counts, marked = compute_sink_maps(so, args)
    worklist = _pending_worklist(so, args, sink_counts, marked)
    if args.pilot:
        cands = [(rid, sink, sink_counts[sink][rid])
                 for sink, ids in worklist.items() for rid in ids]
        picked = set(stratified_pilot(cands, args.pilot, args.pilot_seed))
        worklist = {s: [r for r in ids if r in picked] for s, ids in worklist.items()}
        worklist = {s: ids for s, ids in worklist.items() if ids}
    tasks_dir = os.path.join(args.staging_dir, "tasks")
    results_dir = os.path.join(args.staging_dir, "results")
    os.makedirs(tasks_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)
    staged: list[dict] = []
    tot_tokens = n_trunc = 0
    for sink, rid, rows in _iter_worklist(so, args, worklist, sink_counts):
        if rid in marked:                              # belt over the ledger suspenders
            raise RuntimeError(f"DECISION-A VIOLATION: marked resource {rid} reached select")
        sel = select_chunks(rows, args.token_budget)
        result_path = os.path.join(results_dir, f"{rid}.result.json")
        task = build_task_payload(rid, sink, rows, sel, template, schema, vocab, phash,
                                  args.engine, result_path)
        task_path = os.path.join(tasks_dir, f"{rid}.task.json")
        with open(task_path, "w") as f:
            json.dump(task, f, ensure_ascii=False)
        staged.append({"resource_id": rid, "sink": sink, "task_file": task_path,
                       "result_file": result_path,
                       "est_tokens_selected": sel["est_tokens_selected"],
                       "n_chunks_selected": sel["n_chunks_selected"],
                       "coverage_truncated": sel["coverage_truncated"]})
        tot_tokens += sel["est_tokens_selected"]
        n_trunc += int(sel["coverage_truncated"])
        if len(staged) % 500 == 0:
            print(f"select: staged {len(staged)} tasks ({tot_tokens:,} tokens)", flush=True)
    staged.sort(key=lambda s: s["resource_id"])
    manifest = None
    if args.manifest_out:
        batches = [{"batch_ix": i // args.batch_size,
                    "resource_ids": [s["resource_id"] for s in staged[i:i + args.batch_size]],
                    "task_files": [s["task_file"] for s in staged[i:i + args.batch_size]],
                    "result_files": [s["result_file"] for s in staged[i:i + args.batch_size]]}
                   for i in range(0, len(staged), args.batch_size)]
        manifest = {"run_id": run_id, "engine": args.engine, "prompt_version": LLM_PROMPT_VERSION,
                    "prompt_hash": phash, "token_budget": args.token_budget,
                    "pilot": args.pilot or None, "pilot_seed": args.pilot_seed,
                    "n_tasks": len(staged), "n_batches": len(batches),
                    "batch_size": args.batch_size, "batches": batches}
        with open(args.manifest_out, "w") as f:
            json.dump(manifest, f, indent=2)
    out = {"staged": len(staged), "est_tokens_total": tot_tokens,
           "coverage_truncated_docs": n_trunc, "prompt_hash": phash,
           "token_budget": args.token_budget, "staging_dir": args.staging_dir,
           "manifest_out": args.manifest_out, "by_sink": {s: len(ids) for s, ids in worklist.items()}}
    print(f"select: {json.dumps(out)}", flush=True)
    return out


def phase_llm_census(args, so: dict, run_id: str) -> dict:
    """Deterministic token census over the FULL pending worklist under the exact select policy —
    no staging, no writes (mandate C)."""
    sink_counts, marked = compute_sink_maps(so, args)
    worklist = _pending_worklist(so, args, sink_counts, marked)
    per_doc: list[int] = []
    by_sink: dict[str, dict] = {}
    tot = {"docs": 0, "tokens_selected": 0, "tokens_eligible": 0, "tokens_total": 0,
           "docs_over_budget": 0, "docs_truncated": 0}
    t0 = time.time()
    for sink, rid, rows in _iter_worklist(so, args, worklist, sink_counts):
        if rid in marked:
            raise RuntimeError(f"DECISION-A VIOLATION: marked resource {rid} reached census")
        sel = select_chunks(rows, args.token_budget)
        per_doc.append(sel["est_tokens_selected"])
        s = by_sink.setdefault(sink, {"docs": 0, "tokens_selected": 0})
        s["docs"] += 1
        s["tokens_selected"] += sel["est_tokens_selected"]
        tot["docs"] += 1
        tot["tokens_selected"] += sel["est_tokens_selected"]
        tot["tokens_eligible"] += sel["est_tokens_eligible"]
        tot["tokens_total"] += sel["est_tokens_total"]
        tot["docs_over_budget"] += int(sel["over_budget"])
        tot["docs_truncated"] += int(sel["coverage_truncated"])
        if tot["docs"] % 1000 == 0:
            print(f"census: {tot['docs']:,} docs ({tot['tokens_selected']:,} tokens, "
                  f"{time.time()-t0:.0f}s)", flush=True)
    per_doc.sort()

    def pct(p: float) -> int:
        return per_doc[min(len(per_doc) - 1, int(p * len(per_doc)))] if per_doc else 0
    n = tot["docs"]
    agents_10, agents_20 = -(-n // 10), -(-n // 20)
    report = {
        "run_id": run_id, "token_budget": args.token_budget,
        "token_estimate_method": f"ceil(chars/{CHARS_PER_TOKEN})",
        "selection_policy": {"opening_chunks": N_OPENING_CHUNKS,
                             "tiers": "opening -> scope-header (SCOPE_HDR_RX) -> labor-lexicon "
                                      "(LABOR_LEXICON_RX); fill by (tier, chunk_ix); hard budget"},
        "totals": tot, "by_sink": by_sink,
        "per_doc_selected_tokens": {"p50": pct(0.50), "p90": pct(0.90), "p99": pct(0.99),
                                    "max": per_doc[-1] if per_doc else 0},
        "map_reduce_tail": {"docs_over_budget": tot["docs_over_budget"],
                            "note": "eligible selection exceeds the per-doc budget; staged "
                                    "truncated with coverage_truncated=true — a future engine "
                                    "map-reduces these over multiple windows on the content-hash key"},
        "projected_agents": {"at_10_docs_per_agent": agents_10, "at_20_docs_per_agent": agents_20,
                             "workflows_at_1000_agent_cap":
                                 {"10/agent": -(-agents_10 // 1000), "20/agent": -(-agents_20 // 1000)}},
        "feasibility": (
            f"Full-corpus session-agent run: {n:,} docs / {tot['tokens_selected']:,} selected tokens. "
            f"At 10 docs/agent (≈{10 * args.token_budget // 1000}K doc-tokens per agent context) "
            f"this is {agents_10:,} agent invocations = {-(-agents_10 // 1000)} workflow(s) under the "
            f"1,000-agent-per-workflow cap; at 20 docs/agent, {agents_20:,} agents = "
            f"{-(-agents_20 // 1000)} workflow(s) but ≈{20 * args.token_budget // 1000}K doc-tokens "
            f"per context, which crowds instruction+output headroom. Wall-clock is engine-bound, not "
            f"harness-bound: total = (agent latency per batch) × (batches / concurrency), and "
            f"per-batch latency is unmeasured until the pilot runs — measure there before "
            f"committing to a full-corpus schedule. The harness side (select/stage + validate/land) "
            f"is deterministic streaming compute, resumable per resource via the ledger."),
        "wall_seconds_census": round(time.time() - t0, 1),
    }
    with open(args.report_out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"census: {json.dumps({k: report[k] for k in ('totals', 'per_doc_selected_tokens', 'projected_agents')})}",
          flush=True)
    return report


# ════════════════════════════════════════════════════════════════ phase: ingest (validate + land)
_WS_RX = re.compile(r"\s+")


def _ws_collapse(s: str) -> str:
    return _WS_RX.sub(" ", s).strip()


def _cited_body(task_chunks: dict[str, dict], chunk_ids: list[str]) -> str:
    """Reassemble the cited chunks in chunk_ix order with the SHARED overlap-aware reassembly
    (boundary-straddling quotes must not be false-rejected — anti-pattern #8)."""
    rows = sorted((task_chunks[c] for c in chunk_ids), key=lambda r: r["chunk_ix"])
    body, _ = reassemble_with_offsets([(r["chunk_ix"], r["chunk_id"], r["text"]) for r in rows])
    return body


def validate_result(result: dict, task: dict, vocab: dict, engine_tag: str,
                    run_id: str, now: dt.datetime) -> dict:
    """PURE deterministic validator (hard rule 3; unit-tested). Returns
    {status: pass|partial|quarantined, doc_row, req_rows, rejects:[{reason,...}], n_rows_passed,
    n_rows_rejected, pass_rate}. ONLY validator-passing rows are ever stored; rejected output is
    counted, never written. Confidence encodes validator strength (always < 1.0)."""
    rid = task["resource_id"]
    meta = task["doc_meta"]
    chunks = {c["chunk_id"]: c for c in task["chunks"]}
    rejects: list[dict] = []

    def _quarantine(reason: str) -> dict:
        return {"status": "quarantined", "doc_row": None, "req_rows": [],
                "rejects": [{"reason": reason}], "n_rows_passed": 0, "n_rows_rejected": 0,
                "pass_rate": 0.0}

    if not isinstance(result, dict):
        return _quarantine("result_not_object")
    if result.get("resource_id") != rid:
        return _quarantine(f"resource_id_mismatch:{result.get('resource_id')}")
    raw_reqs = result.get("requirements")
    if not isinstance(raw_reqs, list):
        return _quarantine("requirements_not_list")

    tag_set = set(vocab["capability_tags"])
    labor_set = set(vocab["labor_categories"])
    clr_set = set(vocab["clearance_levels"])
    type_set = set(vocab["requirement_types"])

    req_rows: list[dict] = []
    seen_ids: set[str] = set()
    for i, rr in enumerate(raw_reqs):
        def reject(reason: str, _i=i, _rr=rr) -> None:
            rejects.append({"reason": reason, "index": _i,
                            "requirement_type": (_rr or {}).get("requirement_type")
                            if isinstance(_rr, dict) else None})
        if not isinstance(rr, dict):
            reject("row_not_object")
            continue
        rtype = rr.get("requirement_type")
        if rtype not in type_set:
            reject(f"bad_requirement_type:{rtype}")
            continue
        rval = rr.get("requirement_value")
        if not isinstance(rval, str) or not rval.strip():
            reject("missing_requirement_value")
            continue
        value_norm = _ws_collapse(rval).lower()
        if rtype == "labor_category" and value_norm not in labor_set:
            reject(f"labor_category_out_of_vocab:{value_norm}")
            continue
        clr = rr.get("clearance_level")
        if clr is not None and clr not in clr_set:
            reject(f"bad_clearance_level:{clr}")
            continue
        cited = rr.get("source_chunk_ids")
        if not isinstance(cited, list) or not cited or not all(isinstance(c, str) for c in cited):
            reject("missing_citation")
            continue
        unknown = [c for c in cited if c not in chunks]
        if unknown:
            reject(f"citation_unknown_chunk:{unknown[:2]}")
            continue
        quote = rr.get("evidence_quote")
        if not isinstance(quote, str) or not quote.strip():
            reject("missing_evidence_quote")
            continue
        if len(quote) > EVIDENCE_MAX:
            reject("evidence_quote_too_long")
            continue
        body = _cited_body(chunks, cited)
        if quote in body:
            confidence = LLM_CONF_RAW
        elif _ws_collapse(quote) in _ws_collapse(body):
            confidence = LLM_CONF_NORM
        else:
            reject("quote_mismatch")
            continue
        headcount = rr.get("headcount")
        if headcount is not None and not isinstance(headcount, int):
            reject("bad_headcount_type")
            continue
        if headcount is not None and not 1 <= headcount <= 5000:
            headcount = None
        pops: dict[str, dt.date | None] = {}
        bad_date = False
        for key in ("pop_start", "pop_end"):
            v = rr.get(key)
            if v is None:
                pops[key] = None
                continue
            d = parse_date(v) if isinstance(v, str) else None
            if d is None:
                reject(f"bad_date:{key}")
                bad_date = True
                break
            pops[key] = d
        if bad_date:
            continue
        mand = rr.get("mandatory", False)
        if not isinstance(mand, bool):
            reject("bad_mandatory_type")
            continue
        wage = rr.get("wage_floor")
        if wage is not None and not isinstance(wage, (int, float)):
            reject("bad_wage_floor_type")
            continue
        detail = rr.get("requirement_detail")
        if detail is not None and not isinstance(detail, str):
            reject("bad_detail_type")
            continue
        place = rr.get("place_of_performance_text")
        if place is not None and not isinstance(place, str):
            reject("bad_place_type")
            continue
        rid_hash = requirement_id(rid, rtype, value_norm)
        if rid_hash in seen_ids:
            reject("duplicate_requirement")
            continue
        seen_ids.add(rid_hash)
        req_rows.append({
            "requirement_id": rid_hash,
            "resource_id": rid,
            "notice_id": meta.get("notice_id"),
            "solicitation_number": meta.get("solicitation_number"),
            "naics_code": meta.get("naics_code"),
            "contract_award_unique_key": meta.get("contract_award_unique_key"),
            "requirement_type": rtype,
            "requirement_value": value_norm,
            "requirement_detail": detail[:DETAIL_MAX] if detail else None,
            "mandatory": mand,
            "headcount": headcount,
            "clearance_level": clr,
            "pop_start": pops["pop_start"], "pop_end": pops["pop_end"],
            "place_of_performance_text": place[:120] if place else None,
            "wage_floor": float(wage) if wage is not None else None,
            "source_chunk_ids": sorted(set(cited), key=lambda c: chunks[c]["chunk_ix"]),
            "evidence_quote": quote,
            "validated": True, "marked_resource": False,
            "coverage_truncated": bool(meta.get("coverage_truncated")),
            "extractor": engine_tag, "extractor_version": LLM_PROMPT_VERSION,
            "confidence": confidence,
            "run_id": run_id, "created_at": now,
        })

    summary = result.get("scope_summary")
    if summary is not None and (not isinstance(summary, str) or len(summary) > 2000):
        rejects.append({"reason": "bad_scope_summary"})
        summary = None
    raw_tags = result.get("capability_tags")
    if not isinstance(raw_tags, list):
        raw_tags = []
        rejects.append({"reason": "capability_tags_not_list"})
    tags, n_tag_rejected = [], 0
    for t in raw_tags:
        if isinstance(t, str) and t in tag_set:
            if t not in tags:
                tags.append(t)
        else:
            n_tag_rejected += 1
            rejects.append({"reason": f"tag_out_of_vocab:{t}"})
    doc_row = {
        "resource_id": rid,
        "notice_id": meta.get("notice_id"),
        "solicitation_number": meta.get("solicitation_number"),
        "naics_code": meta.get("naics_code"),
        "contract_award_unique_key": meta.get("contract_award_unique_key"),
        "scope_summary": summary,
        "capability_tags": tags,
        "n_capability_tags_rejected": n_tag_rejected,
        "source_chunk_ids": [c["chunk_id"] for c in task["chunks"]],
        "n_chunks_total": meta.get("n_chunks_total"),
        "n_chunks_selected": meta.get("n_chunks_selected"),
        "coverage_truncated": bool(meta.get("coverage_truncated")),
        "marked_resource": False, "validated": True,
        "extractor": engine_tag, "extractor_version": LLM_PROMPT_VERSION,
        "prompt_hash": task["prompt_hash"],
        "confidence": LLM_DOC_CONFIDENCE,
        "run_id": run_id, "created_at": now,
    }
    n_pass, n_rej = len(req_rows), len(rejects)
    denom = n_pass + n_rej
    return {"status": "pass" if n_rej == 0 else "partial",
            "doc_row": doc_row, "req_rows": req_rows, "rejects": rejects,
            "n_rows_passed": n_pass, "n_rows_rejected": n_rej,
            "pass_rate": (n_pass / denom) if denom else 1.0}


def _land_llm_batch(so: dict, args, docs: list[dict], run_id: str, now: dt.datetime) -> dict:
    """One commit batch under held leases: scoped delete-before-merge on BOTH sinks (hard rule 2),
    then the ledger merge. docs = validated per-doc outcomes (pass/partial/quarantined)."""
    import lance
    import pyarrow as pa
    stats: dict = {}
    landable = [d for d in docs if d["status"] in ("pass", "partial")]
    if landable:
        ids = [d["resource_id"] for d in landable]
        req_rows = [r for d in landable for r in d["req_rows"]]
        assert_schema(args.requirements_uri, requirements_schema(), so)
        ds = lance.dataset(args.requirements_uri, storage_options=so)
        ds.delete(llm_delete_predicate(ids))
        if req_rows:
            tbl = pa.Table.from_pylist(req_rows, schema=requirements_schema())
            res = (ds.merge_insert("requirement_id").when_matched_update_all()
                   .when_not_matched_insert_all().execute(tbl))
            stats["requirements"] = _merge_stats(res)
        assert_schema(args.doc_scope_uri, doc_scope_schema(), so)
        ds = lance.dataset(args.doc_scope_uri, storage_options=so)
        ds.delete(in_predicate("resource_id", ids))
        doc_rows = [d["doc_row"] for d in landable]
        tbl = pa.Table.from_pylist(doc_rows, schema=doc_scope_schema())
        res = (ds.merge_insert("resource_id").when_matched_update_all()
               .when_not_matched_insert_all().execute(tbl))
        stats["doc_scope"] = _merge_stats(res)
    updates = []
    for d in docs:
        if d["status"] in ("pass", "partial"):
            updates.append({"resource_id": d["resource_id"], "llm_state": "done",
                            "model": d["model"], "prompt_hash": d["prompt_hash"],
                            "n_requirements_llm": d["n_rows_passed"],
                            "validation_pass_rate": d["pass_rate"]})
        else:                                          # quarantined: prior rows untouched
            updates.append({"resource_id": d["resource_id"], "llm_state": "quarantined",
                            "model": d["model"], "prompt_hash": d["prompt_hash"],
                            "validation_pass_rate": 0.0})
    stats["ledger"] = merge_ledger_llm(so, args.ledger_uri, updates, run_id, now)
    return stats


def phase_llm_ingest(args, so: dict, run_id: str) -> dict:
    """Validate every result JSON in --results-dir against its staged task file, enforce the
    run-level pass-rate gate (plan Phase 2: <98% → nothing lands unless --force-land), then land
    passing rows via scoped delete-before-merge under the sink leases and update the ledger."""
    _template, _schema, vocab, phash = load_llm_artifacts()
    engine_tag = llm_extractor_tag(args.engine)
    now = dt.datetime.now(dt.timezone.utc)
    tasks_dir = os.path.join(args.staging_dir, "tasks")
    results_dir = args.results_dir or os.path.join(args.staging_dir, "results")
    task_files = sorted(f for f in os.listdir(tasks_dir) if f.endswith(".task.json")) \
        if os.path.isdir(tasks_dir) else []
    if not task_files:
        raise RuntimeError(f"no task files under {tasks_dir} — run --phase select first")

    docs: list[dict] = []
    missing: list[str] = []
    per_doc_report: dict[str, dict] = {}
    for tf in task_files:
        with open(os.path.join(tasks_dir, tf)) as f:
            task = json.load(f)
        rid = task["resource_id"]
        if task.get("prompt_hash") != phash and task.get("prompt_hash") != getattr(args, "allow_prompt_hash", None):
            raise RuntimeError(f"task {rid} staged under prompt_hash {task.get('prompt_hash')[:12]} "
                               f"!= current {phash[:12]} — re-run select; never mix prompt versions "
                               f"(or pass --allow-prompt-hash to land a superseded staging explicitly)")
        rpath = os.path.join(results_dir, f"{rid}.result.json")
        if not os.path.exists(rpath):
            missing.append(rid)
            per_doc_report[rid] = {"status": "missing"}
            continue
        try:
            with open(rpath) as f:
                result = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            v = {"status": "quarantined", "doc_row": None, "req_rows": [],
                 "rejects": [{"reason": f"unreadable_result:{exc}"}],
                 "n_rows_passed": 0, "n_rows_rejected": 0, "pass_rate": 0.0}
        else:
            v = validate_result(result, task, vocab, engine_tag, run_id, now)
        # provenance truth: rows record the STAGED task's hash (the prompt that actually produced the
        # extraction) — equals the current artifact hash except under --allow-prompt-hash.
        v.update({"resource_id": rid, "model": args.engine, "prompt_hash": task.get("prompt_hash") or phash})
        docs.append(v)
        per_doc_report[rid] = {"status": v["status"], "n_rows_passed": v["n_rows_passed"],
                               "n_rows_rejected": v["n_rows_rejected"],
                               "pass_rate": round(v["pass_rate"], 4),
                               "reject_reasons": [r["reason"] for r in v["rejects"]][:10]}

    rows_passed = sum(d["n_rows_passed"] for d in docs)
    rows_rejected = sum(d["n_rows_rejected"] for d in docs)
    denom = rows_passed + rows_rejected
    run_pass_rate = (rows_passed / denom) if denom else 1.0
    gate_ok = run_pass_rate >= args.min_pass_rate or args.force_land
    reasons: dict[str, int] = {}
    for d in docs:
        for r in d["rejects"]:
            key = r["reason"].split(":", 1)[0]
            reasons[key] = reasons.get(key, 0) + 1
    report = {
        "run_id": run_id, "engine_tag": engine_tag, "prompt_hash": phash,
        "results_dir": results_dir,
        "totals": {"tasks": len(task_files), "results": len(docs), "missing": len(missing),
                   "pass": sum(d["status"] == "pass" for d in docs),
                   "partial": sum(d["status"] == "partial" for d in docs),
                   "quarantined": sum(d["status"] == "quarantined" for d in docs),
                   "rows_passed": rows_passed, "rows_rejected": rows_rejected,
                   "run_pass_rate": round(run_pass_rate, 4),
                   "min_pass_rate": args.min_pass_rate, "gate_ok": gate_ok, "landed": False},
        "reject_reasons": reasons,
        "per_doc": per_doc_report,
        "missing_resource_ids": missing[:50],
        "sample_extractions": [
            {k: r[k] for k in ("resource_id", "requirement_type", "requirement_value",
                               "evidence_quote", "source_chunk_ids", "confidence")}
            for d in docs for r in d["req_rows"][:2]][:5],
    }
    if not docs:
        with open(args.report_out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"ingest: no result files found under {results_dir} "
              f"({len(missing)} staged tasks awaiting results)", flush=True)
        return report
    if not gate_ok:
        with open(args.report_out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"ingest: RUN GATE FAILED pass_rate={run_pass_rate:.4f} < {args.min_pass_rate} — "
              f"NOTHING landed (resources stay pending; fix the engine output or --force-land). "
              f"Report: {args.report_out}", flush=True)
        return report

    leases: list[SinkCommitLease] = []
    write_stats: list[dict] = []
    try:
        for uri in (args.requirements_uri, args.doc_scope_uri, args.ledger_uri):
            leases.append(SinkCommitLease(uri, holder=f"llm_ingest:{run_id}",
                                          ttl_s=12 * 3600).acquire())
        for i in range(0, len(docs), LLM_INGEST_FLUSH):
            stats = _land_llm_batch(so, args, docs[i:i + LLM_INGEST_FLUSH], run_id, now)
            write_stats.append(stats)
            print(f"ingest: landed batch {i // LLM_INGEST_FLUSH} {stats}", flush=True)
    finally:
        for lease in leases:
            lease.release()
    report["totals"]["landed"] = True
    report["write_stats"] = write_stats
    with open(args.report_out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"ingest: {json.dumps(report['totals'])}", flush=True)
    return report


def phase_llm_reset(args, so: dict, run_id: str) -> dict:
    """Reverse an LLM landing for explicit resources (decision-A reversibility + smoke hygiene):
    scoped delete of llm:% requirement rows + doc-scope rows, ledger lane back to pending with the
    LLM metric columns cleared. Regex-lane rows and columns are never touched."""
    import lance
    if not args.resource_ids:
        raise RuntimeError("--phase reset-llm requires --resource-ids")
    ids = sorted(set(args.resource_ids))
    leases: list[SinkCommitLease] = []
    out: dict = {"resources": len(ids)}
    try:
        for uri in (args.requirements_uri, args.doc_scope_uri, args.ledger_uri):
            leases.append(SinkCommitLease(uri, holder=f"llm_reset:{run_id}",
                                          ttl_s=3600).acquire())
        assert_schema(args.requirements_uri, requirements_schema(), so)
        lance.dataset(args.requirements_uri, storage_options=so).delete(llm_delete_predicate(ids))
        assert_schema(args.doc_scope_uri, doc_scope_schema(), so)
        lance.dataset(args.doc_scope_uri, storage_options=so).delete(
            in_predicate("resource_id", ids))
        updates = [{"resource_id": rid, "llm_state": "pending", "model": None,
                    "prompt_hash": None, "n_requirements_llm": None,
                    "validation_pass_rate": None, "batch_id": None} for rid in ids]
        out["ledger"] = merge_ledger_llm(so, args.ledger_uri, updates, run_id,
                                         dt.datetime.now(dt.timezone.utc))
    finally:
        for lease in leases:
            lease.release()
    print(f"reset-llm: {json.dumps(out)}", flush=True)
    return out


# ════════════════════════════════════════════════════════════════ main
def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="GovCon Phase-1 regex requirement extraction (build plan PHASE 1; spec §17).")
    p.add_argument("--phase", default="extract",
                   choices=["extract", "index", "bracket", "select", "census", "ingest",
                            "reset-llm"])
    p.add_argument("--sinks", default="scope,pricing,unknown",
                   help="comma list of chunk sinks to read (read-only)")
    p.add_argument("--resource-ids", default=None,
                   help="comma list — explicit slice mode (smoke)")
    p.add_argument("--resource-ids-file", default=None,
                   help="newline-delimited id file (preferred for thousands of ids)")
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
    # ── LLM lane (build-plan PHASE 2) ──────────────────────────────────────────────────────────
    p.add_argument("--doc-scope-uri", default=DOC_SCOPE_URI)
    p.add_argument("--staging-dir", default=LLM_STAGING_DIR,
                   help="select: writes tasks/ + results/ here; ingest: reads tasks/ from here")
    p.add_argument("--results-dir", default=None,
                   help="ingest: result-JSON dir (default <staging-dir>/results)")
    p.add_argument("--pilot", type=int, default=0,
                   help="select: SEEDED stratified sample of N docs (sink × size-bucket strata)")
    p.add_argument("--pilot-seed", type=int, default=20260612)
    p.add_argument("--manifest-out", default=None,
                   help="select: write a batch manifest JSON (batches of --batch-size docs)")
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument("--engine", default=LLM_ENGINE_DEFAULT,
                   help="extractor tag becomes llm:<engine>@<prompt_version>")
    p.add_argument("--token-budget", type=int, default=DOC_TOKEN_BUDGET,
                   help="hard per-doc selected-token budget (chars/4 estimate)")
    p.add_argument("--min-pass-rate", type=float, default=LLM_MIN_PASS_RATE,
                   help="ingest run gate: below this nothing lands (plan Phase 2: 0.98)")
    p.add_argument("--allow-prompt-hash", default=None,
                   help="ingest: additionally accept task files staged under this exact superseded "
                        "prompt_hash (rows still record the staged hash — true provenance). Use to land "
                        "results from a staging that predates a prompt-artifact edit; never mixes "
                        "versions silently.")
    p.add_argument("--force-land", action="store_true",
                   help="ingest: land despite a failed run gate (operator override, logged)")
    p.add_argument("--marking-report",
                   default=os.environ.get("SAM90_MARKING_REPORT", "/tmp/sam_marking_fullbody_report.json"),
                   help="CUI gate: Phase-F reconcile report; bracket/select refuse unless reconcile_overall=PASS")
    p.add_argument("--require-marking-after", default=None,
                   help="CUI gate freshness: require marking report completed_at >= this ISO8601 (e.g. the "
                        "chunk-append time) so a stale prior-run PASS cannot satisfy the gate")
    args = p.parse_args(argv)
    args.sinks = {s.strip() for s in args.sinks.split(",") if s.strip()}
    if args.resource_ids_file:
        with open(args.resource_ids_file) as _fh:
            args.resource_ids = [ln.strip() for ln in _fh if ln.strip()]
    else:
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
    elif args.phase == "index":
        report["index"] = phase_index(args, so)
    elif args.phase == "bracket":
        if args.report_out == REPORT_PATH:
            args.report_out = LLM_BRACKET_REPORT
        report["bracket"] = phase_llm_bracket(args, so, run_id)
    elif args.phase == "select":
        report["select"] = phase_llm_select(args, so, run_id)
    elif args.phase == "census":
        if args.report_out == REPORT_PATH:
            args.report_out = LLM_CENSUS_REPORT
        report["census"] = phase_llm_census(args, so, run_id)
        report = report["census"]                     # census writes its own full report file
    elif args.phase == "ingest":
        if args.report_out == REPORT_PATH:
            args.report_out = LLM_INGEST_REPORT
        report["ingest"] = phase_llm_ingest(args, so, run_id)
        report = report["ingest"]                     # ingest writes its own full report file
    elif args.phase == "reset-llm":
        report["reset_llm"] = phase_llm_reset(args, so, run_id)
    report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    if args.phase not in ("census", "ingest"):        # those phases own their report files
        with open(args.report_out, "w") as f:
            json.dump(report, f, indent=2, default=str)
    summary = {k: v for k, v in report.get("extract", {}).items() if k != "write_stats"}
    if not summary:
        for key in ("index", "bracket", "select", "reset_llm", "totals"):
            if key in report:
                summary = report[key]
                break
    print("RESULT: " + json.dumps(summary or {}, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
