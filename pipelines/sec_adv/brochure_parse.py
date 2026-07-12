#!/usr/bin/env python3
"""Gen-3 Tier-1 parser for SEC ADV Part 2A brochures — deterministic, LLM-free.

Written fresh for core-x (not ported from hq-x). Engine: PyMuPDF. The splitter
segments brochure text into the 18 canonical Form ADV Part 2A items using THREE
independent anchor families, in priority order per item:

  A. Item-token headers  — "Item 8", "ITEM 8:", "Item 8 –" at line start,
                           rejecting TOC dot-leader rows and sub-refs ("Item 8.A.").
  B. SEC-standard titles — the Part 2A instructions mandate exact section names
                           ("Advisory Business", "Fees and Compensation", …);
                           matched as standalone heading lines. Catches filers
                           who omit the "Item N" token in body headings.
  C. PDF outline (TOC)   — PyMuPDF ``doc.get_toc()`` bookmark → page anchors,
                           mapped to items by title. Used to disambiguate, and
                           as last-resort page-granular segmentation.

Outputs per brochure: {item_n: text} for n in 1..18, a confidence score
(items cleanly found), and bracket flags. Image-only PDFs (chars/page below
threshold) are TAGGED and PARKED — no OCR, by design.

Routing gate: confidence >= TIER1_OK_THRESHOLD keeps the brochure fully in the
free tier; below it, the brochure is flagged needs_fallback for the metered pass.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import fitz  # PyMuPDF

# ── Canonical Part 2A items (SEC Form ADV instructions, mandated titles) ──────
ITEM_TITLES: dict[int, str] = {
    1: "Cover Page",
    2: "Material Changes",
    3: "Table of Contents",
    4: "Advisory Business",
    5: "Fees and Compensation",
    6: "Performance-Based Fees and Side-By-Side Management",
    7: "Types of Clients",
    8: "Methods of Analysis, Investment Strategies and Risk of Loss",
    9: "Disciplinary Information",
    10: "Other Financial Industry Activities and Affiliations",
    11: "Code of Ethics, Participation or Interest in Client Transactions and Personal Trading",
    12: "Brokerage Practices",
    13: "Review of Accounts",
    14: "Client Referrals and Other Compensation",
    15: "Custody",
    16: "Investment Discretion",
    17: "Voting Client Securities",
    18: "Financial Information",
}

IMAGE_ONLY_CHARS_PER_PAGE = 80   # below this avg → scanned/image PDF, bracket out
TIER1_OK_THRESHOLD = 10          # >= this many items found → no metered fallback
MIN_SECTION_GAP = 1000           # a real Part 2A body section exceeds this; TOC rows
                                 # are far shorter — used to bound the TOC region
COLLAPSE_TAIL_FRAC = 0.5         # one item holding > this share of body text, with
COLLAPSE_CORE_CHARS = 2000       # items 4/5/8 under this, = a TOC-decoy collapse

# Anchor A: "Item N" at line start; reject sub-question refs ("Item 8.A").
# The lookahead must reject only dot-IMMEDIATELY-followed-by-alnum ("8.A"),
# not the common "Item 2. Material Changes" heading style (dot + space).
_ITEM_TOKEN_RE = re.compile(
    r"^[ \t>#*]*item\s+(\d{1,2})(?!\.[A-Za-z0-9])\b[ \t]*[:.\-–—]?",
    re.IGNORECASE | re.MULTILINE,
)
_DOT_LEADER_RE = re.compile(r"[.…]{4,}")
_TRAILING_PAGENO_RE = re.compile(r"\s(?:\d{1,3}|[ivxlc]{1,6})\s*$", re.IGNORECASE)

# Anchor B: title lines. Short distinctive prefixes of the mandated titles,
# matched as (near-)whole heading lines so prose mentions don't fire.
_TITLE_PATTERNS: list[tuple[int, re.Pattern]] = [
    (2, re.compile(r"^[ \t]*material\s+changes\b.{0,40}$", re.I | re.M)),
    (3, re.compile(r"^[ \t]*table\s+of\s+contents\b.{0,20}$", re.I | re.M)),
    (4, re.compile(r"^[ \t]*advisory\s+business\b.{0,40}$", re.I | re.M)),
    (5, re.compile(r"^[ \t]*fees\s+and\s+compensation\b.{0,40}$", re.I | re.M)),
    (6, re.compile(r"^[ \t]*performance[\s‐-―-]*based\s+fees\b.{0,80}$", re.I | re.M)),
    (7, re.compile(r"^[ \t]*types?\s+of\s+clients\b.{0,40}$", re.I | re.M)),
    (8, re.compile(r"^[ \t]*methods?\s+of\s+analysis\b.{0,100}$", re.I | re.M)),
    (9, re.compile(r"^[ \t]*disciplinary\s+information\b.{0,40}$", re.I | re.M)),
    (10, re.compile(r"^[ \t]*other\s+financial\s+industry\s+activit.{0,60}$", re.I | re.M)),
    (11, re.compile(r"^[ \t]*code\s+of\s+ethics\b.{0,120}$", re.I | re.M)),
    (12, re.compile(r"^[ \t]*brokerage\s+practices\b.{0,40}$", re.I | re.M)),
    (13, re.compile(r"^[ \t]*review\s+of\s+accounts\b.{0,40}$", re.I | re.M)),
    (14, re.compile(r"^[ \t]*client\s+referrals\s+and\s+other\s+compensation\b.{0,40}$", re.I | re.M)),
    (15, re.compile(r"^[ \t]*custody\b.{0,30}$", re.I | re.M)),
    (16, re.compile(r"^[ \t]*investment\s+discretion\b.{0,40}$", re.I | re.M)),
    (17, re.compile(r"^[ \t]*voting\s+client\s+securities\b.{0,40}$", re.I | re.M)),
    (18, re.compile(r"^[ \t]*financial\s+information\b.{0,40}$", re.I | re.M)),
]

# Anchor C: outline-title → item mapping uses the same patterns against
# bookmark titles (single-line strings), so compile a non-multiline variant.
_TITLE_KEYWORDS: list[tuple[int, re.Pattern]] = [
    (n, re.compile(p.pattern.replace("^[ \t]*", "").rstrip("$"), re.I))
    for n, p in _TITLE_PATTERNS
]
_OUTLINE_ITEM_RE = re.compile(r"\bitem\s+(\d{1,2})\b", re.IGNORECASE)


@dataclass
class BrochureParse:
    """Result of a Tier-1 parse."""
    n_pages: int = 0
    n_chars: int = 0
    image_only: bool = False
    items: dict[int, str] = field(default_factory=dict)
    anchor_source: dict[int, str] = field(default_factory=dict)  # item → A|B|C
    confidence: int = 0
    needs_fallback: bool = True


def extract_text(pdf_path: str) -> tuple[str, int]:
    """PyMuPDF per-page text, pages joined with form-feed."""
    with fitz.open(pdf_path) as doc:
        pages = [page.get_text() for page in doc]
        return "\x0c".join(pages), doc.page_count


def _line_of(text: str, pos: int) -> str:
    start = text.rfind("\n", 0, pos) + 1
    end = text.find("\n", pos)
    return text[start: end if end != -1 else len(text)]


def _is_toc_row(line: str) -> bool:
    """TOC rows have dot leaders or a bare trailing page number."""
    return bool(_DOT_LEADER_RE.search(line)) or bool(
        _TRAILING_PAGENO_RE.search(line.rstrip()) and len(line.strip()) < 120
    )


def _token_anchors(text: str) -> list[tuple[int, int]]:
    """Anchor A: every non-TOC 'Item N' header occurrence as (pos, item).

    The TOC check runs on the line REMAINDER after the matched token — a bare
    "Item 5" heading line must not be mistaken for a TOC row just because it
    ends in a digit (the item number itself).
    """
    out: list[tuple[int, int]] = []
    for m in _ITEM_TOKEN_RE.finditer(text):
        n = int(m.group(1))
        if not 1 <= n <= 18:
            continue
        line = _line_of(text, m.start())
        line_start = text.rfind("\n", 0, m.start()) + 1
        remainder = line[m.end() - line_start:]
        if _is_toc_row(remainder) and remainder.strip():
            continue
        out.append((m.start(), n))
    return out


def _title_anchors(text: str) -> list[tuple[int, int]]:
    """Anchor B: every non-TOC standalone title-heading occurrence as (pos, item)."""
    out: list[tuple[int, int]] = []
    for n, pat in _TITLE_PATTERNS:
        for m in pat.finditer(text):
            if _is_toc_row(_line_of(text, m.start())):
                continue
            out.append((m.start(), n))
    return out


MIN_TOC_RUN = 6                  # >= this many consecutive small-gap anchors = a TOC block
TOC_FRONT_FRAC = 0.25            # only the front of the doc holds a TOC; guards real content


def _toc_spans(text: str, cands: list[tuple[int, int, str]]) -> list[tuple[int, int]]:
    """Locate table-of-contents regions so their decoy 'Item N' rows can be dropped.

    A brochure's TOC lists Items 1..18 as a dense run of header-looking lines only a
    few dozen chars apart. When those rows lack dot-leaders / trailing page numbers,
    ``_is_toc_row`` misses them and the TOC forms a complete ascending anchor chain
    that the longest-increasing-chain selector locks onto — collapsing every real
    section after the TOC's last item into a single tail item. Real sections sit
    thousands of chars apart, so a TOC is identifiable by density alone, independent
    of any heading: a front-of-document run of >= MIN_TOC_RUN consecutive anchors each
    within MIN_SECTION_GAP of the next. Returns the [start, end) span(s) to exclude;
    the first substantial section (the anchor after the run) is preserved.
    """
    if not cands:
        return []
    front = TOC_FRONT_FRAC * len(text)
    spans: list[tuple[int, int]] = []
    i, n = 0, len(cands)
    while i < n:
        j = i
        while j + 1 < n and (cands[j + 1][0] - cands[j][0]) < MIN_SECTION_GAP:
            j += 1
        if j - i + 1 >= MIN_TOC_RUN and cands[i][0] <= front:
            spans.append((cands[i][0], cands[j][0] + 1))  # through the run's last anchor
        i = j + 1
    return spans


def _outline_anchors(doc: fitz.Document, page_offsets: list[int]) -> dict[int, int]:
    """Anchor C: PDF bookmarks mapped to items → char offset of their page."""
    out: dict[int, int] = {}
    for _level, title, page_no in doc.get_toc(simple=True) or []:
        if not title or not 1 <= page_no <= len(page_offsets):
            continue
        n = None
        tok = _OUTLINE_ITEM_RE.search(title)
        if tok and 1 <= int(tok.group(1)) <= 18:
            n = int(tok.group(1))
        else:
            for k, pat in _TITLE_KEYWORDS:
                if pat.search(title):
                    n = k
                    break
        if n is not None:
            out.setdefault(n, page_offsets[page_no - 1])
    return out


def split_items(text: str, doc: "fitz.Document | None" = None) -> tuple[dict[int, str], dict[int, str]]:
    """Segment brochure text into items 1..18.

    Collects ALL candidate anchors from the three families, then selects the
    best chain: the longest subsequence of candidates whose item numbers are
    strictly increasing in document order (a real Part 2A lays items out
    1..18 sequentially). This drops TOC rows and stray prose references
    without per-line heuristics. Token anchors (A) outrank title (B) and
    outline (C) anchors at equal chain length.

    Returns ({item: text}, {item: anchor_source}).
    """
    cands: list[tuple[int, int, str]] = []          # (pos, item, src)
    cands += [(pos, n, "A") for pos, n in _token_anchors(text)]
    cands += [(pos, n, "B") for pos, n in _title_anchors(text)]
    if doc is not None:
        offsets, acc = [], 0
        for page in doc:
            offsets.append(acc)
            acc += len(page.get_text()) + 1  # +1 form-feed joiner
        cands += [(pos, n, "C") for n, pos in _outline_anchors(doc, offsets).items()]
    if not cands:
        return {}, {}
    cands.sort(key=lambda c: (c[0], c[2]))          # by position; A before B/C at ties

    items, sources = _chain_items(text, cands)

    # TOC-decoy repair, attempted ONLY when the first pass leaves the core strategy/fee
    # items (4/5/8) starved — the signature of a dense TOC run winning the chain and
    # dumping every real section into one tail item. Brochures that split cleanly have
    # substantial 4/5/8 and never reach here. The re-split is a no-op unless a TOC run
    # is actually found, and is adopted only if it grows the core body — so a lenient
    # trigger cannot corrupt a doc that had no decoy TOC. Zero-downside by construction.
    core = lambda it: sum(len(it.get(i, "")) for i in (4, 5, 8))
    if core(items) < COLLAPSE_CORE_CHARS:
        spans = _toc_spans(text, cands)
        stripped = [c for c in cands if not any(lo <= c[0] < hi for lo, hi in spans)]
        if spans and stripped:
            items2, sources2 = _chain_items(text, stripped)
            if core(items2) > core(items):
                items, sources = items2, sources2
    return items, sources


def _chain_items(text: str, cands: list[tuple[int, int, str]]) -> tuple[dict[int, str], dict[int, str]]:
    """Select the best anchor chain and slice item bodies between consecutive anchors.

    Longest strictly-increasing chain on item number, weighted A=3, B=2, C=1 so the
    true header run beats an equally long chain of weaker anchors. Body of each anchor
    is the text up to the next anchor in the chain.
    """
    if not cands:
        return {}, {}
    W = {"A": 3, "B": 2, "C": 1}
    best_w = [0.0] * len(cands)
    prev = [-1] * len(cands)
    for i, (pos_i, n_i, src_i) in enumerate(cands):
        best_w[i] = W[src_i]
        for j in range(i):
            if cands[j][1] < n_i and best_w[j] + W[src_i] > best_w[i]:
                best_w[i] = best_w[j] + W[src_i]
                prev[i] = j
    end_i = max(range(len(cands)), key=lambda i: best_w[i])
    chain: list[tuple[int, int, str]] = []
    while end_i != -1:
        chain.append(cands[end_i])
        end_i = prev[end_i]
    chain.reverse()

    items: dict[int, str] = {}
    sources: dict[int, str] = {}
    for i, (pos, n, src) in enumerate(chain):
        end = chain[i + 1][0] if i + 1 < len(chain) else len(text)
        body = text[pos:end].strip()
        if body:
            items[n] = body
            sources[n] = src
    return items, sources


def parse_brochure(pdf_path: str) -> BrochureParse:
    """Full Tier-1 parse: extract → bracket check → split → score."""
    r = BrochureParse()
    with fitz.open(pdf_path) as doc:
        r.n_pages = doc.page_count
        text = "\x0c".join(page.get_text() for page in doc)
        r.n_chars = len(text)
        if r.n_pages and (r.n_chars / r.n_pages) < IMAGE_ONLY_CHARS_PER_PAGE:
            r.image_only = True   # parked: no OCR by design
            return r
        r.items, r.anchor_source = split_items(text, doc)
    r.confidence = len(r.items)
    r.needs_fallback = r.confidence < TIER1_OK_THRESHOLD or _is_collapsed(r.items)
    return r


def _is_collapsed(items: dict[int, str]) -> bool:
    """Detect a TOC-decoy collapse the item count alone misses: one item holding
    the bulk of the body while the core strategy/fee items (4/5/8) are starved."""
    if not items:
        return False
    total = sum(len(v) for v in items.values()) or 1
    tail = max(len(v) for v in items.values())
    core = sum(len(items.get(i, "")) for i in (4, 5, 8))
    return tail / total > COLLAPSE_TAIL_FRAC and core < COLLAPSE_CORE_CHARS
