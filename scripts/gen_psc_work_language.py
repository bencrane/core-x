"""Generate the frozen work-language vocabulary for the phrase compiler.

Derives, from the live psc_reference (via the query-sidecar), a mapping of
plain-English work phrases onto PSC code sets:

    "repair bridges"   -> Z2LB (+ any other code whose noun aliases include it)
    "build runways"    -> Y1BD
    "supply dredges"   -> 1955

Structure emitted (apps/catalyst_api/src/psc_work_language.py — FROZEN, the
compiler's vocabulary-is-code doctrine; regenerate + review + re-pin tests to
change):

    WORK_VERBS:  verb token -> verb class  (synonyms collapse: fix/renovate ->
                 'repair'; construct -> 'build'; rent -> 'lease'; ...)
    WORK_NOUNS:  noun alias -> {verb class -> sorted psc code list}

Noun aliases come from the official titles with the verb prefix stripped and
multi-noun titles SPLIT ('HIGHWAYS/ROADS/STREETS/BRIDGES/RAILWAYS' -> five
aliases), so no official slash-string ever needs to be typed. Generic nouns
that would be uselessly broad or collide with existing compiler vocabulary
are blacklisted.

Usage: doppler run -p core-x -c prd -- python3 scripts/gen_psc_work_language.py
"""
from __future__ import annotations

import json
import os
import re
import urllib.request
from collections import defaultdict

OUT_PATH = "apps/query_sidecar_api/../catalyst_api/src/psc_work_language.py"
SIDECAR_URL = "https://query-sidecar-api.onrender.com/api/v1/sql"

# ── verb classes: family prefix rules ─────────────────────────────────────────
# (match: regex on psc_code, verb class). First hit wins.
FAMILY_VERBS: list[tuple[str, str]] = [
    (r"^Y", "build"),
    (r"^Z1", "maintain"),
    (r"^Z2", "repair"),
    (r"^Z", "repair"),          # Z-other (rare): treat as repair
    (r"^C", "design"),
    (r"^M", "operate"),
    (r"^X", "lease"),
    (r"^E", "purchase"),
    (r"^P", "demolish"),
    (r"^J", "repair"),          # equipment repair joins 'repair'
    (r"^K", "modify"),
    (r"^N", "install"),
    (r"^L", "support"),
    (r"^W", "lease"),           # equipment lease joins 'lease'
    (r"^H", "test"),
    (r"^A", "research"),
    (r"^B", "study"),
    (r"^U", "train"),
    (r"^V", "transport"),
    (r"^T", "print"),
    (r"^Q", "treat"),
    (r"^D", "provide"),
    (r"^F", "provide"),
    (r"^G", "provide"),
    (r"^R", "provide"),
    (r"^S", "provide"),
    (r"^[0-9]", "supply"),
]

# verb synonyms accepted in phrases -> verb class
VERB_SYNONYMS: dict[str, str] = {
    "build": "build", "construct": "build",
    "repair": "repair", "fix": "repair", "renovate": "repair", "rebuild": "repair",
    "maintain": "maintain",
    "design": "design", "engineer": "design",
    "operate": "operate", "run": "operate",
    "lease": "lease", "rent": "lease",
    "purchase": "purchase", "buy": "purchase",
    "demolish": "demolish",
    "modify": "modify",
    "install": "install",
    "support": "support",
    "test": "test",
    "research": "research",
    "study": "study",
    "train": "train",
    "transport": "transport",
    "print": "print",
    "treat": "treat",
    "supply": "supply", "sell": "supply",
    "provide": "provide", "deliver": "provide", "perform": "provide",
}

# title prefixes to strip (family-specific, longest first) — what remains is
# the noun phrase.
STRIP_PREFIXES = [
    r"^CONSTRUCTION OF\b", r"^CONSTRUCT\b",
    r"^MAINTENANCE OF\b", r"^MAINT(,| OF)?\b.*?OF\b",
    r"^REPAIR OR ALTERATION OF\b", r"^REPAIR\b(?: OF)?",
    r"^ARCHITECT AND ENGINEERING-\s*CONSTRUCTION:\s*",
    r"^ARCH-ENG SVCS -\s*", r"^ARCHITECT/ENGINEER SERVICES\b[-:]?\s*",
    r"^OPERATION OF\b",
    r"^LEASE/RENTAL OF\b", r"^LEASE OR RENTAL OF\b", r"^LEASE/RENT\b",
    r"^PURCHASE OF\b", r"^PURCH\b",
    r"^DEMOLITION OF\b", r"^SALVAGE[-–]?\s*",
    r"^MODIFICATION OF EQUIPMENT[-–]?\s*",
    r"^INSTALLATION OF EQUIPMENT[-–]?\s*",
    r"^TECHNICAL REP(RESENTATIVE)?[-–]?\s*",
    r"^EQUIP/MATERIALS TESTING[-–]?\s*", r"^QUALITY CONTROL[-–]?\s*",
    r"^MAINT, REPAIR, REBUILD OF EQUIPMENT[-–]?\s*",
    r"^EDUCATION/TRAINING[-–]?\s*", r"^EDUCATION AND TRAINING\b[-:]?\s*",
    r"^R&D[-–]?\s*", r"^SPECIAL STUDIES(/ANALYSIS)?[-–]?\s*",
    r"^IT AND TELECOM[-–]?\s*", r"^IT and Telecom[-–]?\s*",
    r"^SUPPORT[-–]\s*PROFESSIONAL:?\s*",
    r"^NATURAL RESOURCES/CONSERVATION[-–]?\s*",
    r"^ENVIRON SYS PROTECT[-–]?\s*", r"^OTHER ENVIRONMENTAL\b\s*",
    r"^SOCIAL[-–]?\s*",
    r"^UTILITIES[-–]?\s*", r"^HOUSEKEEPING[-–]?\s*",
    r"^ADMINISTRATIVE SUPPORT\b\s*",
]

# noun aliases that are too generic / collide with existing compiler vocabulary
NOUN_BLACKLIST = {
    "equipment", "services", "service", "facilities", "facility", "supplies",
    "other", "misc", "miscellaneous", "general", "not r and d", "n/a", "na",
    "buildings", "structures",  # bare forms too broad; qualified forms survive
    "construction",             # collides with SECTORS['construction'] (NAICS)
    "it services",              # collides with SECTORS
    "engineering", "security services", "janitorial", "landscaping",
}
MIN_ALIAS_LEN = 4
MAX_ALIAS_TOKENS = 4


def fetch_rows() -> list[tuple[str, str]]:
    token = os.popen(
        "doppler secrets get QUERY_SIDECAR_TOKEN -p core-x -c prd --plain"
    ).read().strip()
    body = json.dumps({
        "sql": "SELECT psc_code, psc_name FROM v_psc_names WHERE is_active",
        "limit": 50000}).encode()
    req = urllib.request.Request(
        SIDECAR_URL, data=body, method="POST",
        headers={"authorization": f"Bearer {token}",
                 "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        rows = json.loads(resp.read())["rows"]
    return [(r[0], r[1]) for r in rows]


def verb_for(code: str) -> str | None:
    for pat, verb in FAMILY_VERBS:
        if re.match(pat, code):
            return verb
    return None


def noun_phrase(title: str) -> str:
    t = title.strip()
    for pat in STRIP_PREFIXES:
        t2 = re.sub(pat, "", t, flags=re.I).strip(" -–:.,")
        if t2 != t:
            t = t2
            break
    return t


def aliases(noun: str) -> list[str]:
    """Split a multi-noun phrase into individual lowercase aliases."""
    noun = noun.lower()
    noun = re.sub(r"\(.*?\)", " ", noun)            # drop parentheticals
    parts = re.split(r"[/,;]| and | & ", noun)
    out = []
    for p in parts:
        p = re.sub(r"\s+", " ", p).strip(" -–:.")
        if len(p) < MIN_ALIAS_LEN or p in NOUN_BLACKLIST:
            continue
        if len(p.split()) > MAX_ALIAS_TOKENS:
            continue
        out.append(p)
    # head-noun sub-alias: "airport runways" also binds bare "runways"
    for p2 in list(out):
        words = p2.split()
        if len(words) >= 2:
            head = words[-1]
            if len(head) >= MIN_ALIAS_LEN and head not in NOUN_BLACKLIST \
                    and head.endswith("s"):
                out.append(head)
    # the full (cleaned) phrase is also an alias when it is short enough
    full = re.sub(r"[/,;]", " ", noun)
    full = re.sub(r"\s+", " ", full).strip(" -–:.")
    if MIN_ALIAS_LEN <= len(full) and len(full.split()) <= MAX_ALIAS_TOKENS \
            and full not in NOUN_BLACKLIST:
        out.append(full)
    return sorted(set(out))


def main() -> None:
    rows = fetch_rows()
    nouns: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    skipped = []
    for code, title in rows:
        verb = verb_for(code)
        if verb is None:
            skipped.append((code, title))
            continue
        for al in aliases(noun_phrase(title)):
            nouns[al][verb].add(code)

    print(f"codes: {len(rows)}, aliases: {len(nouns)}, unmapped: {len(skipped)}")
    for c, t in skipped[:10]:
        print("  unmapped:", c, t[:60])

    lines = [
        '"""FROZEN work-language vocabulary — generated by',
        'scripts/gen_psc_work_language.py from the live psc_reference.',
        'Do not hand-edit rows; regenerate, review the diff, re-pin tests."""',
        "",
        "WORK_VERBS = {",
    ]
    for k, v in sorted(VERB_SYNONYMS.items()):
        lines.append(f"    {k!r}: {v!r},")
    lines.append("}")
    lines.append("")
    lines.append("WORK_NOUNS = {")
    for al in sorted(nouns):
        inner = ", ".join(
            f"{vb!r}: {sorted(cs)!r}" for vb, cs in sorted(nouns[al].items()))
        lines.append(f"    {al!r}: {{{inner}}},")
    lines.append("}")
    out = "apps/catalyst_api/src/psc_work_language.py"
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {out}: {len(nouns)} noun aliases")


if __name__ == "__main__":
    main()
