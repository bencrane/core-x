"""naics_psc_labor_profile — the L2 combo→labor-category cache (LLM-grounded, vocabulary-constrained).

One profile per distinct SERVICE (naics_code, psc_code) combo on govcon_active_awards (8,690
combos, 692 NAICS, 97,607 service award rows): which labor categories a winner of that combo
must staff, selected by an LLM from TWO CONSTRAINED vocabularies it physically cannot escape
(json_schema enums): (a) the 830 detailed SOC occupations (OEWS national), (b) the 502 SCA labor
categories (dol_sca_occupations). The prompt carries the deterministic OEWS staffing-pattern
candidates for the combo's NAICS (top-40 by pct_total at the resolved ladder level) so selections
are grounded in real employment structure; an ``off_pattern`` flag lets orthogonal combos (e.g. a
custodial PSC won by an IT-NAICS firm) reach the full SOC vocabulary instead of forcing garbage.

WHY COMBO GRAIN: the LLM runs once per (NAICS × PSC) pair — thousands — and every award/company
joins to the cache for free. Mirrors naics_psc_deliverable (the prior-art LLM combo cache).

STAGES (all fail-closed, resume via the frozen manifest — never re-derived from live data):
    manifest  derive worklist + references + OEWS/EP candidates → _naics_psc_labor_profile_manifest
    submit    build Batches-API requests from the manifest (system prefix cached; enums pinned)
    retrieve  poll batch → validate → materialize the two output datasets + indexes + ledger

    doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=7' --with 'pyarrow>=17' \\
      --with 'duckdb>=1.5,<2' --with 'anthropic>=0.100' --with 'psycopg[binary]>=3.2' \\
      python3 pipelines/reference/materialize_naics_psc_labor_profile.py manifest
    ... submit                       # prints batch_id
    ... retrieve --batch-ids <id>    # poll + materialize

OUTPUT (Lance v2.1, active/):
    naics_psc_labor_profile             8,690 = one row per combo: titles, is_labor_play,
        work_summary, n_awards, total_dollars_obligated, resolution_level, oews_industry_code,
        provenance. BTREE naics_code, psc_code; BITMAP is_labor_play, resolution_level, confidence.
    naics_psc_labor_profile_categories  one row per (combo, rank): soc_code/soc_title/off_pattern,
        sca_code/sca_title, role_class, confidence + deterministic wage/growth enrichment
        (pct_of_industry, a_median from the resolved OEWS row, EP 2024→34 growth). is_labor_play=
        false combos emit ONE placeholder row (null codes) so combo reconciliation stays 8,690.
        BTREE naics_code, psc_code, soc_code, sca_code; BITMAP role_class, confidence, off_pattern.

DETERMINISTIC LADDER (adversarially verified live):
    OEWS national = area='99' (the only area_type='1'), plain i_group (never ', ownership'
    variants), o_group='detailed', NO own_code filter (each naics carries exactly one own_code —
    filtering '5' would drop education/hospitals/postal/government industries). Ladder for award
    NAICS n: 6d(n) → 5d(n[:5]+'0') → 4d(n[:4]+'00') → 4d A-code combo (static 14-entry map, e.g.
    325x→3250A1) → 3d(n[:3]+'000') → sector (31-33/44-45/48-49 hyphen ranges; '92'→'99') →
    cross-industry '000000' terminal. Award NAICS is vintage-mixed: a static 2017→2022 remap
    covers the Information/retail restructures (511x→513x, 515x→516x, 519130→516210,
    4431xx→449210) where even the 3-digit prefix moved; everything else degrades gracefully
    inside the ladder. Candidates: top-40 by pct_total (TRY_CAST DESC NULLS LAST, occ_code ASC;
    unparseable pct_total excluded; a_median sentinels '#'=topcode '*'=n/a annotated).
    EP growth: bls_ep_industry_occupation_matrix, occupation_type='Line Item' (capital I),
    combined rows only (industry_code=naics_code; 611xxx/622xxx ownership splits fall to 3d).

LLM (house batch idiom — classify_sub_self_reported_tags.py): Anthropic Message Batches API
(50% price), model claude-sonnet-4-6, temperature=0, structured output via output_config
json_schema with additionalProperties:false and the SOC/SCA vocabularies as string ENUMS.
System prompt (byte-identical across calls, cache_control ephemeral): task rules + the 502-row
SCA vocabulary (160-char first-sentence excerpts) + the 830-row detailed-SOC vocabulary.
User payload per call: one NAICS (title/description/candidates) × ≤20 PSCs. custom_id =
'{naics}:{chunk:02d}' resolved strictly against the manifest.

PROVENANCE (house convention per naics_psc_deliverable): prompt_version, model_id, generated_at,
source_vintage + candidate provenance (resolution_level, oews_industry_code, candidates_sha256).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

BUCKET = "data-sink"
A = f"s3://{BUCKET}/active/"
MANIFEST_URI = os.environ.get("NPLP_MANIFEST_URI", A + "_naics_psc_labor_profile_manifest/")
PROFILE_URI = os.environ.get("NPLP_PROFILE_URI", A + "naics_psc_labor_profile/")
CATEGORIES_URI = os.environ.get("NPLP_CATEGORIES_URI", A + "naics_psc_labor_profile_categories/")

MODEL = "claude-sonnet-4-6"
# model_id stamped on rows classified by IN-SESSION workflow subagents (--agent-results lane —
# the naics_psc_deliverable house precedent; session-covered, zero API billing).
AGENT_MODEL_ID = "claude-opus-4-8:in-session"
PROMPT_VERSION = "labor_profile_v1"
SOURCE_VINTAGE = "govcon_active_awards_2026-07-01"
DATA_STORAGE_VERSION = "2.1"
PSC_PER_CALL = 20
TOP_N_CANDIDATES = 40
MAX_CATEGORIES = 10

# 2017→2022 NAICS remap for RESOLUTION ONLY (row key keeps the award's original code).
# Only families where even the 3-digit prefix moved (Information / retail restructures) —
# everything else degrades gracefully inside the ladder.
NAICS_2022_REMAP = {
    "511110": "513110", "511120": "513120", "511130": "513130", "511140": "513140",
    "511191": "513191", "511199": "513199", "511210": "513210",
    "515111": "516110", "515112": "516110", "515120": "516120", "515210": "516210",
    "519130": "516210",
    "443120": "449210", "443141": "449210", "443142": "449210",
}

# OEWS 4-digit letter-suffix combination codes (verified live: 14 exist). real-4d-prefix → A-code.
OEWS_4D_COMBO = {
    "3251": "3250A1", "3252": "3250A1", "3253": "3250A1", "3259": "3250A1",
    "3255": "3250A2", "3256": "3250A2",
    "3321": "3320A1", "3322": "3320A1", "3325": "3320A1", "3326": "3320A1", "3329": "3320A1",
    "3323": "3320A2", "3324": "3320A2",
    "3331": "3330A1", "3332": "3330A1", "3334": "3330A1", "3339": "3330A1",
    "3371": "3370A1", "3372": "3370A1", "3379": "3370A1",
    "4231": "4230A1", "4232": "4230A1", "4233": "4230A1", "4234": "4230A1", "4235": "4230A1",
    "4236": "4230A1", "4237": "4230A1", "4238": "4230A1", "4239": "4230A1",
    "4241": "4240A1", "4242": "4240A1", "4243": "4240A1", "4244": "4240A1", "4245": "4240A1",
    "4246": "4240A2", "4247": "4240A2", "4248": "4240A2", "4249": "4240A3",
    "4451": "4450A1", "4452": "4450A1", "4453": "4450A1",
    "4591": "4590A1", "4592": "4590A1", "4593": "4590A1", "4594": "4590A1", "4595": "4590A1", "4599": "4590A1",
    "5221": "5220A1", "5222": "5220A1", "5223": "5220A1",
    "5321": "5320A1", "5322": "5320A1", "5323": "5320A1", "5324": "5320A1",
}

SECTOR_MAP = {"31": "31-33", "32": "31-33", "33": "31-33", "44": "44-45", "45": "44-45",
              "48": "48-49", "49": "48-49", "92": "99"}

ROLE_CLASSES = ["core_deliverable", "support", "overhead"]
CONFIDENCES = ["high", "medium", "low"]

SYSTEM_RULES = """You classify the labor demand implied by U.S. federal contract awards. Each task gives you ONE
NAICS industry (with its real BLS-OEWS occupational staffing pattern as CANDIDATES) and a list of
PSC service codes awarded under that NAICS. For EACH PSC, decide the labor categories a winning
contractor must put on payroll to deliver that PSC's service, in that industry context.

Rules — follow exactly:
1. soc_code MUST come from the DETAILED SOC VOCABULARY below. Prefer the industry CANDIDATES given
   in the task (they are the real staffing pattern); set off_pattern=true ONLY when the PSC's
   deliverable genuinely requires an occupation outside the candidates (e.g. a custodial PSC under
   an IT NAICS). Never force an irrelevant candidate just because it is listed.
2. sca_code MUST come from the SCA VOCABULARY below, or null when no SCA category fits (SCA skews
   blue-collar/technician/admin — many professional SOCs have no SCA equivalent; null is correct).
3. Rank categories by centrality to DELIVERING the PSC (array order = rank 1..N, max 10). role_class:
   core_deliverable = the billable labor that IS the service; support = enabling technical/field
   labor; overhead = PM/admin/QA the contract still forces on payroll. List overhead only when the
   contract scale plainly requires it.
4. is_labor_play=false when the PSC under this NAICS is delivered with negligible dedicated labor
   (pure resale/lease/license pass-through). Then categories MUST be [].
5. work_summary: <=20 words, concrete, what the winner actually does for this PSC in this industry.
6. confidence: high = obvious mapping; medium = reasonable inference; low = thin signal.
Select ONLY codes from the vocabularies. Output must satisfy the JSON schema exactly."""


# ── shared plumbing ─────────────────────────────────────────────────────────────────
def _so() -> dict:
    endpoint = os.environ.get("R2_ENDPOINT")
    if not endpoint and os.environ.get("R2_ACCOUNT_ID"):
        endpoint = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (Doppler core-x/prd).")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _ds(name_or_uri: str):
    import lance
    uri = name_or_uri if name_or_uri.startswith("s3://") else A + name_or_uri + "/"
    return lance.dataset(uri, storage_options=_so())


def _write(tbl, uri: str, btree: list[str], bitmap: list[str]) -> list[str]:
    import lance
    lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        storage_options=_so())
    ds = lance.dataset(uri, storage_options=_so())
    present = {f.name for f in ds.schema}
    built = []
    for kind, cols in (("BTREE", btree), ("BITMAP", bitmap)):
        for c in cols:
            if c not in present:
                continue
            try:
                ds.create_scalar_index(c, index_type=kind, replace=True)
                built.append(f"{kind}:{c}")
            except Exception as exc:  # noqa: BLE001
                print(f"  WARN {kind} {c}: {exc}")
    return built


def _record_run(stats: dict) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: no HQX_DB_URL_POOLED; skipping ops write.")
        return
    try:
        import psycopg
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ops.naics_psc_labor_profile_runs (
                    id bigserial PRIMARY KEY, stage text NOT NULL, status text NOT NULL,
                    combos int, calls int, categories int, model_id text, prompt_version text,
                    stats jsonb, error text, started_at timestamptz, completed_at timestamptz,
                    recorded_at timestamptz NOT NULL DEFAULT now())""")
            cur.execute("""
                INSERT INTO ops.naics_psc_labor_profile_runs
                    (stage,status,combos,calls,categories,model_id,prompt_version,stats,error,
                     started_at,completed_at)
                VALUES (%(stage)s,%(status)s,%(combos)s,%(calls)s,%(categories)s,%(model_id)s,
                     %(prompt_version)s,%(stats)s,%(error)s,%(started_at)s,%(completed_at)s)""",
                {**stats, "stats": json.dumps(stats.get("stats", {}))})
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops write failed: {exc}")


# ── deterministic layer: ladder + candidates ────────────────────────────────────────
def _ladder(n: str, oews_keys: set[tuple[str, str]]) -> tuple[str, str] | None:
    """Return (i_group, oews_naics) for award NAICS n, or None → cross-industry terminal."""
    n = NAICS_2022_REMAP.get(n, n)
    rungs = [("6-digit", n), ("5-digit", n[:5] + "0"), ("4-digit", n[:4] + "00")]
    combo = OEWS_4D_COMBO.get(n[:4])
    if combo:
        rungs.append(("4-digit", combo))
    rungs.append(("3-digit", n[:3] + "000"))
    rungs.append(("sector", SECTOR_MAP.get(n[:2], n[:2])))
    for ig, key in rungs:
        if (ig, key) in oews_keys:
            return ig, key
    return None


def build_manifest() -> dict:
    import duckdb
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")

    # worklist: distinct service combos + award stats
    g = _ds("govcon_active_awards").to_table(
        columns=["naics_code", "naics_description", "psc_code", "psc_description",
                 "psc_is_service", "total_dollars_obligated"])
    con.register("g", g)
    combos = con.execute("""
        SELECT naics_code, psc_code,
               any_value(naics_description) AS award_naics_desc,
               any_value(psc_description)   AS award_psc_desc,
               count(*)                      AS n_awards,
               sum(total_dollars_obligated)  AS total_dollars_obligated
        FROM g WHERE psc_is_service AND naics_code IS NOT NULL AND psc_code IS NOT NULL
        GROUP BY 1,2""").fetchall()
    print(f"worklist: {len(combos):,} service combos")

    # references (dedup psc_reference to 1 row/code; naics_reference title fallback = award desc)
    nref_t = _ds("naics_reference").to_table()
    con.register("nref", nref_t)
    ncols = [f.name for f in nref_t.schema]
    ntitle = next(c for c in ("title", "naics_title", "description") if c in ncols)
    ndesc = next((c for c in ("description", "naics_description") if c in ncols and c != ntitle), ntitle)
    naics_ref = {r[0]: (r[1], r[2]) for r in con.execute(
        f"SELECT naics_code, any_value({ntitle}), any_value({ndesc}) FROM nref GROUP BY 1").fetchall()}

    pref_t = _ds("psc_reference").to_table()
    con.register("pref", pref_t)
    pcols = [f.name for f in pref_t.schema]
    ptitle = next(c for c in ("psc_name", "title", "psc_title", "description") if c in pcols)
    order = []
    if "is_active" in pcols:
        order.append("is_active DESC")
    if "end_date" in pcols:
        order.append("end_date DESC NULLS LAST")
    psc_ref = {r[0]: r[1] for r in con.execute(
        f"SELECT psc_code, {ptitle} FROM (SELECT *, row_number() OVER (PARTITION BY psc_code"
        f" ORDER BY {', '.join(order) or '1'}) rn FROM pref) WHERE rn=1").fetchall()}

    # prior art: naics_psc_deliverable
    dl = _ds("naics_psc_deliverable").to_table(
        columns=["naics_code", "psc_code", "what_was_done", "work_type"])
    con.register("dl", dl)
    prior = {(r[0], r[1]): (r[2], r[3]) for r in con.execute(
        "SELECT naics_code, psc_code, what_was_done, work_type FROM dl").fetchall()}

    # OEWS national detailed rows (plain i_groups, no own_code filter)
    ow = _ds("bls_oews_2025").to_table(
        columns=["area", "i_group", "naics", "naics_title", "o_group",
                 "occ_code", "occ_title", "pct_total", "a_median", "tot_emp"])
    con.register("ow", ow)
    con.execute("""
        CREATE TEMP TABLE oews AS
        SELECT i_group, naics, any_value(naics_title) naics_title, occ_code,
               any_value(occ_title) occ_title,
               any_value(TRY_CAST(pct_total AS DOUBLE)) pct,
               any_value(a_median) a_median
        FROM ow
        WHERE area='99' AND o_group='detailed'
          AND i_group IN ('sector','3-digit','4-digit','5-digit','6-digit','cross-industry')
        GROUP BY i_group, naics, occ_code""")
    oews_keys = {(r[0], r[1]) for r in con.execute(
        "SELECT DISTINCT i_group, naics FROM oews").fetchall()}
    ind_title = {(r[0], r[1]): r[2] for r in con.execute(
        "SELECT i_group, naics, any_value(naics_title) FROM oews GROUP BY 1,2").fetchall()}

    # EP growth: Line Item + combined rows only
    ep = _ds("bls_ep_industry_occupation_matrix_2024_2034").to_table(
        columns=["industry_code", "naics_code", "occupation_code", "occupation_type",
                 "emp_pct_change_2024_2034"])
    con.register("ep", ep)
    ep_growth = {(r[0], r[1]): r[2] for r in con.execute("""
        SELECT naics_code, occupation_code, any_value(emp_pct_change_2024_2034)
        FROM ep WHERE occupation_type='Line Item' AND industry_code=naics_code
              AND naics_code IS NOT NULL
        GROUP BY 1,2""").fetchall()}

    def candidates_for(oews_key: tuple[str, str] | None) -> list[dict]:
        ig, key = oews_key if oews_key else ("cross-industry", "000000")
        rows = con.execute("""
            SELECT occ_code, occ_title, pct, a_median FROM oews
            WHERE i_group=? AND naics=? AND pct IS NOT NULL
            ORDER BY pct DESC, occ_code ASC LIMIT ?""", [ig, key, TOP_N_CANDIDATES]).fetchall()
        out = []
        ep_key = key if key.isdigit() else None
        for occ, title, pct, med in rows:
            growth = ep_growth.get((ep_key, occ)) if ep_key else None
            out.append({"soc": occ, "title": title, "pct": round(pct, 2),
                        "a_median": med, "growth_2034_pct": growth})
        return out

    # assemble manifest rows: group combos by NAICS, chunk PSCs
    by_naics: dict[str, list] = {}
    for naics, psc, ndesc_a, pdesc_a, n_aw, dollars in combos:
        by_naics.setdefault(naics, []).append((psc, pdesc_a, ndesc_a, n_aw, dollars))
    rows = []
    n_calls = 0
    for naics in sorted(by_naics):
        pscs = sorted(by_naics[naics], key=lambda t: t[0])
        rung = _ladder(naics, oews_keys)
        ig, key = rung if rung else ("cross-industry", "000000")
        cands = candidates_for(rung)
        cand_sha = hashlib.sha256(json.dumps(cands, sort_keys=True).encode()).hexdigest()
        title, desc = naics_ref.get(naics, (None, None))
        if not title:
            title = pscs[0][2]  # award-row naics_description fallback (pre-2022 codes)
        for ci in range(0, len(pscs), PSC_PER_CALL):
            chunk = pscs[ci // PSC_PER_CALL * PSC_PER_CALL: ci + PSC_PER_CALL]
            call_id = f"{naics}:{ci // PSC_PER_CALL:02d}"
            n_calls += 1
            for psc, pdesc_a, _nd, n_aw, dollars in chunk:
                wwd = prior.get((naics, psc))
                rows.append({
                    "call_id": call_id, "naics_code": naics, "psc_code": psc,
                    "naics_title": title, "naics_description": (desc or "")[:300] or None,
                    "psc_title": psc_ref.get(psc) or pdesc_a,
                    "n_awards": int(n_aw), "total_dollars_obligated": float(dollars or 0),
                    "resolution_level": ig, "oews_industry_code": key,
                    "oews_industry_title": ind_title.get((ig, key)),
                    "candidates_json": json.dumps(cands, sort_keys=True),
                    "candidates_sha256": cand_sha,
                    "prior_what_was_done": wwd[0] if wwd else None,
                    "prior_work_type": wwd[1] if wwd else None,
                })
    import pyarrow as pa
    tbl = pa.Table.from_pylist(rows)
    built = _write(tbl, MANIFEST_URI, ["naics_code", "psc_code", "call_id"], ["resolution_level"])
    lvl = {}
    for r in rows:
        lvl[r["resolution_level"]] = lvl.get(r["resolution_level"], 0) + 1
    print(f"manifest: {len(rows):,} combos, {len(by_naics)} naics, {n_calls} calls -> {MANIFEST_URI}")
    print(f"resolution levels: {lvl}")
    _record_run({"stage": "manifest", "status": "success", "combos": len(rows), "calls": n_calls,
                 "categories": None, "model_id": MODEL, "prompt_version": PROMPT_VERSION,
                 "stats": {"levels": lvl, "indexes": built}, "error": None,
                 "started_at": started, "completed_at": dt.datetime.now(dt.timezone.utc)})
    return {"combos": len(rows), "calls": n_calls}


# ── vocabularies (system prefix + schema enums) ─────────────────────────────────────
def _vocabularies() -> tuple[list[str], str, list[str], str, dict, dict]:
    import duckdb
    con = duckdb.connect()
    sca_t = _ds("dol_sca_occupations").to_table(
        columns=["occupation_code", "occupation_title", "occupation_definition", "entry_type"])
    con.register("sca", sca_t)
    sca_rows = con.execute("""
        SELECT occupation_code, occupation_title, occupation_definition FROM sca
        WHERE entry_type != 'family' ORDER BY occupation_code""").fetchall()
    sca_lines, sca_titles = [], {}
    for code, title, definition in sca_rows:
        first = re.split(r"(?<=[.!?])\s", re.sub(r"\s+", " ", (definition or "").strip()))[0][:160]
        sca_lines.append(f"{code} | {title} | {first}")
        sca_titles[code] = title
    ow = _ds("bls_oews_2025").to_table(columns=["area", "o_group", "occ_code", "occ_title"])
    con.register("ow", ow)
    soc_rows = con.execute("""
        SELECT occ_code, any_value(occ_title) FROM ow
        WHERE area='99' AND o_group='detailed' AND occ_code != '00-0000'
        GROUP BY 1 ORDER BY 1""").fetchall()
    soc_lines = [f"{c} | {t}" for c, t in soc_rows]
    soc_titles = dict(soc_rows)
    return ([r[0] for r in sca_rows], "\n".join(sca_lines),
            [r[0] for r in soc_rows], "\n".join(soc_lines), sca_titles, soc_titles)


def _output_schema(sca_codes: list[str], soc_codes: list[str]) -> dict:
    return {
        "type": "object", "additionalProperties": False, "required": ["results"],
        "properties": {"results": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["psc_code", "is_labor_play", "work_summary", "categories"],
            "properties": {
                "psc_code": {"type": "string"},
                "is_labor_play": {"type": "boolean"},
                "work_summary": {"type": "string"},
                # NOTE: maxItems is NOT supported by output_config json_schema (verified live —
                # invalid_request_error). The <=MAX_CATEGORIES cap is prompt-instructed and
                # enforced client-side at retrieve.
                "categories": {"type": "array", "items": {
                    "type": "object", "additionalProperties": False,
                    "required": ["soc_code", "off_pattern", "sca_code", "role_class", "confidence"],
                    "properties": {
                        "soc_code": {"type": "string", "enum": soc_codes},
                        "off_pattern": {"type": "boolean"},
                        "sca_code": {"anyOf": [{"type": "string", "enum": sca_codes},
                                               {"type": "null"}]},
                        "role_class": {"type": "string", "enum": ROLE_CLASSES},
                        "confidence": {"type": "string", "enum": CONFIDENCES},
                    }}}}}}}}


def _build_requests() -> tuple[list[dict], dict]:
    """Manifest → Batches-API requests. Returns (requests, manifest_by_call)."""
    sca_codes, sca_block, soc_codes, soc_block, _, _ = _vocabularies()
    system = [{"type": "text",
               "text": (SYSTEM_RULES
                        + "\n\n=== SCA VOCABULARY (code | title | definition excerpt) ===\n" + sca_block
                        + "\n\n=== DETAILED SOC VOCABULARY (code | title) ===\n" + soc_block),
               "cache_control": {"type": "ephemeral"}}]
    schema = _output_schema(sca_codes, soc_codes)

    man = _ds(MANIFEST_URI).to_table().to_pylist()
    by_call: dict[str, list[dict]] = {}
    for r in man:
        by_call.setdefault(r["call_id"], []).append(r)

    requests = []
    for call_id in sorted(by_call):
        rows = sorted(by_call[call_id], key=lambda r: r["psc_code"])
        h = rows[0]
        cands = json.loads(h["candidates_json"])
        cand_lines = []
        for c in cands:
            med = c["a_median"]
            med_s = ("topcoded>=239200" if med == "#" else "n/a" if med in ("*", None) else med)
            g = c.get("growth_2034_pct")
            cand_lines.append(f"  {c['soc']} {c['title']} — {c['pct']}% of industry emp, "
                              f"median ${med_s}" + (f", {g:+.1f}% 2024-34" if g is not None else ""))
        psc_lines = []
        for r in rows:
            extra = f" [prior: {r['prior_what_was_done']} ({r['prior_work_type']})]" \
                if r["prior_what_was_done"] else ""
            psc_lines.append(f"  {r['psc_code']} — {r['psc_title']}{extra}")
        user = (f"NAICS {h['naics_code']} — {h['naics_title']}\n"
                + (f"{h['naics_description']}\n" if h["naics_description"] else "")
                + f"(staffing pattern resolved at OEWS {h['resolution_level']} "
                + f"{h['oews_industry_code']} {h['oews_industry_title'] or ''})\n\n"
                + "INDUSTRY STAFFING-PATTERN CANDIDATES (prefer these):\n" + "\n".join(cand_lines)
                + "\n\nCLASSIFY EACH PSC (one result per PSC, in order):\n" + "\n".join(psc_lines))
        requests.append({"custom_id": call_id.replace(":", "_"), "params": {
            "model": MODEL, "max_tokens": 8000, "temperature": 0,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "output_config": {"format": {"type": "json_schema", "schema": schema}},
        }})
    return requests, by_call


def submit(missing_from: list[str] | None = None) -> None:
    """Submit the classification batch. With --missing-from <batch_ids>, resubmit ONLY the
    calls that did not succeed in those batches (e.g. after a credit-exhaustion partial)."""
    import anthropic
    client = anthropic.Anthropic()
    requests, by_call = _build_requests()
    if missing_from:
        done = set()
        for bid in missing_from:
            for res in client.messages.batches.results(bid):
                if res.result.type == "succeeded":
                    done.add(res.custom_id)
        requests = [r for r in requests if r["custom_id"] not in done]
        print(f"missing-only resubmit: {len(done)} calls already succeeded, {len(requests)} to go")
        if not requests:
            print("nothing to submit — all calls already succeeded")
            return
    print(f"submitting {len(requests)} requests ({len(by_call)} calls) to Batches API, model={MODEL}")
    batch = client.messages.batches.create(requests=requests)
    print(json.dumps({"batch_id": batch.id, "n_requests": len(requests),
                      "model": MODEL, "prompt_version": PROMPT_VERSION}))


def retrieve(batch_ids: list[str], poll_s: int = 60, agent_results: str | None = None) -> None:
    import anthropic
    import duckdb
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    client = anthropic.Anthropic()
    sca_codes, _, soc_codes, _, sca_titles, soc_titles = _vocabularies()
    sca_set, soc_set = set(sca_codes), set(soc_codes)

    man = _ds(MANIFEST_URI).to_table().to_pylist()
    by_call: dict[str, dict[str, dict]] = {}
    for r in man:
        by_call.setdefault(r["call_id"].replace(":", "_"), {})[r["psc_code"]] = r
    expected_combos = len(man)

    # wage/growth lookups for deterministic enrichment
    con = duckdb.connect()
    ow = _ds("bls_oews_2025").to_table(
        columns=["area", "i_group", "naics", "o_group", "occ_code", "pct_total", "a_median"])
    con.register("ow", ow)
    con.execute("""CREATE TEMP TABLE oew AS
        SELECT i_group, naics, occ_code, any_value(TRY_CAST(pct_total AS DOUBLE)) pct,
               any_value(a_median) a_median
        FROM ow WHERE area='99' AND o_group='detailed' GROUP BY 1,2,3""")
    wage = {(r[0], r[1], r[2]): (r[3], r[4]) for r in con.execute(
        "SELECT i_group, naics, occ_code, pct, a_median FROM oew").fetchall()}
    ep = _ds("bls_ep_industry_occupation_matrix_2024_2034").to_table(
        columns=["industry_code", "naics_code", "occupation_code", "occupation_type",
                 "emp_pct_change_2024_2034"])
    con.register("ep", ep)
    growth = {(r[0], r[1]): r[2] for r in con.execute("""
        SELECT naics_code, occupation_code, any_value(emp_pct_change_2024_2034) FROM ep
        WHERE occupation_type='Line Item' AND industry_code=naics_code GROUP BY 1,2""").fetchall()}

    # poll all batches
    for bid in batch_ids:
        while True:
            b = client.messages.batches.retrieve(bid)
            rc = b.request_counts
            print(f"[{bid}] {b.processing_status} processing={rc.processing} "
                  f"succeeded={rc.succeeded} errored={rc.errored} canceled={rc.canceled} "
                  f"expired={rc.expired}", flush=True)
            if b.processing_status == "ended":
                break
            time.sleep(poll_s)

    now = dt.datetime.now(dt.timezone.utc)
    gen_at = now.isoformat()
    prof_rows, cat_rows = [], []
    failures: list[str] = []
    seen_combos: set[tuple[str, str]] = set()

    def prov(m: dict, engine: str) -> dict:
        return {"resolution_level": m["resolution_level"],
                "oews_industry_code": m["oews_industry_code"],
                "candidates_sha256": m["candidates_sha256"],
                "prompt_version": PROMPT_VERSION, "model_id": engine,
                "generated_at": gen_at, "source_vintage": SOURCE_VINTAGE}

    # ── collect one results-list per call: batches ∪ optional in-session agent file ──
    # payloads[cid] = (results_list, engine_model_id). First success wins across batches;
    # the agent file (produced by in-session workflow subagents — the naics_psc_deliverable
    # house precedent, zero API billing) fills calls no batch succeeded on.
    payloads: dict[str, tuple[list, str]] = {}
    for bid in batch_ids:
        for res in client.messages.batches.results(bid):
            cid = res.custom_id
            if cid not in by_call:
                failures.append(f"{cid}: not in manifest")
                continue
            if res.result.type != "succeeded" or cid in payloads:
                continue
            msg = res.result.message
            text = next((blk.text for blk in msg.content if blk.type == "text"), "")
            try:
                payloads[cid] = (json.loads(text).get("results", []), MODEL)
            except json.JSONDecodeError:
                failures.append(f"{cid}: JSON parse failure")
    if agent_results:
        n_agent = 0
        for entry in json.load(open(agent_results)):
            cid = entry.get("custom_id")
            if cid in by_call and cid not in payloads:
                payloads[cid] = (entry.get("results", []), AGENT_MODEL_ID)
                n_agent += 1
        print(f"agent results merged: {n_agent} calls from {agent_results}")
    for cid in by_call:
        if cid not in payloads:
            failures.append(f"{cid}: no succeeded result in any source")

    for cid in sorted(payloads):
        results, engine = payloads[cid]
        call_man = by_call[cid]
        got_pscs = set()
        for r in results:
            psc = str(r.get("psc_code", "")).strip()
            m = call_man.get(psc)
            if m is None:
                failures.append(f"{cid}: unknown psc {psc!r} in response")
                continue
            got_pscs.add(psc)
            is_play = bool(r.get("is_labor_play"))
            cats = (r.get("categories") or [])[:MAX_CATEGORIES]
            if is_play and not cats:
                failures.append(f"{cid}/{psc}: is_labor_play=true but 0 categories")
                continue
            key = (m["naics_code"], psc)
            seen_combos.add(key)
            confs = [c.get("confidence") for c in cats if c.get("confidence") in CONFIDENCES]
            prof_rows.append({
                "naics_code": m["naics_code"], "psc_code": psc,
                "naics_title": m["naics_title"], "psc_title": m["psc_title"],
                "is_labor_play": is_play,
                "work_summary": (r.get("work_summary") or "").strip()[:200] or None,
                "n_categories": len(cats) if is_play else 0,
                "top_confidence": confs[0] if confs else None,
                "n_awards": m["n_awards"],
                "total_dollars_obligated": m["total_dollars_obligated"],
                "oews_industry_title": m["oews_industry_title"], **prov(m, engine)})
            if not is_play:
                cat_rows.append({
                    "naics_code": m["naics_code"], "psc_code": psc, "rank": None,
                    "soc_code": None, "soc_title": None, "off_pattern": None,
                    "sca_code": None, "sca_title": None, "role_class": None,
                    "confidence": None, "pct_of_industry": None, "a_median": None,
                    "ep_growth_2024_2034_pct": None, **prov(m, engine)})
                continue
            ig, key_i = m["resolution_level"], m["oews_industry_code"]
            for rank, c in enumerate(cats, 1):
                soc = c.get("soc_code")
                sca = c.get("sca_code")
                if soc not in soc_set:
                    failures.append(f"{cid}/{psc}: soc {soc!r} outside vocabulary")
                    continue
                if sca is not None and sca not in sca_set:
                    sca = None
                w = wage.get((ig, key_i, soc)) or wage.get(("cross-industry", "000000", soc))
                g = growth.get((key_i, soc)) if key_i.isdigit() else None
                cat_rows.append({
                    "naics_code": m["naics_code"], "psc_code": psc, "rank": rank,
                    "soc_code": soc, "soc_title": soc_titles.get(soc),
                    "off_pattern": bool(c.get("off_pattern")),
                    "sca_code": sca, "sca_title": sca_titles.get(sca) if sca else None,
                    "role_class": c.get("role_class"), "confidence": c.get("confidence"),
                    "pct_of_industry": w[0] if w else None,
                    "a_median": w[1] if w else None,
                    "ep_growth_2024_2034_pct": g, **prov(m, engine)})
        missing = set(call_man) - got_pscs
        for psc in missing:
            failures.append(f"{cid}: response missing psc {psc}")

    print(f"profiles={len(prof_rows):,} categories={len(cat_rows):,} "
          f"combos_seen={len(seen_combos):,}/{expected_combos:,} failures={len(failures)}")
    for f in failures[:25]:
        print("  FAIL:", f)

    # fail-closed reconciliation: every manifest combo must be present
    status = "success"
    if len(seen_combos) != expected_combos:
        status = "partial"
        print(f"RECONCILIATION GAP: {expected_combos - len(seen_combos)} combos missing — "
              "dataset NOT written. Re-submit the missing calls, then retrieve again with all batch ids.")
        _record_run({"stage": "retrieve", "status": "gap", "combos": len(seen_combos),
                     "calls": len(by_call), "categories": len(cat_rows), "model_id": MODEL,
                     "prompt_version": PROMPT_VERSION,
                     "stats": {"failures": failures[:200], "expected": expected_combos},
                     "error": f"{expected_combos - len(seen_combos)} combos missing",
                     "started_at": started, "completed_at": dt.datetime.now(dt.timezone.utc)})
        sys.exit(2)

    ts = pa.array([now] * len(prof_rows), pa.timestamp("us", tz="UTC"))
    prof_t = pa.Table.from_pylist(prof_rows).append_column("ingested_at", ts)
    built_p = _write(prof_t, PROFILE_URI, ["naics_code", "psc_code"],
                     ["is_labor_play", "resolution_level", "top_confidence"])
    ts2 = pa.array([now] * len(cat_rows), pa.timestamp("us", tz="UTC"))
    cat_t = pa.Table.from_pylist(cat_rows).append_column("ingested_at", ts2)
    built_c = _write(cat_t, CATEGORIES_URI, ["naics_code", "psc_code", "soc_code", "sca_code"],
                     ["role_class", "confidence", "off_pattern", "resolution_level"])
    print(f"wrote {len(prof_rows):,} -> {PROFILE_URI} ({built_p})")
    print(f"wrote {len(cat_rows):,} -> {CATEGORIES_URI} ({built_c})")
    _record_run({"stage": "retrieve", "status": status, "combos": len(seen_combos),
                 "calls": len(by_call), "categories": len(cat_rows), "model_id": MODEL,
                 "prompt_version": PROMPT_VERSION,
                 "stats": {"failures": failures[:200], "profile_indexes": built_p,
                           "category_indexes": built_c},
                 "error": None, "started_at": started,
                 "completed_at": dt.datetime.now(dt.timezone.utc)})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="naics_psc_labor_profile — L2 combo→labor-category cache.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("manifest", help="derive + freeze the worklist/candidates manifest")
    sp = sub.add_parser("submit", help="build + submit the Batches-API classification")
    sp.add_argument("--missing-from", help="comma-separated prior batch ids — resubmit ONLY calls "
                                           "that did not succeed in them")
    rp = sub.add_parser("retrieve", help="poll batch, validate, materialize datasets")
    rp.add_argument("--batch-ids", required=True, help="comma-separated batch ids")
    rp.add_argument("--poll-s", type=int, default=60)
    rp.add_argument("--agent-results", help="JSON array of {custom_id, results} from in-session "
                                            "workflow subagents (fills calls no batch succeeded on)")
    args = ap.parse_args(argv)
    if args.cmd == "manifest":
        build_manifest()
    elif args.cmd == "submit":
        submit([b.strip() for b in args.missing_from.split(",")] if args.missing_from else None)
    else:
        retrieve([b.strip() for b in args.batch_ids.split(",") if b.strip()], args.poll_s,
                 agent_results=args.agent_results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
