"""Serving worker — active_award_labor_demand: the CLEAN, scope-corroborated labor-demand
fact table for the staffing GTM UI. One row per (active award × demanded labor role),
kept ONLY when the role is corroborated by the award's OWN scope text.

WHY CORROBORATION (not raw labor_category_values): govcon_award_scope_requirements is built
from an LLM extraction over solicitation attachments that (a) makes context-blind string
false-positives (e.g. "truck driver" pulled from a tailgate-delivery clause; "security_guard"
pulled from "LIFTGATE SECURITY GUARD", a vehicle body part) and (b) FAN-OUT amplifies them: a
single solicitation resource maps to ~15.5k task-order awards via the manifest, so two bad
extractions on one GSA vehicle IDIQ flooded ~85% of all labor awards with bogus
[security_guard, truck_driver]. Self-reported confidence/validated do NOT catch this
(both were confidence=1.0, validated=true). The ONLY reliable discriminator is PER-AWARD
scope corroboration — does the role actually appear in THIS award's scope_summary /
prime_award_base_transaction_description / transaction_description. The fan-out-shared
evidence_quote is deliberately NOT used (it carries the contamination).

GRAIN   1 row per (contract_award_unique_key, labor_role), corroborated only.
SOURCES govcon_active_awards (active_current OR active_potential = future-dated end) ⨝
        govcon_award_scope_requirements (labor_category_values, scope_summary, clearance, headcount).
FILTER  role kept ⇔ ≥1 of the role's natural-language phrases occurs in any of the award's three
        per-award scope texts (lowercased substring). The two artifact roles use strict
        multi-word phrases only (never bare 'guard'/'driver'/'truck') so the fan-out cannot re-enter.
SoR     s3://data-sink/active/active_award_labor_demand/ (Lance v2.1; derived, snapshot-overwrite).
INDICES BTREE(contract_award_unique_key, recipient_uei, naics_code);
        BITMAP(labor_role, naics_sector, req_clearance_level_max, active_current, business_size, type_of_set_aside).

Run:    doppler run -p core-x -c prd -- uv run --no-project --with 'duckdb>=1.5' --with 'pylance>=7' \
          --with 'pyarrow>=17' python3 pipelines/serving/materialize_active_award_labor_demand.py --cmd build
        (… --cmd verify)
"""
from __future__ import annotations

import os

ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("ACTIVE_AWARD_LABOR_DEMAND_URI", f"{ACTIVE}/active_award_labor_demand/")
ACTIVE_AWARDS_URI = f"{ACTIVE}/govcon_active_awards/"
SCOPE_REQ_URI = f"{ACTIVE}/govcon_award_scope_requirements/"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000

BTREE_INDEXES = ["contract_award_unique_key", "recipient_uei", "naics_code"]
BITMAP_INDEXES = ["labor_role", "naics_sector", "req_clearance_level_max",
                  "active_current", "active_potential", "has_clearance",
                  "business_size", "type_of_set_aside"]

# role -> natural-language phrases for per-award scope corroboration. The two fan-out
# artifact roles (security_guard, truck_driver) use STRICT multi-word phrases only.
ROLE_PHRASES: dict[str, list[str]] = {
    "security_guard": ["security guard", "guard service", "security officer", "armed guard",
                       "unarmed guard", "security personnel", "detention officer",
                       "correctional officer", "court security", "physical security services"],
    "truck_driver": ["truck driver", "cdl", "commercial driver", "delivery driver",
                     "bus driver", "motor vehicle operator", "tractor trailer operator"],
    "project_manager": ["project manager", "project management"],
    "program_manager": ["program manager", "program management"],
    "instructor": ["instructor", "instruction", "training", "trainer", "curriculum", "facilitator"],
    "quality_control_manager": ["quality control", "quality assurance", "qc manager",
                                "quality manager", "qa/qc", "qcm"],
    "electrician": ["electrician", "electrical"],
    "safety_officer": ["safety officer", "safety manager", "site safety", "safety and health",
                       "ssho", "safety specialist"],
    "licensed_practical_nurse": ["licensed practical nurse", "lpn", "licensed vocational nurse", "lvn"],
    "medical_assistant": ["medical assistant", "nursing assistant", "certified nursing assistant",
                          "cna", "patient care", "medical support assistant"],
    "welder": ["welder", "welding"],
    "surveyor": ["surveyor", "surveying", "land survey"],
    "site_superintendent": ["superintendent", "site supervisor", "construction supervisor"],
    "painter": ["painter", "painting"],
    "janitor": ["janitor", "janitorial", "custodial"],
    "plumber": ["plumber", "plumbing"],
    "locksmith": ["locksmith", "rekey", "keying", "lockset"],
    "equipment_operator": ["equipment operator", "heavy equipment operator", "machine operator"],
    "mason": ["mason", "masonry", "bricklayer"],
    "custodian": ["custodian", "custodial", "janitorial"],
    "interpreter": ["interpreter", "interpretation", "linguist", "sign language"],
    "crane_operator": ["crane operator", "crane operation", "rigger", "rigging"],
    "dispatcher": ["dispatcher", "dispatch"],
    "carpenter": ["carpenter", "carpentry", "millwork"],
    "registered_nurse": ["registered nurse", "rn ", " rn,"],
    "translator": ["translator", "translation", "linguist"],
    "hvac_technician": ["hvac", "heating ventilation", "air conditioning", "refrigeration technician"],
    "general_laborer": ["general laborer", "laborer", "general labor"],
    "sheet_metal_worker": ["sheet metal"],
    "pipefitter": ["pipefitter", "pipe fitter", "pipefitting", "steamfitter"],
    "roofer": ["roofer", "roofing"],
    "millwright": ["millwright"],
    "pest_control_technician": ["pest control", "exterminat", "pest management"],
    "glazier": ["glazier", "glazing", "glasswork"],
    "heavy_equipment_operator": ["heavy equipment", "equipment operator"],
    "food_service_worker": ["food service", "food preparation", "dietary", "cook", "mess attendant"],
}
ROLE_DISPLAY: dict[str, str] = {
    "security_guard": "Security Guard", "truck_driver": "Truck Driver",
    "project_manager": "Project Manager", "program_manager": "Program Manager",
    "instructor": "Instructor", "quality_control_manager": "Quality Control Manager",
    "electrician": "Electrician", "safety_officer": "Safety Officer",
    "licensed_practical_nurse": "Licensed Practical Nurse (LPN)", "medical_assistant": "Medical Assistant",
    "welder": "Welder", "surveyor": "Surveyor", "site_superintendent": "Site Superintendent",
    "painter": "Painter", "janitor": "Janitor", "plumber": "Plumber", "locksmith": "Locksmith",
    "equipment_operator": "Equipment Operator", "mason": "Mason", "custodian": "Custodian",
    "interpreter": "Interpreter", "crane_operator": "Crane Operator", "dispatcher": "Dispatcher",
    "carpenter": "Carpenter", "registered_nurse": "Registered Nurse (RN)", "translator": "Translator",
    "hvac_technician": "HVAC Technician", "general_laborer": "General Laborer",
    "sheet_metal_worker": "Sheet Metal Worker", "pipefitter": "Pipefitter", "roofer": "Roofer",
    "millwright": "Millwright", "pest_control_technician": "Pest Control Technician",
    "glazier": "Glazier", "heavy_equipment_operator": "Heavy Equipment Operator",
    "food_service_worker": "Food Service Worker",
}

SECTOR_SQL = """CASE
   WHEN naics_code LIKE '5415%' THEN 'IT (5415)'
   WHEN naics_code LIKE '5413%' THEN 'Engineering/Architecture (5413)'
   WHEN naics_code LIKE '5416%' THEN 'Mgmt/Admin Consulting (5416)'
   WHEN naics_code LIKE '5417%' THEN 'R&D (5417)'
   WHEN naics_code LIKE '54%'   THEN 'Other Professional (54)'
   WHEN naics_code LIKE '62%'   THEN 'Health Care (62)'
   WHEN naics_code LIKE '561%' OR naics_code LIKE '562%' THEN 'Admin/Facilities (56)'
   WHEN naics_code LIKE '23%'   THEN 'Construction (23)'
   WHEN naics_code LIKE '48%' OR naics_code LIKE '49%' THEN 'Transportation/Logistics (48-49)'
   WHEN naics_code LIKE '811%'  THEN 'Repair/Maintenance (811)'
   ELSE 'Other' END"""


def _so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _role_values(mapping) -> str:
    """VALUES list (role, phrase) for the corroboration join."""
    rows = []
    for role, phrases in mapping.items():
        for p in phrases:
            rows.append(f"('{role}','{p.replace(chr(39), chr(39)*2)}')")
    return ",".join(rows)


def _display_values() -> str:
    return ",".join(f"('{r}','{d.replace(chr(39), chr(39)*2)}')" for r, d in ROLE_DISPLAY.items())


def build() -> dict:
    import datetime as dt
    import duckdb
    import lance
    import pyarrow as pa

    so = _so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=6; SET memory_limit='10GB';")
    os.makedirs("/tmp/aald_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/aald_spill';")

    con.register("aa_r", lance.dataset(ACTIVE_AWARDS_URI, storage_options=so).scanner(columns=[
        "contract_award_unique_key", "award_id_piid", "naics_code", "naics_description",
        "psc_code", "psc_description", "recipient_uei", "recipient_name", "recipient_parent_name",
        "business_size", "type_of_set_aside", "awarding_sub_agency_name",
        "active_current", "active_potential", "has_option_tail",
        "pop_current_end", "pop_potential_end", "pop_state_code", "pop_city", "pop_country_code",
        "current_total_value_of_award",
        "prime_award_base_transaction_description", "transaction_description"]).to_reader())
    con.execute("CREATE TABLE aa AS SELECT * FROM aa_r WHERE active_current OR active_potential")
    con.register("rq_r", lance.dataset(SCOPE_REQ_URI, storage_options=so).scanner(columns=[
        "contract_award_unique_key", "has_labor_category", "labor_category_values", "scope_summary",
        "req_clearance_level_max", "has_clearance", "labor_headcount_total"]).to_reader())
    con.execute("CREATE TABLE rq AS SELECT * FROM rq_r")

    # award attrs (1 row/award) + the three per-award scope texts, labor awards only
    con.execute("""CREATE TABLE jw AS
        SELECT aa.*, rq.labor_category_values, rq.req_clearance_level_max, rq.has_clearance,
               rq.labor_headcount_total AS award_labor_headcount_total,
               lower(coalesce(rq.scope_summary,'')) AS scope_l,
               lower(coalesce(aa.prime_award_base_transaction_description,'') || ' ' ||
                     coalesce(aa.transaction_description,'')) AS desc_l
        FROM aa JOIN rq ON aa.contract_award_unique_key = rq.contract_award_unique_key
        WHERE rq.has_labor_category""")
    n_labor_awards = con.execute("SELECT count(*) FROM jw").fetchone()[0]

    con.execute("CREATE TABLE role_phrase(role VARCHAR, phrase VARCHAR)")
    con.execute(f"INSERT INTO role_phrase VALUES {_role_values(ROLE_PHRASES)}")
    con.execute("CREATE TABLE role_disp(role VARCHAR, display VARCHAR)")
    con.execute(f"INSERT INTO role_disp VALUES {_display_values()}")

    # explode -> corroborate against per-award scope text only
    con.execute("""CREATE TABLE exploded AS
        SELECT contract_award_unique_key AS ak, lower(trim(role)) AS labor_role, scope_l, desc_l
        FROM (SELECT contract_award_unique_key, unnest(labor_category_values) AS role, scope_l, desc_l FROM jw)
        WHERE nullif(trim(role),'') IS NOT NULL""")
    con.execute("""CREATE TABLE matched AS
        SELECT e.ak, e.labor_role,
               max(CASE WHEN e.scope_l LIKE '%'||rp.phrase||'%' THEN 1 ELSE 0 END) AS in_scope,
               max(CASE WHEN e.desc_l  LIKE '%'||rp.phrase||'%' THEN 1 ELSE 0 END) AS in_desc
        FROM exploded e JOIN role_phrase rp ON e.labor_role = rp.role
        GROUP BY e.ak, e.labor_role
        HAVING max(CASE WHEN e.scope_l LIKE '%'||rp.phrase||'%'
                        OR e.desc_l  LIKE '%'||rp.phrase||'%' THEN 1 ELSE 0 END) = 1""")

    final = con.execute(f"""
        SELECT
            m.ak AS contract_award_unique_key,
            m.labor_role,
            coalesce(rd.display, replace(m.labor_role,'_',' ')) AS labor_role_display,
            {SECTOR_SQL.replace('naics_code','a.naics_code')} AS naics_sector,
            a.naics_code, a.naics_description, a.psc_code, a.psc_description,
            a.awarding_sub_agency_name,
            a.recipient_uei, a.recipient_name, a.recipient_parent_name,
            a.business_size, a.type_of_set_aside,
            a.active_current, a.active_potential, a.has_option_tail,
            a.pop_current_end, a.pop_potential_end,
            a.pop_state_code, a.pop_city, a.pop_country_code,
            a.current_total_value_of_award,
            a.award_labor_headcount_total,
            a.req_clearance_level_max, a.has_clearance,
            CASE WHEN m.in_scope=1 AND m.in_desc=1 THEN 'both'
                 WHEN m.in_scope=1 THEN 'scope_summary' ELSE 'award_description' END AS corroborated_in
        FROM matched m
        JOIN jw a ON m.ak = a.contract_award_unique_key
        LEFT JOIN role_disp rd ON m.labor_role = rd.role
    """).to_arrow_table()

    stamp = pa.array([dt.datetime.now(dt.timezone.utc)] * final.num_rows, pa.timestamp("us", tz="UTC"))
    final = final.append_column("built_at", stamp)

    lance.write_dataset(final, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    built = []
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True); built.append(f"{col}:BTREE")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True); built.append(f"{col}:BITMAP")

    rows = ds.count_rows()
    distinct_awards = con.execute("SELECT count(DISTINCT ak) FROM matched").fetchone()[0]
    distinct_roles = con.execute("SELECT count(DISTINCT labor_role) FROM matched").fetchone()[0]
    print(f"labor awards (pre-corroboration): {n_labor_awards:,}")
    print(f"CORROBORATED rows written: {rows:,}  (distinct awards {distinct_awards:,}, distinct roles {distinct_roles})")
    print(f"indices: {built}")
    con.close()
    return {"uri": SERVING_URI, "rows": rows, "distinct_awards": distinct_awards,
            "distinct_roles": distinct_roles, "labor_awards_pre": n_labor_awards, "indices": built}


def verify() -> dict:
    import json
    import duckdb
    import lance

    so = _so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = duckdb.connect(":memory:")
    con.register("t_r", ds.scanner().to_reader())
    con.execute("CREATE TABLE t AS SELECT * FROM t_r")
    top = con.execute("""SELECT labor_role_display, count(DISTINCT contract_award_unique_key) c
        FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 15""").fetchall()
    sect = con.execute("""SELECT naics_sector, count(DISTINCT contract_award_unique_key) c
        FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 8""").fetchall()
    clr = con.execute("""SELECT count(DISTINCT contract_award_unique_key)
        FROM t WHERE req_clearance_level_max IS NOT NULL
        AND upper(req_clearance_level_max) NOT IN ('NONE','')""").fetchone()[0]
    out = {"uri": SERVING_URI, "rows": ds.count_rows(),
           "distinct_awards": con.execute("SELECT count(DISTINCT contract_award_unique_key) FROM t").fetchone()[0],
           "distinct_roles": con.execute("SELECT count(DISTINCT labor_role) FROM t").fetchone()[0],
           "columns": len(ds.schema.names), "indices": idx,
           "awards_with_clearance": clr,
           "top_roles": [{"role": r[0], "awards": r[1]} for r in top],
           "sectors": [{"sector": s[0], "awards": s[1]} for s in sect]}
    con.close()
    print(json.dumps(out, indent=2, default=str))
    return out


if __name__ == "__main__":
    import argparse
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", default="build", choices=["build", "verify"])
    a = ap.parse_args()
    print(json.dumps(build() if a.cmd == "build" else verify(), indent=2, default=str))
