"""Classify subawardees' self-reported subaward descriptions into the controlled capability vocabulary
via the Anthropic Message Batches API (Haiku 4.5), landing a frozen sidecar
`govcon_sub_self_reported_tags` that the profile build joins by description hash.

WHY LLM, NOT EMBEDDING-SIM. The descriptions are terse, abbreviated procurement text (~41 chars avg:
"POL", "TO68 SWITCHGEAR", "RAD HARD PARTS"). Embedding-similarity-to-anchor covered only ~5,327/25,449
subs and mis-tagged the acronym tail (adversarial review docs/plans/SUBAWARDEE_PATHB_ADVERSARIAL_REVIEW.md).
Controlled-vocab LLM classification resolves the acronyms (POL→fuel_supply, OCONUS→logistics, SCIF→
construction/security) and returns [] for junk ("PER PC"). Structured output enforces a valid subset
of the 77-tag enum per row, so no parse/threshold step.

GRAIN  one row per DISTINCT subaward_description (dedup → ~67K rows, not 200K). The profile build maps
       description→subs via contract_subaward and unions tags per sub.
COST   Batches API = 50% off; cached 77-tag+glossary system prompt across all requests. ~$6-7 one-time.
EGRESS subaward_description is sub-self-reported, PUBLIC USAspending data (usaspending.gov) — egressing it
       to the Anthropic API egresses public records, no CUI. (Same Claude path the scope capability_tags
       were already built through — session-fable; see GOVCON_P2B_EXTRACTION_READINESS.md.)
DETERMINISM  custom_id = sha256(description)[:31] (stable) so retrieve re-derives the map; no local state.

    doppler run -- .venv/bin/python pipelines/sam_gov/classify_sub_self_reported_tags.py <cmd>
      submit                  extract distinct descriptions → submit one batch → print batch_id
      retrieve <batch_id>     poll to completion → write the frozen sidecar (overwrite) + BTREE(desc_sha)
      verify                  row count, tag distribution, coverage
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.sam_attachment_extract_90day import _r2_storage_options  # noqa: E402

ACTIVE = "s3://data-sink/active"
SOURCE_URI = os.environ.get("GOVCON_SUBAWARD_SOURCE_URI", f"{ACTIVE}/usaspending_api_fresh/contract_subaward/")
SIDECAR_URI = os.environ.get("GOVCON_SUB_SELF_REPORTED_TAGS_URI", f"{ACTIVE}/govcon_sub_self_reported_tags/")
DESC_COL = "subaward_description"
MODEL = "claude-haiku-4-5"
DATA_STORAGE_VERSION = "2.1"
MAX_DESC_CHARS = 600          # bound payload; descriptions are short (p99≈1,211, the long tail is SKU dumps)

# Controlled vocabulary — MUST stay identical to apps/catalyst_api/src/map_decoders.py:_CAPABILITY_TAGS
# (77 tags) and the doc_scope capability_tags. Frozen here so the classifier output space is pinned.
CAPABILITY_TAGS = (
    "administrative_office_support", "aircraft_maintenance", "alarm_surveillance_systems",
    "architecture_services", "audio_visual_services", "behavioral_health_services",
    "calibration_inspection_qa", "chaplain_religious_services", "childcare_youth_services",
    "concrete_masonry", "construction_civil_heavy", "construction_general", "construction_vertical",
    "custodial_janitorial", "cybersecurity_services", "data_management_analytics", "demolition",
    "dental_services", "electrical_systems", "elevator_systems", "energy_renewables",
    "engineering_design", "environmental_remediation", "equipment_maintenance",
    "event_conference_support", "excavation_earthwork", "facilities_management", "fencing_barriers",
    "financial_audit_services", "fire_protection_systems", "flooring", "food_services", "fuel_supply",
    "grounds_maintenance_landscaping", "hvac_mechanical", "industrial_equipment_supply",
    "it_services", "laboratory_testing_services", "language_interpretation_translation",
    "laundry_linen_services", "legal_services", "lodging_billeting", "logistics_transportation",
    "mailroom_courier_services", "maintenance_repair_operations", "marine_vessel_services",
    "medical_clinical_services", "medical_equipment_supply", "moving_relocation", "nursing_services",
    "painting_coating", "paving_roadwork", "personnel_security_vetting", "pest_control",
    "physical_security_locksmith", "plumbing_pipefitting", "printing_publishing",
    "program_management_support", "public_affairs_communications", "renovation_alteration",
    "research_development", "roofing", "security_services_guard", "snow_ice_removal",
    "software_development", "staffing_personnel_services", "steel_structural", "supply_commodities",
    "surveying_mapping_gis", "telecom_networking", "training_instruction", "utilities_operation",
    "vehicle_fleet_maintenance", "veterinary_services", "warehousing_distribution",
    "waste_management", "water_wastewater")

GLOSSARY = (
    "Federal-contracting acronym glossary (resolve before tagging):\n"
    "POL = petroleum/oil/lubricants → fuel_supply. JP8/JP-8/AVGAS/DFM = fuel → fuel_supply.\n"
    "OCONUS/CONUS = overseas/continental US movement → logistics_transportation.\n"
    "SCIF/ICD-705 = secure compartmented facility → construction_vertical + physical_security_locksmith.\n"
    "A/E or A&E = architecture/engineering → architecture_services + engineering_design.\n"
    "PWS/SOW/SOO/IDIQ/BPA/task order/TO = contract vehicles (not a capability by themselves).\n"
    "O&M = operations & maintenance → maintenance_repair_operations or facilities_management.\n"
    "MRO = maintenance/repair/operations → maintenance_repair_operations.\n"
    "HVAC/RTU/chiller/AHU = heating/cooling → hvac_mechanical. Switchgear/transformer/UPS/panel = electrical_systems.\n"
    "BOS/base operations support → facilities_management. Grounds/mowing/landscaping → grounds_maintenance_landscaping.\n"
    "IT/SaaS/help desk/sysadmin/network/SATCOM = it_services or telecom_networking. App/software dev → software_development.\n"
    "Avionics/airframe/depot/aircraft = aircraft_maintenance. Vessel/shipboard/marine = marine_vessel_services.\n"
    "RAD HARD/electronic parts/components = supply_commodities (or industrial_equipment_supply).\n"
    "Custodial/janitorial → custodial_janitorial. Refuse/trash/hazmat disposal → waste_management.\n"
    "Guard/protective force → security_services_guard. Locksmith/access control → physical_security_locksmith.\n"
    "Mess/dining/galley/cafeteria/catering → food_services. Billeting/lodging/quarters → lodging_billeting.\n"
    "Clearance/background investigation/vetting → personnel_security_vetting. Calibration/QA/inspection/NDT → calibration_inspection_qa.\n"
    "R&D/SBIR/prototype/test & evaluation → research_development. PM/PMO/program support → program_management_support.\n"
)

SYSTEM = [
    {"type": "text", "text": (
        "You classify a single federal SUBAWARD work description into a controlled vocabulary of "
        "capability tags. The text is the subcontractor's own short, often abbreviated description of "
        "the work they performed under a prime contract. Return ONLY tags whose capability the work "
        "CLEARLY falls under. Prefer precision: if a tag is a stretch, omit it. Return an EMPTY list "
        "for vague/administrative/non-capability text (e.g. 'VARIOUS', 'PER PC', 'SEE ATTACHED', a bare "
        "part number, a dollar amount). Most descriptions map to 0-3 tags; never force a tag.\n\n"
        + GLOSSARY +
        "\nThe ONLY allowed tag values are the controlled vocabulary provided in the output schema."
    ), "cache_control": {"type": "ephemeral"}},
]

OUTPUT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {"tags": {"type": "array", "items": {"type": "string", "enum": list(CAPABILITY_TAGS)}}},
    "required": ["tags"],
}


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _cid(desc: str) -> str:
    return "h" + hashlib.sha256(desc.encode("utf-8")).hexdigest()[:31]


def _distinct_descriptions(so) -> list[str]:
    import lance
    import duckdb
    src = lance.dataset(SOURCE_URI, storage_options=so)
    tbl = src.scanner(columns=[DESC_COL]).to_table()
    con = duckdb.connect(":memory:")
    con.register("c", tbl)
    rows = con.execute(
        f"SELECT DISTINCT trim({DESC_COL}) AS d FROM c "
        f"WHERE {DESC_COL} IS NOT NULL AND length(trim({DESC_COL})) > 0 ORDER BY 1"
    ).fetchall()
    con.close()
    return [r[0][:MAX_DESC_CHARS] for r in rows]


def submit():
    import anthropic
    so = _r2_storage_options()
    descs = _distinct_descriptions(so)
    log(f"distinct descriptions = {len(descs):,}")
    requests = [{
        "custom_id": _cid(d),
        "params": {
            "model": MODEL,
            "max_tokens": 200,
            "system": SYSTEM,
            "messages": [{"role": "user", "content": d}],
            "output_config": {"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
        },
    } for d in descs]
    # dedup custom_ids (distinct descriptions → already unique, but guard collisions)
    seen, uniq = set(), []
    for r in requests:
        if r["custom_id"] in seen:
            continue
        seen.add(r["custom_id"]); uniq.append(r)
    # The cached system prompt + 77-value enum schema repeat in EVERY request, so a single batch POST
    # blows past the 256MB cap. Chunk by request count (≈5KB/req → 15K ≈ 75MB, well under the limit).
    CHUNK = 15_000
    client = anthropic.Anthropic()
    ids = []
    for i in range(0, len(uniq), CHUNK):
        part = uniq[i:i + CHUNK]
        batch = client.messages.batches.create(requests=part)
        ids.append(batch.id)
        log(f"submitted chunk {i // CHUNK + 1}: {batch.id} · {len(part):,} reqs · {batch.processing_status}")
    log(f"SUBMITTED {len(ids)} batches · {len(uniq):,} requests total")
    print(json.dumps({"batch_ids": ids, "n_requests": len(uniq), "model": MODEL}))
    return ids


def retrieve(batch_ids: str, poll_s: int = 60):
    import anthropic
    import lance
    import pyarrow as pa
    client = anthropic.Anthropic()
    ids = [b.strip() for b in batch_ids.split(",") if b.strip()]
    tagset = set(CAPABILITY_TAGS)
    out: dict[str, list[str]] = {}
    for bid in ids:
        while True:
            b = client.messages.batches.retrieve(bid)
            if b.processing_status == "ended":
                break
            log(f"[{bid}] status={b.processing_status} processing={b.request_counts.processing} "
                f"succeeded={b.request_counts.succeeded} errored={b.request_counts.errored}")
            time.sleep(poll_s)
        log(f"[{bid}] ended · succeeded={b.request_counts.succeeded} errored={b.request_counts.errored}")
        for res in client.messages.batches.results(bid):
            if res.result.type != "succeeded":
                continue
            msg = res.result.message
            text = next((blk.text for blk in msg.content if blk.type == "text"), "")
            try:
                tags = json.loads(text).get("tags", [])
            except Exception:  # noqa: BLE001
                tags = []
            out[res.custom_id] = sorted({t for t in tags if t in tagset})

    # re-derive custom_id → description to store the text alongside the tags
    so = _r2_storage_options()
    descs = _distinct_descriptions(so)
    cid2desc = {_cid(d): d for d in descs}
    run_id = f"sub_selftags_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    created = dt.datetime.now(dt.timezone.utc)
    sha_l, desc_l, tags_l, n_l = [], [], [], []
    for cid, d in cid2desc.items():
        tags = out.get(cid, [])
        sha_l.append(cid); desc_l.append(d); tags_l.append(tags); n_l.append(len(tags))
    n = len(sha_l)
    tbl = pa.table({
        "desc_sha": pa.array(sha_l, pa.string()),
        "description": pa.array(desc_l, pa.large_string()),
        "self_reported_capability_tags": pa.array(tags_l, pa.list_(pa.string())),
        "n_self_reported_tags": pa.array(n_l, pa.int32()),
        "model": pa.array([MODEL] * n, pa.string()),
        "run_id": pa.array([run_id] * n, pa.string()),
        "created_at": pa.array([created] * n, pa.timestamp("us", tz="UTC")),
    })
    log(f"writing OVERWRITE → {SIDECAR_URI}  ({n:,} rows · {sum(1 for t in tags_l if t):,} with ≥1 tag)")
    lance.write_dataset(tbl, SIDECAR_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    lance.dataset(SIDECAR_URI, storage_options=so).create_scalar_index(
        "desc_sha", index_type="BTREE", replace=True)
    log(f"DONE → {SIDECAR_URI} rows={n:,} run={run_id}")
    return {"rows": n, "with_tags": sum(1 for t in tags_l if t), "run_id": run_id}


def verify():
    import lance
    import duckdb
    so = _r2_storage_options()
    ds = lance.dataset(SIDECAR_URI, storage_options=so)
    con = duckdb.connect(":memory:")
    con.register("s", ds.scanner(columns=["desc_sha", "self_reported_capability_tags", "n_self_reported_tags"]).to_table())
    rows = con.execute("SELECT count(*) FROM s").fetchone()[0]
    with_tags = con.execute("SELECT count(*) FROM s WHERE n_self_reported_tags > 0").fetchone()[0]
    top = con.execute(
        "SELECT t, count(*) c FROM (SELECT unnest(self_reported_capability_tags) t FROM s) GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    ).fetchall()
    con.close()
    print(json.dumps({"uri": SIDECAR_URI, "rows": rows, "descriptions_with_tags": with_tags,
                      "top_tags": {t: c for t, c in top}}, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["submit", "retrieve", "verify"])
    ap.add_argument("batch_id", nargs="?")
    args = ap.parse_args()
    if args.cmd == "submit":
        submit()
    elif args.cmd == "retrieve":
        if not args.batch_id:
            raise SystemExit("retrieve requires <batch_id>")
        retrieve(args.batch_id)
    else:
        verify()


if __name__ == "__main__":
    main()
