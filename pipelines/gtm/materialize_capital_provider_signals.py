"""Compute worker — Gen-3 materialization of the capital-provider SIGNAL ledger.

Clean-room data plane: psycopg reads the raw jsonb sink (HQX Postgres), the coalesce/normalize is
100% local Python, Lance is written straight to R2. No catalog round-trip. Spec of record:
docs/reference/CAPITAL_PROVIDER_SIGNALS_ASSIGNMENT.md.

WHY THIS WORKER EXISTS
    The claygent enrichment payloads land verbatim (any shape) in gtm.existing_claygent_payloads
    (HQX Postgres) under a string `enrichment_payload_type`. The taxonomy was attempted three
    different ways with three vocabularies (capitalType / classification / financingMode) plus a
    construction-vertical pass. This worker coalesces the USEFUL signals into one lean, native-
    granularity Lance dataset keyed by normalized_domain, so GTM outbound can filter lenders by real
    category (factoring ≠ equipment finance ≠ private credit) without on-the-fly Postgres projection.

    LEDGER SEMANTICS. The dataset is the enrichment ledger. Intermediaries (brokerOrMarketplace /
    advisoryOnly / notCapitalProvider) are KEPT, flagged is_intermediary=true — dropping them would
    make the system re-enrich them and burn LLM spend. Outbound exclusion happens at the query layer.

SOURCE (read-only): gtm.existing_claygent_payloads (HQX_DB_URL_POOLED) + active/companies (curated origins).
TARGET (Gen-3 SoR, native Lance v2.1): s3://data-sink/active/capital_provider_signals/
GRAIN: one row per normalized_domain. Full rebuild-from-source (mode=overwrite) — deterministic, idempotent.

    doppler run -p core-x -c prd -- python3 pipelines/gtm/materialize_capital_provider_signals.py
"""
from __future__ import annotations

import datetime as dt
import os
import re

import lance
import psycopg
import pyarrow as pa

DATASET_URI = os.environ.get("CAPITAL_PROVIDER_SIGNALS_URI", "s3://data-sink/active/capital_provider_signals/")
COMPANIES_URI = os.environ.get("GTM_COMPANIES_URI", "s3://data-sink/active/companies/")
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

DOMAIN_RE = re.compile(r"^(?=.{1,253}$)([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}$")
CONF_RANK = {"very high": 3, "high": 2, "medium": 1, "low": 0}
INTERMEDIARY = {"brokerOrMarketplace", "advisoryOnly", "notCapitalProvider"}
FINANCING_TYPES = [
    "equipment-financing-status-2", "equipment-financing-evidence-2", "equipment-financing-evidence-3",
    "equipment-finance-one", "equipment-finance-status-two", "equipment-financing-status-1",
]
FMODE_MAP = {
    "direct-lender": "direct", "directlender": "direct",
    "broker": "broker", "broker/marketplace": "broker",
    "through-partner": "partner", "partner/referral": "partner",
    "multi-lender": "multi", "unclear": "unclear",
}
CURATED = ("elfa", "sfnet")


def crank(c) -> int:
    return CONF_RANK.get((c or "").strip().lower(), -1)


def valid(d) -> bool:
    return bool(d) and d != "skipped" and bool(DOMAIN_RE.match(d))


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT"); aid = os.environ.get("R2_ACCOUNT_ID")
    if not ep and aid:
        ep = f"https://{aid}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


SCHEMA = pa.schema([
    pa.field("normalized_domain", pa.string(), nullable=False),
    pa.field("capital_type", pa.string()),
    pa.field("provides_capital", pa.bool_()),
    pa.field("is_intermediary", pa.bool_()),
    pa.field("financing_mode", pa.string()),
    pa.field("provides_equipment_financing", pa.bool_()),
    pa.field("construction_equip_financing", pa.string()),
    pa.field("ef_classification", pa.string()),
    pa.field("confidence", pa.string()),
    pa.field("signals", pa.list_(pa.string())),
    pa.field("materialized_at", pa.timestamp("us", tz="UTC"), nullable=False),
])


def main() -> None:
    mat_at = dt.datetime.now(dt.timezone.utc)
    touched: dict[str, set] = {}

    def touch(d, t):
        touched.setdefault(d, set()).add(t)

    pg = psycopg.connect(os.environ["HQX_DB_URL_POOLED"])
    cur = pg.cursor()

    # 1. capital spine — providesCapital=true, dedup by confidence then latest
    cur.execute("""SELECT domain_norm, raw_payload->>'capitalType', raw_payload->>'providesCapital',
                          raw_payload->>'confidence', landed_at
        FROM gtm.existing_claygent_payloads
        WHERE enrichment_payload_type='capital-provider-json-1' AND domain_norm IS NOT NULL""")
    cap: dict[str, tuple] = {}
    for d, ct, pc, conf, la in cur.fetchall():
        if not valid(d):
            continue
        # LEDGER: record the run for EVERY verdict (true/false/no-answer) so the gap query never
        # re-flags an already-enriched domain. Only providesCapital='true' payloads set capital_type.
        touch(d, "capital-provider-json-1")
        if pc == "true":
            key = (crank(conf), la)
            if d not in cap or key > cap[d][0]:
                cap[d] = (key, ct, conf)

    # 2. financing cluster — pef set + winning normalized financing_mode
    cur.execute("""SELECT domain_norm, enrichment_payload_type,
                          lower(raw_payload->>'providesEquipmentFinancing'),
                          raw_payload->>'financingMode', raw_payload->>'confidence', landed_at
        FROM gtm.existing_claygent_payloads
        WHERE enrichment_payload_type = ANY(%s) AND domain_norm IS NOT NULL""", (FINANCING_TYPES,))
    pef_vals: dict[str, set] = {}
    fmode_best: dict[str, tuple] = {}
    unmapped: set = set()
    for d, etype, pef, fm, conf, la in cur.fetchall():
        if not valid(d):
            continue
        touch(d, etype)
        if pef:
            pef_vals.setdefault(d, set()).add(pef)
        if fm is not None and fm.strip():
            norm = FMODE_MAP.get(fm.strip().lower())
            if norm is None:
                unmapped.add(fm); norm = "unclear"
            informative = 0 if norm == "unclear" else 1
            key = (informative, crank(conf), la)
            if d not in fmode_best or key > fmode_best[d][0]:
                fmode_best[d] = (key, norm)

    def pef_bool(d):
        v = pef_vals.get(d)
        if not v:
            return None
        if v & {"yes", "true"}:
            return True
        if v & {"no", "false"}:
            return False
        return None

    # 3. ef_classification (drop skipped/junk)
    cur.execute("""SELECT domain_norm, raw_payload->>'classification', raw_payload->>'confidence', landed_at
        FROM gtm.existing_claygent_payloads
        WHERE enrichment_payload_type='equipment-finance-classification-one'
          AND domain_norm IS NOT NULL AND domain_norm<>'skipped'""")
    efc: dict[str, tuple] = {}
    for d, c, conf, la in cur.fetchall():
        if not valid(d):
            continue
        touch(d, "equipment-finance-classification-one")
        key = (crank(conf), la)
        if d not in efc or key > efc[d][0]:
            efc[d] = (key, c)

    # 4. construction-equipment financing — own vertical signal (conclusion)
    cur.execute("""SELECT domain_norm, lower(raw_payload->>'conclusion'), raw_payload->>'confidence', landed_at
        FROM gtm.existing_claygent_payloads
        WHERE enrichment_payload_type='construction-equipment-financing-1' AND domain_norm IS NOT NULL""")
    constr: dict[str, tuple] = {}
    for d, c, conf, la in cur.fetchall():
        if not valid(d):
            continue
        touch(d, "construction-equipment-financing-1")
        key = (crank(conf), la)
        val = c if c in ("yes", "no", "unclear") else "unclear"
        if d not in constr or key > constr[d][0]:
            constr[d] = (key, val)
    pg.close()

    # 5. curated lender origins from active/companies (elfa/sfnet)
    ds_c = lance.dataset(COMPANIES_URI, storage_options=storage_options())
    ctbl = ds_c.to_table(columns=["normalized_domain", "source_platform"])
    curated: dict[str, str] = {}
    for d, s in zip(ctbl.column("normalized_domain").to_pylist(), ctbl.column("source_platform").to_pylist()):
        if d and s in CURATED:
            dd = d.strip().lower()
            if valid(dd):
                curated.setdefault(dd, s)

    # 6. universe = capital-true ∪ curated{elfa,sfnet} ∪ construction-enriched
    universe = set(cap) | set(curated) | set(constr)

    rows = []
    for d in sorted(universe):
        capj = cap.get(d)
        ct = capj[1] if capj else None
        conf = capj[2] if capj else None
        sig = sorted(touched.get(d, set()))
        if d in curated:
            sig = sorted(set(sig) | {f"source:{curated[d]}"})
        rows.append({
            "normalized_domain": d,
            "capital_type": ct,
            "provides_capital": True if capj else None,
            "is_intermediary": (ct in INTERMEDIARY) if ct else False,
            "financing_mode": fmode_best[d][1] if d in fmode_best else None,
            "provides_equipment_financing": pef_bool(d),
            "construction_equip_financing": constr[d][1] if d in constr else None,
            "ef_classification": efc[d][1] if d in efc else None,
            "confidence": conf,
            "signals": sig,
            "materialized_at": mat_at,
        })

    tbl = pa.Table.from_pylist(rows, schema=SCHEMA)
    lance.write_dataset(tbl, DATASET_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                        storage_options=storage_options())
    ds = lance.dataset(DATASET_URI, storage_options=storage_options())
    ds.create_scalar_index("normalized_domain", index_type="BTREE")

    # ── verification summary ──
    from collections import Counter
    print(f"WROTE {ds.count_rows():,} rows → {DATASET_URI}")
    print(f"distinct normalized_domain: {len({r['normalized_domain'] for r in rows}):,}")
    print(f"universe parts: capital-true={len(cap):,} curated={len(curated):,} construction={len(constr):,}")
    print(f"is_intermediary=true: {sum(1 for r in rows if r['is_intermediary']):,}")
    if unmapped:
        print(f"!! UNMAPPED financingMode values (mapped to unclear): {sorted(unmapped)}")
    print("capital_type:", dict(Counter(r["capital_type"] for r in rows).most_common()))
    print("financing_mode:", dict(Counter(r["financing_mode"] for r in rows).most_common()))
    print("construction_equip_financing:", dict(Counter(r["construction_equip_financing"] for r in rows).most_common()))
    print("provides_equipment_financing:", dict(Counter(r["provides_equipment_financing"] for r in rows).most_common()))
    print("indices:", [ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", str(ix)) for ix in ds.list_indices()])


if __name__ == "__main__":
    main()
