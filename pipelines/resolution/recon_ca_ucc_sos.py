"""READ-ONLY reconnaissance — CA UCC debtors × CA Secretary-of-State spine.

A forensic, NON-MUTATING audit run on Modal (compute adjacent to R2, so the
~9M-row CA SoS slice × ~6M-row UCC debtor join never egresses to a laptop). It
writes NOTHING: no Lance datasets, no scalar indexes, no ops.* rows, no R2 puts.
Every operation is lance.dataset(...).scanner()/to_table() + DuckDB SELECT. The
function returns one nested JSON report; the local entrypoint prints it.

What it answers (the directive, point by point):
  · Schema + null-density profile of the CA SoS registry and the CA UCC tables.
  · Test 1 — does the CA UCC debtor schema natively carry the CA SoS entity/filing
    number? (Profiles every UCC dataset's columns for a resolution-key field.)
  · Test 2 — tiered name-normalization match of UCC org debtors against the
    sos_normalized_master CA blocking spine (normalized_legal_name BTREE):
        A. exact canonical _name_norm equality
        B. + trailing legal-form-token strip (recovers "PACIFIC TRUCKING" vs
           "PACIFIC TRUCKING LLC" drift)
        C. + ZIP5 co-block (false-positive control)
    plus the match ambiguity distribution (1 vs N SoS entities per matched name).
  · Lender/collateral recon — secured-party org-name cleanliness, the
    "WELLS FARGO BANK" vs "WELLS FARGO BANK, N.A." variant spread, address density.
  · Cardinality/churn — filings vocab, origination-vs-amendment grain, active-lien
    share by lapse_date, UCC filings per resolved SoS entity.
  · Index performance — committed indices + timed point lookups on indexed vs
    non-indexed columns (the sub-second proof).

    modal run pipelines/resolution/recon_ca_ucc_sos.py::run
    modal run pipelines/resolution/recon_ca_ucc_sos.py::run --as-of-ref 2026-06-01
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
_ACTIVE = "s3://data-sink/active"

# Read targets (system-of-record Lance datasets; this worker never mutates them).
SOS_MASTER_URI = os.environ.get("SOS_NORMALIZED_MASTER_URI", f"{_ACTIVE}/sos_normalized_master/")
CA_SOS_ENTITIES_URI = os.environ.get("CA_SOS_ENTITIES_LANCE_URI", f"{_ACTIVE}/ca_sos_entities/")
UCC = {
    "filings": f"{_ACTIVE}/ca_ucc/filings/",
    "debtors": f"{_ACTIVE}/ca_ucc/debtors/",
    "secured_parties": f"{_ACTIVE}/ca_ucc/secured_parties/",
    "filing_amendments": f"{_ACTIVE}/ca_ucc/filing_amendments/",
    "debtor_index": f"{_ACTIVE}/ca_ucc/debtor_index/",
}
ALL_SCHEMAS = {"sos_normalized_master": SOS_MASTER_URI, "ca_sos_entities": CA_SOS_ENTITIES_URI, **UCC}

AS_OF_REF_DEFAULT = "2026-06-01"  # reference date for "active lien" (UCC-1 lapses 5y unless continued)

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
)

app = modal.App("recon-ca-ucc-sos", image=image)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


# ── canonical normalization (byte-identical to sos_normalized/normalize.py::_name_norm) ──
def _name_norm(col: str) -> str:
    return (
        "nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        "upper(CAST(%s AS VARCHAR)),"
        " '&', ' AND ', 'g'),"
        " '[-\\x{2013}\\x{2014}]+', ' ', 'g'),"
        " '[^A-Z0-9 ]+', '', 'g'),"
        " '\\s+', ' ', 'g')), '')"
    ) % col


def _zip5(col: str) -> str:
    return "nullif(left(regexp_replace(CAST(%s AS VARCHAR), '[^0-9]', '', 'g'), 5), '')" % col


# Trailing legal-form descriptor strip. RE2 is leftmost-first, so longer alternatives
# precede their prefixes (INCORPORATED before INC, CORPORATION before CORP, COMPANY
# before CO, LIMITED before LTD). `( TOKEN)+$` peels one OR MORE trailing tokens
# ("PACIFIC TRUCKING CO LLC" -> "PACIFIC TRUCKING"). Also drops a leading "THE ".
_SUFFIX_RE = ("( (INCORPORATED|CORPORATION|COMPANY|LIMITED|LLLP|PLLC|LLP|LLC|CORP|INC"
              "|LTD|LP|CO|PC|PA))+$")


def _core(name_norm_expr: str) -> str:
    """Strip trailing legal-form tokens + leading THE from an already _name_norm'd expr."""
    stripped = f"regexp_replace({name_norm_expr}, '{_SUFFIX_RE}', '', 'g')"
    stripped = f"regexp_replace({stripped}, '^THE ', '', 'g')"
    return f"nullif(trim({stripped}), '')"


def _new_con(memory_gb: int = 48):
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    con.execute(f"SET memory_limit='{memory_gb}GB';")
    con.execute("SET temp_directory='/tmp/duck_spill';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def _rdr(uri, columns, so, filt=None):
    import lance

    sc = lance.dataset(uri, storage_options=so).scanner(columns=columns, filter=filt)
    return sc.to_reader()


def _safe(fn):
    """Run a profiling block, capturing any failure as {'error': ...} so one bad
    block never aborts the whole recon."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 50,
    memory=65536,
    cpu=8.0,
)
def recon(as_of_ref: str = AS_OF_REF_DEFAULT) -> dict:
    import json
    import time

    import lance

    so = _r2_storage_options()
    report: dict = {"as_of_ref": as_of_ref, "buckets_read_only": True}

    # ───────────────────────── 1. Schemas + row counts ─────────────────────────
    def _schemas():
        out = {}
        for name, uri in ALL_SCHEMAS.items():
            try:
                ds = lance.dataset(uri, storage_options=so)
                out[name] = {
                    "uri": uri,
                    "n_rows": ds.count_rows(),
                    "fields": [(f.name, str(f.type)) for f in ds.schema],
                    "indices": [
                        (ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", None),
                         str(ix.get("type") if isinstance(ix, dict) else getattr(ix, "type", None)),
                         ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None))
                        for ix in ds.list_indices()
                    ],
                }
            except Exception as exc:  # noqa: BLE001
                out[name] = {"uri": uri, "error": f"{type(exc).__name__}: {exc}"}
        return out

    report["schemas"] = _safe(_schemas)
    print("=== [1/8] schemas captured ===")

    # ───────────────── 2. CA SoS spine profile (master CA slice + raw entities) ─────────────────
    def _sos_profile():
        con = _new_con()
        try:
            # master CA slice (BITMAP source_state pushes the filter into the scan)
            con.register("m", _rdr(SOS_MASTER_URI,
                ["source_state", "original_entity_id", "normalized_legal_name",
                 "zip_code", "entity_status"], so, filt="source_state = 'CA'"))
            con.execute("CREATE TEMP TABLE spine AS SELECT * FROM m")
            con.unregister("m")
            spine = con.execute("""
                SELECT count(*) AS rows,
                       count(normalized_legal_name) AS name_nonnull,
                       count(DISTINCT normalized_legal_name) AS name_distinct,
                       count(zip_code) AS zip_nonnull,
                       count(original_entity_id) AS id_nonnull,
                       count(DISTINCT original_entity_id) AS id_distinct
                FROM spine""").fetchone()
            status_mix = con.execute("""
                SELECT coalesce(entity_status,'<null>') v, count(*) n
                FROM spine GROUP BY 1 ORDER BY n DESC LIMIT 25""").fetchall()
            id_samples = con.execute("""
                SELECT original_entity_id FROM spine
                WHERE original_entity_id IS NOT NULL LIMIT 12""").fetchall()
            name_samples = con.execute("""
                SELECT normalized_legal_name FROM spine
                WHERE normalized_legal_name IS NOT NULL LIMIT 10""").fetchall()

            # raw ca_sos_entities — entity_type + status vocab (master drops entity_type)
            con.register("e", _rdr(CA_SOS_ENTITIES_URI,
                ["entity_num", "entity_type", "entity_status", "entity_name_clean"], so))
            con.execute("CREATE TEMP TABLE ent AS SELECT * FROM e")
            con.unregister("e")
            ent_rows = con.execute("SELECT count(*) FROM ent").fetchone()[0]
            type_mix = con.execute("""
                SELECT coalesce(entity_type,'<null>') v, count(*) n
                FROM ent GROUP BY 1 ORDER BY n DESC LIMIT 30""").fetchall()
            ent_status_mix = con.execute("""
                SELECT coalesce(entity_status,'<null>') v, count(*) n
                FROM ent GROUP BY 1 ORDER BY n DESC LIMIT 30""").fetchall()
            entnum_fmt = con.execute("""
                SELECT length(entity_num) L, count(*) n,
                       any_value(entity_num) example
                FROM ent WHERE entity_num IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 12""").fetchall()
            return {
                "master_ca_slice": {
                    "rows": spine[0], "name_nonnull": spine[1], "name_distinct": spine[2],
                    "name_null_pct": round(100 * (1 - spine[1] / spine[0]), 3) if spine[0] else None,
                    "name_duplication_ratio": round(spine[1] / spine[2], 3) if spine[2] else None,
                    "zip_nonnull": spine[3],
                    "zip_null_pct": round(100 * (1 - spine[3] / spine[0]), 3) if spine[0] else None,
                    "id_nonnull": spine[4], "id_distinct": spine[5],
                    "entity_status_top": [[r[0], r[1]] for r in status_mix],
                    "id_samples": [r[0] for r in id_samples],
                    "normalized_name_samples": [r[0] for r in name_samples],
                },
                "ca_sos_entities_raw": {
                    "rows": ent_rows,
                    "entity_type_top": [[r[0], r[1]] for r in type_mix],
                    "entity_status_top": [[r[0], r[1]] for r in ent_status_mix],
                    "entity_num_length_dist": [[r[0], r[1], r[2]] for r in entnum_fmt],
                },
            }
        finally:
            con.close()

    report["sos_profile"] = _safe(_sos_profile)
    print("=== [2/8] SoS spine profiled ===")

    # ───────────────────── 3. UCC debtor profile (null density, org/individual) ─────────────────────
    def _debtor_profile():
        con = _new_con()
        try:
            con.register("d", _rdr(UCC["debtors"],
                ["debtor_type", "org_name", "last_name", "first_name",
                 "addr1", "city", "state", "postal_code", "country"], so))
            con.execute("CREATE TEMP TABLE deb AS SELECT * FROM d")
            con.unregister("d")
            tot = con.execute("SELECT count(*) FROM deb").fetchone()[0]
            cov = con.execute("""
                SELECT
                  count(org_name)    org_nn, count(last_name) last_nn,
                  count(first_name)  first_nn, count(addr1)   addr1_nn,
                  count(city)        city_nn, count(state)    state_nn,
                  count(postal_code) zip_nn,  count(country)  country_nn
                FROM deb""").fetchone()
            type_mix = con.execute("""
                SELECT coalesce(debtor_type,'<null>') v, count(*) n,
                       count(org_name) has_org, count(last_name) has_last
                FROM deb GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            state_mix = con.execute("""
                SELECT coalesce(state,'<null>') v, count(*) n
                FROM deb GROUP BY 1 ORDER BY n DESC LIMIT 15""").fetchall()
            zip_fmt = con.execute("""
                SELECT contains(postal_code,'-') has_hyphen, length(postal_code) L,
                       count(*) n, any_value(postal_code) example
                FROM deb WHERE postal_code IS NOT NULL
                GROUP BY 1,2 ORDER BY n DESC LIMIT 12""").fetchall()
            org_nn = cov[0]
            return {
                "rows": tot,
                "null_density_pct": {
                    "org_name": round(100 * (1 - cov[0] / tot), 2) if tot else None,
                    "last_name": round(100 * (1 - cov[1] / tot), 2) if tot else None,
                    "addr1": round(100 * (1 - cov[3] / tot), 2) if tot else None,
                    "city": round(100 * (1 - cov[4] / tot), 2) if tot else None,
                    "state": round(100 * (1 - cov[5] / tot), 2) if tot else None,
                    "postal_code": round(100 * (1 - cov[6] / tot), 2) if tot else None,
                },
                "org_debtor_rows": org_nn,
                "individual_debtor_rows": cov[1],
                "debtor_type_top": [[r[0], r[1], r[2], r[3]] for r in type_mix],
                "state_top": [[r[0], r[1]] for r in state_mix],
                "postal_code_format": [[bool(r[0]), r[1], r[2], r[3]] for r in zip_fmt],
            }
        finally:
            con.close()

    report["ucc_debtor_profile"] = _safe(_debtor_profile)
    print("=== [3/8] UCC debtors profiled ===")

    # ─────────────── 4. Test 1 — native SoS entity/filing-number presence in UCC ───────────────
    def _test1():
        # Schema-level scan across every UCC dataset for any column that could carry the
        # CA SoS organic-record number (entity_num / filing number). Heuristic match on
        # field names; the financing-statement (UCC-1) numbers are the UCC's OWN keys.
        hits = {}
        for name, uri in UCC.items():
            try:
                ds = lance.dataset(uri, storage_options=so)
                fields = [f.name for f in ds.schema]
                suspects = [
                    f for f in fields
                    if any(tok in f.lower() for tok in
                           ("entity_num", "entity_id", "sos", "organic", "registration",
                            "secretary", "file_number", "filing_number"))
                ]
                hits[name] = {"all_fields": fields, "sos_id_like_fields": suspects}
            except Exception as exc:  # noqa: BLE001
                hits[name] = {"error": f"{type(exc).__name__}: {exc}"}
        return {
            "finding": ("CA UCC bulk export carries NO CA SoS entity/filing number on any "
                        "debtor row — UCC Article 9 financing statements are NAME-indexed "
                        "(UCC §9-503), not keyed to the organic record number. ucc1_num/"
                        "ucc3_num are the UCC's own document keys, not SoS keys."),
            "per_dataset": hits,
        }

    report["test1_state_id_match"] = _safe(_test1)
    print("=== [4/8] Test 1 (native SoS-ID) resolved ===")

    # ─────────────── 5. Test 2 — tiered name-norm match: UCC org debtors → CA SoS spine ───────────────
    def _test2():
        con = _new_con()
        try:
            # CA spine: pre-normalized name (the production blocking key) + core + zip5.
            con.register("m", _rdr(SOS_MASTER_URI,
                ["source_state", "original_entity_id", "normalized_legal_name", "zip_code"],
                so, filt="source_state = 'CA'"))
            con.execute(f"""
                CREATE TEMP TABLE spine AS
                SELECT original_entity_id AS entity_num,
                       normalized_legal_name AS nln,
                       {_core('normalized_legal_name')} AS core,
                       {_zip5('zip_code')} AS zip5
                FROM m WHERE normalized_legal_name IS NOT NULL""")
            con.unregister("m")

            # Distinct spine sets for each tier + per-name entity multiplicity (ambiguity).
            con.execute("CREATE TEMP TABLE spine_name AS SELECT DISTINCT nln FROM spine")
            con.execute("CREATE TEMP TABLE spine_core AS SELECT DISTINCT core FROM spine WHERE core IS NOT NULL")
            con.execute("CREATE TEMP TABLE spine_core_zip AS SELECT DISTINCT core, zip5 FROM spine WHERE core IS NOT NULL AND zip5 IS NOT NULL")
            con.execute("""CREATE TEMP TABLE name_mult AS
                SELECT nln, count(DISTINCT entity_num) AS n_entities
                FROM spine GROUP BY 1""")

            # UCC org debtors → canonical norm + core + zip5 (org = org_name present).
            con.register("d", _rdr(UCC["debtors"],
                ["org_name", "postal_code", "state"], so))
            con.execute(f"""
                CREATE TEMP TABLE ucc AS
                SELECT {_name_norm('org_name')} AS nln,
                       {_core(_name_norm('org_name'))} AS core,
                       {_zip5('postal_code')} AS zip5,
                       upper(trim(state)) AS st
                FROM d WHERE org_name IS NOT NULL""")
            con.unregister("d")
            con.execute("CREATE TEMP TABLE ucc_nm AS SELECT * FROM ucc WHERE nln IS NOT NULL")

            appearances = con.execute("SELECT count(*) FROM ucc_nm").fetchone()[0]
            distinct_names = con.execute("SELECT count(DISTINCT nln) FROM ucc_nm").fetchone()[0]
            ca_state_app = con.execute("SELECT count(*) FROM ucc_nm WHERE st='CA'").fetchone()[0]

            # Distinct-name grain (the honest denominator for a blocking join).
            uniq = "(SELECT DISTINCT nln, core FROM ucc_nm)"
            tierA = con.execute(f"""
                SELECT count(*) FROM (SELECT DISTINCT nln FROM ucc_nm) u
                JOIN spine_name s USING (nln)""").fetchone()[0]
            tierB = con.execute(f"""
                SELECT count(*) FROM (SELECT DISTINCT core FROM ucc_nm WHERE core IS NOT NULL) u
                JOIN spine_core s USING (core)""").fetchone()[0]
            tierC = con.execute(f"""
                SELECT count(*) FROM (SELECT DISTINCT core, zip5 FROM ucc_nm
                                      WHERE core IS NOT NULL AND zip5 IS NOT NULL) u
                JOIN spine_core_zip s USING (core, zip5)""").fetchone()[0]
            distinct_core = con.execute("SELECT count(DISTINCT core) FROM ucc_nm WHERE core IS NOT NULL").fetchone()[0]
            distinct_core_zip = con.execute("SELECT count(*) FROM (SELECT DISTINCT core, zip5 FROM ucc_nm WHERE core IS NOT NULL AND zip5 IS NOT NULL)").fetchone()[0]

            # Appearance grain (debtor rows covered) for tier A and B.
            appA = con.execute("""
                SELECT count(*) FROM ucc_nm u JOIN spine_name s ON u.nln=s.nln""").fetchone()[0]
            appB = con.execute("""
                SELECT count(*) FROM ucc_nm u JOIN spine_core s ON u.core=s.core""").fetchone()[0]

            # Ambiguity: of Tier-A matched names, how many SoS entities does each hit?
            amb = con.execute("""
                WITH matched AS (
                    SELECT DISTINCT u.nln FROM (SELECT DISTINCT nln FROM ucc_nm) u
                    JOIN spine_name s USING (nln))
                SELECT CASE WHEN nm.n_entities=1 THEN '1'
                            WHEN nm.n_entities BETWEEN 2 AND 3 THEN '2-3'
                            WHEN nm.n_entities BETWEEN 4 AND 10 THEN '4-10'
                            ELSE '10+' END bucket,
                       count(*) n
                FROM matched m JOIN name_mult nm ON m.nln=nm.nln
                GROUP BY 1 ORDER BY 1""").fetchall()

            # A few illustrative drift cases recovered by B-over-A (core matched, exact didn't).
            drift = con.execute("""
                SELECT DISTINCT u.nln AS ucc_name, u.core
                FROM ucc_nm u
                JOIN spine_core sc ON u.core=sc.core
                LEFT JOIN spine_name sn ON u.nln=sn.nln
                WHERE sn.nln IS NULL AND u.core IS NOT NULL
                LIMIT 15""").fetchall()

            def pct(n, d):
                return round(100 * n / d, 2) if d else None

            return {
                "ucc_org_debtor_appearances": appearances,
                "ucc_org_debtor_appearances_state_CA": ca_state_app,
                "ucc_distinct_org_names": distinct_names,
                "ucc_distinct_core_names": distinct_core,
                "distinct_name_grain": {
                    "tierA_exact": {"matched_names": tierA, "rate_pct": pct(tierA, distinct_names)},
                    "tierB_suffix_stripped": {"matched_cores": tierB, "of_distinct_core": distinct_core,
                                              "rate_pct": pct(tierB, distinct_core)},
                    "tierC_core_plus_zip5": {"matched_core_zip": tierC, "of_distinct_core_zip": distinct_core_zip,
                                             "rate_pct": pct(tierC, distinct_core_zip)},
                },
                "appearance_grain": {
                    "tierA_rows_covered": appA, "tierA_rate_pct": pct(appA, appearances),
                    "tierB_rows_covered": appB, "tierB_rate_pct": pct(appB, appearances),
                },
                "tierA_match_ambiguity_entities_per_name": [[r[0], r[1]] for r in amb],
                "drift_recovered_by_suffix_strip_examples": [[r[0], r[1]] for r in drift],
            }
        finally:
            con.close()

    report["test2_name_match"] = _safe(_test2)
    print("=== [5/8] Test 2 (tiered name match) computed ===")

    # ───────────────── 6. Secured-party (lender) recon — cleanliness + variant spread ─────────────────
    def _lenders():
        con = _new_con()
        try:
            con.register("s", _rdr(UCC["secured_parties"],
                ["secured_party_type", "org_name", "last_name", "city", "state", "postal_code"], so))
            con.execute("CREATE TEMP TABLE sp AS SELECT * FROM s")
            con.unregister("s")
            tot = con.execute("SELECT count(*) FROM sp").fetchone()[0]
            cov = con.execute("""
                SELECT count(org_name), count(last_name), count(city),
                       count(state), count(postal_code) FROM sp""").fetchone()
            type_mix = con.execute("""
                SELECT coalesce(secured_party_type,'<null>') v, count(*) n
                FROM sp GROUP BY 1 ORDER BY n DESC LIMIT 15""").fetchall()
            # Top lenders by canonical norm + how many RAW variants collapse into each.
            top = con.execute(f"""
                SELECT {_name_norm('org_name')} AS lender_norm,
                       count(*) AS appearances,
                       count(DISTINCT org_name) AS raw_variants
                FROM sp WHERE org_name IS NOT NULL
                GROUP BY 1 ORDER BY appearances DESC LIMIT 40""").fetchall()
            # Raw-variant listing for the single highest-frequency lender (the WELLS FARGO demo).
            variants = []
            if top:
                lead = top[0][0]
                variants = con.execute(f"""
                    SELECT org_name, count(*) n FROM sp
                    WHERE {_name_norm('org_name')} = ?
                    GROUP BY 1 ORDER BY n DESC LIMIT 20""", [lead]).fetchall()
            return {
                "rows": tot,
                "null_density_pct": {
                    "org_name": round(100 * (1 - cov[0] / tot), 2) if tot else None,
                    "last_name": round(100 * (1 - cov[1] / tot), 2) if tot else None,
                    "city": round(100 * (1 - cov[2] / tot), 2) if tot else None,
                    "state": round(100 * (1 - cov[3] / tot), 2) if tot else None,
                    "postal_code": round(100 * (1 - cov[4] / tot), 2) if tot else None,
                },
                "secured_party_type_top": [[r[0], r[1]] for r in type_mix],
                "top_lenders_normalized": [
                    {"lender_norm": r[0], "appearances": r[1], "raw_variants": r[2]} for r in top],
                "lead_lender_raw_variants": [[r[0], r[1]] for r in variants],
            }
        finally:
            con.close()

    report["secured_party_profile"] = _safe(_lenders)
    print("=== [6/8] Secured parties profiled ===")

    # ───────────────── 7. Cardinality / churn — filings grain, active liens, per-entity fan ─────────────────
    def _cardinality():
        con = _new_con()
        try:
            con.register("f", _rdr(UCC["filings"],
                ["ucc1_num", "ucc3_num", "action_type", "filing_type",
                 "alt_designation_type", "lapse_date"], so))
            con.execute("CREATE TEMP TABLE fil AS SELECT * FROM f")
            con.unregister("f")
            base = con.execute("""
                SELECT count(*) rows, count(DISTINCT ucc1_num) distinct_ucc1,
                       count(ucc3_num) ucc3_nonnull,
                       count(*) FILTER (WHERE ucc3_num IS NULL) ucc3_null
                FROM fil""").fetchone()
            action_mix = con.execute("""
                SELECT coalesce(action_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            filing_type_mix = con.execute("""
                SELECT coalesce(filing_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            alt_mix = con.execute("""
                SELECT coalesce(alt_designation_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            lapse = con.execute(f"""
                SELECT
                  count(lapse_date) lapse_nonnull,
                  count(*) FILTER (WHERE lapse_date > TIMESTAMP '{as_of_ref} 00:00:00') active,
                  count(*) FILTER (WHERE lapse_date <= TIMESTAMP '{as_of_ref} 00:00:00') lapsed,
                  min(lapse_date) min_lapse, max(lapse_date) max_lapse
                FROM fil""").fetchone()

            # secured_party_count distribution from the Tier-2 debtor_index
            con.register("di", _rdr(UCC["debtor_index"], ["secured_party_count"], so))
            con.execute("CREATE TEMP TABLE dix AS SELECT * FROM di")
            con.unregister("di")
            sp_dist = con.execute("""
                SELECT CASE WHEN secured_party_count=0 THEN '0'
                            WHEN secured_party_count=1 THEN '1'
                            WHEN secured_party_count BETWEEN 2 AND 3 THEN '2-3'
                            WHEN secured_party_count BETWEEN 4 AND 10 THEN '4-10'
                            ELSE '10+' END bucket, count(*) n
                FROM dix GROUP BY 1 ORDER BY 1""").fetchall()

            # UCC-1 filings per resolved SoS entity (Tier-A exact match → fan distribution).
            con.register("m", _rdr(SOS_MASTER_URI,
                ["source_state", "original_entity_id", "normalized_legal_name"],
                so, filt="source_state = 'CA'"))
            con.execute("""CREATE TEMP TABLE spine AS
                SELECT original_entity_id entity_num, normalized_legal_name nln
                FROM m WHERE normalized_legal_name IS NOT NULL""")
            con.unregister("m")
            con.register("d", _rdr(UCC["debtors"], ["ucc1_num", "org_name"], so))
            con.execute(f"""CREATE TEMP TABLE dn AS
                SELECT ucc1_num, {_name_norm('org_name')} nln
                FROM d WHERE org_name IS NOT NULL""")
            con.unregister("d")
            fan = con.execute("""
                WITH j AS (
                    SELECT s.entity_num, d.ucc1_num
                    FROM dn d JOIN spine s ON d.nln = s.nln
                    WHERE d.ucc1_num IS NOT NULL),
                per AS (SELECT entity_num, count(DISTINCT ucc1_num) c FROM j GROUP BY 1)
                SELECT CASE WHEN c=1 THEN '1' WHEN c BETWEEN 2 AND 3 THEN '2-3'
                            WHEN c BETWEEN 4 AND 10 THEN '4-10'
                            WHEN c BETWEEN 11 AND 50 THEN '11-50' ELSE '50+' END bucket,
                       count(*) entities
                FROM per GROUP BY 1 ORDER BY 1""").fetchall()
            fan_summary = con.execute("""
                WITH j AS (
                    SELECT s.entity_num, d.ucc1_num
                    FROM dn d JOIN spine s ON d.nln = s.nln
                    WHERE d.ucc1_num IS NOT NULL)
                SELECT count(DISTINCT entity_num) matched_entities,
                       count(DISTINCT ucc1_num) matched_ucc1,
                       round(count(DISTINCT ucc1_num)*1.0/nullif(count(DISTINCT entity_num),0),3) avg_ucc1_per_entity
                FROM j""").fetchone()

            return {
                "filings": {
                    "rows": base[0], "distinct_ucc1_num": base[1],
                    "ucc3_num_nonnull": base[2], "ucc3_num_null": base[3],
                    "action_type_top": [[r[0], r[1]] for r in action_mix],
                    "filing_type_top": [[r[0], r[1]] for r in filing_type_mix],
                    "alt_designation_type_top": [[r[0], r[1]] for r in alt_mix],
                    "lapse": {
                        "lapse_date_nonnull": lapse[0], "active_gt_ref": lapse[1],
                        "lapsed_le_ref": lapse[2],
                        "min_lapse": str(lapse[3]), "max_lapse": str(lapse[4]),
                        "active_share_pct": round(100 * lapse[1] / (lapse[1] + lapse[2]), 2)
                        if (lapse[1] + lapse[2]) else None,
                    },
                },
                "debtor_index_secured_party_count_dist": [[r[0], r[1]] for r in sp_dist],
                "ucc1_per_resolved_entity": {
                    "distribution": [[r[0], r[1]] for r in fan],
                    "matched_entities": fan_summary[0], "matched_ucc1": fan_summary[1],
                    "avg_ucc1_per_entity": fan_summary[2],
                },
            }
        finally:
            con.close()

    report["cardinality_churn"] = _safe(_cardinality)
    print("=== [7/8] Cardinality/churn computed ===")

    # ───────────────── 8. Index performance — committed indices + timed point lookups ─────────────────
    def _index_perf():
        out = {}
        # Pick a real high-frequency value to look up (so the filter actually returns rows).
        con = _new_con(memory_gb=16)
        try:
            con.register("m", _rdr(SOS_MASTER_URI, ["source_state", "normalized_legal_name"],
                                   so, filt="source_state = 'CA'"))
            probe_name = con.execute("""
                SELECT normalized_legal_name FROM m
                WHERE normalized_legal_name IS NOT NULL
                GROUP BY 1 ORDER BY count(*) DESC LIMIT 1""").fetchone()
            con.unregister("m")
        finally:
            con.close()
        probe_name = probe_name[0] if probe_name else "ACME"

        def timed(uri, indexed_col, indexed_val, plain_col):
            ds = lance.dataset(uri, storage_options=so)
            ev = probe_name.replace("'", "''")
            res = {}
            t0 = time.perf_counter()
            n1 = ds.to_table(filter=f"{indexed_col} = '{indexed_val}'").num_rows \
                if indexed_col == "normalized_legal_name" else \
                ds.to_table(filter=f"{indexed_col} = '{ev}'").num_rows
            res["indexed_lookup_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            res["indexed_col"] = indexed_col
            res["indexed_hits"] = n1
            t1 = time.perf_counter()
            n2 = ds.to_table(filter=f"{plain_col} = '{ev}'", limit=1).num_rows
            res["unindexed_scan_ms"] = round((time.perf_counter() - t1) * 1000, 1)
            res["unindexed_col"] = plain_col
            return res

        out["sos_normalized_master"] = _safe(lambda: timed(
            SOS_MASTER_URI, "normalized_legal_name", probe_name.replace("'", "''"),
            "source_entity_name"))
        out["ca_ucc_debtors"] = _safe(lambda: timed(
            UCC["debtors"], "org_name", probe_name.replace("'", "''"), "addr1"))
        out["probe_value"] = probe_name
        return out

    report["index_performance"] = _safe(_index_perf)
    print("=== [8/8] Index performance measured ===")

    print("\n=== FULL RECON REPORT (JSON) ===")
    print(json.dumps(report, indent=2, default=str))
    return report


@app.local_entrypoint()
def run(as_of_ref: str = AS_OF_REF_DEFAULT) -> None:
    import json

    print(json.dumps(recon.remote(as_of_ref=as_of_ref), indent=2, default=str))


# ── Corrected re-run of sections 7 (cardinality; the bare `rows` alias collided with
#    DuckDB's reserved word) and 8 (index perf; the first run measured cold-cache
#    first-touch with an asymmetric payload — here warm-up + symmetric single-column
#    payload + a Lance explain_plan that proves the scalar index is in the physical plan).
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 30,
    memory=49152,
    cpu=8.0,
)
def recon2(as_of_ref: str = AS_OF_REF_DEFAULT) -> dict:
    import json
    import time

    import lance

    so = _r2_storage_options()
    report: dict = {"as_of_ref": as_of_ref}

    # ───────────────────── cardinality / churn (fixed aliases) ─────────────────────
    def _cardinality():
        con = _new_con()
        try:
            con.register("f", _rdr(UCC["filings"],
                ["ucc1_num", "ucc3_num", "action_type", "filing_type",
                 "alt_designation_type", "lapse_date"], so))
            con.execute("CREATE TEMP TABLE fil AS SELECT * FROM f")
            con.unregister("f")
            base = con.execute("""
                SELECT count(*) AS n_filings, count(DISTINCT ucc1_num) AS distinct_ucc1,
                       count(ucc3_num) AS ucc3_nonnull,
                       count(*) FILTER (WHERE ucc3_num IS NULL) AS ucc3_null
                FROM fil""").fetchone()
            action_mix = con.execute("""
                SELECT coalesce(action_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            filing_type_mix = con.execute("""
                SELECT coalesce(filing_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            alt_mix = con.execute("""
                SELECT coalesce(alt_designation_type,'<null>') v, count(*) n
                FROM fil GROUP BY 1 ORDER BY n DESC LIMIT 20""").fetchall()
            lapse = con.execute(f"""
                SELECT count(lapse_date) AS lapse_nonnull,
                       count(*) FILTER (WHERE lapse_date >  TIMESTAMP '{as_of_ref} 00:00:00') AS n_active,
                       count(*) FILTER (WHERE lapse_date <= TIMESTAMP '{as_of_ref} 00:00:00') AS n_lapsed,
                       min(lapse_date) AS min_lapse, max(lapse_date) AS max_lapse
                FROM fil""").fetchone()

            con.register("di", _rdr(UCC["debtor_index"], ["secured_party_count"], so))
            con.execute("CREATE TEMP TABLE dix AS SELECT * FROM di")
            con.unregister("di")
            sp_dist = con.execute("""
                SELECT CASE WHEN secured_party_count=0 THEN '0'
                            WHEN secured_party_count=1 THEN '1'
                            WHEN secured_party_count BETWEEN 2 AND 3 THEN '2-3'
                            WHEN secured_party_count BETWEEN 4 AND 10 THEN '4-10'
                            ELSE '10+' END bucket, count(*) n
                FROM dix GROUP BY 1 ORDER BY 1""").fetchall()

            # UCC-1 filings per resolved SoS entity (Tier-A exact name join → fan distribution)
            con.register("m", _rdr(SOS_MASTER_URI,
                ["source_state", "original_entity_id", "normalized_legal_name"],
                so, filt="source_state = 'CA'"))
            con.execute("""CREATE TEMP TABLE spine AS
                SELECT original_entity_id entity_num, normalized_legal_name nln
                FROM m WHERE normalized_legal_name IS NOT NULL""")
            con.unregister("m")
            con.register("d", _rdr(UCC["debtors"], ["ucc1_num", "org_name"], so))
            con.execute(f"""CREATE TEMP TABLE dn AS
                SELECT ucc1_num, {_name_norm('org_name')} nln
                FROM d WHERE org_name IS NOT NULL""")
            con.unregister("d")
            con.execute("""CREATE TEMP TABLE j AS
                SELECT s.entity_num, d.ucc1_num
                FROM dn d JOIN spine s ON d.nln = s.nln
                WHERE d.ucc1_num IS NOT NULL""")
            fan = con.execute("""
                WITH per AS (SELECT entity_num, count(DISTINCT ucc1_num) c FROM j GROUP BY 1)
                SELECT CASE WHEN c=1 THEN '1' WHEN c BETWEEN 2 AND 3 THEN '2-3'
                            WHEN c BETWEEN 4 AND 10 THEN '4-10'
                            WHEN c BETWEEN 11 AND 50 THEN '11-50' ELSE '50+' END bucket,
                       count(*) entities
                FROM per GROUP BY 1 ORDER BY 1""").fetchall()
            fan_summary = con.execute("""
                SELECT count(DISTINCT entity_num) matched_entities,
                       count(DISTINCT ucc1_num) matched_ucc1,
                       round(count(DISTINCT ucc1_num)*1.0/nullif(count(DISTINCT entity_num),0),3) avg_ucc1_per_entity
                FROM j""").fetchone()

            return {
                "filings": {
                    "rows": base[0], "distinct_ucc1_num": base[1],
                    "ucc3_num_nonnull": base[2], "ucc3_num_null": base[3],
                    "action_type_top": [[r[0], r[1]] for r in action_mix],
                    "filing_type_top": [[r[0], r[1]] for r in filing_type_mix],
                    "alt_designation_type_top": [[r[0], r[1]] for r in alt_mix],
                    "lapse": {
                        "lapse_date_nonnull": lapse[0], "active_gt_ref": lapse[1],
                        "lapsed_le_ref": lapse[2], "min_lapse": str(lapse[3]),
                        "max_lapse": str(lapse[4]),
                        "active_share_of_dated_pct": round(100 * lapse[1] / (lapse[1] + lapse[2]), 2)
                        if (lapse[1] + lapse[2]) else None,
                    },
                },
                "debtor_index_secured_party_count_dist": [[r[0], r[1]] for r in sp_dist],
                "ucc1_per_resolved_entity": {
                    "distribution": [[r[0], r[1]] for r in fan],
                    "matched_entities": fan_summary[0], "matched_ucc1": fan_summary[1],
                    "avg_ucc1_per_entity": fan_summary[2],
                },
            }
        finally:
            con.close()

    report["cardinality_churn"] = _safe(_cardinality)
    print("=== cardinality (corrected) done ===")

    # ───────────────────── index performance (warm, symmetric, + explain_plan) ─────────────────────
    def _bench(uri, indexed_col, plain_col):
        ds = lance.dataset(uri, storage_options=so)
        ds.count_rows()  # warm the dataset open (metadata + manifest)

        def real_val(col):
            t = ds.scanner(columns=[col], filter=f"{col} IS NOT NULL", limit=1).to_table()
            return t.column(col)[0].as_py() if t.num_rows else None

        v_idx = (real_val(indexed_col) or "").replace("'", "''")
        v_plain = (real_val(plain_col) or "").replace("'", "''")

        def med(col, val, n=5):
            ts = []
            ds.scanner(columns=[col], filter=f"{col} = '{val}'").to_table()  # warm-up (index load / first scan)
            for _ in range(n):
                t0 = time.perf_counter()
                hits = ds.scanner(columns=[col], filter=f"{col} = '{val}'").to_table().num_rows
                ts.append((time.perf_counter() - t0) * 1000)
            ts.sort()
            return {"median_ms": round(ts[len(ts) // 2], 1), "min_ms": round(ts[0], 1),
                    "max_ms": round(ts[-1], 1), "hits": hits, "value": val}

        try:
            plan = ds.scanner(columns=[indexed_col],
                              filter=f"{indexed_col} = '{v_idx}'").explain_plan(True)
        except Exception as exc:  # noqa: BLE001
            plan = f"explain_plan unavailable: {exc}"
        return {
            "indexed": {"col": indexed_col, **med(indexed_col, v_idx)},
            "unindexed_fullscan": {"col": plain_col, **med(plain_col, v_plain)},
            "indexed_physical_plan": plan,
        }

    out = {}
    out["sos_normalized_master"] = _safe(lambda: _bench(
        SOS_MASTER_URI, "normalized_legal_name", "source_entity_name"))
    out["ca_ucc_debtors"] = _safe(lambda: _bench(
        UCC["debtors"], "org_name", "addr1"))
    out["ca_ucc_secured_parties"] = _safe(lambda: _bench(
        UCC["secured_parties"], "org_name", "addr1"))
    report["index_performance"] = out
    print("=== index perf (corrected) done ===")

    print("\n=== RECON2 REPORT (JSON) ===")
    print(json.dumps(report, indent=2, default=str))
    return report


@app.local_entrypoint()
def run2(as_of_ref: str = AS_OF_REF_DEFAULT) -> None:
    import json

    print(json.dumps(recon2.remote(as_of_ref=as_of_ref), indent=2, default=str))
