"""Pure transformation for the SEC Form ADV Schedule D 7.B.1 (private funds) / 7.B.2 (reliance)
/ 7.A (financial-industry affiliations) families — the child tables that live in the ADV
``adv-filing-data-*-part2.zip`` the base ingest (pipelines/sec_adv/ingest.py) deliberately skips.

No Modal / no R2 / no I/O here — just the DuckDB SQL builders + field specs + the robust CSV
parser. Imported by the ``ingest_private_funds`` Modal worker and by local build scripts alike, so
the projection logic has exactly one home.

Two load-bearing findings from the live-source probe drive the design:

  1. These SEC CSVs quote text with ``"`` but DO NOT escape embedded quotes — a Fund Type Other
     free-text cell reads ``"...BOTH "VENTURE CAPITAL FUND" AND..."``. DuckDB's C CSV parser
     desyncs on this and silently corrupts the CLO / venture / credit rows (the highest-value
     ones). Python's stdlib ``csv`` (strict=False) tolerates unescaped inner quotes. So we parse
     with ``csv`` → Arrow (``csv_to_arrow``), NOT read_csv. Genuinely-ragged rows (unbalanced
     quotes, <0.005%) are flagged ``_parse_ok=false`` and retained in ``_raw_line`` — never dropped.

  2. Grain. The 7.B.1 spine is one row per private fund per filing, keyed (FilingID, ReferenceID);
     Fund ID is the persistent 805- identifier. Every child table hangs off (FilingID, ReferenceID)
     and is nested as a LIST<STRUCT>; sub-reference children carry SubreferenceID inside the struct.
     ``build_funds_sql`` serves BOTH grains via ``cf_sql``:
       • current-state → cf = sec_adv_part1 (latest filing per adviser); output = the live fund
         roster, one clean row per current fund (matches sec_adv_part1's own dedup on the same file).
       • full-history  → cf = the complete FilingID→CRD map from the ADV base file; output = every
         fund-filing row across all history (partition by FilingID hash to bound memory).
     crd_number is attached from cf; a lossless ``raw_filing`` JSON is always retained.

Datasets produced (all s3://data-sink/active/):
    sec_adv_private_funds           7.B.1 current-state   (build_funds_sql, current cf)
    sec_adv_private_funds_history   7.B.1 full history    (build_funds_sql, map cf, bucketed)
    sec_adv_private_fund_reliance   7.B.2                 (build_reliance_sql)
    sec_adv_related_persons         7.A current-state     (build_related_persons_sql)
"""
from __future__ import annotations

import csv

# ── ZIP member selection. Basename regexes within adv-filing-data-*-part2.zip. The short key is
#    also the DuckDB view name (t_<short>) the worker registers each parsed table under. ──
PF_MEMBERS: dict[str, str] = {
    "7B1": r"IA_Schedule_D_7B1_\d",
    "7B1A3a": r"IA_Schedule_D_7B1A3a_", "7B1A3b": r"IA_Schedule_D_7B1A3b_",
    "7B1A5": r"IA_Schedule_D_7B1A5_", "7B1A6b": r"IA_Schedule_D_7B1A6b_",
    "7B1A7": r"IA_Schedule_D_7B1A7_", "7B1A7d1": r"IA_Schedule_D_7B1A7d1_",
    "7B1A7d2": r"IA_Schedule_D_7B1A7d2_", "7B1A7f": r"IA_Schedule_D_7B1A7f_",
    "7B1A17b": r"IA_Schedule_D_7B1A17b_", "7B1A18b": r"IA_Schedule_D_7B1A18b_",
    "7B1A22": r"IA_Schedule_D_7B1A22_", "7B1A23": r"IA_Schedule_D_7B1A23_",
    "7B1A24": r"IA_Schedule_D_7B1A24_", "7B1A25": r"IA_Schedule_D_7B1A25_",
    "7B1A26": r"IA_Schedule_D_7B1A26_", "7B1A28": r"IA_Schedule_D_7B1A28_\d",
    "7B1A28_websites": r"IA_Schedule_D_7B1A28_websites_", "7B2": r"IA_Schedule_D_7B2_",
}
RP_MEMBERS: dict[str, str] = {
    "7A": r"IA_Schedule_D_7A_\d", "7A10b": r"IA_Schedule_D_7A10b_", "7A_CIK": r"IA_Schedule_D_7A_CIK_",
}

# ── typed-cell SQL helpers. Optional table prefix disambiguates base-spine columns from
#    equally-named nested child out-cols (e.g. base "Custodians" Y/N vs nested custodians list). ──
def S(c, p=""): return f"nullif(trim({p}\"{c}\"),'')"
def B(c, p=""): return f"CASE upper(nullif(trim({p}\"{c}\"),'')) WHEN 'Y' THEN true WHEN 'N' THEN false ELSE NULL END"
def I(c, p=""): return f"TRY_CAST(nullif(trim({p}\"{c}\"),'') AS BIGINT)"
def F(c, p=""): return f"TRY_CAST(nullif(trim({p}\"{c}\"),'') AS DOUBLE)"
def cell(kind, c, p=""): return {"S": S, "B": B, "I": I, "F": F}[kind](c, p)

# ── 7.B.1 base spine typed fields (out, kind, source header). ──
BASE_FIELDS = [
    ("fund_name", "S", "Fund Name"), ("state", "S", "State"), ("country", "S", "Country"),
    ("excl_3c1", "B", "3(c)(1) Exclusion"), ("excl_3c7", "B", "3(c)(7) Exclusion"),
    ("master_fund", "B", "Master Fund"), ("feeder_fund", "B", "Feeder Fund"),
    ("master_fund_name", "S", "Master Fund Name"), ("master_fund_id", "S", "Master Fund ID"),
    ("fund_of_funds", "B", "Fund of Funds"),
    ("fund_invested_self_or_related", "B", "Fund Invested Self or Related"),
    ("fund_invested_in_securities", "B", "Fund Invested in Securities"),
    ("fund_type", "S", "Fund Type"), ("fund_type_other", "S", "Fund Type Other"),
    ("gross_asset_value", "I", "Gross Asset Value"), ("minimum_investment", "I", "Minimum Investment"),
    ("owners", "I", "Owners"), ("pct_owned_you_or_related", "F", "%Owned You or Related"),
    ("pct_owned_funds", "F", "%Owned Funds"), ("sales_limited", "B", "Sales Limited"),
    ("pct_owned_non_us", "F", "%Owned Non-US"), ("subadviser", "B", "Subadviser"),
    ("other_ias_advise", "B", "Other IAs Advise"), ("clients_solicited", "B", "Clients Solicited"),
    ("percentage_invested", "F", "Percentage Invested"), ("exempt_from_registration", "B", "Exempt from Registration"),
    ("annual_audit", "B", "Annual Audit"), ("gaap", "B", "GAAP"), ("fs_distributed", "B", "FS Distributed"),
    ("unqualified_opinion", "B", "Unqualified Opinion"), ("has_prime_brokers", "B", "Prime Brokers"),
    ("has_custodians", "B", "Custodians"), ("has_administrator", "B", "Administrator"),
    ("pct_assets_valued", "F", "% Assets Valued"), ("has_marketing", "B", "Marketing"),
]

# ── 7.B.1 child tables (out_col, member_key, has_subref, [(field, kind, src)]). ──
CHILDREN = [
    ("gp_names", "7B1A3a", False, [("name", "S", "Name of Partner, etc.")]),
    ("relying_advisers_3b", "7B1A3b", False, [("filing_relying_adviser", "S", "Filing/Relying Adviser")]),
    ("foreign_reg_authorities", "7B1A5", False, [("authority", "S", "Foreign Regulatory Authority")]),
    ("related_funds_6b", "7B1A6b", False, [("fund_name", "S", "Private Fund Name"), ("fund_id", "S", "Fund ID")]),
    ("subfunds", "7B1A7", True, [("fund_name", "S", "Private Fund Name"), ("fund_id", "S", "Fund ID"),
        ("state", "S", "State"), ("country", "S", "Country"), ("excl_3c1", "B", "3(c)(1) Exclusion"),
        ("excl_3c7", "B", "3(c)(7) Exclusion")]),
    ("subfund_gps", "7B1A7d1", True, [("name", "S", "Name of General Partner, etc.")]),
    ("subfund_relying_advisers", "7B1A7d2", True, [("filing_relying_adviser", "S", "Filing/Relying Adviser")]),
    ("subfund_foreign_reg", "7B1A7f", True, [("authority", "S", "Foreign Regulatory Authority")]),
    ("advisers_17b", "7B1A17b", False, [("name", "S", "Name of Adviser"), ("sec_file_number", "S", "SEC File Number"), ("crd_number", "S", "CRD Number")]),
    ("advisers_18b", "7B1A18b", False, [("name", "S", "Name of Adviser"), ("sec_file_number", "S", "SEC File Number"), ("crd_number", "S", "CRD Number")]),
    ("form_d_links", "7B1A22", False, [("form_d_file_number", "S", "Form D File Number")]),
    ("auditors", "7B1A23", False, [("name", "S", "Name of Auditing Firm"), ("city", "S", "City"), ("state", "S", "State"),
        ("country", "S", "Country"), ("independent", "B", "Independent"), ("pcaob_registered", "B", "PCAOB Registered"),
        ("pcaob_number", "S", "PCAOB Number"), ("pcaob_inspected", "B", "PCAOB Inspected")]),
    ("prime_brokers", "7B1A24", False, [("name", "S", "Name of Prime Broker"), ("sec_number", "S", "SEC Number"),
        ("crd_number", "S", "CRD Number"), ("city", "S", "City"), ("state", "S", "State"), ("country", "S", "Country"),
        ("custodian", "S", "Custodian")]),
    ("custodians", "7B1A25", False, [("legal_name", "S", "Legal Name of Custodian"), ("primary_business_name", "S", "Primary Business Name"),
        ("city", "S", "City"), ("state", "S", "State"), ("country", "S", "Country"), ("related_person", "S", "Related Person"),
        ("sec_number", "S", "SEC Number"), ("lei", "S", "Legal Entity Identifier")]),
    ("administrators", "7B1A26", False, [("name", "S", "Name of Administrator"), ("city", "S", "City"), ("state", "S", "State"),
        ("country", "S", "Country"), ("related_person", "S", "Related Person"), ("statements", "S", "Statements"),
        ("who_sends_statements", "S", "Who Sends Statements")]),
    ("marketers", "7B1A28", True, [("related_person", "S", "Related Person"), ("name", "S", "Name of Marketer"),
        ("sec_number", "S", "SEC Number"), ("crd_number", "S", "CRD Number"), ("city", "S", "City"), ("state", "S", "State"),
        ("country", "S", "Country"), ("websites", "S", "Websites")]),
    ("marketer_websites", "7B1A28_websites", True, [("website", "S", "Website Address")]),
]

# ── 7.A related-person spine. 5a-5p = the 16 financial-industry-affiliation type flags (broker-
#    dealer / other-IA / muni-advisor / SB-swap-dealer / major-SB-swap-participant / CPO-or-CTA /
#    FCM / bank-or-thrift / trust-co / accountant / lawyer / insurance / pension-consultant /
#    RE-broker / LP-sponsor / pooled-vehicle-GP), kept as source codes + lossless raw_filing. ──
RP_FIELDS = [
    ("related_person_legal_name", "S", "Legal Name"), ("related_person_business_name", "S", "Business Name"),
    ("related_person_sec_number", "S", "SEC Number or Other"), ("related_person_crd", "S", "CRD Number"),
    ("related_person_type", "S", "Type"),
] + [(f"affil_5{c}", "B", f"5{c}") for c in "abcdefghijklmnop"] + [
    ("control_controlled", "S", "Control/Controlled"), ("common_control", "B", "Common Control"),
    ("custodian", "B", "Custodian"), ("exam_exempt", "B", "Exam Exempt"),
    ("street1", "S", "Street 1"), ("street2", "S", "Street 2"), ("city", "S", "City"), ("state", "S", "State"),
    ("country", "S", "Country"), ("postal_code", "S", "Postal Code"), ("private_residence", "B", "Private Residence"),
    ("ia_exempt", "B", "IA Exempt"), ("exemption", "S", "Exemption"), ("foreign_registered", "B", "Foreign Registered"),
    ("share_persons", "B", "Share Persons"), ("share_location", "B", "Share Location"),
]
RP_CHILDREN = [
    ("foreign_reg_authorities", "7A10b", [("authority", "S", "Foreign Regulatory Authority")]),
    ("related_person_ciks", "7A_CIK", [("cik", "S", "CIK")]),
]

# Default current-state cf: the latest filing per adviser (sec_adv_part1), carrying adviser context.
CF_CURRENT = ("SELECT DISTINCT filing_id, crd_number, filer_type, legal_name AS adviser_legal_name, "
              "regulatory_aum AS adviser_regulatory_aum, lei AS adviser_lei "
              "FROM part1 WHERE filing_id IS NOT NULL")


def _child_cte(src, out, member, has_subref, fields):
    fs = ([("subreference_id", "S", "SubreferenceID")] if has_subref else []) + fields
    packs = ", ".join(f"{fo} := {cell(k, s)}" for fo, k, s in fs)
    return (f'{out}_agg AS (SELECT "FilingID" AS fid, "ReferenceID" AS rid, '
            f"list(struct_pack({packs})) AS {out} FROM {src(member)} "
            f'WHERE "FilingID" IN (SELECT filing_id FROM cf) GROUP BY 1,2)')


def build_funds_sql(src, snapshot, cf_sql=CF_CURRENT):
    """7.B.1 fund projection. ``src(member_key)`` → a table expression (registered view or
    read_parquet). ``cf_sql`` selects the filing set + adviser context (current vs history)."""
    ctes = [f"cf AS ({cf_sql})",
            f"raw AS (SELECT \"FilingID\" AS fid, \"ReferenceID\" AS rid, to_json(t) AS raw_filing FROM {src('7B1')} t)",
            f"base AS (SELECT * FROM {src('7B1')})"]
    ctes += [_child_cte(src, o, m, hs, f) for o, m, hs, f in CHILDREN]
    base_cols = ",\n    ".join(f"{cell(k, s, 'base.')} AS {o}" for o, k, s in BASE_FIELDS)
    nested = ",\n    ".join(f"{o}_agg.{o}" for o, *_ in CHILDREN)
    joins = "\n".join(f"LEFT JOIN {o}_agg ON {o}_agg.fid=base.\"FilingID\" AND {o}_agg.rid=base.\"ReferenceID\""
                      for o, *_ in CHILDREN)
    return ("WITH " + ",\n".join(ctes) + "\nSELECT\n    cf.crd_number, cf.filer_type, cf.adviser_legal_name,\n"
            "    cf.adviser_regulatory_aum, cf.adviser_lei,\n"
            f"    {S('FilingID', 'base.')} AS filing_id,\n    {S('Fund ID', 'base.')} AS fund_id,\n"
            f"    {S('ReferenceID', 'base.')} AS reference_id,\n    {base_cols},\n"
            "    NOT base.\"_parse_ok\" AS is_ragged,\n    base.\"_raw_line\" AS raw_line,\n"
            f"    {nested},\n    raw.raw_filing,\n    'IA_Schedule_D_7B1' AS source_file,\n"
            f"    CAST('{snapshot}' AS DATE) AS snapshot_date,\n    now() AS ingested_at\n"
            "FROM base\n    JOIN cf ON cf.filing_id = base.\"FilingID\"\n"
            "    LEFT JOIN raw ON raw.fid=base.\"FilingID\" AND raw.rid=base.\"ReferenceID\"\n" + joins + "\n")


def build_reliance_sql(src, snapshot, cf_sql=CF_CURRENT):
    """7.B.2 — private funds reported in reliance on another (umbrella) adviser's registration."""
    return ("WITH cf AS (" + cf_sql + ")\nSELECT cf.crd_number, cf.filer_type, cf.adviser_legal_name,\n"
            f"    {S('FilingID')} AS filing_id, {S('Fund ID')} AS fund_id, {S('Fund Name')} AS fund_name,\n"
            f"    {S('Adviser Name')} AS reliance_adviser_name, {S('Adviser SEC Number')} AS reliance_adviser_sec_number,\n"
            f"    {B('Clients Solicited?')} AS clients_solicited,\n"
            "    NOT b.\"_parse_ok\" AS is_ragged, b.\"_raw_line\" AS raw_line,\n"
            f"    'IA_Schedule_D_7B2' AS source_file, CAST('{snapshot}' AS DATE) AS snapshot_date, now() AS ingested_at\n"
            f"FROM {src('7B2')} b\n    JOIN cf ON cf.filing_id = b.\"FilingID\"\n")


def build_related_persons_sql(src, snapshot, cf_sql=None):
    """7.A financial-industry affiliations / related persons (spine + nested foreign-reg + CIK)."""
    cf_sql = cf_sql or ("SELECT DISTINCT filing_id, crd_number, filer_type, legal_name AS adviser_legal_name "
                        "FROM part1 WHERE filing_id IS NOT NULL")
    def rp_child(out, member, fields):
        packs = ", ".join(f"{fo} := {cell(k, s)}" for fo, k, s in fields)
        return (f'{out}_agg AS (SELECT "FilingID" AS fid, "ReferenceID" AS rid, list(struct_pack({packs})) AS {out} '
                f'FROM {src(member)} WHERE "FilingID" IN (SELECT filing_id FROM cf) GROUP BY 1,2)')
    ctes = [f"cf AS ({cf_sql})",
            f"raw AS (SELECT \"FilingID\" AS fid, \"ReferenceID\" AS rid, to_json(t) AS raw_filing FROM {src('7A')} t)",
            f"base AS (SELECT * FROM {src('7A')})"] + [rp_child(*c) for c in RP_CHILDREN]
    base_cols = ",\n    ".join(f"{cell(k, s, 'base.')} AS {o}" for o, k, s in RP_FIELDS)
    nested = ",\n    ".join(f"{o}_agg.{o}" for o, *_ in RP_CHILDREN)
    joins = "\n".join(f"LEFT JOIN {o}_agg ON {o}_agg.fid=base.\"FilingID\" AND {o}_agg.rid=base.\"ReferenceID\""
                      for o, *_ in RP_CHILDREN)
    return ("WITH " + ",\n".join(ctes) + "\nSELECT\n    cf.crd_number, cf.filer_type, cf.adviser_legal_name,\n"
            f"    {S('FilingID', 'base.')} AS filing_id,\n    {S('ReferenceID', 'base.')} AS reference_id,\n"
            f"    {base_cols},\n    NOT base.\"_parse_ok\" AS is_ragged, base.\"_raw_line\" AS raw_line,\n"
            f"    {nested},\n    raw.raw_filing,\n    'IA_Schedule_D_7A' AS source_file,\n"
            f"    CAST('{snapshot}' AS DATE) AS snapshot_date, now() AS ingested_at\n"
            "FROM base\n    JOIN cf ON cf.filing_id = base.\"FilingID\"\n"
            "    LEFT JOIN raw ON raw.fid=base.\"FilingID\" AND raw.rid=base.\"ReferenceID\"\n" + joins + "\n")


# Index plans (BTREE = resolution / join keys; BITMAP = low-cardinality categoricals).
INDEX_PLAN = {
    "sec_adv_private_funds":         {"btree": ["crd_number", "fund_id", "filing_id", "reference_id"],
                                      "bitmap": ["fund_type", "filer_type", "state"]},
    "sec_adv_private_funds_history": {"btree": ["crd_number", "fund_id", "filing_id", "reference_id"],
                                      "bitmap": ["fund_type", "state"]},
    "sec_adv_private_fund_reliance": {"btree": ["crd_number", "fund_id", "filing_id"], "bitmap": ["filer_type"]},
    "sec_adv_related_persons":       {"btree": ["crd_number", "related_person_crd", "filing_id", "reference_id"],
                                      "bitmap": ["state", "related_person_type"]},
}


def csv_to_arrow(path, batch_rows=400_000):
    """Robust parse of one transcoded (cp1252→utf8) SEC CSV → pyarrow.Table. Python ``csv``
    (strict=False) tolerates the unescaped inner quotes DuckDB desyncs on. Every source row →
    exactly one output row (row-count parity). All columns kept as strings + ``_parse_ok`` +
    ``_raw_line`` (populated only for the rare ragged row). Yields via a RecordBatchReader-friendly
    list of batches to bound memory on the 4M-row custodian table."""
    import pyarrow as pa

    csv.field_size_limit(50_000_000)
    batches = []
    with open(path, newline="", encoding="utf-8") as fh:
        r = csv.reader(fh)
        hdr = next(r)
        nc = len(hdr)
        names = hdr + ["_parse_ok", "_raw_line"]
        buf, okb, rawb = [], [], []

        def _flush():
            if not buf:
                return
            arrays = [pa.array([row[i] for row in buf], type=pa.string()) for i in range(nc)]
            arrays.append(pa.array(okb, type=pa.bool_()))
            arrays.append(pa.array(rawb, type=pa.string()))
            batches.append(pa.RecordBatch.from_arrays(arrays, names=names))
            buf.clear(); okb.clear(); rawb.clear()

        for row in r:
            L = len(row)
            if L == nc:
                vals, ok, raw = row, True, None
            elif L < nc:
                vals, ok, raw = row + [""] * (nc - L), False, "|".join(row)
            else:
                vals, ok, raw = row[: nc - 1] + [",".join(row[nc - 1:])], False, "|".join(row)
            buf.append(vals); okb.append(ok); rawb.append(raw)
            if len(buf) >= batch_rows:
                _flush()
        _flush()
    if not batches:  # header-only file
        schema = pa.schema([(h, pa.string()) for h in hdr] + [("_parse_ok", pa.bool_()), ("_raw_line", pa.string())])
        return pa.table({n: pa.array([], type=t) for n, t in zip(schema.names, schema.types)})
    return pa.Table.from_batches(batches)
