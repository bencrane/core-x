"""Compute worker — DLA DIBBS Solicitation PDF + Index + Approved Maker parser (Phase 2).

Extracts structured solicitation metadata, Section B technical/quality/packaging/inspection
requirements, Approved Maker CAGE + Part Numbers (merging ``as{yymmdd}.txt`` and Section B),
and the full 20-year ``Procurement History`` table from every daily ``ca{yymmdd}.zip`` archive
stored in R2 (`raw/dibbs_rfq_daily/solicitation_zip/...`).

Engine: PyMuPDF (`pymupdf`) in-memory stream parser with coordinate-sorted word-line
reconstruction and dynamic early exit at the end of Section B (skipping boilerplate FAR/DFARS
pages in Sections C–M).

Also materializes the 3 Gen-3 Lance SoR datasets at:
  - s3://data-sink/active/dibbs_solicitation_items/   (BTREE on nsn, solicitation_number)
  - s3://data-sink/active/dibbs_approved_makers/      (BTREE on nsn, maker_cage)
  - s3://data-sink/active/dibbs_procurement_history/  (BTREE on nsn, winner_cage)

    modal deploy pipelines/dibbs/parse_rfq_pdfs.py
    # Spawn full backfill across all captured days in R2:
    #   modal.Function.from_name("dibbs-pdf-parser", "backfill_all_days").spawn()
"""

from __future__ import annotations

import datetime as dt
import io
import os
import random
import re
import shutil
import tempfile
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import modal

BUCKET = "data-sink"

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dibbs;

CREATE TABLE IF NOT EXISTS ops.dibbs_pdf_parse_runs (
    post_date DATE PRIMARY KEY,
    ca_r2_key TEXT NOT NULL,
    total_pdfs INT NOT NULL,
    parsed_pdfs INT NOT NULL,
    solicitations_ingested INT NOT NULL,
    history_rows_ingested INT NOT NULL,
    approved_makers_ingested INT NOT NULL,
    anomaly_count INT NOT NULL,
    duration_s NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'error')),
    detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dibbs.solicitation_items (
    post_date DATE NOT NULL,
    solicitation_number TEXT NOT NULL,
    pr_number TEXT NOT NULL,
    nsn TEXT NOT NULL,
    fsc TEXT,
    nomenclature TEXT,
    quantity NUMERIC,
    unit_of_issue TEXT,
    return_date DATE,
    buyer_code_index TEXT,
    amsc TEXT,
    amc TEXT,
    set_aside TEXT,
    pdf_name TEXT,
    naics_code TEXT,
    buyer_name TEXT,
    buyer_code TEXT,
    buyer_email TEXT,
    buyer_phone TEXT,
    delivery_days INT,
    fob_point TEXT,
    inspection_point TEXT,
    acceptance_point TEXT,
    packaging_standard TEXT,
    has_additive_mfg_ban BOOLEAN,
    has_fat BOOLEAN,
    has_csi BOOLEAN,
    has_higher_quality BOOLEAN,
    has_cfolders BOOLEAN,
    has_nmr_waived BOOLEAN,
    no_history_available BOOLEAN,
    history_row_count INT,
    item_description TEXT,
    PRIMARY KEY (post_date, solicitation_number, pr_number)
);

CREATE INDEX IF NOT EXISTS idx_dibbs_sol_items_nsn ON dibbs.solicitation_items (nsn);
CREATE INDEX IF NOT EXISTS idx_dibbs_sol_items_amsc ON dibbs.solicitation_items (amsc);
CREATE INDEX IF NOT EXISTS idx_dibbs_sol_items_insp ON dibbs.solicitation_items (inspection_point);

CREATE TABLE IF NOT EXISTS dibbs.approved_makers (
    nsn TEXT NOT NULL,
    maker_cage TEXT NOT NULL,
    part_number TEXT NOT NULL,
    maker_name TEXT,
    first_seen_date DATE NOT NULL,
    last_seen_date DATE NOT NULL,
    PRIMARY KEY (nsn, maker_cage, part_number)
);

CREATE INDEX IF NOT EXISTS idx_dibbs_approved_makers_cage ON dibbs.approved_makers (maker_cage);
CREATE INDEX IF NOT EXISTS idx_dibbs_approved_makers_nsn ON dibbs.approved_makers (nsn);

CREATE TABLE IF NOT EXISTS dibbs.procurement_history (
    nsn TEXT NOT NULL,
    fsc TEXT,
    contract_number TEXT NOT NULL,
    winner_cage TEXT NOT NULL,
    award_date DATE NOT NULL,
    quantity NUMERIC NOT NULL,
    unit_cost NUMERIC NOT NULL,
    total_cost NUMERIC NOT NULL,
    surplus_material BOOLEAN NOT NULL,
    source_post_date DATE NOT NULL,
    source_pdf TEXT NOT NULL,
    PRIMARY KEY (nsn, contract_number)
);

CREATE INDEX IF NOT EXISTS idx_dibbs_proc_hist_winner ON dibbs.procurement_history (winner_cage);
CREATE INDEX IF NOT EXISTS idx_dibbs_proc_hist_nsn ON dibbs.procurement_history (nsn);
CREATE INDEX IF NOT EXISTS idx_dibbs_proc_hist_date ON dibbs.procurement_history (award_date);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "pymupdf>=1.24",
    "boto3>=1.34",
    "psycopg[binary]>=3.2",
    "pyarrow>=17.0",
    "duckdb>=1.1",
    "pylance>=1.0",
)

app = modal.App("dibbs-pdf-parser", image=image)

# ── Regexes for deterministic SF-18 / DLA Solicitation extraction ─────────────────
RE_HIST_HDR = re.compile(r"Procurement History for NSN/FSC:\s*([0-9A-Z]+)/([0-9A-Z]+)")
RE_HIST_ROW = re.compile(
    r"^([A-Z0-9]{5})\s+([A-Z0-9\-]{10,25})\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s+(\d{8})\s+([YN])\b"
)
RE_NAICS = re.compile(r"NORTH AMERICAN INDUSTRY CLASSIFICATION SYSTEM\s+(\d{6})")
RE_BUYER = re.compile(
    r"Name:\s*(.+?)\s+Buyer Code:\s*([A-Z0-9]+)(?:\s+Tel:\s*(.+?))?(?:\s+Fax:.*)?$",
    re.M,
)
RE_EMAIL = re.compile(r"Email:\s*([^\s]+@[^\s]+)")
RE_DELIV_DAYS = re.compile(r"DELIVERY \(IN DAYS\):\s*0*(\d+)")
RE_INSP = re.compile(r"INSPECTION POINT:\s*([A-Z]+)")
RE_ACCPT = re.compile(r"ACCEPTANCE POINT:\s*([A-Z]+)")
RE_FOB = re.compile(r"DELIVER FOB:\s*([A-Z]+)")
RE_CAGE_PN = re.compile(
    r"^([A-Z0-9][A-Z0-9\s\.\,\-\&\/\'\"()]{2,60}?)\s+([A-Z0-9]{5})\s+P/N\s+(.+)$"
)


def _page_lines(page, ytol: float = 2.0) -> list[str]:
    words = page.get_text("words")
    words.sort(key=lambda w: ((w[1] + w[3]) / 2.0, w[0]))
    lines: list[list] = []
    cur: list = []
    cury: float | None = None
    for w in words:
        ym = (w[1] + w[3]) / 2.0
        if cury is None or abs(ym - cury) <= ytol:
            cur.append(w)
            if cury is None:
                cury = ym
        else:
            lines.append(cur)
            cur = [w]
            cury = ym
    if cur:
        lines.append(cur)
    return [" ".join(x[4] for x in sorted(l, key=lambda w: w[0])) for l in lines]


def extract_one_pdf(args: tuple[str, bytes]) -> dict:
    """Worker function: parse a single PDF in memory via PyMuPDF."""
    import pymupdf

    pymupdf.TOOLS.mupdf_display_errors(False)
    pymupdf.TOOLS.mupdf_display_warnings(False)

    pdf_name, pdf_bytes = args
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    seen_b = False
    all_lines: list[str] = []
    sec_b_lines: list[str] = []
    hist_rows: list[tuple] = []
    has_hist_hdr = False
    no_hist = False
    cur_fsc: str | None = None
    cur_nsn_9: str | None = None

    for pno, page in enumerate(doc):
        raw = page.get_text("text")
        if "SECTION B" in raw:
            seen_b = True
        elif seen_b and (
            "SECTION F" in raw
            or "SECTION E" in raw
            or "SECTION D" in raw
            or "SECTION C" in raw
        ):
            if "SECTION B" not in raw:
                break

        if (
            pno == 0
            or "Procurement History" in raw
            or "NORTH AMERICAN INDUSTRY" in raw
            or "SECTION B" in raw
            or seen_b
        ):
            ls = _page_lines(page)
            all_lines.extend(ls)
            if seen_b:
                sec_b_lines.extend(ls)
            if "Procurement History" in raw or has_hist_hdr:
                if "Procurement History" in raw:
                    has_hist_hdr = True
                    hm = RE_HIST_HDR.search(raw)
                    if hm:
                        cur_nsn_9, cur_fsc = hm.group(1).strip(), hm.group(2).strip()
                if "No History Available" in raw:
                    no_hist = True
                for l in ls:
                    m = RE_HIST_ROW.match(l)
                    if m:
                        c, contr, q, u, d, s = m.groups()
                        try:
                            qv = float(q.replace(",", ""))
                            uv = float(u.replace(",", ""))
                            full_nsn = (
                                f"{cur_fsc}{cur_nsn_9}"
                                if (cur_fsc and cur_nsn_9 and len(cur_fsc) == 4 and len(cur_nsn_9) == 9)
                                else ""
                            )
                            hist_rows.append(
                                (full_nsn, cur_fsc or "", contr, c, d, qv, uv, round(qv * uv, 2), s == "Y")
                            )
                        except ValueError:
                            pass
        else:
            all_lines.extend(raw.splitlines())

    full_txt = "\n".join(all_lines)
    sec_b_txt = "\n".join(sec_b_lines)
    full_upper = full_txt.upper()
    sec_b_upper = sec_b_txt.upper()

    bm = RE_BUYER.search(full_txt)
    buyer_name = bm.group(1).strip() if bm else None
    buyer_code = bm.group(2).strip() if bm else None
    buyer_phone = bm.group(3).strip() if (bm and bm.group(3)) else None
    em = RE_EMAIL.search(full_txt)
    buyer_email = em.group(1).strip() if em else None
    nm = RE_NAICS.search(full_txt)
    naics = nm.group(1) if nm else None

    dm = RE_DELIV_DAYS.search(sec_b_txt)
    deliv_days = int(dm.group(1)) if dm else None
    im = RE_INSP.search(sec_b_txt)
    insp = im.group(1) if im else None
    am = RE_ACCPT.search(sec_b_txt)
    accpt = am.group(1) if am else None
    fm = RE_FOB.search(sec_b_txt)
    fob = fm.group(1) if fm else None

    if "MIL-STD-2073" in sec_b_upper:
        pkg = "MIL-STD-2073"
    elif "ASTM D3951" in sec_b_upper:
        pkg = "ASTM D3951"
    elif "MMS NO. 1" in sec_b_upper or "MEDICAL MARKING" in sec_b_upper:
        pkg = "MMS NO. 1"
    else:
        pkg = "OTHER"

    has_l31_am_ban = ("L31 ADDITIVE MANUFACTURING" in full_upper) or (
        "MADE USING THE ADDITIVE" in full_upper
    )
    has_fat = ("FIRST ARTICLE" in sec_b_upper) or ("FAT " in sec_b_upper)
    has_csi = ("CRITICAL SAFETY ITEM" in full_upper) or (
        "CRITICAL APPLICATION ITEM" in full_upper
    )
    has_higher_quality = (
        ("HIGHER-LEVEL CONTRACT QUALITY" in full_upper)
        or ("ISO 9001" in full_upper)
        or ("AS9100" in full_upper)
        or ("RQ001" in sec_b_upper)
        or ("RQ002" in sec_b_upper)
        or ("RQ009" in sec_b_upper)
    )
    has_cfolders = ("PCF1X.BSM.DLA.MIL/CFOLDERS" in full_upper) or (
        "CFOLDERS" in full_upper
    )
    has_nmr_waived = ("NON-MANUFACTURER RULE IS WAIVED" in full_upper) or (
        "NMR WAIVED" in full_upper
    )

    desc_lines: list[str] = []
    in_desc = False
    for l in sec_b_lines:
        ls_clean = l.strip()
        if ls_clean == "ITEM DESCRIPTION":
            in_desc = True
            continue
        if in_desc:
            if ls_clean.startswith(
                (
                    "RA001:",
                    "RP001:",
                    "RQ0",
                    "RD0",
                    "CLIN ",
                    "MERCURY OR",
                    "APPLICABLE TO ALL",
                    "ADEQUATE DATA FOR",
                    "CRITICAL APPLICATION",
                    "CRITICAL SAFETY",
                )
            ):
                break
            if ls_clean and ls_clean != ".":
                desc_lines.append(ls_clean)
            if len(desc_lines) >= 12:
                break

    makers: list[tuple[str, str, str]] = []
    for l in sec_b_lines:
        mm = RE_CAGE_PN.match(l.strip())
        if mm:
            cname, ccage, cpn = mm.groups()
            cname = cname.strip()
            for prefix in (
                "CRITICAL APPLICATION ITEM",
                "EVALUATE ALTERNATE OFFERS.",
                "GOVERNMENT DESIGN CONTROL ACTIVITY.",
                "PERMITTED IN THIS ITEM",
            ):
                cname = cname.replace(prefix, "").strip()
            if len(cname) >= 2 and not cname.startswith(("CLIN", "NSN", "CONTINUE", "PAGE ")):
                makers.append((ccage.strip(), cpn.strip(), cname))

    anomaly = has_hist_hdr and (not no_hist) and (len(hist_rows) == 0)

    return {
        "pdf": pdf_name.upper(),
        "naics": naics,
        "buyer_name": buyer_name,
        "buyer_code": buyer_code,
        "buyer_email": buyer_email,
        "buyer_phone": buyer_phone,
        "deliv_days": deliv_days,
        "fob": fob,
        "insp": insp,
        "accpt": accpt,
        "pkg": pkg,
        "has_l31_am_ban": has_l31_am_ban,
        "has_fat": has_fat,
        "has_csi": has_csi,
        "has_higher_quality": has_higher_quality,
        "has_cfolders": has_cfolders,
        "has_nmr_waived": has_nmr_waived,
        "no_hist": no_hist,
        "hist_rows": hist_rows,
        "makers": makers,
        "desc": " | ".join(desc_lines),
        "anomaly": anomaly,
    }


def _s3_client():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _pg_conn():
    import psycopg

    return psycopg.connect(os.environ["HQX_DB_URL_POOLED"])


def _parse_index_text(txt: str) -> dict[str, list[dict]]:
    by_pdf: dict[str, list[dict]] = defaultdict(list)
    for line in txt.splitlines():
        if len(line) < 137:
            continue
        sol = line[0:13].strip()
        nsn = line[13:58].strip().replace("-", "")
        pr = line[62:72].strip()
        ret_str = line[72:80].strip()
        pdf = line[80:99].strip().upper()
        qty_str = line[99:106].strip()
        qty = int(qty_str) if qty_str.isdigit() else 0
        uoi = line[106:108].strip()
        nom = line[108:129].strip()
        buyer = line[129:134].strip()
        amsc = line[134].strip()
        amc = line[135].strip()
        set_aside = line[136].strip()
        ret_date = None
        if len(ret_str) == 8 and ret_str[2] == "/" and ret_str[5] == "/":
            try:
                mm, dd, yy = int(ret_str[0:2]), int(ret_str[3:5]), int(ret_str[6:8])
                ret_date = dt.date(2000 + yy, mm, dd)
            except ValueError:
                ret_date = None
        by_pdf[pdf].append(
            {
                "sol": sol,
                "nsn": nsn,
                "fsc": nsn[:4] if len(nsn) == 13 else "",
                "pr": pr,
                "ret_date": ret_date,
                "pdf": pdf,
                "qty": qty,
                "uoi": uoi,
                "nom": nom,
                "buyer": buyer,
                "amsc": amsc,
                "amc": amc,
                "set_aside": set_aside,
            }
        )
    return by_pdf


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    cpu=4.0,
    memory=4096,
    max_containers=20,
    timeout=1800,
)
def parse_one_day(day_info: dict) -> dict:
    """Download & parse one day's (ca_zip, in_txt, bq_zip) from R2 and bulk-COPY to Postgres."""
    t0 = time.time()
    post_date = dt.date.fromisoformat(day_info["post_date"])
    ca_key = day_info["ca_key"]
    in_key = day_info.get("in_key")
    bq_key = day_info.get("bq_key")

    s3 = _s3_client()

    # 1. Read index file
    by_pdf: dict[str, list[dict]] = {}
    if in_key:
        try:
            in_bytes = s3.get_object(Bucket=BUCKET, Key=in_key)["Body"].read()
            by_pdf = _parse_index_text(in_bytes.decode("latin-1", errors="replace"))
        except Exception as exc:
            print(f"[{post_date}] WARN reading index {in_key}: {exc}")

    # 2. Read approved makers from bq*.zip -> as*.txt
    as_makers: dict[tuple[str, str, str], str | None] = {}
    if bq_key:
        try:
            bq_bytes = s3.get_object(Bucket=BUCKET, Key=bq_key)["Body"].read()
            with zipfile.ZipFile(io.BytesIO(bq_bytes)) as zf:
                for name in zf.namelist():
                    if name.lower().startswith("as") and name.lower().endswith(".txt"):
                        for raw in zf.read(name).decode("latin-1", errors="replace").splitlines():
                            parts = [p.strip().strip('"') for p in raw.split(",")]
                            if len(parts) >= 3:
                                nsn = parts[0].replace("-", "").strip()
                                cage = parts[1].strip()
                                pn = parts[2].strip()
                                if len(nsn) == 13 and len(cage) == 5 and pn:
                                    as_makers[(nsn, cage, pn)] = None
        except Exception as exc:
            print(f"[{post_date}] WARN reading bq {bq_key}: {exc}")

    # 3. Download ca*.zip and parse all PDFs across 4 cores
    ca_bytes = s3.get_object(Bucket=BUCKET, Key=ca_key)["Body"].read()
    pdf_tasks: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(ca_bytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".pdf"):
                pdf_tasks.append((os.path.basename(name).upper(), zf.read(name)))

    total_pdfs = len(pdf_tasks)
    parsed_results: list[dict] = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        for res in pool.map(extract_one_pdf, pdf_tasks, chunksize=32):
            parsed_results.append(res)

    # 4. Assemble rows for bulk Postgres COPY + deterministic-order upsert
    sol_rows: list[tuple] = []
    hist_map: dict[tuple[str, str], tuple] = {}
    anomaly_count = 0

    for p in parsed_results:
        if p["anomaly"]:
            anomaly_count += 1
        pdf_name = p["pdf"]
        idx_items = by_pdf.get(pdf_name, [])
        primary_nsn = idx_items[0]["nsn"] if idx_items else ""
        primary_fsc = idx_items[0]["fsc"] if idx_items else ""

        if primary_nsn:
            for ccage, cpn, cname in p["makers"]:
                if len(ccage) == 5 and cpn:
                    as_makers[(primary_nsn, ccage, cpn)] = cname

        for h_nsn, h_fsc, contr, wcage, d_str, qv, uv, tv, surp in p["hist_rows"]:
            nsn_use = h_nsn or primary_nsn
            fsc_use = h_fsc or primary_fsc or (nsn_use[:4] if len(nsn_use) == 13 else "")
            if not nsn_use or not contr or len(d_str) != 8:
                continue
            try:
                awd_dt = dt.date(int(d_str[0:4]), int(d_str[4:6]), int(d_str[6:8]))
            except ValueError:
                continue
            hist_map[(nsn_use, contr)] = (
                nsn_use,
                fsc_use,
                contr,
                wcage,
                awd_dt,
                qv,
                uv,
                tv,
                surp,
                post_date,
                pdf_name,
            )

        if idx_items:
            for item in idx_items:
                sol_rows.append(
                    (
                        post_date,
                        item["sol"],
                        item["pr"],
                        item["nsn"],
                        item["fsc"],
                        item["nom"],
                        item["qty"],
                        item["uoi"],
                        item["ret_date"],
                        item["buyer"],
                        item["amsc"],
                        item["amc"],
                        item["set_aside"],
                        pdf_name,
                        p["naics"],
                        p["buyer_name"],
                        p["buyer_code"],
                        p["buyer_email"],
                        p["buyer_phone"],
                        p["deliv_days"],
                        p["fob"],
                        p["insp"],
                        p["accpt"],
                        p["pkg"],
                        p["has_l31_am_ban"],
                        p["has_fat"],
                        p["has_csi"],
                        p["has_higher_quality"],
                        p["has_cfolders"],
                        p["has_nmr_waived"],
                        p["no_hist"],
                        len(p["hist_rows"]),
                        p["desc"],
                    )
                )
        else:
            sol_num = pdf_name.replace(".PDF", "")
            sol_rows.append(
                (
                    post_date,
                    sol_num,
                    "0000000000",
                    primary_nsn,
                    primary_fsc,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    pdf_name,
                    p["naics"],
                    p["buyer_name"],
                    p["buyer_code"],
                    p["buyer_email"],
                    p["buyer_phone"],
                    p["deliv_days"],
                    p["fob"],
                    p["insp"],
                    p["accpt"],
                    p["pkg"],
                    p["has_l31_am_ban"],
                    p["has_fat"],
                    p["has_csi"],
                    p["has_higher_quality"],
                    p["has_cfolders"],
                    p["has_nmr_waived"],
                    p["no_hist"],
                    len(p["hist_rows"]),
                    p["desc"],
                )
            )

    # Sort all 3 collections by PK to prevent lock-order deadlocks across parallel workers
    maker_rows = sorted(
        [
            (nsn, cage, pn, cname, post_date, post_date)
            for (nsn, cage, pn), cname in as_makers.items()
        ],
        key=lambda r: (r[0], r[1], r[2]),
    )
    hist_rows_dedup = sorted(hist_map.values(), key=lambda r: (r[0], r[2]))
    sol_dedup_map = {(r[0], r[1], r[2]): r for r in sol_rows}
    sol_rows_dedup = sorted(sol_dedup_map.values(), key=lambda r: (r[0], r[1], r[2]))

    dur = round(time.time() - t0, 2)

    for attempt in range(6):
        conn = None
        try:
            conn = _pg_conn()
            with conn.cursor() as cur:
                if sol_rows_dedup:
                    cur.execute(
                        "CREATE TEMP TABLE _tmp_sol (LIKE dibbs.solicitation_items INCLUDING DEFAULTS) ON COMMIT DROP"
                    )
                    with cur.copy(
                        """
                        COPY _tmp_sol (
                            post_date, solicitation_number, pr_number, nsn, fsc, nomenclature,
                            quantity, unit_of_issue, return_date, buyer_code_index, amsc, amc,
                            set_aside, pdf_name, naics_code, buyer_name, buyer_code, buyer_email,
                            buyer_phone, delivery_days, fob_point, inspection_point, acceptance_point,
                            packaging_standard, has_additive_mfg_ban, has_fat, has_csi,
                            has_higher_quality, has_cfolders, has_nmr_waived, no_history_available,
                            history_row_count, item_description
                        ) FROM STDIN
                        """
                    ) as copy:
                        for row in sol_rows_dedup:
                            copy.write_row(row)
                    cur.execute(
                        """
                        INSERT INTO dibbs.solicitation_items
                        SELECT * FROM _tmp_sol
                        ORDER BY post_date, solicitation_number, pr_number
                        ON CONFLICT (post_date, solicitation_number, pr_number) DO UPDATE SET
                            naics_code = EXCLUDED.naics_code,
                            buyer_name = EXCLUDED.buyer_name,
                            buyer_email = EXCLUDED.buyer_email,
                            delivery_days = EXCLUDED.delivery_days,
                            inspection_point = EXCLUDED.inspection_point,
                            packaging_standard = EXCLUDED.packaging_standard,
                            has_fat = EXCLUDED.has_fat,
                            has_csi = EXCLUDED.has_csi,
                            no_history_available = EXCLUDED.no_history_available,
                            history_row_count = EXCLUDED.history_row_count,
                            item_description = EXCLUDED.item_description
                        """
                    )

                if maker_rows:
                    cur.execute(
                        "CREATE TEMP TABLE _tmp_makers (LIKE dibbs.approved_makers INCLUDING DEFAULTS) ON COMMIT DROP"
                    )
                    with cur.copy(
                        "COPY _tmp_makers (nsn, maker_cage, part_number, maker_name, first_seen_date, last_seen_date) FROM STDIN"
                    ) as copy:
                        for row in maker_rows:
                            copy.write_row(row)
                    cur.execute(
                        """
                        INSERT INTO dibbs.approved_makers
                        SELECT * FROM _tmp_makers
                        ORDER BY nsn, maker_cage, part_number
                        ON CONFLICT (nsn, maker_cage, part_number) DO UPDATE SET
                            maker_name = COALESCE(EXCLUDED.maker_name, dibbs.approved_makers.maker_name),
                            first_seen_date = LEAST(EXCLUDED.first_seen_date, dibbs.approved_makers.first_seen_date),
                            last_seen_date = GREATEST(EXCLUDED.last_seen_date, dibbs.approved_makers.last_seen_date)
                        """
                    )

                if hist_rows_dedup:
                    cur.execute(
                        "CREATE TEMP TABLE _tmp_hist (LIKE dibbs.procurement_history INCLUDING DEFAULTS) ON COMMIT DROP"
                    )
                    with cur.copy(
                        """
                        COPY _tmp_hist (
                            nsn, fsc, contract_number, winner_cage, award_date,
                            quantity, unit_cost, total_cost, surplus_material,
                            source_post_date, source_pdf
                        ) FROM STDIN
                        """
                    ) as copy:
                        for row in hist_rows_dedup:
                            copy.write_row(row)
                    cur.execute(
                        """
                        INSERT INTO dibbs.procurement_history
                        SELECT * FROM _tmp_hist
                        ORDER BY nsn, contract_number
                        ON CONFLICT (nsn, contract_number) DO UPDATE SET
                            source_post_date = GREATEST(EXCLUDED.source_post_date, dibbs.procurement_history.source_post_date)
                        """
                    )

                cur.execute(
                    """
                    INSERT INTO ops.dibbs_pdf_parse_runs (
                        post_date, ca_r2_key, total_pdfs, parsed_pdfs, solicitations_ingested,
                        history_rows_ingested, approved_makers_ingested, anomaly_count,
                        duration_s, status, detail, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ok', 'ok', now())
                    ON CONFLICT (post_date) DO UPDATE SET
                        ca_r2_key = EXCLUDED.ca_r2_key,
                        total_pdfs = EXCLUDED.total_pdfs,
                        parsed_pdfs = EXCLUDED.parsed_pdfs,
                        solicitations_ingested = EXCLUDED.solicitations_ingested,
                        history_rows_ingested = EXCLUDED.history_rows_ingested,
                        approved_makers_ingested = EXCLUDED.approved_makers_ingested,
                        anomaly_count = EXCLUDED.anomaly_count,
                        duration_s = EXCLUDED.duration_s,
                        status = 'ok',
                        detail = 'ok',
                        updated_at = now()
                    """,
                    (
                        post_date,
                        ca_key,
                        total_pdfs,
                        len(parsed_results),
                        len(sol_rows_dedup),
                        len(hist_rows_dedup),
                        len(maker_rows),
                        anomaly_count,
                        dur,
                    ),
                )
            conn.commit()
            conn.close()
            break
        except Exception as exc:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            if attempt == 5:
                raise
            sleep_s = (attempt + 1) * 1.5 + random.uniform(0.1, 1.0)
            print(f"[{post_date}] DB upsert retry {attempt + 1}/6 after {exc}; sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

    res_summary = {
        "post_date": str(post_date),
        "total_pdfs": total_pdfs,
        "solicitations": len(sol_rows_dedup),
        "history_rows": len(hist_rows_dedup),
        "makers": len(maker_rows),
        "anomalies": anomaly_count,
        "duration_s": round(time.time() - t0, 2),
    }
    print(f"Completed {res_summary}")
    return res_summary


def _publish_lance_table(table_sql: str, uri: str, indices: list[tuple[str, str]]) -> int:
    """Materialize a Postgres query to a Lance dataset on R2 with scalar BTREE indices."""
    import lance
    import pyarrow as pa

    s3 = _s3_client()
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(table_sql)
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    conn.close()

    if not rows:
        return 0

    col_arrays = [list(col) for col in zip(*rows)]
    pa_table = pa.Table.from_pydict(dict(zip(cols, col_arrays)))

    prefix = uri.replace(f"s3://{BUCKET}/", "")
    stage = tempfile.mkdtemp(prefix="lance_dibbs_", dir="/tmp")
    local_ds = os.path.join(stage, "ds")
    try:
        lance.write_dataset(pa_table, local_ds, mode="create")
        ds = lance.dataset(local_ds)
        for col, kind in indices:
            ds.create_scalar_index(col, kind)

        keys: list[dict] = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            keys.extend({"Key": o["Key"]} for o in page.get("Contents", []))
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys[i : i + 1000], "Quiet": True})
        for root, _dirs, files in os.walk(local_ds):
            for f in files:
                lp = os.path.join(root, f)
                rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
                s3.upload_file(lp, BUCKET, prefix + rel)
        return len(rows)
    finally:
        shutil.rmtree(stage, ignore_errors=True)


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=4 * 60 * 60,
    memory=8192,
    cpu=2.0,
)
def backfill_all_days(force: bool = False, publish_lance: bool = True) -> dict:
    """Coordinate parallel parsing across all captured days in R2 and publish Lance datasets."""
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT post_date, stream, r2_key
            FROM ops.dibbs_rfq_daily_r2_ingest_runs
            WHERE status = 'ok'
              AND stream IN ('solicitation_zip', 'index', 'batch_quote_zip')
            ORDER BY post_date
            """
        )
        rows = cur.fetchall()

        cur.execute(
            "SELECT post_date FROM ops.dibbs_pdf_parse_runs WHERE status = 'ok'"
        )
        done_dates = {r[0] for r in cur.fetchall()}
    conn.close()

    by_date: dict[dt.date, dict[str, str]] = defaultdict(dict)
    for pdate, stream, r2_key in rows:
        by_date[pdate][stream] = r2_key

    tasks: list[dict] = []
    for pdate in sorted(by_date):
        streams = by_date[pdate]
        if "solicitation_zip" not in streams:
            continue
        if not force and pdate in done_dates:
            continue
        tasks.append(
            {
                "post_date": pdate.isoformat(),
                "ca_key": streams["solicitation_zip"],
                "in_key": streams.get("index"),
                "bq_key": streams.get("batch_quote_zip"),
            }
        )

    print(f"Found {len(by_date)} total solicitation_zip days; {len(tasks)} pending parse.")
    ok_cnt = 0
    err_cnt = 0
    tot_pdfs = 0
    tot_hist = 0

    if tasks:
        results = list(parse_one_day.map(tasks, return_exceptions=True))
        for r in results:
            if isinstance(r, Exception):
                err_cnt += 1
                print(f"Day error: {r}")
            else:
                ok_cnt += 1
                tot_pdfs += r["total_pdfs"]
                tot_hist += r["history_rows"]

    lance_counts: dict[str, int] = {}
    if publish_lance:
        print("Publishing Lance datasets to s3://data-sink/active/dibbs_*/ ...")
        lance_counts["dibbs_solicitation_items"] = _publish_lance_table(
            "SELECT * FROM dibbs.solicitation_items",
            "s3://data-sink/active/dibbs_solicitation_items/",
            [("nsn", "BTREE"), ("solicitation_number", "BTREE")],
        )
        lance_counts["dibbs_approved_makers"] = _publish_lance_table(
            "SELECT * FROM dibbs.approved_makers",
            "s3://data-sink/active/dibbs_approved_makers/",
            [("nsn", "BTREE"), ("maker_cage", "BTREE")],
        )
        lance_counts["dibbs_procurement_history"] = _publish_lance_table(
            "SELECT * FROM dibbs.procurement_history",
            "s3://data-sink/active/dibbs_procurement_history/",
            [("nsn", "BTREE"), ("winner_cage", "BTREE")],
        )
        print(f"Published Lance datasets: {lance_counts}")

    summary = {
        "status": "ok" if err_cnt == 0 else "partial_error",
        "days_parsed": ok_cnt,
        "days_failed": err_cnt,
        "total_pdfs_parsed": tot_pdfs,
        "total_history_rows": tot_hist,
        "lance_counts": lance_counts,
    }
    print(f"Backfill complete: {summary}")
    return summary
