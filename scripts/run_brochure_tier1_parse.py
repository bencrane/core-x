#!/usr/bin/env python3
"""Driver: Tier-1 parse of the landed SEC ADV Part 2A brochure PDFs → Lance.

Reads the 947 raw brochure PDFs at
``s3://data-sink/active/_sec_iapd_brochure_pdfs_raw/`` (layout
``crd={CRD}/{filing_date}/{version_id}.pdf``), runs the FROZEN deterministic
Tier-1 parser (``pipelines/sec_adv/brochure_parse.py`` — PyMuPDF, no LLM, no
OCR), and emits one row per PDF to
``s3://data-sink/active/sec_adv_part_2_brochures_lance/`` (Lance, SoR).

Priority ordering comes from ``_crd_worklist_private_credit`` — declared-tier
CRDs first (descending ``pc_gav``), then name_signal, then any PDF whose CRD is
absent from the worklist. Ordering only affects processing sequence; every PDF
is parsed.

Hard boundaries (enforced here):
  * NO LLM, NO OCR. Tier-2/Tier-3 out of scope — ``needs_fallback`` rows are
    flagged and left. ``image_only`` PDFs are tagged and parked.
  * The parser is frozen and imported as-is; it is never patched here.
  * A parse failure on one PDF is caught and recorded (confidence=0, error note
    in ``anchor_source``); the run never aborts for a single bad PDF.

Run:
  doppler run -p core-x -c prd -- \
    uv run --no-project --with pylance --with duckdb --with pymupdf --with pyarrow \
    python3 scripts/run_brochure_tier1_parse.py
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
import tempfile
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.fs as pafs

BUCKET = "data-sink"
RAW_PREFIX = f"{BUCKET}/active/_sec_iapd_brochure_pdfs_raw"
WORKLIST = f"s3://{BUCKET}/active/_crd_worklist_private_credit/"
DST = f"s3://{BUCKET}/active/sec_adv_part_2_brochures_lance/"

TIER1_OK_CONFIDENCE = 10  # mirrors parser TIER1_OK_THRESHOLD
_KEY_RE = re.compile(r"crd=(?P<crd>[^/]+)/(?P<filing_date>[^/]+)/(?P<vid>[^/]+)\.pdf$")


# ── Frozen parser import (by file path; never modified) ───────────────────────
def _load_parser():
    root = Path(__file__).resolve().parents[1]
    mod_path = root / "pipelines" / "sec_adv" / "brochure_parse.py"
    spec = importlib.util.spec_from_file_location("brochure_parse", mod_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclass introspection needs the module registered
    spec.loader.exec_module(mod)
    return mod


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "aws_endpoint": ep,
        "aws_region": "auto",
    }


def r2_fs() -> pafs.S3FileSystem:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return pafs.S3FileSystem(
        access_key=os.environ["R2_ACCESS_KEY_ID"],
        secret_key=os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_override=ep,
        region="auto",
    )


def load_worklist() -> dict[str, tuple[str, float]]:
    """crd_number → (worklist_tier, pc_gav as float, NaN→0)."""
    ds = lance.dataset(WORKLIST, storage_options=storage_options())
    t = ds.to_table(columns=["crd_number", "worklist_tier", "pc_gav"])
    d = t.to_pydict()
    out: dict[str, tuple[str, float]] = {}
    for crd, tier, gav in zip(d["crd_number"], d["worklist_tier"], d["pc_gav"]):
        try:
            g = float(gav) if gav is not None else 0.0
        except (TypeError, ValueError):
            g = 0.0
        out[str(crd)] = (tier, g)
    return out


def list_pdfs(fs: pafs.S3FileSystem) -> list[tuple[str, str, str, str]]:
    """Return (path, crd, filing_date, version_id) for every landed PDF."""
    infos = fs.get_file_info(pafs.FileSelector(RAW_PREFIX, recursive=True))
    rows = []
    for info in infos:
        if info.type != pafs.FileType.File or not info.path.endswith(".pdf"):
            continue
        m = _KEY_RE.search(info.path)
        if not m:
            continue
        rows.append((info.path, m.group("crd"), m.group("filing_date"), m.group("vid")))
    return rows


def order_pdfs(pdfs, worklist) -> list:
    """Declared first (desc pc_gav), then name_signal (desc pc_gav), then untiered."""
    tier_rank = {"declared": 0, "name_signal": 1}

    def key(rec):
        _path, crd, _fd, _vid = rec
        tier, gav = worklist.get(str(crd), (None, 0.0))
        return (tier_rank.get(tier, 2), -gav, crd)

    return sorted(pdfs, key=key)


def main() -> None:
    parser = _load_parser()
    fs = r2_fs()
    worklist = load_worklist()
    pdfs = order_pdfs(list_pdfs(fs), worklist)
    total = len(pdfs)
    print(f"landed PDFs: {total}   worklist CRDs: {len(worklist)}")

    records: list[dict] = []
    for idx, (path, crd, filing_date, vid) in enumerate(pdfs, 1):
        tier, _gav = worklist.get(str(crd), (None, 0.0))
        row = {
            "crd_number": crd,
            "brochure_version_id": vid,
            "filing_date": filing_date,
            "worklist_tier": tier,
            "n_pages": 0,
            "n_chars": 0,
            "image_only": False,
            "confidence": 0,
            "needs_fallback": True,
            "anchor_source": "{}",
            **{f"item_{n}": None for n in range(1, 19)},
        }
        tmp = None
        try:
            data = fs.open_input_file(path).readall()
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as fh:
                fh.write(data)
                tmp = fh.name
            r = parser.parse_brochure(tmp)
            row.update(
                n_pages=r.n_pages,
                n_chars=r.n_chars,
                image_only=r.image_only,
                confidence=r.confidence,
                needs_fallback=r.needs_fallback,
                anchor_source=json.dumps({str(k): v for k, v in r.anchor_source.items()}),
            )
            for n, txt in r.items.items():
                row[f"item_{n}"] = txt
        except Exception as e:  # noqa: BLE001 — one bad PDF must never abort the run
            row["anchor_source"] = json.dumps({"error": f"{type(e).__name__}: {e}"})
        finally:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
        records.append(row)
        if idx % 50 == 0 or idx == total:
            print(f"  parsed {idx}/{total}")

    # ── Build Arrow table (explicit nullable-string item schema) ───────────────
    schema = pa.schema(
        [
            ("crd_number", pa.string()),
            ("brochure_version_id", pa.string()),
            ("filing_date", pa.string()),
            ("worklist_tier", pa.string()),
            ("n_pages", pa.int64()),
            ("n_chars", pa.int64()),
            ("image_only", pa.bool_()),
            ("confidence", pa.int64()),
            ("needs_fallback", pa.bool_()),
            ("anchor_source", pa.string()),
        ]
        + [(f"item_{n}", pa.string()) for n in range(1, 19)]
    )
    tbl = pa.Table.from_pylist(records, schema=schema)

    # cast any large_string → string so Lance scalar indices accept the columns
    fields = [
        pa.field(f.name, pa.string()) if pa.types.is_large_string(f.type) else f
        for f in tbl.schema
    ]
    tbl = tbl.cast(pa.schema(fields))

    lance.write_dataset(tbl, DST, storage_options=storage_options(), mode="overwrite")
    out = lance.dataset(DST, storage_options=storage_options())
    for col, kind in [
        ("crd_number", "BTREE"),
        ("brochure_version_id", "BTREE"),
        ("needs_fallback", "BITMAP"),
    ]:
        try:
            out.create_scalar_index(col, index_type=kind)
        except Exception as e:  # noqa: BLE001
            print(f"  index {col}: {e}")

    _verify(tbl, out)


def _verify(tbl: pa.Table, out: lance.LanceDataset) -> None:
    import duckdb

    con = duckdb.connect()
    con.register("b", tbl)
    n = out.count_rows()
    print("\n── VERIFICATION ─────────────────────────────────────────────")
    print(f"row count (== PDFs processed): {n:,}")

    dist = con.execute(
        f"""
        SELECT
          sum(CASE WHEN NOT image_only AND confidence >= {TIER1_OK_CONFIDENCE} THEN 1 ELSE 0 END) AS tier1_ok,
          sum(CASE WHEN needs_fallback AND NOT image_only THEN 1 ELSE 0 END)                        AS needs_fallback_non_image,
          sum(CASE WHEN needs_fallback THEN 1 ELSE 0 END)                                           AS needs_fallback_total,
          sum(CASE WHEN image_only THEN 1 ELSE 0 END)                                               AS image_only
        FROM b
        """
    ).fetchdf().iloc[0]
    print(
        f"tier1_ok (conf>={TIER1_OK_CONFIDENCE}): {int(dist.tier1_ok)} "
        f"({100*dist.tier1_ok/n:.1f}%)   "
        f"needs_fallback: {int(dist.needs_fallback_total)} "
        f"({100*dist.needs_fallback_total/n:.1f}%)   "
        f"image_only: {int(dist.image_only)} ({100*dist.image_only/n:.1f}%)"
    )

    dec = con.execute(
        """
        SELECT
          count(*) AS declared_rows,
          sum(CASE WHEN item_4 IS NOT NULL THEN 1 ELSE 0 END) AS i4,
          sum(CASE WHEN item_5 IS NOT NULL THEN 1 ELSE 0 END) AS i5,
          sum(CASE WHEN item_8 IS NOT NULL THEN 1 ELSE 0 END) AS i8
        FROM b WHERE worklist_tier = 'declared'
        """
    ).fetchdf().iloc[0]
    dr = int(dec.declared_rows)
    if dr:
        print(
            f"declared-tier rows: {dr}   "
            f"item_4 non-null: {int(dec.i4)} ({100*dec.i4/dr:.1f}%)   "
            f"item_5 non-null: {int(dec.i5)} ({100*dec.i5/dr:.1f}%)   "
            f"item_8 non-null: {int(dec.i8)} ({100*dec.i8/dr:.1f}%)"
        )

    fee_re = re.compile(r"fee|compensation|advisory fee|management fee|\bbps\b|basis point|%", re.I)
    spot = con.execute(
        "SELECT crd_number, brochure_version_id, item_5 FROM b "
        "WHERE worklist_tier = 'declared' AND item_5 IS NOT NULL LIMIT 3"
    ).fetchall()
    print("spot-check item_5 fee language (3 declared rows):")
    for crd, vid, item5 in spot:
        hit = bool(fee_re.search(item5 or ""))
        print(f"  crd={crd} vid={vid}: fee-language={'YES' if hit else 'NO'}  "
              f"| head: {(item5 or '')[:80].replace(chr(10),' ')!r}")

    print("indices:")
    for ix in out.list_indices():
        print(f"  {ix}")


if __name__ == "__main__":
    main()
