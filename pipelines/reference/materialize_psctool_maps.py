"""Reference loader — psctool_* maps: the four PSC Tool (psctool.us) crosswalk exports as canonical
Lance lookups under s3://data-sink/active/psctool/. The source of truth for PSC -> object-class,
NAICS <-> PSC (suggested), the spend-category taxonomy, and PSC -> spend-category.

TABLES (all Lance v2.1; derived, mode=overwrite — idempotent re-import)
  active/psctool/psc_object_class_map/     PSC -> federal object class code; ~2,344 rows.
      psc_code, object_class_code, object_class_code_description, + provenance.
      BTREE: psc_code | BITMAP: object_class_code.
  active/psctool/naics_psc_map/            suggested NAICS <-> PSC pairs; ~1,695 rows.
      psc_code, naics_code, naics_description, naics_extended_description, + provenance.
      BTREE: psc_code, naics_code.
  active/psctool/spend_categories/         spend-category taxonomy; ~92 rows.
      spend_category, title, + provenance.   BTREE: spend_category.
  active/psctool/spend_category_psc_map/   PSC -> spend category (2 levels); ~2,344 rows.
      psc_code, category_level_1, category_level_1_name, category_level_2, category_level_2_name,
      + provenance.   BTREE: psc_code | BITMAP: category_level_1, category_level_2.

RAW    each source CSV is snapshotted verbatim to s3://data-sink/landing/psctool/<file>.csv for
       provenance (psctool.us is an external, mutable asset host); that landing copy is
       transport/archive only — the record is the Lance dataset, never the CSV.
CLEAN  every column is read as VARCHAR to preserve dotted hierarchical codes (spend_category '2.1',
       object_class_code '31.0', category_level_2 '12.4') and any leading zeros a numeric sniff
       would destroy. Values are trimmed (the source left-pads some psc_code/naics_code with
       spaces); '' and the JavaScript sentinel 'undefined' fold to NULL. No column is dropped and
       no row is dropped — Lance row count is asserted equal to the raw CSV data-row count.
SOURCE https://psctool.us/assets/pscmap/{psc_occ_map,naics_psc_map,spend_categories,
       spend_categories_psc_map}.csv  — CORS-enabled public assets. Refresh by re-running `build`.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'duckdb>=1.1' --with 'pylance>=7' --with 'pyarrow>=17' --with 'boto3>=1.34' \
      python3 pipelines/reference/materialize_psctool_maps.py <build|verify>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import tempfile
import urllib.request

ACTIVE = os.environ.get("PSCTOOL_ACTIVE_PREFIX", "s3://data-sink/active/psctool")
LANDING_BUCKET = os.environ.get("PSCTOOL_LANDING_BUCKET", "data-sink")
LANDING_PREFIX = os.environ.get("PSCTOOL_LANDING_PREFIX", "landing/psctool/")
DATA_STORAGE_VERSION = "2.1"
BASE_URL = "https://psctool.us/assets/pscmap"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

# Each dataset: source file, projection (all-VARCHAR, trimmed, sentinel-folded), index plan.
# `key` is the primary resolution column used for grain/verify reporting.
DATASETS = [
    {
        "name": "psc_object_class_map",
        "file": "psc_occ_map.csv",
        "core": """
            nullif(trim(psc_code), '') AS psc_code,
            nullif(trim(object_class_code), '') AS object_class_code,
            nullif(nullif(trim(object_class_code_description), ''), 'undefined') AS object_class_code_description
        """,
        "btree": ["psc_code"],
        "bitmap": ["object_class_code"],
        "key": "psc_code",
    },
    {
        "name": "naics_psc_map",
        "file": "naics_psc_map.csv",
        "core": """
            nullif(trim(psc_code), '') AS psc_code,
            nullif(trim(naics_code), '') AS naics_code,
            nullif(nullif(trim(naics_description), ''), 'undefined') AS naics_description,
            nullif(nullif(trim(naics_extended_description), ''), 'undefined') AS naics_extended_description
        """,
        "btree": ["psc_code", "naics_code"],
        "bitmap": [],
        "key": "psc_code",
    },
    {
        "name": "spend_categories",
        "file": "spend_categories.csv",
        "core": """
            nullif(trim(spend_category), '') AS spend_category,
            nullif(nullif(trim(title), ''), 'undefined') AS title
        """,
        "btree": ["spend_category"],
        "bitmap": [],
        "key": "spend_category",
    },
    {
        "name": "spend_category_psc_map",
        "file": "spend_categories_psc_map.csv",
        "core": """
            nullif(trim(psc_code), '') AS psc_code,
            nullif(trim(category_level_1), '') AS category_level_1,
            nullif(nullif(trim(category_level_1_name), ''), 'undefined') AS category_level_1_name,
            nullif(trim(category_level_2), '') AS category_level_2,
            nullif(nullif(trim(category_level_2_name), ''), 'undefined') AS category_level_2_name
        """,
        "btree": ["psc_code"],
        "bitmap": ["category_level_1", "category_level_2"],
        "key": "psc_code",
    },
]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _s3_client():
    import boto3
    from botocore.config import Config
    so = _r2_so()
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=Config(signature_version="s3v4"))


def _fetch(url: str, dest: str) -> int:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=90) as r, open(dest, "wb") as f:
        body = r.read()
        f.write(body)
    return len(body)


def uri(name: str) -> str:
    return f"{ACTIVE}/{name}/"


def build():
    import duckdb
    import lance

    so = _r2_so()
    s3 = _s3_client()
    con = duckdb.connect()
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    tmp = tempfile.mkdtemp(prefix="psctool_")
    results = {}

    for ds in DATASETS:
        name, fname = ds["name"], ds["file"]
        url = f"{BASE_URL}/{fname}"
        local = os.path.join(tmp, fname)
        nbytes = _fetch(url, local)
        log(f"[{name}] fetched {fname}: {nbytes:,} bytes")

        # provenance snapshot -> landing (archive, not record)
        with open(local, "rb") as f:
            s3.put_object(Bucket=LANDING_BUCKET, Key=f"{LANDING_PREFIX}{fname}",
                          Body=f.read(), ContentType="text/csv")
        log(f"[{name}] snapshot -> s3://{LANDING_BUCKET}/{LANDING_PREFIX}{fname}")

        raw_rows = con.execute(
            f"SELECT count(*) FROM read_csv('{local}', header=true, all_varchar=true, sample_size=-1)"
        ).fetchone()[0]

        prov = (f", '{url}' AS source_url, '{fname}' AS source_file, "
                f"'{ts}' AS ingested_at")
        sql = (f"SELECT {ds['core'].strip()}{prov} "
               f"FROM read_csv('{local}', header=true, all_varchar=true, sample_size=-1)")
        tbl = con.sql(sql).to_arrow_table()
        if tbl.num_rows != raw_rows:
            raise RuntimeError(f"[{name}] row drift: projected {tbl.num_rows} != raw {raw_rows}")

        u = uri(name)
        lance.write_dataset(tbl, u, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        d = lance.dataset(u, storage_options=so)
        for col in ds["btree"]:
            d.create_scalar_index(col, index_type="BTREE", replace=True)
            log(f"[{name}]   BTREE ✓ {col}")
        for col in ds["bitmap"]:
            d.create_scalar_index(col, index_type="BITMAP", replace=True)
            log(f"[{name}]   BITMAP ✓ {col}")

        written = d.count_rows()
        if written != raw_rows:
            raise RuntimeError(f"[{name}] LOST DATA: lance {written} != raw {raw_rows}")
        log(f"[{name}] DONE → {u} rows={written:,} cols={tbl.num_columns}")
        results[name] = {"uri": u, "rows": written, "raw_rows": raw_rows,
                         "columns": tbl.column_names,
                         "btree": ds["btree"], "bitmap": ds["bitmap"]}

    print(json.dumps(results, indent=2, default=str))
    return results


def verify():
    import lance
    so = _r2_so()
    out = {}
    for ds in DATASETS:
        name, key = ds["name"], ds["key"]
        u = uri(name)
        try:
            d = lance.dataset(u, storage_options=so)
        except Exception as exc:  # noqa: BLE001
            out[name] = {"uri": u, "error": str(exc)}
            continue
        keys = d.scanner(columns=[key]).to_table().column(key).to_pylist()
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in d.list_indices()]
        out[name] = {
            "uri": u,
            "rows": d.count_rows(),
            "columns": d.schema.names,
            f"distinct_{key}": len({k for k in keys if k is not None}),
            f"null_{key}": sum(1 for k in keys if k is None),
            "indices": idx,
            "sample": d.scanner(limit=3).to_table().to_pylist(),
        }
    print(json.dumps(out, indent=2, default=str))
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "build":
        build()
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
