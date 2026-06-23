"""Modal reindex companion for company_addresses.

The local materializer (materialize_company_addresses.py) writes the dataset fine
but external-sort merges on the high-cardinality BTREE columns (legal_business_name,
company_linkedin_url) exceed the local laptop's available RAM. This helper runs the
same scalar-index builds on Modal with 32 GiB RAM so they actually commit.

Idempotent: skips any index whose <col>_idx is already present. Safe to re-run.

    modal run pipelines/serving/reindex_company_addresses_modal.py::reindex_only
"""

from __future__ import annotations

import os

import modal


SERVING_URI = os.environ.get(
    "COMPANY_ADDRESSES_URI",
    "s3://data-sink/active/company_addresses/",
)

# Mirrors materialize_company_addresses.py — keep these two lists in sync if the
# materializer's index plan changes.
BTREE_COLS = ["entity_key", "uei", "domain_norm", "company_linkedin_url",
              "primary_naics", "legal_business_name"]
BITMAP_COLS = ["address_source", "winner_state", "winner_country_code",
               "had_sam_physical", "had_sam_mailing", "had_prospeo", "had_blitz"]


image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("company-addresses-reindex", image=image)


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _committed_index_names(ds) -> set[str]:
    out: set[str] = set()
    for ix in ds.list_indices():
        out.add(ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", str(ix)))
    return out


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60,
    memory=32768,
    cpu=4.0,
)
def reindex() -> dict:
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    rows = ds.count_rows()
    present_cols = set(ds.schema.names)
    existing = _committed_index_names(ds)
    print(f"company_addresses: {rows:,} rows · {len(existing)} existing indexes")

    results: list[dict] = []
    for index_type, cols in (("BTREE", BTREE_COLS), ("BITMAP", BITMAP_COLS)):
        for col in cols:
            if col not in present_cols:
                results.append({"col": col, "type": index_type, "status": "missing_col"})
                print(f"  {index_type:<6} ~ {col}: column not in schema")
                continue
            name = f"{col}_idx"
            if name in existing:
                results.append({"col": col, "type": index_type, "status": "exists"})
                print(f"  {index_type:<6} = {col}: already committed")
                continue
            try:
                ds.create_scalar_index(col, index_type=index_type)
                results.append({"col": col, "type": index_type, "status": "created"})
                print(f"  {index_type:<6} ✓ {col}: created")
            except Exception as exc:  # noqa: BLE001
                results.append({"col": col, "type": index_type, "status": "error", "error": str(exc)})
                print(f"  {index_type:<6} ✗ {col}: {exc}")

    final = _committed_index_names(lance.dataset(SERVING_URI, storage_options=so))
    print(f"final committed indexes: {sorted(final)}")
    return {"uri": SERVING_URI, "rows": rows, "results": results, "indexes": sorted(final)}


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))
