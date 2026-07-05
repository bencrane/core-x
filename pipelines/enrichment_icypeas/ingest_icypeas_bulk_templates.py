"""Icypeas bulk-import template registry -> s3://data-sink/active/icypeas_bulk_templates/.

One row per Icypeas bulk tool. `headers` carries the EXACT ordered column headers the
Icypeas UI batch import expects (verbatim, including the 'EMAIL ADRESSES' typo — do not
"fix" them; the import maps by header text). To emit an import CSV for a tool, write
`headers` as the header row and one input per data row.

Source of truth for the header strings: the template .xlsx files exported from the
Icypeas UI on 2026-07-05 (sha256 recorded per row). The xlsx files are transport-only;
this module embeds their content verbatim so the build needs no local files.

`headers_verified=False` marks a template whose source xlsx was empty at export time
(reverse-email-lookup: sheet had no cells). Re-export from the Icypeas UI and update
TEMPLATES before generating CSVs for that tool.

Run:
    doppler run -- python pipelines/enrichment_icypeas/ingest_icypeas_bulk_templates.py
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import lance
import pyarrow as pa

URI = "s3://data-sink/active/icypeas_bulk_templates/"
DATA_STORAGE_VERSION = "2.1"

# (template_key, tool_name, headers, example_row, credit_cost, headers_verified,
#  source_filename, source_sha256, notes)
TEMPLATES: list[dict] = [
    {
        "template_key": "company_url_finder",
        "tool_name": "Company URL Finder",
        "headers": ["COMPANY NAME", "DOMAIN (you should fill at least one of the two columns)"],
        "example_row": ["Icypeas", ""],
        "credit_cost": "1 credit per found",
        "headers_verified": True,
        "source_filename": "icypeas-company-url-finder.xlsx",
        "source_sha256": "0a4ffbde16da76c8965c660c7b1193cc55a51ade779971b5c358a25ef26e08e0",
        "notes": "Company name and/or domain -> company LinkedIn URL. Output appends "
                 "'LinkedIn URL (company)'. Measured 2026-07-05 on the DSBS "
                 "no-blitz/no-PDL gap list: 1,209/24,553 found (4.9%).",
    },
    {
        "template_key": "company_linkedin_profile_scraper",
        "tool_name": "Company Profile Scraper",
        "headers": ["LINKEDIN COMPANY"],
        "example_row": ["https://www.linkedin.com/company/icypeas"],
        "credit_cost": "0.5 credits per scrape",
        "headers_verified": True,
        "source_filename": "icypeas-company-profile-scraper.xlsx",
        "source_sha256": "ff8d01a1b1d6deee6b40f5a7836beeb56fcbed7c2c79cf87e9f5bec69e18b284",
        "notes": "Company LinkedIn URL -> company profile (about, employee count, industry, "
                 "HQ...). Same engine as the in-repo synchronous rail "
                 "(pipelines/enrichment_company_scrape -> gtm.icypeas_company_scrapes); this "
                 "template is the UI bulk-import path.",
    },
    {
        "template_key": "profile_url_finder",
        "tool_name": "Profile URL Finder",
        "headers": ["FIRSTNAME (required)", "LASTNAME (required)", "COMPANY NAME",
                    "COMPANY DOMAIN",
                    "JOB TITLE (company name or company domain or job title should at least be filled)"],
        "example_row": ["Pierre-Baptiste", "Landoin", "Icypeas", "", ""],
        "credit_cost": "1 credit per found",
        "headers_verified": True,
        "source_filename": "icypeas-profile-url-finder.xlsx",
        "source_sha256": "0087e317f130878ab365bf47ea75661dfbb773b05bfd86660d4e8cfb6f80bfe5",
        "notes": "Person name + (company name | company domain | job title) -> person "
                 "LinkedIn URL. Used for the 3c slice 2026-07-04; results appended to "
                 "gtm_sam_person_identity as match_method='icypeas_profile_finder' (403 rows).",
    },
    {
        "template_key": "person_linkedin_profile_scraper",
        "tool_name": "LinkedIn Profile Scraper",
        "headers": ["LINKEDIN PROFILE"],
        "example_row": ["https://www.linkedin.com/in/pierre-baptiste-landoin-icypeas"],
        "credit_cost": None,
        "headers_verified": True,
        "source_filename": "icypeas-linkedin-profile-scraper.xlsx",
        "source_sha256": "0231ef4fbb2009b0975ea8f1d7f98595bc51a63d81726741de9bc83ca68bc1b2",
        "notes": "Person LinkedIn URL -> profile scrape (title, company, history).",
    },
    {
        "template_key": "email_finder",
        "tool_name": "Email Finder",
        "headers": ["FIRSTNAME", "LASTNAME", "DOMAIN"],
        "example_row": ["John", "Doe", "icypeas.com"],
        "credit_cost": None,
        "headers_verified": True,
        "source_filename": "icypeas-email-finder.xlsx",
        "source_sha256": "445c40a80a59136bd65b1cc864745f0e95192ffdfd21a69a6eb8d31a1941625a",
        "notes": "first + last + domain -> work email. The in-repo bulk rail "
                 "(pipelines/enrichment_icypeas_mv, icypeas API + MillionVerifier) is the "
                 "preferred path at volume; this template is the UI bulk-import path.",
    },
    {
        "template_key": "email_verifier",
        "tool_name": "Email Verifier",
        "headers": ["EMAIL ADRESSES"],
        "example_row": ["john.doe@icypeas.com"],
        "credit_cost": None,
        "headers_verified": True,
        "source_filename": "icypeas-email-verifier.xlsx",
        "source_sha256": "417bfc36f26d2bef5c8212501c2f8cab1a0e4db91b66a3d3e4643249c624ec52",
        "notes": "Email list -> deliverability verification. Header typo 'ADRESSES' is "
                 "verbatim from the template — emit it exactly.",
    },
    {
        "template_key": "domain_scan",
        "tool_name": "Domain Scan",
        "headers": ["DOMAIN OR COMPANY"],
        "example_row": ["icypeas.com"],
        "credit_cost": None,
        "headers_verified": True,
        "source_filename": "icypeas-domain-scan.xlsx",
        "source_sha256": "d1ab3c5ed4bdc9fe2682a7c77a3ac0eb1b0dc39da5c3c501e969749996108799",
        "notes": "Domain or company name -> emails discovered on the domain.",
    },
    {
        "template_key": "reverse_email_lookup",
        "tool_name": "Reverse Email Lookup",
        "headers": [],
        "example_row": [],
        "credit_cost": None,
        "headers_verified": False,
        "source_filename": "icypeas-reverse-email-lookup.xlsx",
        "source_sha256": "28446be4aea7d2af7b7a85113f6b7b0442d52e9638bd13a59e4d603a3d3b6fa9",
        "notes": "Email -> person/company identity. Source xlsx exported EMPTY "
                 "(sheetData had no cells) — headers unknown. Re-export the template from "
                 "the Icypeas UI and update this row before generating import CSVs.",
    },
]


def csv_header(template_key: str) -> list[str]:
    """Exact ordered header row for an Icypeas bulk-import CSV."""
    t = next(t for t in TEMPLATES if t["template_key"] == template_key)
    if not t["headers_verified"]:
        raise ValueError(f"{template_key}: headers unverified (empty source template) — "
                         f"re-export from the Icypeas UI first")
    return list(t["headers"])


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def main() -> None:
    now = datetime.now(timezone.utc).isoformat()
    table = pa.table({
        "template_key": pa.array([t["template_key"] for t in TEMPLATES], pa.string()),
        "tool_name": pa.array([t["tool_name"] for t in TEMPLATES], pa.string()),
        "headers": pa.array([t["headers"] for t in TEMPLATES], pa.list_(pa.string())),
        "n_cols": pa.array([len(t["headers"]) for t in TEMPLATES], pa.int32()),
        "example_row": pa.array([t["example_row"] for t in TEMPLATES], pa.list_(pa.string())),
        "credit_cost": pa.array([t["credit_cost"] for t in TEMPLATES], pa.string()),
        "headers_verified": pa.array([t["headers_verified"] for t in TEMPLATES], pa.bool_()),
        "source_filename": pa.array([t["source_filename"] for t in TEMPLATES], pa.string()),
        "source_sha256": pa.array([t["source_sha256"] for t in TEMPLATES], pa.string()),
        "notes": pa.array([t["notes"] for t in TEMPLATES], pa.string()),
        "materialized_at": pa.array([now] * len(TEMPLATES), pa.string()),
    })
    so = _r2_storage_options()
    try:
        v_before = lance.dataset(URI, storage_options=so).version
    except Exception:  # noqa: BLE001 — first write
        v_before = None
    lance.write_dataset(table, URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(URI, storage_options=so)
    print(f"{URI} v{v_before} -> v{ds.version} rows={ds.count_rows()}")
    for row in ds.to_table(columns=["template_key", "n_cols", "headers_verified"]).to_pylist():
        print(f"  {row['template_key']:26s} cols={row['n_cols']} verified={row['headers_verified']}")


if __name__ == "__main__":
    main()
