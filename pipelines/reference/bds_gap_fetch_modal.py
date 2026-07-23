"""One-off Modal fetch: the 11 BDS CSVs Census's WAF blocks from the local IP.

Runs from Modal's cloud egress (fresh IP to Census), lands each file verbatim in
s3://data-sink/landing/census/bds/ — the industry-cost-structure bds stream then
resumes entirely from landing. Throwaway after the batch closes.

    doppler run -p core-x -c prd -- modal run pipelines/reference/bds_gap_fetch_modal.py
"""
from __future__ import annotations

import os

import modal

app = modal.App("bds-gap-fetch")

FILES = [
    "bds2023_vcn3_ifz.csv", "bds2023_vcn3_ifzc.csv", "bds2023_vcn4.csv",
    "bds2023_vcn4_ea.csv", "bds2023_vcn4_eac.csv", "bds2023_vcn4_fa.csv",
    "bds2023_vcn4_fac.csv", "bds2023_vcn4_fz.csv", "bds2023_vcn4_fzc.csv",
    "bds2023_vcn4_ifz.csv", "bds2023_vcn4_ifzc.csv",
]
BDS_DIR = "https://www2.census.gov/programs-surveys/bds/tables/time-series/2023/"
LANDING = "landing/census/bds/"
_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126 Safari/537.36")

image = modal.Image.debian_slim().pip_install("requests", "boto3")


@app.function(image=image, timeout=900,
              secrets=[modal.Secret.from_dict({
                  "R2_ACCOUNT_ID": os.environ.get("R2_ACCOUNT_ID", ""),
                  "R2_ENDPOINT": os.environ.get("R2_ENDPOINT", ""),
                  "R2_ACCESS_KEY_ID": os.environ.get("R2_ACCESS_KEY_ID", ""),
                  "R2_SECRET_ACCESS_KEY": os.environ.get("R2_SECRET_ACCESS_KEY", ""),
              })])
def fetch_all() -> list[str]:
    import time

    import boto3
    import requests

    endpoint = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    s3 = boto3.client("s3", endpoint_url=endpoint,
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    done = []
    for fn in FILES:
        resp = requests.get(BDS_DIR + fn, headers={"User-Agent": _UA}, timeout=300)
        resp.raise_for_status()
        if resp.content.lstrip()[:1] == b"<":
            raise RuntimeError(f"{fn}: WAF rejection from Modal egress too")
        s3.put_object(Bucket="data-sink", Key=f"{LANDING}{fn}", Body=resp.content)
        done.append(f"{fn}: {len(resp.content)} bytes")
        time.sleep(1.0)
    return done


@app.local_entrypoint()
def main() -> None:
    for line in fetch_all.remote():
        print(line)
