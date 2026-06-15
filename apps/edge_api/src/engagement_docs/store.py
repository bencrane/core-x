"""Standalone R2 store for rendered engagement-mandate PDFs.

Own boto3 client, own segregated namespace (``engagement-mandates/``) — separate from the proposal
preview namespace and the data-lake ``active/`` SoR. Reads R2 creds straight from the environment
(Doppler ``core-x/prd``). boto3 is synchronous; blocking calls run in a worker thread.
"""
from __future__ import annotations

import asyncio
import os

import boto3
from botocore.config import Config as _BotoConfig

MANDATE_PREFIX = "engagement-mandates/"

_client = None


class StoreError(RuntimeError):
    """R2 unconfigured, or a put/sign call failed."""


def _bucket() -> str:
    return os.environ.get("R2_PROPOSAL_BUCKET", "data-sink")


def _s3():
    global _client
    if _client is None:
        endpoint = os.environ.get("R2_ENDPOINT")
        access_key = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
        if not (endpoint and access_key and secret_key):
            raise StoreError("R2 not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY)")
        _client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",  # R2 ignores region; boto3 requires one
            config=_BotoConfig(signature_version="s3v4"),
        )
    return _client


async def put_pdf(key: str, data: bytes) -> None:
    def _put() -> None:
        _s3().put_object(Bucket=_bucket(), Key=key, Body=data, ContentType="application/pdf")

    try:
        await asyncio.to_thread(_put)
    except Exception as exc:  # botocore ClientError / endpoint errors
        raise StoreError(f"R2 put failed for {key}: {exc}") from exc


async def presigned_get_url(key: str, *, expires_seconds: int = 3600) -> str:
    def _sign() -> str:
        return _s3().generate_presigned_url(
            "get_object", Params={"Bucket": _bucket(), "Key": key}, ExpiresIn=expires_seconds
        )

    try:
        return await asyncio.to_thread(_sign)
    except Exception as exc:
        raise StoreError(f"R2 presign failed for {key}: {exc}") from exc
