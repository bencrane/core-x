"""R2 (S3-compatible) storage for rendered archetype-version PDFs — own namespace, own lazy client.

Rendered PDFs land under ``archetype-versions/`` in the configured bucket (never the data-lake
``active/`` SoR namespace) and are handed back as a short-lived presigned GET URL the browser opens
directly — R2 hosts the bytes, edge_api never proxies them back. boto3 is synchronous; the put/sign
calls run in a worker thread so the event loop stays free. The client is built lazily + cached.
"""
from __future__ import annotations

import asyncio

import boto3
from botocore.config import Config as _BotoConfig

from .. import config

# Segregated namespace — operator-facing render artifacts, kept out of the data-lake SoR.
PREFIX = "archetype-versions/"

_client = None


class StoreError(RuntimeError):
    """A put/sign call failed (transient → 502)."""


class StoreConfigError(StoreError):
    """R2 is unconfigured (missing endpoint/creds) → surface as 503, not 502."""


def _s3():
    global _client
    if _client is None:
        endpoint = config.r2_endpoint()
        access_key = config.r2_access_key_id()
        secret_key = config.r2_secret_access_key()
        if not (endpoint and access_key and secret_key):
            raise StoreConfigError(
                "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY)"
            )
        _client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",  # R2 ignores the region but boto3 requires one
            config=_BotoConfig(signature_version="s3v4"),
        )
    return _client


async def put_pdf(key: str, data: bytes) -> None:
    """Upload PDF bytes to the configured bucket under ``key``."""

    def _put() -> None:
        _s3().put_object(
            Bucket=config.r2_proposal_bucket(),
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )

    try:
        await asyncio.to_thread(_put)
    except StoreError:
        raise  # already typed (e.g. unconfigured) — keep the 503/502 distinction
    except Exception as exc:  # botocore ClientError / endpoint errors
        raise StoreError(f"R2 put failed for {key}: {exc}") from exc


async def presigned_get_url(key: str, *, expires_seconds: int = 3600) -> str:
    """Return a time-limited GET URL the browser can open without credentials."""

    def _sign() -> str:
        return _s3().generate_presigned_url(
            "get_object",
            Params={"Bucket": config.r2_proposal_bucket(), "Key": key},
            ExpiresIn=expires_seconds,
        )

    try:
        return await asyncio.to_thread(_sign)
    except StoreError:
        raise  # already typed (e.g. unconfigured) — keep the 503/502 distinction
    except Exception as exc:
        raise StoreError(f"R2 presign failed for {key}: {exc}") from exc
