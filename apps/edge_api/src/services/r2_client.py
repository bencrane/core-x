"""Cloudflare R2 (S3-compatible) — store proposal preview PDFs, hand back a presigned link.

edge_api's first object-storage use. Endpoint + creds come from Doppler ``core-x/prd``
(``R2_ENDPOINT`` / ``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` — the same R2 the data lake
uses). Preview objects live under a segregated ``proposals/template-previews/`` prefix in the
configured bucket, never the ``active/`` SoR namespace.

The operator authoring a template clicks "Send to DocRaptor"; DocRaptor returns the PDF bytes,
we PUT them here and return a short-lived **presigned GET URL** the browser opens directly — R2
is the host, edge_api never proxies the bytes back.

boto3 is synchronous; the blocking put/sign calls run in a worker thread so the event loop stays
free. The client is built lazily + cached (no R2 work at import/boot).
"""
from __future__ import annotations

import asyncio

import boto3
from botocore.config import Config as _BotoConfig

from .. import config

# Segregated namespace — keeps operator-facing preview artifacts out of the data-lake SoR.
PREVIEW_PREFIX = "proposals/template-previews/"

_client = None


class R2Error(RuntimeError):
    """R2 is unconfigured, or a put/sign call failed."""


def _s3():
    global _client
    if _client is None:
        endpoint = config.r2_endpoint()
        access_key = config.r2_access_key_id()
        secret_key = config.r2_secret_access_key()
        if not (endpoint and access_key and secret_key):
            raise R2Error(
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
    """Upload PDF bytes to the proposal bucket under ``key``."""

    def _put() -> None:
        _s3().put_object(
            Bucket=config.r2_proposal_bucket(),
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )

    try:
        await asyncio.to_thread(_put)
    except Exception as exc:  # botocore ClientError / endpoint errors
        raise R2Error(f"R2 put failed for {key}: {exc}") from exc


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
    except Exception as exc:
        raise R2Error(f"R2 presign failed for {key}: {exc}") from exc
