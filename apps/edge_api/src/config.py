"""Runtime configuration for edge_api.

Secrets come from the environment (Doppler ``core-x/prd`` locally and on the
deployed public service). Nothing is committed. The proposal-template authoring
surface uses R2 for preview PDFs (creds already in ``core-x/prd``); the rest of
edge_api reads the same ``HQX_*`` Postgres and ``MANAGED_*`` /
``DMAAS_MCP_BEARER_TOKEN`` values already present there.
"""
from __future__ import annotations

import os


def service_token() -> str | None:
    """The shared secret the platform-api BFF (and Trigger.dev) present as
    ``Authorization: Bearer`` on the agent-runs + pipeline surface. When unset
    (local dev) the gate warns and allows; every deployed environment sets it."""
    return os.environ.get("EDGE_API_SERVICE_TOKEN")


def trigger_shared_secret() -> str | None:
    """The shared secret Trigger.dev tasks present as ``Authorization: Bearer`` on the
    ``/internal/*`` surface (``callHqx`` → TRIGGER_SHARED_SECRET). When unset (local dev)
    the gate allows; every deployed environment sets it, so enforcement is live. Distinct
    from ``service_token`` (the BFF-facing ``/api/v1`` surface)."""
    return os.environ.get("TRIGGER_SHARED_SECRET")


def documenso_api_key() -> str | None:
    """Documenso Cloud API key (format ``api_...``), server-side only. From ``core-x/prd``."""
    return os.environ.get("DOCUMENSO_API_KEY")


def documenso_api_url() -> str:
    """Documenso instance base URL. The ``host`` passed to the embed MUST match this — a doc
    created here cannot be signed against a different instance."""
    return os.environ.get("DOCUMENSO_API_URL", "https://app.documenso.com").rstrip("/")


def documenso_webhook_secret() -> str | None:
    """Shared secret Documenso echoes verbatim in the ``X-Documenso-Secret`` header. When unset
    the webhook route refuses (503) rather than accepting unverified events."""
    return os.environ.get("DOCUMENSO_WEBHOOK_SECRET")


def cal_webhook_secret() -> str | None:
    """Secret cal.com signs each delivery with — HMAC-SHA256 over the RAW body, sent as
    ``X-Cal-Signature-256``. When unset, ``/webhooks/cal`` refuses (503) rather than accepting
    unverified events. From ``core-x/prd`` (``CAL_WEBHOOK_SECRET``); must match the secret set on
    the cal.com webhook."""
    return os.environ.get("CAL_WEBHOOK_SECRET")


def docraptor_api_key() -> str | None:
    """DocRaptor API key. Used in LIVE mode (``test=false``) — test output is watermarked."""
    return os.environ.get("DOCRAPTOR_API_KEY")


def stripe_mode() -> str:
    """``live`` | ``test`` — selects which Stripe key set to use. From ``core-x/prd`` (``STRIPE_MODE``);
    defaults to ``test`` so an unset/typo'd mode can never accidentally hit live rails."""
    return os.environ.get("STRIPE_MODE", "test").strip().lower()


def _stripe_secret(base: str) -> str | None:
    """Resolve a mode-suffixed Stripe secret. ``core-x/prd`` carries ``{base}_LIVE`` / ``{base}_TEST``
    selected by ``STRIPE_MODE``; fall back to the bare ``{base}`` for any env that sets it directly."""
    suffix = "LIVE" if stripe_mode() == "live" else "TEST"
    return os.environ.get(f"{base}_{suffix}") or os.environ.get(base)


def stripe_secret_key() -> str | None:
    """Stripe secret key (``sk_...``), server-side ONLY — never sent to the browser. Resolved from
    ``STRIPE_SECRET_KEY_{LIVE,TEST}`` by ``STRIPE_MODE`` (``core-x/prd``). When unset, the
    payment-intent route refuses (503) instead of charging."""
    return _stripe_secret("STRIPE_SECRET_KEY")


def stripe_publishable_key() -> str | None:
    """Stripe publishable key (``pk_...``). Public by design — surfaced to the browser (via the BFF)
    to mount the ACH PaymentElement. Resolved from ``STRIPE_PUBLISHABLE_KEY_{LIVE,TEST}`` by
    ``STRIPE_MODE``. Single source of truth: the SPA never carries its own copy."""
    return _stripe_secret("STRIPE_PUBLISHABLE_KEY")


def stripe_webhook_secret() -> str | None:
    """Signing secret (``whsec_...``) for the Stripe webhook endpoint. Resolved from
    ``STRIPE_WEBHOOK_SECRET_{LIVE,TEST}`` by ``STRIPE_MODE``. When unset, ``/webhooks/stripe`` refuses
    (503) rather than accepting unverified events. Must match the secret on the Stripe webhook."""
    return _stripe_secret("STRIPE_WEBHOOK_SECRET")


# ── Operator-selectable Stripe mode (the document-payment flow) ───────────────────────────────────
# The document-payment flow lets the (single) operator pick test/live at runtime via
# operator_settings.stripe_mode, AUGMENTING the env STRIPE_MODE. The selection indirects through the
# STRIPE_MODE_{TEST,LIVE} env values the operator configured; an unset selection (None) falls back to
# the STRIPE_MODE env default. The key getters below resolve the sk_/pk_ for an EXPLICIT mode so the
# mint can honor the per-request selection without touching the env-based getters the proposal flow uses.
def resolve_stripe_mode(selection: str | None) -> str:
    """Effective Stripe mode ('test'|'live') for a document payment. ``selection`` is
    operator_settings.stripe_mode; indirects through STRIPE_MODE_{TEST,LIVE} env, else STRIPE_MODE env."""
    if selection in ("test", "live"):
        return (os.environ.get(f"STRIPE_MODE_{selection.upper()}") or selection).strip().lower()
    return stripe_mode()


def _stripe_secret_for_mode(base: str, mode: str) -> str | None:
    suffix = "LIVE" if mode == "live" else "TEST"
    return os.environ.get(f"{base}_{suffix}") or os.environ.get(base)


def stripe_secret_key_for_mode(mode: str) -> str | None:
    """``sk_...`` for an explicit mode — server-side only."""
    return _stripe_secret_for_mode("STRIPE_SECRET_KEY", mode)


def stripe_publishable_key_for_mode(mode: str) -> str | None:
    """``pk_...`` for an explicit mode — surfaced to the browser."""
    return _stripe_secret_for_mode("STRIPE_PUBLISHABLE_KEY", mode)


def stripe_webhook_secrets() -> list[str]:
    """ALL configured webhook signing secrets (test + live + bare), de-duplicated. The single
    ``/webhooks/stripe`` endpoint receives events signed by whichever mode an intent was created in, so
    a runtime-toggleable mode requires verifying against any of them."""
    seen: set[str] = set()
    out: list[str] = []
    for v in (
        os.environ.get("STRIPE_WEBHOOK_SECRET_TEST"),
        os.environ.get("STRIPE_WEBHOOK_SECRET_LIVE"),
        os.environ.get("STRIPE_WEBHOOK_SECRET"),
    ):
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def rs_signer_name() -> str:
    """Rare Structure's signatory name pre-rendered into the agreement (Managing Director)."""
    return os.environ.get("RS_SIGNER_NAME", "Benjamin Crane")


def partner_platform_base_url() -> str:
    """Public base URL of the consumer proposal page — used to build shareable proposal links."""
    return os.environ.get("PARTNER_PLATFORM_BASE_URL", "http://localhost:3000").rstrip("/")


# ── Cloudflare R2 (S3-compatible) — proposal preview/artifact storage ─────────────────
# edge_api's first object-storage use. Endpoint + creds already live in core-x/prd
# (the same R2 the data lake uses). Only the bucket is service-specific.
def r2_endpoint() -> str | None:
    """R2 S3 endpoint, e.g. ``https://<account>.r2.cloudflarestorage.com``."""
    return os.environ.get("R2_ENDPOINT")


def r2_access_key_id() -> str | None:
    return os.environ.get("R2_ACCESS_KEY_ID")


def r2_secret_access_key() -> str | None:
    return os.environ.get("R2_SECRET_ACCESS_KEY")


def r2_proposal_bucket() -> str:
    """Bucket for proposal preview PDFs. Defaults to the data-lake bucket; preview objects live
    under a segregated ``proposals/`` prefix (never the ``active/`` SoR namespace). Override with
    ``R2_PROPOSAL_BUCKET`` to point at a dedicated bucket."""
    return os.environ.get("R2_PROPOSAL_BUCKET", "data-sink")


# ── Map /ask TRANSLATE (forced-tool Anthropic Messages → filter object) ──────
def anthropic_api_key() -> str | None:
    """Key for the forced-tool Messages call that compiles a map NL query into a
    constrained filter object. DISTINCT from ANTHROPIC_MANAGED_AGENTS_API_KEY (the
    gtm-agent surface). When unset, the /ask route refuses (503)."""
    return os.environ.get("ANTHROPIC_API_KEY")


def map_compiler_model() -> str:
    """Model id for the map NL→filter translate (forced single-tool extraction over a
    cached prompt). Defaults to ``claude-opus-4-7``: the map search box is low-volume
    and operator-facing, so translation QUALITY is worth the cost over a faster Haiku.
    Override with ``MAP_COMPILER_MODEL``."""
    return os.environ.get("MAP_COMPILER_MODEL", "claude-opus-4-7")


# ── catalyst_api (the map EXECUTE tier edge_api delegates the read to) ───────
def catalyst_base_url() -> str | None:
    """Base URL of the catalyst_api read-tier where the map EXECUTE endpoint lives
    (Railway private net in prod). When unset, the /ask route refuses (503)."""
    base = os.environ.get("CATALYST_API_BASE_URL")
    return base.rstrip("/") if base else None


def catalyst_service_token() -> str | None:
    """Bearer edge_api presents to catalyst_api's ``require_operator`` gate — the same
    secret value catalyst reads as ``CATALYST_API_TOKEN``."""
    return os.environ.get("CATALYST_API_TOKEN")


def port() -> int:
    """Bind port. The deployed service injects ``$PORT``; default for a bare local run."""
    return int(os.environ.get("PORT", "8080"))


def host() -> str:
    """Bind address. Defaults to ``0.0.0.0`` — edge_api is a PUBLIC service
    (Anthropic's platform calls the MCP mounts; Trigger.dev calls the pipeline),
    unlike the private, IPv6-only catalyst_api. Override with ``HOST`` if needed."""
    return os.environ.get("HOST", "0.0.0.0")


# ── Startup schema apply (sql/*.sql → control-plane Postgres) ─────────────────────────────────────
def db_migrate_on_boot() -> bool:
    """Whether to apply the committed DDL (``sql/*.sql``) to the control-plane Postgres at boot — over
    ``HQX_DB_URL_DIRECT`` (session mode) under a session advisory lock, falling back to the pooled DSN
    in local dev (see ``src/migrate.py``). Default ON: the schema is code, re-applied idempotently every
    deploy so a column added to a committed file can never lag prod again — the ``document_payments.rail``
    500 outage, where a SELECT referenced a column never applied to prod. Escape hatch:
    ``EDGE_API_SKIP_DB_MIGRATE=1`` lets an unrelated hotfix ship if a bad DDL file ever wedges the boot."""
    return os.environ.get("EDGE_API_SKIP_DB_MIGRATE", "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    )
