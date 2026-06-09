"""Unit tests for the anchor-based Documenso signature placement.

No network. These pin the contract that replaced blind-coordinate field placement:

  1. The ``[[CLIENT_SIGNATURE]]`` / ``[[CLIENT_DATE]]`` anchors survive token substitution
     (they are NOT ``{{merge}}`` grammar) and reach the rendered HTML — both the published-
     template SHELL and the built-in Strategic Origination Mandate.
  2. ``documenso_client.create_signing_envelope`` builds the ``field/create-many`` payload using
     the PLACEHOLDER branch (the exact shared anchor strings + recipientId), with NO coordinate
     or page keys — matching Documenso v2 ``ZPlaceholderPositionSchema``.

The single source of truth for the marker strings is ``signing_anchors``; tests import the same
constants the renderer and client do, so a drift in either side fails here.
"""
from __future__ import annotations

import asyncio
import datetime as dt

from apps.edge_api.src.proposals import agreement_template, template_render
from apps.edge_api.src.proposals.models import Proposal
from apps.edge_api.src.proposals.signing_anchors import (
    CLIENT_DATE_ANCHOR,
    CLIENT_SIGNATURE_ANCHOR,
)
from apps.edge_api.src.services import documenso_client


def _proposal() -> Proposal:
    return Proposal(
        ref="prop_test123",
        template_id="strategic_origination_mandate",
        client_name="Angular Capital Partners LP",
        client_signer_name="Dana Reyes",
        client_title="Managing Partner",
        client_email="dana@angularcap.example",
        effective_date=dt.date(2026, 6, 9),
        monthly_fee_cents=2_500_000,
        quarterly_total_cents=7_500_000,
        rs_signer_name="Benjamin Crane",
        status="draft",
    )


# ── 1. Anchors are NOT merge tokens and survive substitution ─────────────────────────────────
def test_anchor_grammar_is_not_merge_grammar():
    """The [[...]] sentinel must not match the {{snake_case}} token regex."""
    for anchor in (CLIENT_SIGNATURE_ANCHOR, CLIENT_DATE_ANCHOR):
        assert template_render.extract_tokens(anchor) == []
        assert "{{" not in anchor and "}}" not in anchor


def test_extract_tokens_ignores_anchors_in_assembled_doc():
    """A document carrying both real merge tokens and the anchors reports ONLY the merge tokens."""
    html_doc = template_render.render_template_html("Fee is {{monthly_fee}} per {{period}}.")
    tokens = template_render.extract_tokens(html_doc)
    # Real merge tokens are surfaced…
    assert "monthly_fee" in tokens and "period" in tokens
    assert "client_signer_name" in tokens  # a SHELL token
    # …but the anchors never appear as merge fields (they have no {{}} form).
    assert "CLIENT_SIGNATURE" not in tokens
    assert "CLIENT_DATE" not in tokens


def test_substitute_tokens_leaves_anchors_intact():
    """Substituting an empty value map must not touch the literal [[...]] anchors."""
    html_doc = template_render.render_template_html("Body {{client_signer_name}}.")
    # Pre-condition: the assembled SHELL actually carries both anchors.
    assert CLIENT_SIGNATURE_ANCHOR in html_doc
    assert CLIENT_DATE_ANCHOR in html_doc

    out = template_render.substitute_tokens(html_doc, {})
    assert CLIENT_SIGNATURE_ANCHOR in out
    assert CLIENT_DATE_ANCHOR in out


def test_substitute_tokens_preserves_anchors_while_filling_merge_fields():
    """Filling merge fields leaves the anchors byte-for-byte."""
    html_doc = template_render.render_template_html("Hello {{client_signer_name}}.")
    out = template_render.substitute_tokens(html_doc, {"client_signer_name": "Dana Reyes"})
    assert "Dana Reyes" in out
    assert "{{client_signer_name}}" not in out  # merge token consumed
    assert CLIENT_SIGNATURE_ANCHOR in out       # anchor untouched
    assert CLIENT_DATE_ANCHOR in out


# ── 2. Both render paths emit the anchors as selectable text ─────────────────────────────────
def test_shell_render_emits_both_anchors():
    html_doc = template_render.render_template_html("# Body\n\nSome clause.")
    # Exactly ONE LIVE marker each — counted as the styled span, so the explanatory HTML comment
    # (which also names the anchors) is not mistaken for a placement target.
    assert html_doc.count(f'<span class="sig-anchor">{CLIENT_SIGNATURE_ANCHOR}</span>') == 1
    assert html_doc.count(f'<span class="sig-anchor">{CLIENT_DATE_ANCHOR}</span>') == 1
    # Sentinels must be fully resolved — no leftover «...» placeholder in the output.
    assert "«CLIENT_SIGNATURE_ANCHOR»" not in html_doc
    assert "«CLIENT_DATE_ANCHOR»" not in html_doc
    assert "«BODY»" not in html_doc and "«STYLE»" not in html_doc


def test_builtin_mandate_emits_both_anchors():
    html_doc = agreement_template.render_agreement_html(_proposal())
    assert CLIENT_SIGNATURE_ANCHOR in html_doc
    assert CLIENT_DATE_ANCHOR in html_doc
    assert "«CLIENT_SIGNATURE_ANCHOR»" not in html_doc
    assert "«CLIENT_DATE_ANCHOR»" not in html_doc
    # The RS side stays merge-driven and fully resolved (RS is not a Documenso signer).
    assert "Benjamin Crane" in html_doc
    assert "«RS_NAME»" not in html_doc


def test_anchors_render_inside_sig_anchor_span():
    """The marker must ride in a styled-but-real text span (selectable for findText), not be
    hidden. Assert it sits inside a .sig-anchor span in both render paths."""
    for html_doc in (
        template_render.render_template_html("# Body"),
        agreement_template.render_agreement_html(_proposal()),
    ):
        assert f'<span class="sig-anchor">{CLIENT_SIGNATURE_ANCHOR}</span>' in html_doc
        assert f'<span class="sig-anchor">{CLIENT_DATE_ANCHOR}</span>' in html_doc


def test_body_content_cannot_be_mistaken_for_anchor_sentinel():
    """If authored body text literally contains a shell sentinel, it must pass through as body
    text — the shell's own sentinels are resolved BEFORE «BODY» injection."""
    html_doc = template_render.render_template_html("Literal «CLIENT_SIGNATURE_ANCHOR» in prose.")
    # Exactly ONE live signature-anchor span — from the shell, on the client sig line. The body's
    # «...» sentinel text was NOT resolved (it is injected last), so it never becomes a 2nd span.
    assert html_doc.count(f'<span class="sig-anchor">{CLIENT_SIGNATURE_ANCHOR}</span>') == 1
    # The body's literal sentinel survives as inert text (it is not the resolved [[...]] marker).
    assert "«CLIENT_SIGNATURE_ANCHOR»" in html_doc


# ── 3. documenso_client builds the placeholder payload (no network) ──────────────────────────
class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status_code = status
        self.text = "ok"
        self.headers: dict[str, str] = {}
        self.content = b""

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    """Captures every POST/GET body; returns canned envelope responses. No sockets opened."""

    def __init__(self, recorder: dict):
        self._rec = recorder

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    async def post(self, path: str, **kwargs) -> _FakeResponse:
        self._rec.setdefault("posts", []).append({"path": path, **kwargs})
        if path.endswith("/envelope/create"):
            return _FakeResponse({"id": "env_abc"})
        return _FakeResponse({"ok": True})

    async def get(self, path: str, **kwargs) -> _FakeResponse:
        self._rec.setdefault("gets", []).append({"path": path, **kwargs})
        # The read-back after create: one matching SIGNER recipient + numeric secondaryId.
        return _FakeResponse(
            {
                "id": "env_abc",
                "secondaryId": "document_42",
                "recipients": [
                    {"id": 7, "email": "dana@angularcap.example", "token": "tok_xyz", "role": "SIGNER"}
                ],
            }
        )


def _run_create_envelope(monkeypatch_recorder: dict):
    client = _FakeClient(monkeypatch_recorder)
    # Replace the network client factory wholesale — bypasses auth + sockets entirely.
    original = documenso_client._client
    documenso_client._client = lambda: client  # type: ignore[assignment]
    try:
        return asyncio.run(
            documenso_client.create_signing_envelope(
                b"%PDF-1.7 fake bytes",
                title="Strategic Origination Mandate — Angular Capital Partners LP",
                signer_name="Dana Reyes",
                signer_email="dana@angularcap.example",
                external_id="prop_test123",
            )
        )
    finally:
        documenso_client._client = original  # type: ignore[assignment]


def test_field_create_payload_uses_placeholder_branch():
    rec: dict = {}
    result = _run_create_envelope(rec)

    # The envelope was created and the token/ids read back.
    assert result.envelope_id == "env_abc"
    assert result.client_token == "tok_xyz"
    assert result.document_id == 42

    create_field_posts = [
        p for p in rec["posts"] if p["path"].endswith("/envelope/field/create-many")
    ]
    assert len(create_field_posts) == 1
    fields = create_field_posts[0]["json"]["data"]
    assert create_field_posts[0]["json"]["envelopeId"] == "env_abc"
    assert len(fields) == 2

    by_type = {f["type"]: f for f in fields}
    sig, date = by_type["SIGNATURE"], by_type["DATE"]

    # Placeholder branch: the EXACT shared anchor strings, recipientId set.
    assert sig["placeholder"] == CLIENT_SIGNATURE_ANCHOR
    assert date["placeholder"] == CLIENT_DATE_ANCHOR
    assert sig["recipientId"] == 7 and date["recipientId"] == 7


def test_field_create_payload_has_no_coordinate_or_page_keys():
    rec: dict = {}
    _run_create_envelope(rec)
    fields = next(
        p for p in rec["posts"] if p["path"].endswith("/envelope/field/create-many")
    )["json"]["data"]

    forbidden = {"page", "positionX", "positionY"}
    for f in fields:
        assert forbidden.isdisjoint(f.keys()), f"coordinate/page key leaked into {f}"
        # Only the placeholder + optional size overrides are allowed alongside type/recipientId.
        assert set(f.keys()) <= {"type", "recipientId", "placeholder", "width", "height"}


def test_field_create_payload_size_overrides_present():
    """width/height overrides are intentional (the marker's own box is too small to sign on)."""
    rec: dict = {}
    _run_create_envelope(rec)
    fields = next(
        p for p in rec["posts"] if p["path"].endswith("/envelope/field/create-many")
    )["json"]["data"]
    by_type = {f["type"]: f for f in fields}
    assert by_type["SIGNATURE"]["width"] == 32.0 and by_type["SIGNATURE"]["height"] == 7.0
    assert by_type["DATE"]["width"] == 22.0 and by_type["DATE"]["height"] == 4.0


def test_distribute_called_without_email():
    """Sanity: the embedded-signing flow distributes with method NONE (no Documenso email)."""
    rec: dict = {}
    _run_create_envelope(rec)
    distribute = [p for p in rec["posts"] if p["path"].endswith("/envelope/distribute")]
    assert len(distribute) == 1
    assert distribute[0]["json"]["meta"]["distributionMethod"] == "NONE"
