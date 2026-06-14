#!/usr/bin/env python3
"""Create the two Active Operators agreement TEMPLATES in Documenso via the v2 API.

For each archetype (term_only, term_plus_greater_of):
    build_html  ->  DocRaptor PDF  ->  POST /api/v2/envelope/create (type=TEMPLATE, 2 recipients, PDF)
    ->  read recipient ids  ->  POST /api/v2/envelope/field/create-many
        (every [[anchor]] / {{token}} placed BY PLACEHOLDER via findText — no coordinates)

Recipients are template PLACEHOLDERS (overridden per-deal at /template/use):
    Provider    = Benjamin J. Crane (signs the Rare Structure side)
    Participant = the counterparty (signs + supplies identity)

Field routing:
    [[PROVIDER_*]]    + every commercial-term {{token}}  -> Provider
    [[PARTICIPANT_*]] + every {{participant_*}} token     -> Participant
    [[...SIGNATURE]] -> SIGNATURE,  [[...DATE]] -> DATE,  {{...}} -> TEXT

Run (doppler injects DOCRAPTOR_API_KEY, DOCUMENSO_API_KEY, DOCUMENSO_API_URL):
    doppler run --project core-x --config prd -- python3 scripts/documenso_push_templates.py

CREATES REAL TEMPLATES in the configured Documenso account.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import sys

import httpx

# Reuse the render harness (build_html / archetype selection) — single source of truth for content.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import render_ao_preview as R  # noqa: E402

DOCRAPTOR_URL = "https://docraptor.com/docs"
ARCHETYPES = ("term_only", "term_plus_greater_of")  # term_only first — fail fast before the second
TITLES = {
    "term_only": "AO Strategic Origination Agreement — Term Only",
    "term_plus_greater_of": "AO Strategic Origination Agreement — Term + Success Fee",
}

PROVIDER = {"name": "Benjamin J. Crane", "email": "benjaminjcrane@gmail.com", "role": "SIGNER"}
PARTICIPANT = {"name": "Participant", "email": "participant@example.com", "role": "SIGNER"}

# Field box SIZE override — PERCENT of the page (0-100). Position comes from the placeholder (findText).
SIZE = {
    "SIGNATURE": {"width": 30.0, "height": 7.0},
    "DATE": {"width": 20.0, "height": 4.0},
    "TEXT": {"width": 16.0, "height": 3.6},
}

_ANCHOR_RE = re.compile(r"\[\[[A-Z_]+\]\]")
_TOKEN_RE = re.compile(r"\{\{[a-z0-9_]+\}\}")


def docraptor_pdf(html: str, api_key: str) -> bytes:
    payload = {
        "test": False,
        "document_type": "pdf",
        "name": "agreement.pdf",
        "document_content": html,
        "prince_options": {"media": "print", "javascript": False},
    }
    r = httpx.post(DOCRAPTOR_URL, json=payload, auth=(api_key, ""), timeout=httpx.Timeout(120.0, connect=10.0))
    if r.status_code // 100 != 2:
        sys.exit(f"docraptor {r.status_code}: {r.text[:400]}")
    return r.content


def label_for(token: str) -> str:
    return token.strip("{}").replace("_", " ").title()


def classify(ph: str, provider_id, participant_id) -> dict:
    is_participant = ("PARTICIPANT" in ph) or ph.startswith("{{participant_")
    rid = participant_id if is_participant else provider_id
    if ph.startswith("[["):
        ftype = "SIGNATURE" if "SIGNATURE" in ph else "DATE"
        field = {"type": ftype, "recipientId": rid, "placeholder": ph, "matchAll": True, **SIZE[ftype]}
    else:
        field = {
            "type": "TEXT",
            "recipientId": rid,
            "placeholder": ph,
            "matchAll": True,
            "fieldMeta": {"type": "text", "label": label_for(ph)},
            **SIZE["TEXT"],
        }
    return field


def main() -> None:
    dr_key = os.environ.get("DOCRAPTOR_API_KEY") or sys.exit("DOCRAPTOR_API_KEY not set")
    dm_key = os.environ.get("DOCUMENSO_API_KEY") or sys.exit("DOCUMENSO_API_KEY not set")
    if not dm_key.startswith("api_"):
        dm_key = "api_" + dm_key
    base = os.environ.get("DOCUMENSO_API_URL", "https://app.documenso.com").rstrip("/")
    client = httpx.Client(base_url=base, headers={"Authorization": dm_key}, timeout=httpx.Timeout(90.0, connect=10.0))

    for arch in ARCHETYPES:
        title = TITLES[arch]
        print(f"\n=== {arch} :: {title} ===")
        html = R.build_html(arch)
        placeholders = list(dict.fromkeys(_ANCHOR_RE.findall(html) + _TOKEN_RE.findall(html)))
        pdf = docraptor_pdf(html, dr_key)
        print(f"  rendered PDF ({len(pdf):,} bytes); {len(placeholders)} distinct placeholders")

        # 1) create the TEMPLATE envelope (multipart: payload JSON + PDF)
        payload = {"type": "TEMPLATE", "title": title, "recipients": [PARTICIPANT, PROVIDER]}
        created = client.post(
            "/api/v2/envelope/create",
            data={"payload": json.dumps(payload)},
            files={"files": (f"{title}.pdf", pdf, "application/pdf")},
        )
        if created.status_code // 100 != 2:
            sys.exit(f"  [{arch}] envelope/create {created.status_code}: {created.text[:600]}")
        env_id = created.json().get("id")
        print(f"  template envelope created: {env_id}")

        # 2) read recipient ids back
        env = client.get(f"/api/v2/envelope/{env_id}").json()
        recips = env.get("recipients") or []

        def rid_for(email: str):
            for r in recips:
                if str(r.get("email") or "").lower() == email.lower():
                    return r.get("id")
            return None

        prov_id, part_id = rid_for(PROVIDER["email"]), rid_for(PARTICIPANT["email"])
        if prov_id is None or part_id is None:
            sys.exit(f"  [{arch}] could not resolve recipient ids: {[(r.get('email'), r.get('id')) for r in recips]}")
        print(f"  recipients: provider={prov_id} participant={part_id}")

        # 3) place every field by placeholder
        fields = [classify(ph, prov_id, part_id) for ph in placeholders]
        placed = client.post("/api/v2/envelope/field/create-many", json={"envelopeId": env_id, "data": fields})
        if placed.status_code // 100 != 2:
            sys.exit(f"  [{arch}] field/create-many {placed.status_code}: {placed.text[:800]}")
        made = placed.json()
        n = len(made) if isinstance(made, list) else len(made.get("fields", made.get("data", [])) or fields)
        secondary = env.get("secondaryId")
        print(f"  fields placed: {n} (from {len(fields)} placeholders)")
        print(f"  DONE -> envelopeId={env_id}  secondaryId={secondary}")

    client.close()
    print("\nAll templates created. Open Documenso → Templates to set branding + per-recipient field tweaks.")


if __name__ == "__main__":
    main()
