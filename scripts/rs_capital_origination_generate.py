#!/usr/bin/env python3
"""Generate the Rare Structure CAPITAL-ORIGINATION engagement TEMPLATES — one Documenso template per
variant (system fee x success %), from ONE parameterized content source.

Per business.global_input_content_variants row:
    substitute params into the v3 parameterized HTML  ->  DocRaptor PDF
    ->  POST /api/v2/envelope/create  (type=TEMPLATE, 2 placeholder recipients)
    ->  POST /api/v2/envelope/field/create-many  (fields placed by findText over invisible [[ANCHOR]] spans)
    ->  register business.documenso_templates (recipients jsonb, archetype_id, global_input_content_id,
        global_input_content_variant_id) — archiving any prior active row for that variant first.

The PRINTED fee/% (baked into the PDF) and the STORED fee (variant.params.system_fee_cents, which the
payment lane reads) come from the SAME variant row → single source, no drift.

Run (doppler injects DOCRAPTOR_API_KEY / DOCUMENSO_API_KEY / DOCUMENSO_API_URL / HQX_DB_URL_POOLED):
    doppler run --project core-x --config prd -- python3 scripts/rs_capital_origination_generate.py [opts]

  --only SLUG     generate just one variant (e.g. 25k-2_5pct)
  --limit N       generate at most N variants
  --verify        DRY: test-mode PDF, create+place+read-back, then DELETE the template; NO DB write.
                  Use to confirm findText placement before the real (billed, persisted) run.

CREATES REAL Documenso templates and writes business.documenso_templates rows (unless --verify).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys

import httpx
import psycopg

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT = ROOT / "apps/edge_api/content/rare-structure/docraptor-to-documenso-template/capital-origination/v3/global_engagement_content"
REGISTRY_PATH = "docraptor-to-documenso-template/capital-origination/v3"
ARCHETYPE_KEY = "setup_plus_percentage"

PARTICIPANT = {"name": "Participant", "email": "participant@example.com", "role": "SIGNER"}
PROVIDER = {"name": "Benjamin J. Crane", "email": "provider@example.com", "role": "SIGNER"}

# anchor -> (field type, party, prefill label, editable?)
# TEXT labels ARE the prefill keys (must match opportunity_specific_content.field_values keys);
# editable=True labels go into editable_field_labels so originate prefills-but-leaves-unlocked.
FIELDS = [
    ("[[LEN]]", "TEXT", "participant", "Legal Entity Name", True),
    ("[[DBA]]", "TEXT", "participant", "DBA", True),
    ("[[PNM]]", "TEXT", "participant", "Full Name", True),
    ("[[PTL]]", "TEXT", "participant", "Title", True),
    ("[[PSG]]", "SIGNATURE", "participant", None, False),
    ("[[PDT]]", "DATE", "participant", None, False),
    ("[[VSG]]", "SIGNATURE", "provider", None, False),
    ("[[VDT]]", "DATE", "provider", None, False),
    ("[[EFD]]", "DATE", "provider", None, False),  # effective date — provider stamps at countersign
]
SIZE = {"SIGNATURE": {"width": 30.0, "height": 7.0}, "DATE": {"width": 18.0, "height": 4.0}, "TEXT": {"width": 20.0, "height": 4.0}}


def docraptor_pdf(html: str, key: str, *, test: bool) -> bytes:
    r = httpx.post(
        "https://docraptor.com/docs",
        json={"test": test, "document_type": "pdf", "document_content": html,
              "prince_options": {"media": "print", "javascript": False}},
        auth=(key, ""), timeout=httpx.Timeout(120.0, connect=10.0),
    )
    if r.status_code // 100 != 2:
        sys.exit(f"docraptor {r.status_code}: {r.text[:400]}")
    return r.content


def numeric_id(env: dict) -> str:
    return "".join(ch for ch in str(env.get("secondaryId") or "") if ch.isdigit())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()

    dr = os.environ["DOCRAPTOR_API_KEY"]
    dm = os.environ["DOCUMENSO_API_KEY"]
    dm = dm if dm.startswith("api_") else "api_" + dm
    base = os.environ.get("DOCUMENSO_API_URL", "https://app.documenso.com").rstrip("/")
    dburl = os.environ["HQX_DB_URL_POOLED"]

    html_tmpl = (CONTENT / "rare_structure_strategic_origination.html").read_text()
    css = (CONTENT / "styles/plain.css").read_text()

    client = httpx.Client(base_url=base, headers={"Authorization": dm}, timeout=httpx.Timeout(90.0, connect=10.0))
    conn = psycopg.connect(dburl, autocommit=False)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM business.global_input_content WHERE path=%s", (REGISTRY_PATH,))
        r = cur.fetchone()
        if not r:
            sys.exit(f"registry row missing for {REGISTRY_PATH}")
        gic_id = r[0]
        cur.execute("SELECT id FROM business.engagement_archetypes WHERE key=%s", (ARCHETYPE_KEY,))
        archetype_id = cur.fetchone()[0]
        cur.execute("SELECT organization_id FROM business.documenso_templates WHERE documenso_template_id='14423'")
        org_id = cur.fetchone()[0]
        cur.execute(
            "SELECT id, slug, label, params, is_default FROM business.global_input_content_variants "
            "WHERE global_input_content_id=%s AND status='active' ORDER BY slug",
            (gic_id,),
        )
        variants = cur.fetchall()

    if args.only:
        variants = [v for v in variants if v[1] == args.only]
    if args.limit:
        variants = variants[: args.limit]
    mode = "VERIFY (dry, will delete)" if args.verify else "LIVE (persist)"
    print(f"gic={gic_id} archetype={archetype_id} org={org_id}  |  {len(variants)} variant(s)  |  {mode}")

    editable_labels = [lab for (_a, _t, _p, lab, ed) in FIELDS if ed and lab]
    ok = 0
    for vid, slug, label, params, is_default in variants:
        fee_cents = int(params["system_fee_cents"])
        fee_str = f"${fee_cents // 100:,}"
        pct_str = f"{params['success_fee_pct']}%"
        title = f"Rare Structure Strategic Origination — {fee_str} / {pct_str}"
        print(f"\n=== {slug} :: {title} ===")

        doc = (html_tmpl.replace("__STYLESHEET__", css)
               .replace("__SYSTEM_FEE__", fee_str).replace("__SUCCESS_PCT__", pct_str))
        pdf = docraptor_pdf(doc, dr, test=args.verify)
        sha = hashlib.sha256(pdf).hexdigest()
        print(f"  PDF {len(pdf):,} bytes  sha256={sha[:12]}…")

        cr = client.post("/api/v2/envelope/create",
                         data={"payload": json.dumps({"type": "TEMPLATE", "title": title, "recipients": [PARTICIPANT, PROVIDER]})},
                         files={"files": (f"{slug}.pdf", pdf, "application/pdf")})
        if cr.status_code // 100 != 2:
            sys.exit(f"  envelope/create {cr.status_code}: {cr.text[:500]}")
        env_id = cr.json().get("id")
        env = client.get(f"/api/v2/envelope/{env_id}").json()
        recips = {x.get("email"): x.get("id") for x in env.get("recipients", [])}
        rid = {"participant": recips.get(PARTICIPANT["email"]), "provider": recips.get(PROVIDER["email"])}
        num = numeric_id(env)
        print(f"  envelope {env_id} (numeric {num}) recipients {recips}")

        data = []
        for anchor, ftype, party, lab, _ed in FIELDS:
            f = {"type": ftype, "recipientId": rid[party], "placeholder": anchor, "matchAll": False, **SIZE[ftype]}
            if ftype == "TEXT":
                f["fieldMeta"] = {"type": "text", "label": lab}
            data.append(f)
        pl = client.post("/api/v2/envelope/field/create-many", json={"envelopeId": env_id, "data": data})
        if pl.status_code // 100 != 2:
            try:  # don't leave an orphan fieldless template behind on a placement failure
                client.post("/api/v2/template/delete", json={"templateId": int(num)})
            except Exception:  # noqa: BLE001
                pass
            sys.exit(f"  field/create-many {pl.status_code}: {pl.text[:700]}")

        placed = client.get(f"/api/v2/envelope/{env_id}").json().get("fields", [])
        print(f"  placed {len(placed)}/{len(data)} fields:")
        for f in placed:
            fm = f.get("fieldMeta") or {}
            print(f"    {str(f.get('type')):10} recip={f.get('recipientId')} "
                  f"x={round(float(f.get('positionX') or 0), 1)} y={round(float(f.get('positionY') or 0), 1)} label={fm.get('label')!r}")

        complete = len(placed) == len(data)
        if not complete:
            print(f"  !! PLACEMENT INCOMPLETE ({len(placed)}/{len(data)}) — some anchors not found by findText.")

        if args.verify or not complete:
            # tear the throwaway/bad template down; never persist an incompletely-fielded template
            try:
                d = client.post("/api/v2/template/delete", json={"templateId": int(num)})
                print(f"  cleanup template/delete({num}): {d.status_code}")
            except Exception as e:  # noqa: BLE001
                print(f"  cleanup failed: {e} — delete template {num} manually")
            if not complete and not args.verify:
                sys.exit("  aborting LIVE run on incomplete placement")
            continue

        recipients_json = {
            "recipients": [
                {"id": rid["participant"], "role": "SIGNER", "email": PARTICIPANT["email"], "party": "participant", "signing_order": 1},
                {"id": rid["provider"], "role": "SIGNER", "email": PROVIDER["email"], "party": "provider", "signing_order": 2},
            ],
            "prospect_recipient_id": rid["participant"],
            "editable_field_labels": editable_labels,
            "default_field_values": {},
            "text_fields": editable_labels,
            "envelope_id": env_id,
            "secondary_id": f"template_{num}",
        }
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE business.documenso_templates SET status='archived', updated_at=now() "
                "WHERE global_input_content_variant_id=%s AND status='active'",
                (vid,),
            )
            cur.execute(
                "INSERT INTO business.documenso_templates "
                "(id, documenso_template_id, organization_id, name, slug, status, recipients, "
                " archetype_id, global_input_content_id, global_input_content_variant_id, source_pdf_sha256) "
                "VALUES (gen_random_uuid(), %s, %s, %s, %s, 'active', %s::jsonb, %s, %s, %s, %s)",
                (num, org_id, title, slug, json.dumps(recipients_json), archetype_id, gic_id, vid, sha),
            )
        conn.commit()
        ok += 1
        print(f"  ✓ registered documenso_templates (numeric={num}, default={is_default})")

    conn.close()
    client.close()
    print(f"\nDONE — {ok} template(s) persisted.")


if __name__ == "__main__":
    main()
