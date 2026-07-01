#!/usr/bin/env python3
"""Push SFNet directory main contacts → Close.com, structured for cold-calling + the
Insights call-sync wiring.

GRAIN: one Close **Lead** per SFNet member company; the directory **main contact** nested
as a **Contact**. Native Close fields carry the dialer/email payload; custom fields carry
the resolution keys (incl. the `company_domain` anchor the Close→Insights webhook bridge
resolves on) and the MillionVerifier email verdict.

SOURCES (Gen-3 Lance, read via R2):
    active/sfnet_main_contacts  — the 168 main contacts (spine)
    active/people               — clean first/last name (via resolved_contact_id)
    active/phone_resolutions    — best phone (prefer phone_type='mobile')   [Power Dialer fuel]
    active/work_emails          — best work email + MillionVerifier verdict (mv_result/mv_quality)
    active/companies            — company_linkedin_url (via resolved_company_id)
    Phone/email joined by person_id OR normalized person_linkedin_url (max coverage).

CLOSE MAPPING:
    Lead.name                 ← company_name
    Lead.url                  ← company_website
    Lead.custom.company_domain        ← normalized_domain   (★ Insights bridge anchor = rsh_domain)
    Lead.custom.company_linkedin_url  ← active/companies linkedin
    Lead.custom.sfnet_company_id      ← sfnet_company_id
    Contact.name              ← person_name        (Close derives first/last for templating)
    Contact.title             ← person_title
    Contact.emails[office]    ← best work email
    Contact.phones[mobile]    ← best phone
    Contact.urls[url]         ← person_linkedin_url
    Contact.custom.first_name / last_name      ← active/people split (cleaner than name-parse)
    Contact.custom.rsh_contact_id              ← resolved_contact_id  (active/people passthrough)
    Contact.custom.sfnet_person_id             ← sfnet_person_id
    Contact.custom.email_status / email_mv_result / email_mv_quality  ← verification verdict

SAFETY (outward-facing live CRM write):
    * DRY-RUN by default — assembles + prints; NO Close writes. --commit to write.
    * Idempotent — skips any company whose `company_domain` custom field already exists in
      Close, and records a ledger (active/close_sfnet_leads) mapping our keys ↔ Close ids.
    * --limit N caps the push (test-first gate: --commit --limit 1, verify, then full).
    * Custom fields are created only if absent (matched by name).

LEDGER / CROSSWALK (the Insights bridge input):
    active/close_sfnet_leads — (sfnet_company_id, sfnet_person_id, normalized_domain,
    resolved_contact_id, close_lead_id, close_contact_id, pushed_at). Append-only.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \
        --with pylance --with duckdb --with pyarrow --with requests \
        python3 pipelines/gtm/push_sfnet_to_close.py            # dry-run
        python3 pipelines/gtm/push_sfnet_to_close.py --commit --limit 1   # test one
        python3 pipelines/gtm/push_sfnet_to_close.py --commit             # full
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time

import duckdb
import lance
import pyarrow as pa
import requests

ACTIVE = "s3://data-sink/active"
LEDGER_URI = os.environ.get("CLOSE_LEDGER_URI", f"{ACTIVE}/close_sfnet_leads/")
BASE = "https://api.close.com/api/v1"
AUTH = (os.environ["CLOSE_API_KEY"], "") if os.environ.get("CLOSE_API_KEY") else None

# Custom field specs (created if absent). (scope, name, type)
LEAD_FIELDS = [("company_domain", "text"), ("company_linkedin_url", "text"), ("sfnet_company_id", "text")]
CONTACT_FIELDS = [("first_name", "text"), ("last_name", "text"), ("rsh_contact_id", "text"),
                  ("sfnet_person_id", "text"), ("email_status", "text"),
                  ("email_mv_result", "text"), ("email_mv_quality", "text")]


def _so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ["R2_ENDPOINT"], "region": "auto"}


def _norm_li(c):
    return (f"nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
            f"lower(trim({c})), '\\?.*$',''), '^https?://',''), '^www\\.',''), '/+$',''), '')")


def _rule(t): print(f"\n{'='*78}\n{t}\n{'='*78}", flush=True)


# ── Close HTTP with light 429 backoff ───────────────────────────────────────
def _req(method, path, **kw):
    for i in range(5):
        r = requests.request(method, f"{BASE}{path}", auth=AUTH, timeout=45, **kw)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("retry-after", 2)) + i)
            continue
        r.raise_for_status()
        return r.json() if r.text else {}
    raise RuntimeError(f"rate-limited 5x on {method} {path}")


# ── assemble the push payload from Lance ────────────────────────────────────
def assemble():
    so = _so()
    con = duckdb.connect(); con.execute("SET memory_limit='6GB';")
    reg = {
        "mc": (f"{ACTIVE}/sfnet_main_contacts/", None),
        "ppl": (f"{ACTIVE}/people/", ["person_id", "first_name", "last_name"]),
        "ph": (f"{ACTIVE}/phone_resolutions/", ["person_id", "person_linkedin_url", "phone", "phone_type", "phone_status", "resolved_at"]),
        "we": (f"{ACTIVE}/work_emails/", ["person_id", "person_linkedin_url", "email", "verification_status", "mv_result", "mv_quality", "resolved_at"]),
        "co": (f"{ACTIVE}/companies/", ["company_id", "company_linkedin_url", "firmo_linkedin_url"]),
    }
    for name, (uri, cols) in reg.items():
        sc = lance.dataset(uri, storage_options=so).scanner(columns=cols) if cols else \
             lance.dataset(uri, storage_options=so).scanner()
        con.register(f"{name}_r", sc.to_reader())
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_r")

    # best phone per key (prefer mobile, then most recent)
    con.execute(f"""CREATE TABLE ph_best AS
        SELECT person_id, {_norm_li('person_linkedin_url')} li, phone, phone_type FROM (
          SELECT *, row_number() OVER (PARTITION BY person_id
            ORDER BY (lower(coalesce(phone_type,''))='mobile') DESC, resolved_at DESC) rn
          FROM ph WHERE phone IS NOT NULL) WHERE rn=1""")
    con.execute(f"""CREATE TABLE ph_li AS
        SELECT li, any_value(phone) phone, any_value(phone_type) phone_type FROM ph_best
        WHERE li IS NOT NULL GROUP BY li""")
    # best email per key (prefer good quality, then recent)
    con.execute(f"""CREATE TABLE we_best AS
        SELECT person_id, {_norm_li('person_linkedin_url')} li, email, verification_status, mv_result, mv_quality FROM (
          SELECT *, row_number() OVER (PARTITION BY person_id ORDER BY
            (lower(coalesce(mv_quality,''))='good') DESC,
            (lower(coalesce(verification_status,'')) IN ('ok','valid','deliverable')) DESC,
            resolved_at DESC) rn
          FROM we WHERE email IS NOT NULL) WHERE rn=1""")
    con.execute("""CREATE TABLE we_li AS
        SELECT li, any_value(email) email, any_value(verification_status) verification_status,
               any_value(mv_result) mv_result, any_value(mv_quality) mv_quality
        FROM we_best WHERE li IS NOT NULL GROUP BY li""")

    rows = con.execute(f"""
        SELECT
          mc.sfnet_company_id, mc.sfnet_person_id, mc.company_name, mc.company_website,
          mc.normalized_domain, mc.person_name, mc.person_title, mc.linkedin_url_raw AS person_linkedin_url,
          mc.resolved_contact_id, mc.resolved_company_id,
          coalesce(p.first_name, split_part(mc.person_name,' ',1)) AS first_name,
          coalesce(p.last_name, nullif(regexp_replace(mc.person_name,'^\\S+\\s+',''),'')) AS last_name,
          coalesce(pc.phone, pl.phone)                AS phone,
          coalesce(pc.phone_type, pl.phone_type)      AS phone_type,
          coalesce(ec.email, el.email)                AS email,
          coalesce(ec.verification_status, el.verification_status) AS email_status,
          coalesce(ec.mv_result, el.mv_result)        AS email_mv_result,
          coalesce(ec.mv_quality, el.mv_quality)      AS email_mv_quality,
          coalesce(co.company_linkedin_url, co.firmo_linkedin_url) AS company_linkedin_url
        FROM mc
        LEFT JOIN ppl p     ON p.person_id = mc.resolved_contact_id
        LEFT JOIN ph_best pc ON pc.person_id = mc.resolved_contact_id
        LEFT JOIN ph_li   pl ON pl.li = mc.linkedin_url AND mc.linkedin_url IS NOT NULL
        LEFT JOIN we_best ec ON ec.person_id = mc.resolved_contact_id
        LEFT JOIN we_li   el ON el.li = mc.linkedin_url AND mc.linkedin_url IS NOT NULL
        LEFT JOIN co        ON co.company_id = mc.resolved_company_id
        ORDER BY mc.company_name, mc.person_name
    """).fetch_arrow_table().to_pylist()
    con.close()

    # group contacts under their company → leads
    leads = {}
    for r in rows:
        key = r["sfnet_company_id"] or r["normalized_domain"] or r["company_name"]
        leads.setdefault(key, {"company": r, "contacts": []})
        leads[key]["contacts"].append(r)
    return list(leads.values())


def _lead_body(lead, F):
    c = lead["company"]
    body = {"name": c["company_name"]}
    if c.get("company_website"):
        body["url"] = c["company_website"]
    body[f"custom.{F['lead']['company_domain']}"] = c.get("normalized_domain")
    body[f"custom.{F['lead']['company_linkedin_url']}"] = c.get("company_linkedin_url")
    body[f"custom.{F['lead']['sfnet_company_id']}"] = c.get("sfnet_company_id")
    contacts = []
    for p in lead["contacts"]:
        cd = {"name": p["person_name"]}
        if p.get("person_title"):
            cd["title"] = p["person_title"]
        if p.get("email"):
            cd["emails"] = [{"email": p["email"], "type": "office"}]
        if p.get("phone"):
            ptype = "mobile" if (p.get("phone_type") or "").lower() == "mobile" else "office"
            cd["phones"] = [{"phone": p["phone"], "type": ptype}]
        if p.get("person_linkedin_url"):
            cd["urls"] = [{"url": p["person_linkedin_url"], "type": "url"}]
        for fname, val in [("first_name", p.get("first_name")), ("last_name", p.get("last_name")),
                           ("rsh_contact_id", p.get("resolved_contact_id")),
                           ("sfnet_person_id", p.get("sfnet_person_id")),
                           ("email_status", p.get("email_status")),
                           ("email_mv_result", p.get("email_mv_result")),
                           ("email_mv_quality", p.get("email_mv_quality"))]:
            cd[f"custom.{F['contact'][fname]}"] = val
        contacts.append(cd)
    body["contacts"] = contacts
    # strip null custom values (Close rejects some nulls)
    return {k: v for k, v in body.items() if v is not None or k in ("contacts",)}


def ensure_fields(commit):
    """Return {'lead': {name:id}, 'contact': {name:id}} — create absent ones (if commit)."""
    out = {"lead": {}, "contact": {}}
    for scope, specs in (("lead", LEAD_FIELDS), ("contact", CONTACT_FIELDS)):
        existing = {f["name"]: f["id"] for f in _req("GET", f"/custom_field/{scope}/", params={"_limit": 100}).get("data", [])}
        for name, ftype in specs:
            if name in existing:
                out[scope][name] = existing[name]
            elif commit:
                created = _req("POST", f"/custom_field/{scope}/", json={"name": name, "type": ftype})
                out[scope][name] = created["id"]
                print(f"    created {scope} custom field: {name} -> {created['id']}", flush=True)
            else:
                out[scope][name] = f"<would-create:{scope}.{name}>"
    return out


def pushed_company_ids():
    """sfnet_company_ids already pushed (idempotency guard) — read from the ledger.
    Keyed on sfnet_company_id (always present) so null-domain leads dedup correctly too."""
    so = _so()
    try:
        ds = lance.dataset(LEDGER_URI, storage_options=so)
        vals = ds.scanner(columns=["sfnet_company_id"]).to_table().column("sfnet_company_id").to_pylist()
        return set(v for v in vals if v)
    except Exception:
        return set()


def write_ledger(ledger_rows):
    if not ledger_rows:
        return
    so = _so()
    schema = pa.schema([(k, pa.string()) for k in
                        ["sfnet_company_id", "sfnet_person_id", "normalized_domain",
                         "resolved_contact_id", "close_lead_id", "close_contact_id"]] +
                       [("pushed_at", pa.timestamp("us", tz="UTC"))])
    tbl = pa.Table.from_pylist(ledger_rows, schema=schema)
    mode = "append"
    try:
        lance.dataset(LEDGER_URI, storage_options=so)
    except Exception:
        mode = "overwrite"
    lance.write_dataset(tbl, LEDGER_URI, mode=mode, data_storage_version="2.1", storage_options=so)
    ds = lance.dataset(LEDGER_URI, storage_options=so)
    for col in ("sfnet_company_id", "normalized_domain", "close_lead_id"):
        try:
            ds.create_scalar_index(col, index_type="BTREE")
        except Exception:
            pass
    print(f"    ledger {mode} -> {LEDGER_URI} ({ds.count_rows():,} rows)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="actually write to Close (else dry-run)")
    ap.add_argument("--limit", type=int, default=None, help="cap number of leads pushed")
    args = ap.parse_args()

    leads = assemble()
    nlead = len(leads)
    ncontact = sum(len(l["contacts"]) for l in leads)
    nphone = sum(1 for l in leads for p in l["contacts"] if p.get("phone"))
    nemail = sum(1 for l in leads for p in l["contacts"] if p.get("email"))
    _rule("assembled")
    print(f"    leads (companies): {nlead}   contacts: {ncontact}", flush=True)
    print(f"    contacts w/ phone: {nphone}   w/ work email: {nemail}", flush=True)

    F = ensure_fields(args.commit)
    if not args.commit:
        _rule("DRY-RUN — sample lead payloads (2)")
        for l in leads[:2]:
            print(json.dumps(_lead_body(l, F), indent=2, default=str), flush=True)
        print(f"\n[dry-run] would create {nlead} leads. Re-run with --commit (custom fields are placeholders above).", flush=True)
        return 0

    skip = pushed_company_ids()
    target = [l for l in leads if (l["company"].get("sfnet_company_id") or "") not in skip]
    if skip:
        print(f"    (idempotency: {len(skip)} companies already in ledger — skipping them)", flush=True)
    if args.limit:
        target = target[:args.limit]
    _rule(f"PUSH — creating {len(target)} leads in Close")
    ledger, created = [], 0
    for l in target:
        body = _lead_body(l, F)
        res = _req("POST", "/lead/", json=body)
        lead_id = res["id"]
        contact_ids = [c["id"] for c in res.get("contacts", [])]
        created += 1
        for p, cid in zip(l["contacts"], contact_ids):
            ledger.append({
                "sfnet_company_id": l["company"].get("sfnet_company_id"),
                "sfnet_person_id": p.get("sfnet_person_id"),
                "normalized_domain": l["company"].get("normalized_domain"),
                "resolved_contact_id": p.get("resolved_contact_id"),
                "close_lead_id": lead_id, "close_contact_id": cid,
                "pushed_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0),
            })
        print(f"    [{created}/{len(target)}] {l['company']['company_name']} -> {lead_id} ({len(contact_ids)} contact)", flush=True)
        time.sleep(0.2)
    write_ledger(ledger)

    _rule("VERIFY — read one back from Close")
    if ledger:
        lid = ledger[0]["close_lead_id"]
        lead = _req("GET", f"/lead/{lid}/")
        cf = {k: v for k, v in lead.items() if k.startswith("custom.")}
        ct = lead.get("contacts", [{}])[0]
        print(f"    lead {lid}: {lead.get('display_name')}  url={lead.get('url')}", flush=True)
        print(f"    lead custom: {json.dumps(cf, default=str)[:300]}", flush=True)
        print(f"    contact: name={ct.get('name')} title={ct.get('title')} "
              f"phones={ct.get('phones')} emails={ct.get('emails')}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
