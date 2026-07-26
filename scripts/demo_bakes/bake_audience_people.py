"""Bake — equipment audience people mart (person grain, both planes).
ONE BUTTON. Rebuilds:
  s3://data-sink/active/equipment_audience_people/       (person grain, tiered)
  ~/Desktop/hq/equipment_audience_people_{YYYY-MM-DD}.csv (Clay-import cut: T1+T2)

Spec + audit: ~/Desktop/hq/2026-07-26-equipment-audience-people-audit-and-spec.md
Tiering (operator-ruled 2026-07-26): the bar is budget authority WITHOUT approval.
  T1 = owner/exec/GM/branch-manager class (signs).
  T2 = kept-adjacent: ops managers, sales leadership, BD, sales managers (flagged
       'sales_manager' — sequenced only at small companies), bare director/manager.
  T3 = titled but off-register (marketing/HR/finance/safety/product/eng/fixed-ops,
       vendor-side account roles — NEVER sequenced; emails may still be harvested).
  T4 = no title.
SAM dm_class_v2='dm' maps to T1 unless the title classifier contradicts (title wins).

Sources: clay_find_people (domain plane) + gtm_audience_people/equipment_yard_profile
(SAM plane), overlap deduped on linkedin_url_norm. Region ONLY via
equipment_company_demo_region; SAM-only rows carry uei, region NULL (no uei<->domain
bridge — do not derive one here).

Run: doppler run -p core-x -c prd -- python3 scripts/demo_bakes/bake_audience_people.py
"""
from __future__ import annotations
import os, re, json, csv, datetime as dt
from collections import Counter
import lance, pyarrow as pa
from _shared import so, norm_domain as norm

ACTIVE = "s3://data-sink/active"
PSEUDO = {"facebook.com", "linkedin.com", "instagram.com", "youtube.com", "twitter.com", "x.com"}

# ---- title classifier (order matters) ----------------------------------------------
RE_T1_CORE = re.compile(
    r'owner|co-owner|partner|principal\b|president|chief executive|\bceo\b|chief operating'
    r'|\bcoo\b|chief financial|\bcfo\b|general manager|branch manager')
RE_EXCLUDE = re.compile(
    r'account (executive|director|manager)|client director|account exec'
    r'|marketing|human resources|\bhr\b|recruit|talent|payroll|accounting|controller'
    r'|director of finance|finance manager|safety|environmental|compliance'
    r'|product (management|design|marketing)|product manager|software|data science'
    r'|engineering|engineer\b|audio visual|information technology|\bit\b manager'
    r'|service manager|parts manager|warranty|customer (service|support|success)')
RE_T1_EXT = re.compile(
    r'vice president|\bvp\b|\bevp\b|\bsvp\b|director of operations|operations director'
    r'|regional manager\b|area manager\b|district manager\b')
RE_T2_OPS = re.compile(r'operations manager|operations supervisor|\bops manager')
RE_T2_SALES_LEAD = re.compile(r'director of sales|sales director|head of sales|national sales manager')
RE_T2_BIZDEV = re.compile(r'business development|\bbd manager|director of business development')
RE_T2_SALESMGR = re.compile(r'(territory|regional|area|district|inside|outside|equipment|rental)?\s*sales manager|territory manager|rental manager')
RE_T2_GENERIC = re.compile(r'^(senior )?(director|manager)$|general operations')

def classify(title: str):
    """-> (tier, title_class)"""
    t = (title or "").lower().strip()
    if not t:
        return "T4", "untitled"
    if RE_T1_CORE.search(t):
        return "T1", "exec_owner"
    if RE_EXCLUDE.search(t):
        return "T3", "off_register"
    if RE_T1_EXT.search(t):
        return "T1", "senior_leadership"
    if RE_T2_OPS.search(t):
        return "T2", "operations"
    if RE_T2_SALES_LEAD.search(t):
        return "T2", "sales_leadership"
    if RE_T2_BIZDEV.search(t):
        return "T2", "bizdev"
    if RE_T2_SALESMGR.search(t):
        return "T2", "sales_manager"   # sequence only at small companies (n_people_at_domain)
    if RE_T2_GENERIC.search(t):
        return "T2", "generic_mgmt"
    return "T3", "other_titled"

def L(name, **kw):
    return lance.dataset(f"{ACTIVE}/{name}", storage_options=so()).to_table(**kw).to_pydict()

def main():
    now = dt.datetime.now(dt.timezone.utc)
    stamp = now.isoformat()

    # ---- company-side lookups -------------------------------------------------------
    dr = L("equipment_company_demo_region", columns=["domain_norm", "macro_region", "demo_region"])
    region = {d: (m, r) for d, m, r in zip(dr["domain_norm"], dr["macro_region"], dr["demo_region"])}

    ind = L("industries_served", columns=["domain_norm", "industries_served"])
    industries = {}
    for d, s in zip(ind["domain_norm"], ind["industries_served"]):
        industries.setdefault(norm(d), s)

    cat = L("equipment_catalog", columns=["domain_norm", "equipment_item_names"])
    equip = {}
    for d, s in zip(cat["domain_norm"], cat["equipment_item_names"]):
        try:
            items = json.loads(s) if s else []
        except Exception:
            items = []
        equip.setdefault(norm(d), "; ".join(items[:5]))

    mm = L("equipment_matchmaking", columns=["domain_norm", "matched_psc_count"])
    pscn = {norm(d): c for d, c in zip(mm["domain_norm"], mm["matched_psc_count"])}

    epname = L("equipment_provider", columns=["domain_norm", "company_domain"])
    # company_name proxy: registrable domain label (no clean name mart on domain plane)
    def coname(d): return d.split(".")[0] if d else None

    # ---- domain plane: clay_find_people, deduped per person -------------------------
    cfp = L("clay_find_people", columns=[
        "linkedin_url_norm", "domain_norm", "full_name", "first_name", "last_name",
        "matched_job_title", "latest_experience_title", "loc_city", "loc_state", "landed_at"])
    people = {}   # person_key -> row dict
    dom_people_count = Counter()
    for i in range(len(cfp["linkedin_url_norm"])):
        li = cfp["linkedin_url_norm"][i]
        d = norm(cfp["domain_norm"][i])
        if not li or d not in region or d in PSEUDO:
            continue
        title = cfp["matched_job_title"][i] or cfp["latest_experience_title"][i]
        prev = people.get(li)
        if prev is None or (title and not prev["title"]):
            mac, dem = region[d]
            people[li] = dict(
                person_key=li, linkedin_url_norm=li, full_name=cfp["full_name"][i],
                first_name=cfp["first_name"][i], last_name=cfp["last_name"][i],
                title=title, domain_norm=d, company_name=coname(d), uei=None,
                macro_region=mac, demo_region=dem,
                industries_topline=industries.get(d), equipment_sample=equip.get(d),
                matched_psc_count=pscn.get(d), dm_class=None,
                email=None, email_status=None, email_source=None,
                phone=None, phone_status=None, source_plane="domain",
                loc_city=cfp["loc_city"][i], loc_state=cfp["loc_state"][i])
    for r in people.values():
        dom_people_count[r["domain_norm"]] += 1

    # ---- SAM plane: yard UEIs -------------------------------------------------------
    prof = L("equipment_yard_profile", columns=["uei", "is_equipment_provider"])
    yard_ueis = {u for u, y in zip(prof["uei"], prof["is_equipment_provider"]) if y}
    gap = L("gtm_audience_people", columns=[
        "sam_person_id", "uei", "display_name", "first_name", "last_name", "title",
        "dm_class_v2", "person_linkedin_url_norm", "email", "email_verification_status",
        "email_source_vendor", "mv_result", "phone", "phone_status"])
    n_overlap = 0
    for i in range(len(gap["uei"])):
        if gap["uei"][i] not in yard_ueis:
            continue
        li = gap["person_linkedin_url_norm"][i]
        if li and li in people:                      # overlap: enrich the domain row
            r = people[li]
            r.update(uei=gap["uei"][i], dm_class=gap["dm_class_v2"][i],
                     email=gap["email"][i], email_status=gap["mv_result"][i] or gap["email_verification_status"][i],
                     email_source=gap["email_source_vendor"][i],
                     phone=gap["phone"][i], phone_status=gap["phone_status"][i],
                     source_plane="both")
            n_overlap += 1
            continue
        key = li or f"sam:{gap['sam_person_id'][i]}"
        if key in people:
            continue
        people[key] = dict(
            person_key=key, linkedin_url_norm=li, full_name=gap["display_name"][i],
            first_name=gap["first_name"][i], last_name=gap["last_name"][i],
            title=gap["title"][i], domain_norm=None, company_name=None, uei=gap["uei"][i],
            macro_region=None, demo_region=None, industries_topline=None,
            equipment_sample=None, matched_psc_count=None, dm_class=gap["dm_class_v2"][i],
            email=gap["email"][i],
            email_status=gap["mv_result"][i] or gap["email_verification_status"][i],
            email_source=gap["email_source_vendor"][i],
            phone=gap["phone"][i], phone_status=gap["phone_status"][i],
            source_plane="sam", loc_city=None, loc_state=None)

    # ---- tier + finalize -------------------------------------------------------------
    rows = []
    for r in people.values():
        tier, tclass = classify(r["title"])
        if tier in ("T3", "T4") and r["dm_class"] == "dm" and tclass in ("untitled", "other_titled"):
            tier, tclass = "T1", "sam_dm"           # dm flag promotes only when title is silent
        r["priority_tier"], r["title_class"] = tier, tclass
        r["n_people_at_domain"] = dom_people_count.get(r["domain_norm"]) if r["domain_norm"] else None
        r["materialized_at"] = stamp
        rows.append(r)

    cols = ["person_key", "linkedin_url_norm", "full_name", "first_name", "last_name",
            "title", "priority_tier", "title_class", "dm_class", "domain_norm",
            "company_name", "uei", "macro_region", "demo_region", "industries_topline",
            "equipment_sample", "matched_psc_count", "n_people_at_domain",
            "email", "email_status", "email_source", "phone", "phone_status",
            "source_plane", "loc_city", "loc_state", "materialized_at"]
    tbl = pa.table({c: [r.get(c) for r in rows] for c in cols})
    lance.write_dataset(tbl, f"{ACTIVE}/equipment_audience_people", storage_options=so(), mode="overwrite")

    csv_path = os.path.expanduser(f"~/Desktop/hq/equipment_audience_people_{now:%Y-%m-%d}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["priority_tier"], x["domain_norm"] or "~")):
            if r["priority_tier"] in ("T1", "T2"):
                w.writerow({c: r.get(c) for c in cols})

    tiers = Counter((r["priority_tier"], r["title_class"]) for r in rows)
    print(f"total people: {len(rows)}  (overlap merged: {n_overlap})")
    for (t, c), n in sorted(tiers.items()):
        print(f"  {t:3} {c:20} {n}")
    by_tier = Counter(r["priority_tier"] for r in rows)
    print("tiers:", dict(sorted(by_tier.items())))
    print("csv (T1+T2):", csv_path)

if __name__ == "__main__":
    main()
