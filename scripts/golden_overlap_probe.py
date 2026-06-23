"""Recon — the GOLDEN TRIPLE OVERLAP.

Side 1+2 (where the work is + the yard's geo): govcon_firm_construction_proximity
         (firm_domain x psc_code -> nearby_award_count, nearby_total_award_value).
Side 3 (what the yard can supply): equipment_matchmaking (domain_norm -> supported_pscs).

Join on (firm_domain == domain_norm) AND (psc_code in supported_pscs) to convert raw proximity
into CAPABILITY-QUALIFIED proximity: nearby federal construction demand the yard can actually
capture because it stocks the right iron.

Read-only. Prints the analysis backbone; writes a firm-level golden JSONL for materialization.

Run: doppler run -p core-x -c prd -- python3 scripts/golden_overlap_probe.py
"""

from __future__ import annotations

import collections
import json
import os

import lance


PROX_URI = "s3://data-sink/active/govcon_firm_construction_proximity/"
MM_URI = "s3://data-sink/active/equipment_matchmaking/"
OUT_JSONL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "reports", "golden_overlap_firm_level.jsonl")

PSC_NAME = {
    "Z2AA": "Repair/Alteration of Office Buildings",
    "Y1DA": "Construction of Hospitals & Infirmaries",
    "Z1DA": "Maintenance of Hospitals & Infirmaries",
    "Z2DA": "Repair/Alteration of Hospitals & Infirmaries",
    "Y1LB": "Construction of Highways/Roads/Bridges",
    "Z1LB": "Maintenance/Repair of Highways/Roads/Bridges",
    "Y1PC": "Construction of Unimproved Land",
    "Y1NE": "Construction of Water Supply Facilities",
    "Y1KD": "Construction of Mine Subsidence Control",
    "Y1PZ": "Construction of Other Non-Building Facilities",
    "Z2KA": "Repair/Alteration of Dams / Dredging",
    "Z1KF": "Maintenance/Repair of Dredging Facilities",
    "P400": "Demolition of Buildings",
    "F108": "Environmental Remediation",
    "F014": "Tree Thinning",
}
MAPPED15 = set(PSC_NAME)


def _so():
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _fmt(n):
    return f"{n:,.0f}"


def main():
    so = _so()
    prox = lance.dataset(PROX_URI, storage_options=so).to_table(
        columns=["firm_domain", "psc_code", "nearby_award_count", "nearby_total_award_value"]).to_pylist()
    mm = lance.dataset(MM_URI, storage_options=so).to_table(
        columns=["domain_norm", "supported_pscs", "matched_psc_count"]).to_pylist()

    cap = {r["domain_norm"]: set(r["supported_pscs"] or []) for r in mm}
    cap_matched = {d for d, s in cap.items() if s}

    prox_domains = {r["firm_domain"] for r in prox if r["firm_domain"]}
    mm_domains = set(cap)

    inter = prox_domains & mm_domains            # geo-placed AND scraped-for-capability
    inter_capable = inter & cap_matched          # ...and capable of >=1 mapped PSC

    print("=" * 80)
    print("DOMAIN RECONCILIATION")
    print(f"  proximity distinct firm_domain (geo-placed candidates) : {_fmt(len(prox_domains))}")
    print(f"  matchmaking distinct domain_norm (scraped catalogs)    : {_fmt(len(mm_domains))}")
    print(f"  matchmaking with >=1 PSC capability                    : {_fmt(len(cap_matched))}")
    print(f"  INTERSECTION (geo + scraped)                           : {_fmt(len(inter))}")
    print(f"  INTERSECTION (geo + capable)                           : {_fmt(len(inter_capable))}")

    # ---- per-firm rollups over the intersection ----
    firm = collections.defaultdict(lambda: {
        "all_cnt": 0, "all_val": 0.0,
        "mapped_cnt": 0, "mapped_val": 0.0,
        "qual_cnt": 0, "qual_val": 0.0,
        "qual_pscs": collections.defaultdict(lambda: [0, 0.0]),  # psc -> [cnt, val]
        "mapped_pscs_near": set(),
    })
    # aggregate proximity totals (all firms, for context) + intersection details
    grand_all_pairs = 0
    psc_demand = collections.defaultdict(lambda: {
        "demand": 0, "firms_near": set(), "capable_firms": set(), "qual_demand": 0, "qual_val": 0.0})
    uncovered_demand = collections.Counter()  # top PSCs in intersection NOT in MAPPED15

    for r in prox:
        d = r["firm_domain"]
        grand_all_pairs += r["nearby_award_count"] or 0
        if d not in inter:
            continue
        p = r["psc_code"]
        cnt = r["nearby_award_count"] or 0
        val = r["nearby_total_award_value"] or 0.0
        f = firm[d]
        f["all_cnt"] += cnt
        f["all_val"] += val
        if p in MAPPED15:
            f["mapped_cnt"] += cnt
            f["mapped_val"] += val
            f["mapped_pscs_near"].add(p)
            pd = psc_demand[p]
            pd["demand"] += cnt
            pd["firms_near"].add(d)
            if p in cap.get(d, ()):  # capability-qualified
                f["qual_cnt"] += cnt
                f["qual_val"] += val
                f["qual_pscs"][p][0] += cnt
                f["qual_pscs"][p][1] += val
                pd["capable_firms"].add(d)
                pd["qual_demand"] += cnt
                pd["qual_val"] += val
        else:
            uncovered_demand[p] += cnt

    golden = {d: f for d, f in firm.items() if f["qual_cnt"] > 0}
    sum_mapped = sum(f["mapped_cnt"] for f in firm.values())
    sum_qual = sum(f["qual_cnt"] for f in firm.values())
    sum_qual_val = sum(f["qual_val"] for f in firm.values())

    print("\n" + "=" * 80)
    print("FUNNEL — raw proximity -> capability-qualified")
    print(f"  all firm-award proximity pairs (entire matrix, 333 PSCs)     : {_fmt(grand_all_pairs)}")
    print(f"  pairs among intersection firms, mapped-15 PSCs               : {_fmt(sum_mapped)}")
    print(f"  pairs among intersection firms, CAPABILITY-QUALIFIED         : {_fmt(sum_qual)}")
    print(f"  qualified value exposure ($, double-counts shared awards)    : ${_fmt(sum_qual_val)}")
    print(f"  GOLDEN firms (>=1 qualified nearby project)                  : {_fmt(len(golden))}")

    # density distribution of qualified demand
    qbands = collections.Counter()
    for f in golden.values():
        q = f["qual_cnt"]
        b = "1" if q == 1 else "2-5" if q <= 5 else "6-25" if q <= 25 else "26-100" if q <= 100 else "100+"
        qbands[b] += 1
    print("\n  qualified nearby-project density (golden firms):")
    for b in ["1", "2-5", "6-25", "26-100", "100+"]:
        print(f"     {b:7s}: {qbands.get(b,0)}")

    # capture ratio
    caps = [f["qual_cnt"] / f["mapped_cnt"] for f in firm.values() if f["mapped_cnt"] > 0]
    if caps:
        caps.sort()
        print(f"\n  capability-capture ratio (qualified / mapped-15 nearby demand):")
        print(f"     mean {sum(caps)/len(caps):.1%} · median {caps[len(caps)//2]:.1%}")

    print("\n" + "=" * 80)
    print("PSC BEACHHEAD — among intersection firms (where capability x proximity concentrate)")
    print(f"{'PSC':5s} {'name':46s} {'demand':>9s} {'near':>6s} {'capable':>8s} {'qual_dem':>9s} {'capture':>8s}")
    rows = []
    for p, pd in psc_demand.items():
        rows.append((p, pd["demand"], len(pd["firms_near"]), len(pd["capable_firms"]),
                     pd["qual_demand"], pd["qual_val"]))
    rows.sort(key=lambda x: -x[4])  # by qualified demand
    for p, dem, near, capf, qdem, qval in rows:
        cap_ratio = (qdem / dem) if dem else 0
        print(f"{p:5s} {PSC_NAME[p][:46]:46s} {dem:>9,d} {near:>6,d} {capf:>8,d} {qdem:>9,d} {cap_ratio:>7.0%}")

    print("\n" + "=" * 80)
    print("TOP 20 GOLDEN FIRMS — by qualified nearby-project count")
    top = sorted(golden.items(), key=lambda kv: -kv[1]["qual_cnt"])[:20]
    for d, f in top:
        qp = sorted(f["qual_pscs"].items(), key=lambda kv: -kv[1][0])
        qp_s = ", ".join(f"{p}:{int(v[0])}" for p, v in qp[:6])
        print(f"  {d[:34]:34s} qual={f['qual_cnt']:>5,d}  mapped={f['mapped_cnt']:>5,d}  ${_fmt(f['qual_val']):>16s}  [{qp_s}]")

    print("\n" + "=" * 80)
    print("GAP A — near mapped demand but CAN SERVE NONE (capability gap)")
    gapA = [(d, f) for d, f in firm.items() if f["mapped_cnt"] > 0 and f["qual_cnt"] == 0]
    gapA.sort(key=lambda kv: -kv[1]["mapped_cnt"])
    print(f"  count: {_fmt(len(gapA))} firms near mapped-PSC demand they can't supply")
    for d, f in gapA[:8]:
        near = ",".join(sorted(f["mapped_pscs_near"]))
        cp = ",".join(sorted(cap.get(d, set()))) or "(none)"
        print(f"    {d[:30]:30s} mapped_near={f['mapped_cnt']:>5,d}  near_pscs=[{near}]  serves=[{cp}]")

    print("\n" + "=" * 80)
    print("GAP B — TOP DEMAND INVISIBLE TO THE 15-PSC SEED (reference-expansion lever)")
    print("  (proximity demand among intersection firms in PSCs NOT in psc_equipment_mapping)")
    for p, c in uncovered_demand.most_common(8):
        print(f"    {p:6s} {c:>8,d}")

    # ---- firm-level golden JSONL for materialization ----
    os.makedirs(os.path.dirname(OUT_JSONL), exist_ok=True)
    with open(OUT_JSONL, "w", encoding="utf-8") as fh:
        for d, f in golden.items():
            qp = sorted(f["qual_pscs"].items(), key=lambda kv: -kv[1][0])
            fh.write(json.dumps({
                "firm_domain": d,
                "qualified_pscs": [p for p, _ in qp],
                "qualified_nearby_award_count": int(f["qual_cnt"]),
                "qualified_value_exposure": round(f["qual_val"], 2),
                "mapped_nearby_award_count": int(f["mapped_cnt"]),
                "all_nearby_award_count": int(f["all_cnt"]),
                "capability_capture_ratio": round(f["qual_cnt"] / f["mapped_cnt"], 4) if f["mapped_cnt"] else 0.0,
                "qualified_psc_demand": {p: int(v[0]) for p, v in qp},
            }, ensure_ascii=False) + "\n")
    print(f"\nwrote firm-level golden records -> {OUT_JSONL} ({len(golden)} firms)")


if __name__ == "__main__":
    main()
