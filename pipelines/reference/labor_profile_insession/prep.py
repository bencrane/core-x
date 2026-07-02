"""Step 1 — PREP: render the in-session classification inputs from the frozen manifest.

Reads the frozen `_naics_psc_labor_profile_manifest` (built by `materialize_naics_psc_labor_profile.py
manifest`, optionally over an external worklist via NPLP_WORKLIST_CSV) and renders, into the scratch
root:
    system.txt          the shared task rules + SCA + detailed-SOC vocabulary block (read once/agent)
    calls/<cid>.txt      one slim per-call payload (NAICS + OEWS candidates + PSC list) per call
    cids.json            sorted list of every call id
    slices.json          calls chunked into slices of SLICE_SIZE (default 15)
    npsc.json            {cid: expected PSC count}
    expected.json        {cid: [sorted expected psc_code strings]}

READ-ONLY vs R2/manifest. NO Anthropic API. NO dataset writes.

Env:
    NPLP_SCRATCH   scratch root (default /tmp/nplp)
    NPLP_SLICE_SIZE  calls per slice (default 15)

Run under: doppler run -p core-x -c prd -- python3 -m pipelines.reference.labor_profile_insession.prep
(or `python3 pipelines/reference/labor_profile_insession/prep.py` from the repo root).
"""
from __future__ import annotations

import glob
import json
import os

from ._util import load_module

M = load_module()

SCRATCH = os.environ.get("NPLP_SCRATCH", "/tmp/nplp")
SLICE_SIZE = int(os.environ.get("NPLP_SLICE_SIZE", "15"))
CALLS = os.path.join(SCRATCH, "calls")
RESULTS = os.path.join(SCRATCH, "results")


def main() -> None:
    for d in (SCRATCH, CALLS, RESULTS):
        os.makedirs(d, exist_ok=True)

    # clear any prior per-call payloads so the run is uniform (results are NOT cleared here —
    # a partial results/ dir is a resume point; delete it manually for a from-scratch re-run).
    cleared = [os.remove(f) for f in glob.glob(f"{CALLS}/*.txt")]
    print(f"cleared {len(cleared)} old call files")

    requests, by_call = M._build_requests()  # in-session prompt rendering only (no API)
    open(f"{SCRATCH}/system.txt", "w").write(requests[0]["params"]["system"][0]["text"])

    expected = {cid.replace(":", "_"): sorted(str(r["psc_code"]) for r in rows)
                for cid, rows in by_call.items()}
    for req in requests:
        open(f"{CALLS}/{req['custom_id']}.txt", "w").write(req["params"]["messages"][0]["content"])

    cids = sorted(req["custom_id"] for req in requests)
    slices = [cids[i:i + SLICE_SIZE] for i in range(0, len(cids), SLICE_SIZE)]
    npsc = {c: len(expected[c]) for c in cids}
    json.dump(expected, open(f"{SCRATCH}/expected.json", "w"))
    json.dump(cids, open(f"{SCRATCH}/cids.json", "w"))
    json.dump(slices, open(f"{SCRATCH}/slices.json", "w"))
    json.dump(npsc, open(f"{SCRATCH}/npsc.json", "w"))

    n_combos = sum(len(v) for v in expected.values())
    sizes = [os.path.getsize(f"{CALLS}/{c}.txt") for c in cids]
    sys_tok = os.path.getsize(f"{SCRATCH}/system.txt") // 4
    call_tok = (sum(sizes) // len(sizes)) // 4 if sizes else 0
    print(f"calls={len(cids)}  combos={n_combos}  slices={len(slices)} (SLICE_SIZE={SLICE_SIZE})  "
          f"waves@C=4={-(-len(slices) // 4)}")
    print(f"slim call bytes: mean={sum(sizes) // len(sizes) if sizes else 0} max={max(sizes) if sizes else 0}")
    print(f"~input/slice ~{sys_tok + SLICE_SIZE * call_tok:,} tok (200K context limit)")
    print(f"scratch: {SCRATCH}  (results dir has "
          f"{len(glob.glob(f'{RESULTS}/*.json'))} pre-existing files)")


if __name__ == "__main__":
    main()
