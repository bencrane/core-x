"""Step 2/3 — GEN WORKFLOW: emit the sliced in-session classification workflow (.mjs).

Generates a workflow over a slice range [start, end) that fans out in-session Opus 4.8 / xhigh
subagents, WAVES OF 4 CONCURRENT (hard cap — a wider fan-out tripped a session HTTP 429), each
agent reading its slice's call files, classifying every PSC, and writing one result JSON per call
to <scratch>/results/<cid>.json. Loop-until-complete within the batch (up to 3 re-drive rounds).

Usage:
    python3 pipelines/reference/labor_profile_insession/gen_workflow.py <start> <end>
    # omit start/end to cover ALL slices in one workflow file.
Then run the emitted <scratch>/workflow_<start>_<end>.mjs in-session. Run in batches (a few slices
at a time) and CHECKPOINT between batches so a crash resumes from R2.

Env: NPLP_SCRATCH (default /tmp/nplp), NPLP_CONCURRENCY (default 4; do NOT raise above ~4).
"""
from __future__ import annotations

import json
import os
import sys

SCRATCH = os.environ.get("NPLP_SCRATCH", "/tmp/nplp")
CONCURRENCY = int(os.environ.get("NPLP_CONCURRENCY", "4"))


def main(argv: list[str]) -> None:
    slices = json.load(open(f"{SCRATCH}/slices.json"))
    npsc = json.load(open(f"{SCRATCH}/npsc.json"))
    start = int(argv[0]) if len(argv) > 0 else 0
    end = int(argv[1]) if len(argv) > 1 else len(slices)
    batch = slices[start:end]
    batch_cids = [c for s in batch for c in s]

    head = f"""export const meta = {{
  name: 'nplp-batch-{start}-{end}',
  description: 'Opus 4.8 (xhigh) classify — slices {start}..{end - 1} ({len(batch_cids)} calls), waves of {CONCURRENCY}',
  phases: [{{ title: 'Classify', detail: '{len(batch)} slices, {CONCURRENCY} concurrent, opus/xhigh' }}],
}}
"""

    body = r"""
const RESULTS_DIR = SCRATCH + '/results'
const CALLS_DIR = SCRATCH + '/calls'
const SYSTEM_TXT = SCRATCH + '/system.txt'
const SCHEMA = { type: 'object', additionalProperties: false, required: ['completed', 'failed'],
  properties: { completed: { type: 'array', items: { type: 'string' } }, failed: { type: 'array', items: { type: 'string' } } } }
const ALL = SLICES.flat()

function chunk(a, n) { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o }

function slicePrompt(cids) {
  const lines = cids.map(c => '  ' + CALLS_DIR + '/' + c + '.txt  (expected ' + NPSC[c] + ' PSCs)').join('\n')
  return (
'You classify the labor demand implied by U.S. federal contract awards.\n\n' +
'Shared task rules and the TWO controlled vocabularies (SCA labor categories; detailed SOC occupations) are in ' + SYSTEM_TXT + '. READ THAT FILE ONCE, IN FULL, FIRST — it is authoritative.\n\n' +
'Then process EACH of these ' + cids.length + ' calls IN TURN (skip none):\n' + lines + '\n\n' +
'For each call:\n' +
'1. Read its file (ONE NAICS industry + its OEWS staffing-pattern candidates + the PSC codes to classify).\n' +
'2. Classify EVERY PSC it lists — exactly one result object per PSC, in the given order.\n' +
'3. Use the Write tool to save EXACTLY {"custom_id":"<cid>","results":[...]} to ' + RESULTS_DIR + '/<cid>.json.\n\n' +
'OUTPUT SHAPE — every result object MUST have EXACTLY these keys, ALL mandatory (never omit any):\n' +
'  {"psc_code":"<code>","is_labor_play":true|false,"work_summary":"<=20 words","categories":[{"soc_code":"##-####","off_pattern":true|false,"sca_code":"#####"|null,"role_class":"core_deliverable|support|overhead","confidence":"high|medium|low"}]}\n' +
'- The PSC key MUST be exactly "psc_code" (NOT "psc"), echoing the exact code from the call file.\n' +
'- role_class AND confidence are REQUIRED on EVERY category object — never leave either out.\n' +
'- soc_code MUST literally appear in the DETAILED SOC VOCABULARY in system.txt; prefer the call candidates; off_pattern=true only when the deliverable needs an occupation outside them.\n' +
'- sca_code MUST literally appear in the SCA VOCABULARY, or be null when none fits.\n' +
'- work_summary <= 20 words. is_labor_play=false => categories []. Otherwise list ALL labor categories genuinely required, ranked by centrality (array order = rank, max 10): the core_deliverable labor that IS the service, PLUS the support and overhead roles the contract forces on payroll — do not collapse to only the top role.\n' +
'- Do NOT emit placeholder/test/tag-only output; every field must be a real classification.\n\n' +
'When ALL ' + cids.length + ' files are written, return {"completed":[cids you wrote],"failed":[cids you could not]} via structured output.'
  )
}

const completed = new Set()
async function runSlices(sliceList, tag) {
  const waves = chunk(sliceList, CONCURRENCY)
  for (let w = 0; w < waves.length; w++) {
    log(tag + ' wave ' + (w + 1) + '/' + waves.length + ' (' + waves[w].length + ' slices)')
    const res = await parallel(waves[w].map((cids, i) => () =>
      agent(slicePrompt(cids), { label: tag + '-w' + (w + 1) + '-s' + i, phase: tag, schema: SCHEMA, model: 'opus', effort: 'xhigh', agentType: 'general-purpose' })
        .then(r => ({ r })).catch(e => ({ r: null }))
    ))
    for (const { r } of res) { if (r && Array.isArray(r.completed)) for (const c of r.completed) completed.add(c) }
  }
}

phase('Classify')
await runSlices(SLICES, 'Classify')

let round = 0
while (round < 3) {
  const remaining = ALL.filter(c => !completed.has(c))
  if (remaining.length === 0) break
  round++
  log('re-drive round ' + round + ': ' + remaining.length + ' call(s) remain')
  await runSlices(chunk(remaining, SLICE_SIZE), 'Redrive' + round)
}

const remaining = ALL.filter(c => !completed.has(c))
log('BATCH DONE ' + completed.size + '/' + ALL.length + ' calls; remaining ' + remaining.length)
return { done: completed.size, total: ALL.length, remaining }
"""

    slice_size = len(slices[0]) if slices else 15
    path = f"{SCRATCH}/workflow_{start}_{end}.mjs"
    with open(path, "w") as fh:
        fh.write(head)
        fh.write(f"const SCRATCH = {json.dumps(SCRATCH)};\n")
        fh.write(f"const CONCURRENCY = {CONCURRENCY};\n")
        fh.write(f"const SLICE_SIZE = {slice_size};\n")
        fh.write("const SLICES = " + json.dumps(batch) + ";\n")
        fh.write("const NPSC = " + json.dumps(npsc) + ";\n")
        fh.write(body)
    print(f"{path}: slices {start}..{end - 1} = {len(batch)} slices, {len(batch_cids)} calls, "
          f"{sum(npsc[c] for c in batch_cids)} combos, "
          f"{-(-len(batch) // CONCURRENCY)} wave(s) @ C={CONCURRENCY}")


if __name__ == "__main__":
    main(sys.argv[1:])
