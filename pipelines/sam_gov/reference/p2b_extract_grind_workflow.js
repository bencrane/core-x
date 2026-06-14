export const meta = {
  name: 'govcon-p2b-grind',
  description: 'Autonomous P2b extraction grind: agents read per-batch file lists, throttled concurrency, all-failure circuit breaker stops doomed launches when the account session cap hits. Ingest is handled by the external committer daemon.',
  phases: [ { title: 'Extract', detail: 'throttled groups; circuit-breaker on mass failure' } ],
}
// edit these two constants per cycle (args fallback if provided)
const A = (typeof args === 'object' && args) ? args : {}
const DIR = A.dir || '/tmp/p2b_grind_c6'
const NB = A.n_batches || 285
const CONC = A.conc || 6
const NOTE = 'You are an extraction lane agent. Read the file list at the path given below (one task-file path per line). For EACH task file, in order: if its result file already exists (the task file states the exact output path), skip it; otherwise Read the task file, follow the embedded instructions exactly (it contains the document chunks, controlled vocabulary, output JSON schema, and the exact output path), and Write the result JSON to that exact path. Use ONLY the chunk text inside each task file; do NOT access R2, any dataset, or any other file; evidence quotes must be verbatim single proving sentences under 300 characters; tags from the embedded vocabulary only; emit the schema-conformant empty result when nothing is confidently extractable. Return one terse line per task file: filename -> written|skipped|failed(reason). Do not run any ingest command.'

phase('Extract')
const ixs = []
for (let i = 0; i < NB; i++) ixs.push(i)
let reported = 0, dryGroups = 0, launched = 0, stopped = false
for (let g = 0; g < ixs.length; g += CONC) {
  const group = ixs.slice(g, g + CONC)
  launched += group.length
  const r = (await parallel(group.map(ix => () =>
    agent(NOTE + '\n\nFILE LIST: ' + DIR + '/batch_' + ix + '.txt', { label: 'g:b' + ix, phase: 'Extract', model: 'opus' })
  )))
  const ok = r.filter(Boolean).length
  reported += ok
  if (ok === 0) {
    dryGroups += 1
    log('group @' + g + ': ALL ' + group.length + ' failed (dry ' + dryGroups + ') — likely session cap/outage')
    if (dryGroups >= 2) { stopped = true; log('circuit breaker: 2 consecutive all-fail groups — stopping launches at ' + launched + '/' + NB); break }
  } else {
    dryGroups = 0
    if (g % (CONC * 10) === 0) log('progress: ' + reported + ' batches reported, ' + launched + '/' + NB + ' launched')
  }
}
return { dir: DIR, n_batches: NB, launched, reported, stopped_early: stopped }
