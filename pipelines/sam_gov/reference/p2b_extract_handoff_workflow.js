export const meta = {
  name: 'govcon-handoff-extract',
  description: 'Account-handoff extraction: isolated per-batch agents over /tmp/p2b_handoff, AUP-block-tolerant (a blocked batch fails alone and is skipped), circuit breaker on session cap. Committer daemon lands results.',
  phases: [ { title: 'Extract', detail: 'isolated batches, 5 concurrent' } ],
}
const DIR = '/tmp/p2b_handoff'
const NB = 280            // batch_0..279; missing/empty indices are tolerated
const CONC = 5
const NOTE = 'You are an isolated extraction agent. Read the file at the path below — it lists up to ~10 task-file paths, one per line. If the file does not exist or is empty, reply "empty" and stop. For EACH task file listed: if its result file already exists (the task file states the exact output path under .../results/), skip it; otherwise Read the task file and follow the instructions embedded inside it EXACTLY (it contains the document chunks, controlled vocabulary, output JSON schema, and the exact output path), then Write the result JSON to that path. Use ONLY the chunk text inside each task file — do NOT access R2, any dataset, or any other file. Evidence quotes: verbatim single proving sentences under 300 characters; capability tags from the embedded vocabulary only; emit the schema-conformant empty result when nothing is confidently extractable. If a specific document causes your response to be blocked by the usage policy, that document is permanently unextractable — do not retry it; it simply stays without a result. Do NOT run any ingest command. Return one terse line per task file: filename -> written|skipped|failed(reason).'
phase('Extract')
const ixs = []; for (let i = 0; i < NB; i++) ixs.push(i)
let reported = 0, dry = 0, stopped = false
for (let g = 0; g < ixs.length; g += CONC) {
  const group = ixs.slice(g, g + CONC)
  const r = (await parallel(group.map(ix => () =>
    agent(NOTE + '\n\nFILE LIST: ' + DIR + '/batch_' + ix + '.txt', { label: 'h:b' + ix, phase: 'Extract', model: 'opus' })
  )))
  const ok = r.filter(Boolean).length
  reported += ok
  if (ok === 0) { dry += 1; if (dry >= 2) { stopped = true; log('circuit breaker: 2 all-fail groups (session cap) — stopping at ' + g); break } }
  else dry = 0
}
return { reported, stopped_early: stopped }
