export const meta = {
  name: 'bigthree-v2-grind-wave',
  description: 'Big-Three v2 free-form labor extraction — ONE paced wave of session-opus agents. Each agent owns a disjoint 2-hex resource-id PREFIX and processes its own pending task files (resumable: skips any task whose result already exists). args = {prefixes:[..], tasksDir, resultsDir, conc}.',
  phases: [ { title: 'Extract', detail: 'CONC=5 opus agents, one 2-hex prefix each' } ],
}

let A = args
if (typeof A === 'string') { try { A = JSON.parse(A) } catch (e) { A = {} } }
const prefixes = (A && A.prefixes) || []
const TASKS = (A && A.tasksDir) || '/tmp/govcon_llm_stage_v2/tasks'
const RESULTS = (A && A.resultsDir) || '/tmp/govcon_llm_stage_v2/results'
const CONC = (A && A.conc) || 5

const NOTE = (prefix) => [
  'You are a GovCon extraction lane agent (prompt v2-freeform-labor), session-opus lane.',
  'Your assigned bucket is resource-id prefix "' + prefix + '".',
  '',
  'STEP 1 — list your pending task files. Run:',
  '  ls ' + TASKS + '/' + prefix + '*.task.json 2>/dev/null',
  'For EACH task file found, derive its result file = ' + RESULTS + '/<basename with .task.json -> .result.json>.',
  'If that result file ALREADY EXISTS (test -f), SKIP the task (do not re-process).',
  '',
  'STEP 2 — for each NOT-yet-done task file, process it ONE AT A TIME:',
  '  - Read the task JSON. It contains "instructions", document "chunks", controlled "vocabulary",',
  '    "output_schema", and the exact "result_path".',
  '  - Follow the embedded instructions EXACTLY. Extract using ONLY the chunk text in the task file —',
  '    do NOT access R2, any dataset, the network, or any other file.',
  '  - labor_category rows: extract the EXACT raw job title as written (e.g. "Senior Java Developer") —',
  '    do NOT map to the vocabulary, do NOT skip non-member titles. capability_tags ONLY from the',
  '    embedded vocabulary. Every requirement row needs source_chunk_ids from the task + a VERBATIM',
  '    evidence_quote (one proving sentence, <=300 chars). Empty requirements + null scope_summary is a',
  '    VALID result when nothing is confidently extractable. If a document trips a usage-policy block it',
  '    is permanently unextractable — skip it, leave it without a result, do not retry.',
  '  - Write the result JSON to the EXACT "result_path" from the task file (valid JSON, schema-conformant).',
  '  - Do NOT run any pipeline/ingest command.',
  '',
  'Return ONE terse line per task file: <resource_id> -> written | skipped | failed(<reason>). At the end,',
  'state how many you wrote and how many were already done.',
].join('\n')

phase('Extract')
log('wave: ' + prefixes.length + ' prefix buckets, CONC=' + CONC + ', tasksDir=' + TASKS)

const reports = []
for (let g = 0; g < prefixes.length; g += CONC) {
  const group = prefixes.slice(g, g + CONC)
  const r = await parallel(group.map((p) => () =>
    agent(NOTE(p), { label: 'opus:' + p, phase: 'Extract', model: 'opus' })
  ))
  reports.push(...r.filter(Boolean))
  log('progress: ' + Math.min(g + CONC, prefixes.length) + '/' + prefixes.length + ' prefix buckets dispatched')
}
return { prefixes: prefixes.length, conc: CONC, agent_reports: reports }
