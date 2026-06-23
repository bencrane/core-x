export const meta = {
  name: 'equipment-matchmaking',
  description: 'Agentic PSC↔rental-yard semantic matchmaking over all sharded domains',
  phases: [
    { title: 'Match', detail: 'one agent per shard: semantic PSC matchmaking + verbatim inventory grounding' },
  ],
}

// Defaults baked in (args optionally overrides for targeted re-runs).
const BASE = '/Users/benjamincrane/core-x/.claude/worktrees/nostalgic-williams-0a492f/reports'
const shardDir = (args && args.shardDir) || `${BASE}/mm_shards`
const outDir = (args && args.outDir) || `${BASE}/mm_out`
const DEFAULT_NUM_SHARDS = 129
if (!shardDir || !outDir) throw new Error('args.shardDir and args.outDir required')
const pad = (n) => String(n).padStart(5, '0')
let shardPaths
if (args && Array.isArray(args.shardIds) && args.shardIds.length) {
  shardPaths = args.shardIds.map((n) => `${shardDir}/shard_${pad(n)}.json`)
} else {
  const num = (args && args.numShards) || DEFAULT_NUM_SHARDS
  shardPaths = []
  for (let i = 0; i < num; i++) shardPaths.push(`${shardDir}/shard_${pad(i)}.json`)
}

const PSC_DICT = `
Z2AA | Repair or Alteration of Office Buildings | Scissor Lifts, Boom Lifts, Telehandlers, Portable Generators, Light Towers, Skid Steers
Y1DA | Construction of Hospitals and Infirmaries | Excavators, Bulldozers, Rough-Terrain Cranes, Crawler Cranes, High-Reach Telehandlers
Z1DA | Maintenance of Hospitals and Infirmaries | Towable Generators, Temporary Chiller Units, Boom Lifts, Electric Scissor Lifts
Z2DA | Repair or Alteration of Hospitals and Infirmaries | Skid Steers, Grapple Buckets, Telehandlers, Electric Slab Scissor Lifts
Y1LB | Construction of Highways Roads Streets and Bridges | Motor Graders, Smooth Drum Compactors, Pneumatic Rollers, Wheel Loaders, Articulated Dump Trucks, Water Trucks, Asphalt Pavers, Milling Machines
Z1LB | Maintenance or Repair of Highways Roads Streets and Bridges | Milling Machines, Asphalt Pavers, Material Transfer Vehicles, Smooth Drum Compactors, Pneumatic Rollers, Sweepers, Water Trucks, Variable Message Boards
Y1PC | Construction of Unimproved Real Property (Land) | Bulldozers, Pull-Type Scrapers, Excavators, Articulated Off-Road Dump Trucks, Sheepsfoot Compactors
Y1NE | Construction of Water Supply Facilities | Crawler Excavators, Pipe Layers, Sideboom Dozers, Wheel Loaders, Trench Boxes, Heavy Shoring Equipment
Y1KD | Construction of Mine Subsidence Control Facilities | Rotary Drilling Rigs, Grout Pumps, Concrete Pumps, Dry-Bulk Trailers, Skid Steers, Wheel Loaders
Y1PZ | Construction of Other Non-Building Facilities | Excavators, Bulldozers, Articulated Dump Trucks, Rough-Terrain Cranes
Z2KA | Repair or Alteration of Dams / Dredging Facilities | Long-Reach Excavators, Amphibious Excavators, Swamp Buggies, Industrial Dewatering Pumps, Rough-Terrain Cranes, Articulated Off-Road Dump Trucks
Z1KF | Maintenance or Repair of Dredging Facilities | Long-Reach Excavators, Amphibious Excavators, Swamp Buggies, Bulldozers, Track Loaders, Industrial Pumps
P400 | Demolition of Buildings | Excavators, Crusher Attachments, Shear Attachments, Heavy Wheel Loaders, Skid Steers
F108 | Environmental Remediation | Bulldozers, Articulated Off-Road Dump Trucks, Excavators
F014 | Tree Thinning | Forestry Mulchers, Heavy Bulldozers, Track Loaders
`.trim()

const ACK_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    shard_id: { type: 'integer' },
    output_path: { type: 'string' },
    domains_in: { type: 'integer' },
    domains_written: { type: 'integer' },
    domains_matched: { type: 'integer' },
    ok: { type: 'boolean' },
    note: { type: 'string' },
  },
  required: ['shard_id', 'output_path', 'domains_in', 'domains_written', 'domains_matched', 'ok'],
}

function shardIdOf(p) {
  const m = p.match(/shard_(\d+)\.json/)
  return m ? parseInt(m[1], 10) : -1
}

function buildPrompt(shardPath, outPath, shardId) {
  return `You are the FINAL BOUNCER in a federal-contracting equipment matchmaking engine. You map a rental yard's scraped equipment catalog to the 15 federal PSC (Product/Service Code) construction categories it can credibly serve.

# THE 15-PSC DICTIONARY (ground truth — code | name | required_equipment)
${PSC_DICT}

# YOUR INPUT
Read the JSON file at this exact path:
${shardPath}
It is {"shard_id": ${shardId}, "domains": [ {domain_norm, payload_kind, provider_modes[], category_names[], equipment_item_names[]}, ... ]}.
The union of category_names + equipment_item_names is that domain's full scraped inventory.

# THE TASK — for EVERY domain in the shard, decide which of the 15 PSCs it supports.

## Rule 1 — Signature-machine threshold (suppress false positives)
Include a PSC ONLY if the yard stocks at least one SIGNATURE machine for that PSC as a RENTABLE WHOLE MACHINE — the defining earthmoving / aerial / paving / lifting / drilling / marine iron in that PSC's required_equipment, bridging terminology semantically (e.g. yard "Pump Rentals" / "Dewatering Pumps" satisfies "Industrial Dewatering Pumps"; "66in Skidsteer" satisfies "Skid Steers"; "Track Hoe" satisfies "Excavators"; "Tow-Behind Bucket Lift" satisfies "Boom Lifts"; "Telescopic Forklift" satisfies "Telehandlers"; "Brush Cutter" satisfies "Forestry Mulchers").
Do NOT match on incidental support gear alone. A bare generator, light tower, pump, hand tool, pressure washer, or welder does NOT qualify a yard for a heavy-civil PSC UNLESS that item is itself the signature for that PSC (Portable Generators + Light Towers + Skid Steers ARE the signature support set for Z2AA office reno; Industrial Dewatering Pumps IS signature for Z2KA).

## Rule 2 — Verbatim inventory grounding
For each matched PSC, record the EXACT catalog strings (copied verbatim, character-for-character, from this domain's category_names or equipment_item_names) that triggered the match. Never paraphrase, normalize, or invent a name — it must appear verbatim in the arrays you were given. These roll up into the domain's distinct verified_inventory_matches list.

## The BOUNCER — reject non-rental-yards (return supported_pscs: [])
Even if keyword overlap exists, return an empty match when the firm is NOT a deployable-construction-iron rental yard:
- PARTS / REMAN / COMPONENT vendor — inventory is spare parts (pistons, bucket teeth, cutting edges, undercarriage, bearings, hydraulic cylinders/pumps sold as parts, rubber tracks, reconditioned assemblies), not whole machines. REJECT (e.g. a yard whose items are "bucket teeth / cutting edges / track groups / final drives").
- EVENT / PARTY rental — tables, chairs, linens, tents, catering, concessions, bounce houses. REJECT.
- AV / STAGE-PRODUCTION house — audio, lighting consoles, video walls, trussing, staging; its generators/fencing/barricades are event-grade. REJECT.
- SURVEY / GEOPHYSICAL / NDT instrument house — GPR, magnetometers, seismic, resistivity, LiDar sensors. REJECT.
- STATIONARY processing / manufacturing lines or a machinery DEALER selling (not renting) fixed plant (e.g. sawmill debarkers/edgers/kilns/planers). REJECT — UNLESS it stocks genuine mobile field iron (e.g. mobile wood chippers / forestry mulchers / harvesters), in which case match only the PSC(s) that iron serves (typically F014) and say so.
- Pure homeowner hand-tool / lawn-and-garden shops with no construction machines. REJECT.
When rejecting, supported_pscs = [] and verified_inventory_matches = [], and justification_payload states the archetype and why.

# OUTPUT — write a JSON file, then return an ack.
1. Using the Write tool, write a JSON ARRAY to this EXACT path:
${outPath}
   One object per domain in the shard (matched AND rejected — every domain gets a row), in shard order:
   {
     "domain_norm": "<verbatim from input>",
     "supported_pscs": ["Y1LB", ...],                         // PSC codes only, distinct; [] if none
     "verified_inventory_matches": ["<verbatim catalog string>", ...],  // distinct, verbatim; cap ~18 most-signature; [] if none
     "justification_payload": "<compact JSON string>"          // see below
   }
   justification_payload is a COMPACT JSON STRING (stringified, one line) of:
   {"archetype":"<e.g. full-line rental / heavy-civil fleet / light yard / parts vendor / event / AV / instruments / sawmill dealer>",
    "verdict":"matched"|"rejected",
    "rejected_reason":"<only when rejected>",
    "per_psc":{"<code>":"<short: which verbatim machines satisfy which required_equipment, + strength STRONG/MODERATE/WEAK>"}}
   Keep each per_psc note to one short sentence. Output the file as valid minified-or-pretty JSON (must parse).
2. THEN call StructuredOutput with the ack: shard_id=${shardId}, output_path="${outPath}", domains_in=<count in shard>, domains_written=<objects you wrote — MUST equal domains_in>, domains_matched=<how many had a non-empty supported_pscs>, ok=true, note=<optional>.

Be decisive and consistent. Most domains are general rental yards that match a handful of building/earthmoving PSCs; full-line catalogs match many; specialist non-yards match none. Evaluate all ${24} (or fewer) domains in the shard before returning.`
}

phase('Match')

const thunks = shardPaths.map((sp) => {
  const sid = shardIdOf(sp)
  const outPath = `${outDir}/shard_${String(sid).padStart(5, '0')}.json`
  return () => agent(buildPrompt(sp, outPath, sid), {
    label: `match:shard_${String(sid).padStart(5, '0')}`,
    phase: 'Match',
    schema: ACK_SCHEMA,
    agentType: 'general-purpose',
  })
})

const acks = (await parallel(thunks)).filter(Boolean)

const failed = []
let totalIn = 0
let totalMatched = 0
let okCount = 0
for (const a of acks) {
  if (a && a.ok && a.domains_written === a.domains_in) {
    okCount++
    totalIn += a.domains_in
    totalMatched += a.domains_matched || 0
  } else {
    failed.push(a ? a.shard_id : 'null-ack')
  }
}

// shards that produced no ack at all (agent error / skip)
const ackedIds = new Set(acks.map((a) => a && a.shard_id))
for (const sp of shardPaths) {
  const sid = shardIdOf(sp)
  if (!ackedIds.has(sid)) failed.push(sid)
}

log(`matchmaking pass: ${okCount}/${shardPaths.length} shards ok · ${totalIn} domains evaluated · ${totalMatched} with >=1 PSC match · ${failed.length} failed`)

return {
  total_shards: shardPaths.length,
  shards_ok: okCount,
  domains_evaluated: totalIn,
  domains_matched: totalMatched,
  failed_shards: failed,
  out_dir: outDir,
}
