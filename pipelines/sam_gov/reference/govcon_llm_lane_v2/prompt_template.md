# GovCon requirement extraction — task instructions (prompt v2-freeform-labor)

You are an extraction engine. Your ONLY input is the `chunks` array in this task file — selected
text chunks from one federal solicitation/award attachment. You must not use outside knowledge
about the award, the agency, or the contractor, and you must not infer requirements that the text
does not state.

## Output
Write ONE JSON file at exactly the path given in `result_path`, conforming to `output_schema`
(also embedded in this task file). Top-level fields:

- `resource_id`: copy verbatim from this task file.
- `engine`: your engine identity string (given as `engine` in this task file).
- `scope_summary`: 3–6 factual sentences describing what the contractor must do or provide,
  grounded ONLY in the chunk text. ≤ 1200 characters. `null` if the chunks carry no scope content
  (e.g. pure pricing tables).
- `capability_tags`: the work domains this document demands, chosen ONLY from
  `vocabulary.capability_tags`. Tag what the document requires the contractor to DO — not the
  agency's mission. Out-of-vocabulary tags are rejected at ingest. Empty list if nothing fits.
- `requirements`: every concrete, checkable contractor requirement stated in the chunks. One entry
  per distinct (requirement_type, requirement_value). Types are the closed enum in
  `vocabulary.requirement_types`.

## Requirement rules (rows violating these are rejected by a deterministic validator)
1. **Citations are non-negotiable.** `source_chunk_ids` must list chunk_ids FROM THIS TASK FILE
   whose text contains the evidence. `evidence_quote` (≤ 300 chars) must be copied VERBATIM from
   the cited chunk text — the validator substring-matches it after whitespace normalization and
   rejects any row whose quote does not appear in the cited chunks. Never paraphrase inside
   `evidence_quote`. `evidence_quote` MUST be a SINGLE sentence that by itself proves the claimed
   requirement — quote only that one proving sentence, never the surrounding paragraph or multiple
   sentences. It is a HARD maximum of 300 characters; if the proving sentence exceeds 300 characters,
   quote the verbatim sub-span of that sentence that still proves the claim. A quote over 300
   characters is rejected outright.
2. **Normalize `requirement_value`** to lowercase snake/colon form for NON-labor types; reuse the
   exact forms in `vocabulary.value_norm_hints` whenever the document states the same requirement
   (e.g. a Secret personnel clearance → `clearance:secret` with `clearance_level: "SECRET"`).
3. **labor_category rows — FREE-FORM RAW JOB TITLES (v2 change).** Extract the EXACT job title or
   labor category EXACTLY AS WRITTEN in the text — e.g. `Senior Java Developer`, `Systems
   Administrator`, `Help Desk Technician (Tier II)`, `Cybersecurity Analyst`, `Program Manager`.
   Put the verbatim title in `requirement_value`. Do **NOT** map, normalize, or coerce it to any
   controlled vocabulary, and do **NOT** skip a title merely because it is not a member of
   `vocabulary.labor_categories` — that 36-token list is **no longer a constraint on labor rows**.
   Emit one row per distinct job title the document requires the contractor to staff or provide.
   `requirement_value` is still lowercased + whitespace-collapsed on store (so `Senior Java
   Developer` lands as `senior java developer`); the exact surface form is preserved in the
   verbatim `evidence_quote`. A labor row WITHOUT a verbatim-matching `evidence_quote` is still
   rejected — quote the single sentence that names the role (and headcount/clearance/wage if stated).
4. **clearance_level** is the closed enum in `vocabulary.clearance_levels`; null when the text
   states a clearance requirement without a level.
5. **mandatory**: true only for shall/must/required/mandatory language; false for should/may/
   preferred. Negated requirements ("no clearance required") must NOT produce a row.
6. Dates are `YYYY-MM-DD`; `headcount` is an integer 1–5000; `wage_floor` is a number (USD/hour).
   Use null when the text does not state the field — never guess.
7. Do not duplicate: one row per distinct (requirement_type, requirement_value); cite the clearest
   occurrence.

## Honesty contract
Extract only what is written. An empty `requirements` array and a null `scope_summary` is a valid,
correct result for a document with no extractable content. For labor specifically, **recall the
real job titles the text names** — including IT/cyber/software/engineering roles the old controlled
vocabulary could not represent — but never invent a title the text does not state. Every stored row
becomes citable evidence in live outreach: the `evidence_quote` must prove it.
