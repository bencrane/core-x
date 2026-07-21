# Rare Structure — Master Network Capacity & Routing Agreement — v2

The **flat prepaid Capacity Seat** engagement (Operator / Principal framing), built on the
**field-prefill model**. DocRaptor renders this file directly to a PDF; that PDF is the source document
for a Documenso **template**. The render+push lane places **NO** fields — the operator drops Documenso
fields onto the underscore blanks in the editor afterward.

## What changed from v1

- **§1.1 The Capacity Seat** extended: "…private routing network **designed to detect corporate
  inflection points and originate facilitated digital connections**."
- Three protective clauses added to Section 1 (Operator/Principal framing; fee cross-reference points
  at Section 5):
  - **1.3 Discretion, Execution & Infrastructure Autonomy** — methods/architecture discretion; Operator
    bears all infrastructure costs; Principal owes nothing beyond Section 5 fees.
  - **1.4 Non-Brokerage & Liability Acknowledgment** — data-routing/lead-gen only; no advice, no
    brokerage; Principal owns due diligence, contracting, operational decisions.
  - **1.5 Non-Exclusive Engagement** — no exclusive rights to the pipeline/network/deal flow; Operator
    may engage competing firms and route freely.
- Blanks and dynamic slots unchanged from v1.

House reference: `content/government-contracted/docraptor-to-documenso-template/prepaid-introductions/v2`
(the current construction convention — pure underscores, no anchors, no baked values). Operator executes
as **Rare Structure LLC** (Benjamin J. Crane, Managing Director); the Principal carries a `d/b/a` line
in the preamble.

## Dynamic value slots — defined once

| Defined term | Section | Underscore slot | Notes |
| --- | --- | --- | --- |
| **Capacity Seat Fee** | §5.1 | the flat, non-refundable amount | the primary per-instantiation value; referenced in §5.2, §5.3, §7.1 as the capitalized term |
| **Primary Term** | §7.1 | the discrete term, in days | referenced in §5.1, §7.2 as the capitalized term |

Fixed statics in the economics:

- **§5.2 Fee Acknowledgement** — the fee secures the seat + calibration only; no guaranteed routing
  yield; fully earned and non-refundable.
- **§5.3 Zero Yield Encumbrance** — 0.00% take-rate; no equity, success fees, basis points, or
  secondary compensation.
- **§7.2 Expiration** — no auto-renew, no carry-forward; a new engagement requires a net-new agreement
  and a new Capacity Seat Fee.

## Signer blanks

Plain underscore runs / ruled lines, fields dropped by hand in the editor:

- **Preamble** — Effective Date, Principal legal entity name, Principal `d/b/a` name.
- **Signature block** — Operator column: ruled signature line + Date blank (entity/Name/Title pre-set to
  Rare Structure LLC / Benjamin J. Crane / Managing Director). Principal column: entity blank + ruled
  signature line + Name + Title + Date blanks.

Source-draft notes: the preamble follows the house form (`…is entered into by Rare Structure LLC (the
"Operator") and the participating entity, ____ d/b/a ____, hereby defined as the "Principal"`); the
source draft's stray `a {` state/entity-type fragment was dropped entirely by operator decision. The
`{{OPERATOR_*}}` handlebars were baked to the pre-set Rare Structure LLC signatory per house convention.

## Layout

```
global_engagement_content/
  rare_structure_capacity_calibration.html   # define-once body, underscore blanks, signature block
  styles/
    plain.css                                # white page / black text (sent today)
    branded.css                              # dark Rare Structure identity (alternate)
  manifest.json                              # per-document metadata + the style flag; NO "inputs"
```

## Fields & convention

Static PDF body — **no `{{handlebars}}`, no merge tokens, no `[[XX]]` anchors.** Every dynamic value
(economic value + signer blank) is a visible underscore run. Documenso fields are dropped onto these
slots in the editor; nothing is filled into the HTML before render. Underscore-run width is cosmetic —
the Documenso field's box is sized in the editor, not driven by the run length.

## Render + Push lane

**Content-only** addition — no lane code changes, no DB row. The render lane performs zero token
substitution (`assemble_html` with no tokens), so the underscore body renders verbatim. Catalog
discovery (`catalog.py`) surfaces this pack the moment the directory exists; the Settings → "Create a
Documenso Template" cascade lists it as `rare-structure / docraptor-to-documenso-template /
capacity-calibration / v1`. With no `inputs`, the page shows no Values form — just the Create button.
(`business.global_input_content` registration is only for the internal Trigger.dev `registryPath` lane
and is not required here — same as prepaid-introductions v2.)
