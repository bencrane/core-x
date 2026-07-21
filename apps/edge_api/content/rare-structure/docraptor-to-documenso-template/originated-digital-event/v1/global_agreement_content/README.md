# Rare Structure — Strategic Origination Agreement (Originated Digital Event) — v1

The **hosted digital event** engagement (Operator / Principal framing), built on the
**field-prefill model**. DocRaptor renders this file directly to a PDF; that PDF is the source document
for a Documenso **template**. The render+push lane places **NO** fields — the operator drops Documenso
fields onto the underscore blanks in the editor afterward.

House reference: `content/government-contracted/docraptor-to-documenso-template/prepaid-introductions/v2`
(the current construction convention — pure underscores, no anchors, no baked values). Operator executes
as **Rare Structure LLC** (Benjamin J. Crane, Managing Director); the Principal carries a `d/b/a` line
in the preamble.

## The offer

Operator sources the audience and hosts a live video conference (the **Originated Digital Event**) on
Principal's behalf, for a flat Prepaid Event Fee:

- **§2 The Event** — Operator provides the hosting platform and delivers the Event, duration-capped.
- **§3 Addressable Universe** — Principal submits Filtering Parameters (NAICS/PSC/obligated value/prime
  vs sub); Operator targets Functional Decision-Makers; Principal signs off on an anonymized summary
  (the Approved Target Audience — no PII pre-event).
- **§4 Timeline** — 30-day date-selection window, ≥30-day pipeline runway, hard 90-day execution window;
  Principal-driven cancellation forfeits the fee and releases the headcount guarantee (reschedule at
  Operator's discretion via a Reprovisioning Fee).
- **§5 Minimum Headcount** — guaranteed live attendees, audited by a snapshot at start+5 minutes; the
  sole make-good is a Supplemental Event within 90 days (no refund/pro-ration/chargeback).
- **§6 Post-Event** — consent-gated Attendee Roster delivered within 96 hours.

## Dynamic value slots — defined once

| Defined term | Section | Underscore slot | Notes |
| --- | --- | --- | --- |
| **Prepaid Event Fee** | §2.1 | the flat prepaid amount | the primary per-instantiation value; referenced in §4.1–4.3, §5.3 as the capitalized term |
| **Maximum Event Duration** | §2.2 | the Event's duration cap, in minutes | |
| **Minimum Headcount** | §5.1 | the guaranteed live-attendee count | referenced in §4.3, §5.2, §5.3 as the capitalized term |

All other timing values (30-day windows, 90-day drop-dead, 5-minute snapshot, 96-hour roster) are
**fixed static text**.

## Signer blanks

Plain underscore runs / ruled lines, fields dropped by hand in the editor:

- **Preamble** — Effective Date, Principal legal entity name, Principal `d/b/a` name.
- **Signature block** — Operator column: ruled signature line + Date blank (entity/Name/Title pre-set to
  Rare Structure LLC / Benjamin J. Crane / Managing Director). Principal column: entity blank + ruled
  signature line + Name + Title + Date blanks.

Source-draft notes: the source titled the document "Strategic Origination Agreement" while its preamble
and witness clause said "Master Network Capacity & Routing Agreement" (carryover from the
capacity-calibration sibling) — normalized to the title. The `{{OPERATOR_*}}` handlebars were baked to
the pre-set Rare Structure LLC signatory per house convention.

## Layout

```
global_agreement_content/
  rare_structure_originated_digital_event.html   # define-once body, underscore blanks, signature block
  styles/
    plain.css                                    # white page / black text (sent today)
    branded.css                                  # dark Rare Structure identity (alternate)
  manifest.json                                  # per-document metadata + the style flag; NO "inputs"
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
originated-digital-event / v1`. With no `inputs`, the page shows no Values form — just the Create
button. (`business.global_input_content` registration is only for the internal Trigger.dev
`registryPath` lane and is not required here — same as prepaid-introductions v2.)
