# Rare Structure — Strategic Origination & Capacity Calibration Agreement — v1

The **flat prepaid capacity** engagement, built on the **field-prefill model**. DocRaptor renders this
file directly to a PDF; that PDF is the source document for a Documenso **template**. The render+push
lane places **NO** fields — the operator drops Documenso fields onto the underscore blanks in the
editor afterward.

House reference: `content/government-contracted/docraptor-to-documenso-template/prepaid-introductions/v2`
(the current construction convention — pure underscores, no anchors, no baked values). Provider executes
as **Rare Structure LLC** (Benjamin J. Crane, Managing Director); the Participant carries a `d/b/a` line
in the preamble.

## Dynamic value slot — defined once, in Section 3

| Defined term | Section | Underscore slot | Notes |
| --- | --- | --- | --- |
| **Prepaid Fee** | §3.1 | the non-refundable upfront amount | the ONLY per-instantiation economic value; referenced in §3.4, §3.5 as the capitalized term |

Everything else in the financial architecture is **fixed static text**:

- **§3.2 Zero Yield Participation** — 0.00% take-rate; 100% of upside retained by the Participant. No
  success fee.
- **§3.3 Primary Fulfillment Term** — flat **90 days** (static, not a slot); the Capacity Seat expires
  unconditionally at term end. No rolling ownership, no survival/tail.
- **§3.5 Non-Refundable Infrastructure Toll** — strictly non-refundable; no rollovers, prorated refunds,
  or volume guarantees.

At instantiation, the price is the one value prefilled against the template.

## Signer blanks

Plain underscore runs / cleared rows, fields dropped by hand in the editor:

- **Preamble** — Effective Date, Participant legal entity name, Participant `d/b/a` name.
- **Signature block** — Provider column: ruled signature line + Date blank (By/Title pre-set to
  Benjamin J. Crane / Managing Director). Participant column: ruled signature line + By + Title + Date
  blanks.

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
