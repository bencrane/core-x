# Rare Structure (Government-Contracted) — Strategic Origination Agreement (Prepaid Introductions) — v2

The **field-prefill** version of the prepaid-introductions agreement. DocRaptor renders this file directly
to a PDF; that PDF is the source document for a Documenso **template**. The render+push lane places **NO**
fields — the operator drops Documenso fields onto the underscore blanks in the editor afterward.

## What changed from v1

| | v1 | v2 (this) |
| --- | --- | --- |
| Economic values | **baked** into the PDF via `{{token}}` substituted server-side (`values.py`) | **not baked** — each is a plain underscore blank (`________`) |
| Operator inputs at creation | `amount`, `introductions`, `term_days` (a Values form) | **none** — `manifest.json` declares no `inputs`, so no Values form renders |
| Where values are supplied | at template-creation (baked once, fixed) | at **document prefill** — instantiate off the template, prefill the fields |
| Repetition | each value re-printed at every mention | **defined once**, referenced by its capitalized term everywhere downstream |

This reverts the template to its originally-intended house convention (see the v1 README: *"convert every
handlebar `{{token}}` to an underscore run"*), minus the optional `[[XX]]` findText anchors — **pure
underscores, placed by hand in Documenso.**

## Dynamic value slots — defined once, all in Section 3

| Defined term | Section | Underscore slot | Notes |
| --- | --- | --- | --- |
| **Prepaid Fee** | §3.1 | the non-refundable upfront amount | also referenced in §3.3 |
| **Introduction Allocation** | §3.1 | the count of Introductions secured | referenced in §3.2, §3.4 |
| **Per-Introduction Rate** | §3.1 | the unit cost | referenced in §3.4 Option A / B |
| **Primary Term** | §3.2 | the fulfillment window, in days | referenced in §3.4, §3.5 |

Each value's actual figure prints **exactly once** (at its underscore); every downstream mention uses the
capitalized defined term as static text. Four value fields total → four Documenso fields to place once,
prefill per instantiation.

> **Per-Introduction Rate is derived** (`Prepaid Fee ÷ Introduction Allocation`). v1 computed it
> server-side so it could not drift. v2 has no server derivation — the operator supplies the derived figure
> at prefill (Documenso does no arithmetic). Keeping the three economic fields consistent is a prefill-time
> responsibility.

## Layout

```
global_agreement_content/
  government_contracted_prepaid_introductions.html   # define-once body, underscore blanks, signature block
  styles/
    plain.css                                        # white page / black text (sent today)
    branded.css                                      # dark Rare Structure identity (alternate)
  manifest.json                                      # per-document metadata + the style flag; NO "inputs"
```

## Fields & convention

Static PDF body — **no `{{handlebars}}`, no merge tokens, no `[[XX]]` anchors.** Every dynamic value
(economic value + signer blank) is a visible underscore run. Documenso fields are dropped onto these slots
in the editor; nothing is filled into the HTML before render. Underscore-run width is cosmetic — the
Documenso field's box is sized in the editor, not driven by the run length.

## Render + Push lane

Identical to v1 — this is a **content-only** addition; no lane code changes. The render lane performs zero
token substitution (`assemble_html` with no tokens), so the underscore body renders verbatim. Catalog
discovery (`catalog.py`) surfaces this version the moment the directory exists; the Settings → "Create a
Documenso Template" cascade lists it as `government-contracted / docraptor-to-documenso-template /
prepaid-introductions / v2`. With no `inputs`, the page shows no Values form — just the Create button.
