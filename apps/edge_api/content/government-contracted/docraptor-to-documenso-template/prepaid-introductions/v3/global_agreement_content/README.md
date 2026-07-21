# Rare Structure (Government-Contracted) — Strategic Origination Agreement (Prepaid Introductions) — v3

Same **field-prefill / underscore-blank** doctrine as v2 (DocRaptor renders this HTML directly to a PDF;
that PDF is the source document for a Documenso **template**; the render+push lane places **NO** fields —
the operator drops Documenso fields onto the underscore blanks in the editor afterward). v3 is a body
revision, not a lane change.

## What changed from v2

| | v2 | v3 (this) |
| --- | --- | --- |
| Party nomenclature | Provider / Participant | **Operator / Principal / Network Counterparty** |
| Defined-time terms | Effective Date only | adds **Payment Date** and **Commencement Date** (§5.2) |
| Structure | §1–§7 (origination framing) | restructured — Routing Seat (§1), Exclusion Roster (§2.3), Ledger Settlement (§5.3), Facility Rollover (§5.4) |
| Value slots | four, in §3 | four, in **§5** (unchanged doctrine — define-once, referenced by capitalized term) |
| Signer block | Provider preset / Participant blank | Operator preset (Benjamin J. Crane / Managing Director) / Principal blank |

## Dynamic value slots — defined once, all in Section 5

| Defined term | Section | Underscore run | Notes |
| --- | --- | --- | --- |
| **Prepaid Fee** | §5.1 | 20 | referenced as term in §5.4 (no second blank) |
| **Introduction Allocation** | §5.1 | 12 | referenced in §5.3 |
| **Per-Introduction Rate** | §5.1 | 20 | derived = Prepaid Fee ÷ Introduction Allocation; operator supplies at prefill |
| **Primary Term** | §5.2 | 12 | the fulfillment window, in days |

Signer blanks: Effective Date (20), Principal entity (30) / d/b/a (28), and each sig-block Date/Name/Title
run (24). Operator side is preset inline. **No `{{handlebars}}`, no merge tokens, no `[[XX]]` anchors.**

## Render + Push lane

Content-only addition; no lane code changes. `manifest.json` declares no `inputs`, so no Values form
renders — catalog discovery surfaces this as `government-contracted / docraptor-to-documenso-template /
prepaid-introductions / v3` the moment the directory exists on the deployed image.
