# Rare Structure (Government-Contracted) — Strategic Origination Agreement (Prepaid Introductions, RANGE) — v1

New archetype `prepaid_introductions_range`, derived from `prepaid-introductions/v3`. Same
**field-prefill / underscore-blank** doctrine (DocRaptor renders the HTML to PDF; that PDF becomes a
Documenso **template**; the lane places NO fields — the operator drops Documenso fields onto the
blanks once, in the editor). Two structural changes from v3:

1. **One-page body.** The entire agreement body is page 1; the execution block is page 2
   (`.sig-wrap` `page-break-before: always`). Typography in this version's `styles/plain.css` is
   compacted (10pt / 1.35, tighter heading + paragraph margins) to hold the one-page constraint —
   verified with ~2 lines of headroom at Letter/1in margins.
2. **Range economics (ruled 2026-07-21).** The allocation is a MIN–MAX range, not a fixed count:
   - Per-Introduction Price = Prepaid Fee ÷ Introduction **Minimum** (the min side of the range).
   - Ledger Settlement refund = `max(0, IntroNumMin − delivered) × PricePerIntroMin`.
   - Delivery ≥ min settles in full; intros beyond min up to max are included at no charge.
   - Zero delivery refunds the full Prepaid Fee (fee is NOT styled non-refundable in this archetype).

## Documenso field checklist — drop one TEXT field per blank, label EXACTLY as below

| Documenso label | Blank location | Entered / Derived | Post-mint intent |
| --- | --- | --- | --- |
| *(Effective Date — operator's naming TBD)* | preamble | Entered | Read-Only |
| `Legal Entity Name` | preamble | Entered | Read-Only |
| `D/B/A Name` | preamble | Entered | Read-Only |
| `PrepaidFee` | §2.1 | Entered | Read-Only — **machine-read charge key** (`FEE_KEYS`) |
| `IntroNumMin` | §2.1 | Entered | Read-Only |
| `IntroNumMax` | §2.1 | Entered | Read-Only |
| `PricePerIntroMin` | §2.1 | **Derived** = PrepaidFee ÷ IntroNumMin (operator supplies the figure — Documenso does no arithmetic) | Read-Only |
| `DaysToFill` | §2.2 | Entered | Read-Only |
| `Full Name` | sig block (Principal Name) | Entered/signer | per operator ruling |
| `Title` | sig block (Principal Title) | Entered/signer | per operator ruling |
| signature / date fields | both sig columns | Documenso native | Required |

Counterparty-visible labels are presentation-optimized (`Legal Entity Name`, `D/B/A Name`,
`Full Name`, `Title`); pass-through labels (`PrepaidFee`, `IntroNumMin`, `IntroNumMax`,
`PricePerIntroMin`, `DaysToFill`) are never seen by the counterparty. `PricePerIntroMax` is
intentionally omitted — it has no operative role in the ruled economics.

## Render + Push lane

Content-only addition; no lane code changes. `manifest.json` declares no `inputs`, so no Values form
renders — catalog discovery surfaces this as `government-contracted / docraptor-to-documenso-template /
prepaid-introductions-range / v1` the moment the directory exists on the deployed image.
