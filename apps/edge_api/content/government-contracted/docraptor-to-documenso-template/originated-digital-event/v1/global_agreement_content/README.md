# Rare Structure (Government-Contracted) — Strategic Origination Agreement (Originated Digital Event) — v1

Archetype `digital_event`, brand `government-contracted`, derived from the rare-structure
`originated-digital-event/v1` draft under the current ontology. Same **field-prefill /
underscore-blank** doctrine and **one-page body** discipline as `prepaid-introductions-range/v1`
(body page 1, execution block page 2 via `page-break-before`, compact per-version `plain.css`;
fit verified ~1 line of headroom at Letter/1in).

## Rulings encoded (2026-07-21)

- **Sides**: operator = **Principal** (Rare Structure LLC, pre-set); counterparty = **Participant**.
- **Single Event** per agreement; duration is **fixed text** (60 minutes in v1 — changing it is a
  new version, not a variable).
- **Attendance range**: `MinAttendeesNum` (guarantee floor) to `MaxAttendeesNum` (declarative
  ceiling, no charge logic); attendees beyond min up to max included at no charge.
- **Settlement**: prorated refund is the **default and automatic** remedy = shortfall below min ×
  `AttendeePriceMin` (= PrepaidFee ÷ MinAttendeesNum, derived). A supplemental event is the
  **exception**: Participant may request; granting is at Principal's sole discretion; only granted
  **and accepted** displaces the refund. Zero attendees ⇒ full fee refunded.
- Attendance audited at scheduled start + 5 minutes; consent-gated roster within 96 hours.

## Documenso field checklist — 13 fields, labels EXACTLY as below

| Documenso label | Location | Entered / Derived | Side |
| --- | --- | --- | --- |
| `Date` | preamble (Effective Date) | auto (stamps on execution) | Principal |
| `Legal Entity Name` | preamble | Entered | Participant |
| `D/B/A Name` | preamble | Entered | Participant |
| `PrepaidFee` | §2.1 | Entered — **machine-read charge key** (`FEE_KEYS`) | Participant |
| `MinAttendeesNum` | §2.1 | Entered | Participant |
| `MaxAttendeesNum` | §2.1 | Entered | Participant |
| `AttendeePriceMin` | §2.1 | **Derived** = PrepaidFee ÷ MinAttendeesNum | Participant |
| `Signature` | Principal sig column | auto | Principal |
| `Date` | Principal sig column | auto | Principal |
| `Signature` | Participant sig column | auto | Participant |
| `Full Name` | Participant sig column | Entered | Participant |
| `Title` | Participant sig column | Entered | Participant |
| `Date` | Participant sig column | auto | Participant |

Template placeholder recipients: `principal@example.com` (Principal / operator),
`participant@example.com` (Participant) — the side-conformance checks resolve through these emails.

## Lane

Content-only addition; no lane code changes. Discoverable as
`government-contracted / docraptor-to-documenso-template / originated-digital-event / v1` once
deployed; `externalId` at push = `digital_event/v1`.
