# GAP: firm-profile contact channels (emails/phones) — 2026-07-21

**Surface:** catalyst `POST /api/v1/market-slice/firm` (core-x #1303) — the award-drawer
flip's "The people" section (gc-hq-new #95).

**What's served today:** `sam_pocs` name/title/geo (no email/phone columns at SAM source —
disclosed on the wire) + `gtm_audience_entities.n_dialable/n_emailable` coverage counts.

**The gap:** actual contact channels (emails, phones, LinkedIn per person) live in the
enrichment layer (`gtm_sam_person_contactability` — sorted `sam_person_id`, NOT uei;
`icypeas_person_profiles`; `dsbs_poc_linkedin`) with no clean uei-sorted per-person
channel mart. A `gtm_person_channels` mart at (uei, person) grain — sorted `uei`, columns
name/title/email/phone/linkedin/source — would make the firm profile's people section
fully loaded in the same ms-class point-read.

**Demand evidence:** operator directive 2026-07-21 ("we are showing it all.. even
contacts") for the capital-video firm profile. Promote when a second surface asks.
