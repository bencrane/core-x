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

---

## DISPOSITION (2026-07-22) — PROMOTED, shipped

**Verdict:** promoted. Shipped `gtm_person_channels` (1/(uei, sam_person_id) · 2.25M · sorted uei) in
the `query_sidecar_20260722T032457Z` rebuild — `gtm_sam_people` LEFT JOIN
`gtm_sam_person_contactability` ON `sam_person_id`, carrying display_name/first_name/last_name/
best_title/email/email_verification_status/phone/phone_status/person_linkedin_url_norm/is_govt_poc/
is_ebiz_poc/n_sources. Row-preserving (parity exact: 2,252,385 = source), zero fan-out (contactability
is 1:1 on sam_person_id, EXPLAIN-gated no nested-loop).

Consumed by catalyst `/market-slice/firm` (core-x #1308) — POC probe moved `sam_pocs` →
`gtm_person_channels`; the firm drawer's people section renders email · phone (or linkedin) where
resolved, name/title otherwise (gc-hq-new #101). **Coverage is thin (~10% of people have email/phone)**
— an honest data limit, not a defect; the enrichment layer simply hasn't resolved the rest. Verified
end-to-end: Thundercat Technology (UER4AJLUB8D5) returns 17 emails + phones through catalyst→BFF.
