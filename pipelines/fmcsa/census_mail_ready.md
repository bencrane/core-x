# FMCSA `census_mail_ready` — direct-mail serving dataset

`s3://data-sink/active/fmcsa/census_mail_ready/` · Lance v2 · 4,437,561 rows · one
row per carrier (USDOT). Built by
[`census_mail_ready.py`](census_mail_ready.py) (Modal app `fmcsa-derived`).

A **non-destructive** projection of the `active/fmcsa/census` system-of-record,
shaped for Lob direct mail and demand-side identity resolution. It never mutates
census, so it cannot race the daily ingest or break the SAM↔FMCSA domain bridge.

## Why this exists (and what the source actually looked like)

The conditioning directive assumed `fmcsa/census` and `fmcsa/carrier` were stored
with unparsed positional headers (`column00…columnN`) and needed an MCMIS
positional remap rewritten in place. Live read-back (2026-06-01) shows that is not
the case, so the destructive in-place overwrite was **not** performed. Instead this
derived dataset realizes the achievable intent. Ground truth:

| dataset | shape | mail fields? |
|---|---|---|
| `active/fmcsa/census` | **155 named columns**, 4.44M rows, already BTREE-indexed on `carrier_dot` + `proxy_domain` | **yes** — `legal_name`, `phy_*`, `carrier_mailing_*`, `phone`, `email_address`, `company_officer_1/2`, `docket{1,2,3}` are all present and populated |
| `active/fmcsa/carrier` | positional `column00…column42`, 5,369 rows | **no** — this is the authority/status file (docket@00, USDOT@01, A/I status, N/Y authority flags); it carries no address/officer/email |

So census needed **field conditioning + indexing of the mail block**, not parsing;
and `carrier` (the only `column00` feed) is not a mail source at all.

Measured source coverage over 4,437,561 census rows: `legal_name` ~100%,
`phy_street` ~100%, `phy_zip` 99.97%, `phone` 96.6%, `company_officer_1` 85.6%,
`email_address` 65.5%, `dba_name` 26.5%, `proxy_domain` 16.6% (corporate, consumer
suppressed).

## What this dataset adds

- **Standardized mailing block** — structured `phy_*` + `mail_*` fields plus a
  ready-to-render `mail_to_block` (mailing address preferred, physical fallback)
  and a `mailable` deliverability flag (99.98% true).
- **Officer names + company email kept STRICTLY separate** — `company_officer_1`,
  `company_officer_2`, and `email_address` are distinct columns. There is
  deliberately **no** glued officer↔email anchor: FMCSA does not assert the officer
  owns the mailbox, and the on-file email is frequently a generic company inbox
  (`info@`/`dispatch@`/`safety@`). Gluing them would manufacture a false 1:1 and
  mis-target downstream GTM.
- **Contact anchors** — `phone`/`fax`/`cell_phone`, `email_address`, raw
  `email_domain`, and the suppressed corporate `proxy_domain` (the bridge join key,
  passed through from census).
- **MC number** recovered from the census docket pairs (39.1%).
- **Low-cardinality status flags**, labelled and BTREE-indexed: status
  (Active/Inactive), `carrier_operation` (Interstate/Intrastate), entity type,
  power units.

## Indexes

BTREE scalar indexes: `carrier_dot`, `proxy_domain` (the preserved census
resolution keys), `status_code`, `carrier_operation`, `business_org_id`,
`power_units`.

## Cadence

Chained off census in [`src/trigger/fmcsa_daily.ts`](../../src/trigger/fmcsa_daily.ts):
after the daily census ingest reports success, the dispatcher invokes
`fmcsa-derived.build_mail_ready`, which re-projects and overwrites this prefix.
Manual: `modal run pipelines/fmcsa/census_mail_ready.py`.

## Data dictionary

Machine-readable column dictionary (for the cohort-builder / LLM query context):
[`census_mail_ready_dictionary.json`](census_mail_ready_dictionary.json).

## Sample (read-back from R2)

```
carrier_dot           125071
mc_number             126271
legal_name            KODIAK TRANSFER CO
dba_name              KODIAK TRANSFER INC
mail_to_block         KODIAK TRANSFER CO
                      DBA KODIAK TRANSFER INC
                      5152 TOM STILES RD BLDG A
                      KODIAK, AK 99615
email_address         kodiaktransfer@alaska.net
email_domain          alaska.net
proxy_domain          alaska.net
company_officer_1     GREG WAKEFIELD
company_officer_2     (null)
status_label          Active
carrier_operation_label  Interstate
entity_type           INDIVIDUAL
power_units           4
```
