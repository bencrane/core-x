---
name: dibbs-outbound-ops
description: Autonomous GTM Engineering & DIBBS Bucket B Outbound Operations skill (replicating Eric Nowoslawski's GrokBot + EmailBison + Hypertide + LeadMagic/Clay setup). Owns the 3 Expansion Decoy Brands (forgeexpansion.com, scaleexpansion.com, primeexpansion.com) and their paired scale* reserves (scaledtoday.com, scaledplus.com, scaleactivated.com), manufacturer-part assignment, clinical RFQ copy, 5-minute reply triage & margin math, noon volume check, and Friday domain hot-swap.
---

# DIBBS Bucket B Outbound Operations & Reply Triage Skill

## 1. Architecture Overview

This skill orchestrates our end-to-end outbound quote-discovery engine across the **3 warmed 52-inbox `*expansion.com` Hypertide brands** in EmailBison (`https://app.outboundsolutions.com`):

| Brand Name | Primary Domain | Reserve Hot-Swap Domain | Warmed Inboxes | Warmup Score | Daily Send Cap | Target Vertical (`FSG`) | Assigned Wave-1 Manufacturers |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- | :---: |
| **Forge Expansion Industrial** | `forgeexpansion.com` | `scaledtoday.com` (`52/52`, score `98.9`) | `49 / 52` | `93.2` | `104 / day` (`1,040` ramp) | Fluid Power, Valves, Pumps, Compressors & Mechanical (`FSG 23–49`) | `108` domain-verified (`140` total) |
| **Scale Expansion Supply** | `scaleexpansion.com` | `scaledplus.com` (`49/52`, score `92.9`) | `49 / 52` | `93.0` | `104 / day` (`1,040` ramp) | Fasteners, Bearings, Bushings, Mounts & Hardware (`FSG 15–20, 31–36, 51–56, 71–95`) | `122` domain-verified (`180` total) |
| **Prime Expansion Technical Systems** | `primeexpansion.com` | `scaleactivated.com` (`48/52`, score `91.2`) | `52 / 52` | `98.5` | `104 / day` (`1,040` ramp) | Electrical, Relays, Switches, Connectors, Batteries & Instrumentation (`FSG 58–70`) | `132` domain-verified (`168` total) |

**Quarantined Domains (Never Attach to Live Campaigns):**
- `activeoperators.com` (`0/52` connected — Microsoft OAuth expired)
- `scaleactivation.com` (`38/52` warmup disabled; `3.8%` warmup reply rate)
- `scaleinvent.com` (`4.2%` warmup reply rate anomaly — `95` replies on `2,259` warmup sends)

---

## 2. Canonical File & Script Paths

All launch assets live in `/Users/benjamincrane/core-x/exports/outbound_launch_pack/`:

- **Master Manufacturer + Part Assignments (All `488` `Bucket B` `DESTINATION` Makers):**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/manufacturer_part_assignments_all.csv`
- **Wave-1 Domain-Verified Manufacturers (`362` Makers with Root Domains & SAM POCs):**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/wave1_top_manufacturers_with_domains.csv`
- **Per-Brand Lead CSVs:**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/brand_forgeexpansion_forgeexpansion.com_leads.csv` (`108` makers)
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/brand_scaleexpansion_scaleexpansion.com_leads.csv` (`122` makers)
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/brand_primeexpansion_primeexpansion.com_leads.csv` (`132` makers)
- **Clay CLI / LeadMagic Enrichment Input Table:**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/clay_cli_enrichment_input.csv`
- **Static Brand Websites (Ready for Cloudflare Pages / Vercel Deployment):**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/sites/forgeexpansion.com/index.html`
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/sites/scaleexpansion.com/index.html`
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/sites/primeexpansion.com/index.html`
- **Automation Scripts:**
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/scripts/enrich_manufacturer_contacts.py`
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/scripts/stage_emailbison_campaigns.py`
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/scripts/grokbot_reply_handler.py`
  - `/Users/benjamincrane/core-x/exports/outbound_launch_pack/scripts/grokbot_deliverability_watchdog.py`
