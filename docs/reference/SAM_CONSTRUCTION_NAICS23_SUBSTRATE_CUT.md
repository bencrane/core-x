# Construction (NAICS 23) Attachment Substrate — Cross-Cache Cut

**Date:** 2026-06-08. Construction-sector (NAICS 23 — 236 building / 237 heavy-civil / 238 specialty
trades) document substrate held across the two SAM.gov attachment caches: the **90-day prime-award
winners** (within 90 days) and the **broader historical active-opps solicitation harvest** (not
90-day-bounded). All figures from live Lance ledgers, queried 2026-06-08. The two caches use different
schemas, so the construction cut is exact for one and partial for the other — stated per §3.

---

## 1. 90-day prime-award winners (within 90 days) — EXACT

Source: `active/sam_attachment_files_90day/` (per-file `naics_code` + `mime_declared`; complete).

- Total downloaded: **126,901 files / 213.72 GB**.
- **Construction (NAICS 23): 32,711 files / 126.65 GB** — **25.8% of files but 59.3% of bytes** (construction drawings/specs are materially larger than the cross-sector mean).
- **Construction PDFs: 25,543 / 115.45 GB.**
- Construction text-extractable (pdf/docx/doc/txt): **30,659 files / 116.65 GB.**

Construction by file type:

| mime | files | GB | extractable text? |
|---|--:|--:|---|
| pdf | 25,543 | 115.45 | yes |
| docx | 4,691 | 1.11 | yes |
| doc | 345 | 0.08 | yes |
| txt | 80 | 0.00 | yes |
| zip | 288 | 7.79 | no |
| xlsx | 1,186 | 0.15 | no |
| jpg/jpeg | 392 | 0.79 | no |
| mp4/mov | 18 | 0.94 | no |
| pptx/ppt/png/rtf/accdb | 88 | 0.31 | mixed |

Top construction NAICS (standard Census labels):

| NAICS | label | files |
|---|---|--:|
| 236220 | Commercial & Institutional Building Construction | 15,481 |
| 237310 | Highway, Street & Bridge Construction | 3,593 |
| 238220 | Plumbing, Heating & Air-Conditioning Contractors | 3,443 |
| 237990 | Other Heavy & Civil Engineering Construction | 2,307 |
| 238210 | Electrical Contractors | 1,661 |
| 237110 | Water & Sewer Line & Related Structures | 1,097 |
| 238990 | All Other Specialty Trade Contractors | 960 |
| 238290 | Other Building Equipment Contractors | 891 |
| 238160 | Roofing Contractors | 683 |
| 238910 | Site Preparation Contractors | 383 |

---

## 2. Historical active-opps solicitations (broader / not 90-day-bounded)

Source: `active/sam_attachment_files/` (per-file `worklist_tier` + magic-byte `mime_sniffed`; complete.
**No `naics_code`** in this ledger — NAICS only via manifest join, which is partial).

- Total downloaded: **54,952 files / 82.47 GiB**.
- PDFs (magic-byte sniff): **44,076 / 85.9 GB (80.2%)**.

Construction signal — two complementary measures:

**(a) Construction-trigger tiers — COMPLETE.** Tiers T0+T2 and T1 were gated at harvest on
`trigger_relevant = NAICS 23 ∪ PSC N063/C1AZ`, so they are the construction-targeted segment by
construction: **18,330 files / 52.5 GB**. (Tiers T3 + T4 = 36,622 files / 36.0 GB are all-sector.)

| tier | gate | files | GB |
|---|---|--:|--:|
| T1 | trigger · all text · 10KB–50MB | 14,099 | 43.0 |
| T0+T2 | trigger · SOW/PWS-name or primary-doc | 4,231 | 9.5 |
| T3 | all-sector · SOW/PWS-name | 4,385 | 3.1 |
| T4 | all-sector · all text | 32,237 | 32.9 |

**(b) NAICS-23 by manifest join — PARTIAL.** Of 54,952 files, **26,982 (49.1%) resolve** to a manifest
`naics_code`; **27,970 (50.9%) do not** (the T4 source-manifest-overwrite lineage loss, documented in
`SAM_ATTACHMENT_90DAY_HARVEST_AND_FORENSIC_RECORD.md` §1). Of the resolvable subset: **18,513 are
NAICS 23**, and **15,694 of those are PDFs**. True historical construction count is therefore
**≥ 18,513 (lower bound)** — the unmeasurable remainder sits in the unresolvable T4 set.

---

## 3. Combined construction PDF substrate held

- 90-day (exact): **25,543 construction PDFs / 115.45 GB**.
- Historical (lower bound, resolvable only): **≥ 15,694 construction PDFs**.
- **Combined construction PDFs: ≥ 41,237** (90-day portion exact; historical a floor due to the lineage gap).

**Overlap caveat:** the caches are isolated (separate CAS prefixes, not cross-deduplicated). Some
`resource_id`s appear in both (the forensic audit found 5,195 historical files trace to the 90-day
manifest), so the count of *distinct* construction documents is below the sum of the two caches.

---

## 4. Operative notes

- **The 90-day construction set is the clean, extraction-ready one** — `naics_code` + `file_name` +
  `mime_declared` per row, BITMAP-indexed on `mime_declared`. Pushdown:
  ```sql
  SELECT resource_id, file_name, stored_uri, naics_code
  FROM   active/sam_attachment_files_90day/
  WHERE  status='downloaded' AND naics_code LIKE '23%' AND mime_declared IN ('pdf','docx','doc','txt')
  ```
  → 30,659 construction text documents (25,543 PDFs).
- **Historical construction is only partially attributable by NAICS** (T4 lineage loss); there the
  trigger-tier provenance (18,330 files, complete) is the reliable construction segment.
- NAICS 23 is the single largest sector in the 90-day cache by bytes (59.3%).

---

## 5. Verification basis (2026-06-08)

- **90-day:** `active/sam_attachment_files_90day/` (Lance) — `status='downloaded'`, grouped by
  `naics_code LIKE '23%'` × `mime_declared`. Complete (`naics_code` present for all but 168 of 126,901).
- **Historical:** `active/sam_attachment_files/` (Lance) for `worklist_tier` + `mime_sniffed` (complete);
  NAICS via LEFT JOIN to (`sam_opps_attachment_manifest` ∪ `sam_opps_attachment_manifest_90day_winners`)
  on `resource_id` (resolvable 26,982 / 54,952).
- Standard NAICS labels are from the U.S. Census NAICS 2022 manual; all other figures are measured.
