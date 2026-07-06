-- Cohort: DM-classified people with an owned mobile, at active SAM registrants.
-- Contract: MUST return sam_person_id. Extra columns whose names match Close custom
-- fields (lead- or contact-level) are attached to the push automatically; anything
-- else is reported and ignored. Edit freely — the engine never changes.
--
--   doppler run -p core-x -c prd -- python3 pipelines/gtm/push_sam_to_close.py \
--       --cohort pipelines/gtm/close_cohorts/example_dm_with_mobile.sql          # dry run
--
-- Available views: gtm_sam_people, gtm_sam_entities, gtm_sam_person_identity,
-- gtm_sam_person_titles, gtm_sam_person_firm_emails, phone_resolutions, work_emails.

SELECT
    p.sam_person_id,
    e.physical_state,          -- matches a lead custom field → rides onto the Lead
    e.primary_naics            -- ditto
FROM gtm_sam_people p
JOIN gtm_sam_entities e USING (uei)
JOIN gtm_sam_person_titles t USING (sam_person_id)
JOIN gtm_sam_person_identity i USING (sam_person_id)
JOIN phone_resolutions ph
  ON CASE WHEN ph.person_linkedin_url IS NOT NULL
          AND regexp_extract(lower(ph.person_linkedin_url), 'linkedin\.com/in/([^/?#]+)', 1) != ''
          THEN 'linkedin.com/in/' || regexp_extract(lower(ph.person_linkedin_url), 'linkedin\.com/in/([^/?#]+)', 1)
     END = i.person_linkedin_url_norm
WHERE t.dm_class = 'dm'
  AND e.sam_is_active
  AND ph.phone_type = 'mobile'
LIMIT 25
