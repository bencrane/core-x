-- SBIR/STTR award data — structural profiling harness (read-only, non-mutating).
-- Reproduces docs/sbir_structural_diagnostic.md against the landed CSVs.
--
--   rclone copy r2:data-sink/landing/sbir /tmp/sbir_audit
--   duckdb :memory: < pipelines/sbir/profile.sql
--
-- Out-of-core discipline: bounded memory + local NVMe spill. all_varchar=true so raw
-- null sentinels, zero-padding loss, and whitespace survive inspection (a typed read
-- coerces them away silently). sample_size=-1 = full-scan, no estimation.

SET memory_limit='6GB';
SET temp_directory='/tmp/sbir_audit/spill';
SET preserve_insertion_order=false;
.mode box

CREATE TEMP TABLE lean AS
  SELECT * FROM read_csv('/tmp/sbir_audit/award_data_no_abstract.csv',
                         all_varchar=true, header=true, sample_size=-1);

-- ── Parse gate: full text file must match the lean row count under strict RFC4180 ──
.print '== PARSE GATE: row parity (lean vs full, strict) =='
SELECT
  (SELECT count(*) FROM lean) AS lean_rows,
  (SELECT count(*) FROM read_csv('/tmp/sbir_audit/award_data.csv',
                                 all_varchar=true, header=true, sample_size=-1,
                                 ignore_errors=false)) AS full_rows_strict;

.print '== EXACT BYTE-IDENTICAL DUPLICATE ROWS =='
SELECT (SELECT count(*) FROM lean) AS total_rows,
       (SELECT count(*) FROM (SELECT DISTINCT * FROM lean)) AS distinct_rows,
       (SELECT count(*) FROM lean) - (SELECT count(*) FROM (SELECT DISTINCT * FROM lean)) AS exact_dup_rows;

-- ── All-column fill rate / distinct / literal-sentinel scan ──
.print '== FILL RATE / DISTINCT / SENTINELS (n=219502) =='
SELECT col,
  count(*) FILTER (WHERE val IS NOT NULL AND trim(val)<>'') AS populated,
  round(100.0*count(*) FILTER (WHERE val IS NOT NULL AND trim(val)<>'')/219502,2) AS fill_pct,
  count(DISTINCT val) AS ndistinct,
  count(*) FILTER (WHERE lower(trim(val)) IN ('nan','null','n/a','none','na','#n/a')) AS sentinel_hits
FROM (UNPIVOT lean ON COLUMNS(*) INTO NAME col VALUE val)
GROUP BY col ORDER BY fill_pct DESC;

-- ── Primary-key candidate uniqueness ──
.print '== PK CANDIDATE UNIQUENESS =='
SELECT 'Agency Tracking Number' AS cand,
  count(*) FILTER (WHERE trim("Agency Tracking Number")<>'') AS populated,
  count(DISTINCT CASE WHEN trim("Agency Tracking Number")<>'' THEN "Agency Tracking Number" END) AS distinct_vals,
  count(*) FILTER (WHERE trim("Agency Tracking Number")<>'')
    - count(DISTINCT CASE WHEN trim("Agency Tracking Number")<>'' THEN "Agency Tracking Number" END) AS dup_overhang FROM lean
UNION ALL SELECT 'Contract', count(*) FILTER (WHERE trim("Contract")<>''),
  count(DISTINCT CASE WHEN trim("Contract")<>'' THEN "Contract" END),
  count(*) FILTER (WHERE trim("Contract")<>'') - count(DISTINCT CASE WHEN trim("Contract")<>'' THEN "Contract" END) FROM lean
UNION ALL SELECT 'ATN+Phase+Agency', count(*),
  count(DISTINCT ("Agency Tracking Number"||'|'||"Phase"||'|'||"Agency")),
  count(*) - count(DISTINCT ("Agency Tracking Number"||'|'||"Phase"||'|'||"Agency")) FROM lean;

-- ── Identifier formatting (UEI 12-alnum, DUNS 9-digit, zero-pad need) ──
.print '== ID FORMAT CONFORMANCE =='
SELECT
  count(*) FILTER (WHERE trim("UEI")<>'' AND NOT regexp_full_match(trim("UEI"),'[A-Za-z0-9]{12}')) AS uei_nonconform,
  count(*) FILTER (WHERE trim("UEI")<>'' AND regexp_full_match(trim("UEI"),'[0-9]{12}')) AS uei_all_digits,
  count(*) FILTER (WHERE trim("Duns")<>'' AND NOT regexp_full_match(trim("Duns"),'[0-9]{9}')) AS duns_not_9digit,
  count(*) FILTER (WHERE trim("Duns")<>'' AND regexp_full_match(trim("Duns"),'[0-9]{1,8}')) AS duns_short_needs_pad
FROM lean;
.print '== DUNS length histogram =='
SELECT length(trim("Duns")) AS len, count(*) n FROM lean WHERE trim("Duns")<>'' GROUP BY 1 ORDER BY len;
.print '== ZIP length histogram =='
SELECT length(trim("Zip")) AS len, count(*) n FROM lean WHERE trim("Zip")<>'' GROUP BY 1 ORDER BY n DESC;
.print '== STATE values not 2-letter (full-name detection) =='
SELECT "State" AS state, count(*) n FROM lean
WHERE trim("State")<>'' AND NOT regexp_full_match("State",'[A-Z]{2}') GROUP BY 1 ORDER BY n DESC LIMIT 12;

-- ── Socio-economic flag domains ──
.print '== SOCIO-ECONOMIC FLAG DOMAINS (HUBZone / SocEconDis / Woman) =='
SELECT 'HUBZone Owned' AS flag, coalesce("HUBZone Owned",'<NULL>') AS val, count(*) n FROM lean GROUP BY 2
UNION ALL SELECT 'Socially+EconDisadv', coalesce("Socially and Economically Disadvantaged",'<NULL>'), count(*) FROM lean GROUP BY 2
UNION ALL SELECT 'Woman Owned', coalesce("Woman Owned",'<NULL>'), count(*) FROM lean GROUP BY 2
ORDER BY flag, n DESC;

-- ── Financial / agency / phase / program strata ──
.print '== AWARD AMOUNT DISTRIBUTION =='
WITH a AS (SELECT TRY_CAST(replace(replace("Award Amount",'$',''),',','') AS DOUBLE) amt FROM lean)
SELECT count(*) FILTER (WHERE amt IS NOT NULL) cast_ok,
  count(*) FILTER (WHERE amt=0) zero_amt, count(*) FILTER (WHERE amt<0) neg_amt,
  round(min(amt),0) min, round(quantile_cont(amt,0.25),0) p25, round(median(amt),0) median,
  round(avg(amt),0) mean, round(quantile_cont(amt,0.75),0) p75, round(quantile_cont(amt,0.95),0) p95,
  round(quantile_cont(amt,0.99),0) p99, round(max(amt),0) max FROM a;
.print '== PROGRAM / PHASE / AGENCY =='
SELECT coalesce("Program",'<NULL>') AS k, count(*) awards,
  round(sum(TRY_CAST(replace(replace("Award Amount",'$',''),',','') AS DOUBLE))/1e9,3) total_usd_bn FROM lean GROUP BY 1 ORDER BY awards DESC;
SELECT coalesce("Phase",'<NULL>') AS k, count(*) awards,
  round(avg(TRY_CAST(replace(replace("Award Amount",'$',''),',','') AS DOUBLE)),0) avg_usd FROM lean GROUP BY 1 ORDER BY awards DESC;
SELECT coalesce("Agency",'<NULL>') AS k, count(*) awards,
  round(sum(TRY_CAST(replace(replace("Award Amount",'$',''),',','') AS DOUBLE))/1e9,3) total_usd_bn FROM lean GROUP BY 1 ORDER BY awards DESC;
.print '== AWARD YEAR RANGE =='
SELECT min(TRY_CAST("Award Year" AS INT)) min_yr, max(TRY_CAST("Award Year" AS INT)) max_yr FROM lean;

-- ── Abstract audit (full file only) ──
.print '== ABSTRACT: fill / length / token proxy =='
CREATE TEMP TABLE ab AS
  SELECT "Abstract" AS ab FROM read_csv('/tmp/sbir_audit/award_data.csv',
                                        all_varchar=true, header=true, sample_size=-1);
SELECT count(*) total, count(*) FILTER (WHERE trim(ab)<>'') populated,
  round(100.0*count(*) FILTER (WHERE trim(ab)<>'')/count(*),2) fill_pct,
  min(length(ab)) FILTER (WHERE trim(ab)<>'') min_chars,
  round(avg(length(ab)) FILTER (WHERE trim(ab)<>''),0) mean_chars,
  CAST(median(length(ab)) FILTER (WHERE trim(ab)<>'') AS INT) median_chars,
  CAST(quantile_cont(length(ab),0.95) FILTER (WHERE trim(ab)<>'') AS INT) p95_chars,
  CAST(quantile_cont(length(ab),0.99) FILTER (WHERE trim(ab)<>'') AS INT) p99_chars,
  max(length(ab)) max_chars,
  CAST(avg(len(string_split_regex(trim(ab),'\s+'))) FILTER (WHERE trim(ab)<>'') AS INT) mean_words
FROM ab;
.print '== ABSTRACT: special-char scan (RFC4180-legal, naive-parser-breaking) =='
SELECT count(*) FILTER (WHERE contains(ab,chr(10))) has_LF,
  count(*) FILTER (WHERE contains(ab,chr(13))) has_CR,
  count(*) FILTER (WHERE contains(ab,chr(9))) has_TAB,
  count(*) FILTER (WHERE contains(ab,'"')) has_dquote,
  count(*) FILTER (WHERE contains(ab,',')) has_comma
FROM ab WHERE trim(ab)<>'';
.print '== ABSTRACT: placeholder sentinels (<50 chars) =='
SELECT trim(ab) AS value, count(*) n FROM ab WHERE trim(ab)<>'' AND length(trim(ab))<50 GROUP BY 1 ORDER BY n DESC LIMIT 15;
.print '== ABSTRACT: effective embeddable corpus (>=50 chars) =='
SELECT count(*) FILTER (WHERE trim(ab)<>'') raw_populated,
  count(*) FILTER (WHERE trim(ab)<>'' AND length(trim(ab))>=50) embeddable,
  round(100.0*count(*) FILTER (WHERE trim(ab)<>'' AND length(trim(ab))>=50)/count(*),2) embeddable_pct_of_all
FROM ab;
