import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending full-database bulk ingest, Phase 2 (Lance ingestion).
 *
 * A BOUNDED ingest of one point-in-time database dump (snapshot 2026-05-06) — no
 * cron. Phase 1 (exploding the 161 GiB pg_dump ZIP into per-table R2 landing
 * objects) is run out of band via the Modal `stage` entrypoint. This task
 * sequences the 51 landed analytical tables one-by-one through the Universal
 * Dispatcher into `ingest_table` / `ingest_giant_table`. Each table suspends on a
 * durable waitpoint token, so the run consumes no compute while Modal works and
 * is immune to HTTP timeouts.
 *
 * Sequential on purpose: it mirrors the proven SAM/PPP durable-callback pattern.
 * Per-table = per-dataset, so the tables are mutually conflict-free and this is
 * safe to parallelize later (bounded pool) once concurrent-waitpoint semantics
 * are exercised — deferred to keep v1 on the proven path. Giants are ordered
 * first so the longest poles start earliest in the logs.
 *
 * The 2 raw.* source tables are staged but intentionally NOT ingested (scope
 * decision: the rpt.* denormalized layer is what the platform resolves against).
 *
 * Trigger with an empty payload to ingest all 51 defaults, or pass
 * { tables: [{schema, table, giant?}] } for a subset.
 */

interface IngestTarget {
  schema: string;
  table: string;
  giant?: boolean;
}

// 51 analytical tables (Tiers 1-3 minus the 2 raw.* members). Giants (>= 3 GiB
// gz) route to the large-ephemeral-disk Modal function. Mirrors TABLE_REGISTRY
// (ingest=true) in pipelines/usaspending/usaspending_bulk.py — the SoT.
const DEFAULT_TABLES: IngestTarget[] = [
  { schema: "rpt", table: "transaction_search_fpds", giant: true },
  { schema: "rpt", table: "award_search", giant: true },
  { schema: "rpt", table: "transaction_search_fabs", giant: true },
  { schema: "public", table: "financial_accounts_by_awards", giant: true },
  { schema: "rpt", table: "subaward_search", giant: true },
  { schema: "rpt", table: "recipient_lookup" },
  { schema: "rpt", table: "recipient_profile" },
  { schema: "public", table: "financial_accounts_by_program_activity_object_class" },
  { schema: "rpt", table: "summary_state_view" },
  { schema: "int", table: "duns" },
  { schema: "public", table: "historic_parent_duns" },
  { schema: "public", table: "gtas_sf133_balances" },
  { schema: "public", table: "uei_crosswalk" },
  { schema: "public", table: "uei_crosswalk_2021" },
  { schema: "public", table: "appropriation_account_balances" },
  { schema: "rpt", table: "parent_award" },
  { schema: "public", table: "reporting_agency_tas" },
  { schema: "public", table: "references_cfda" },
  { schema: "public", table: "ref_city_county_state_code" },
  { schema: "public", table: "historical_appropriation_account_balances" },
  { schema: "public", table: "reporting_agency_missing_tas" },
  { schema: "public", table: "office" },
  { schema: "public", table: "ref_program_activity" },
  { schema: "public", table: "treasury_appropriation_account" },
  { schema: "public", table: "submission_attributes" },
  { schema: "public", table: "zips_grouped" },
  { schema: "public", table: "reporting_agency_overview" },
  { schema: "public", table: "naics" },
  { schema: "public", table: "program_activity_park" },
  { schema: "public", table: "frec_map" },
  { schema: "public", table: "psc" },
  { schema: "rpt", table: "covid_faba_spending" },
  { schema: "public", table: "federal_account" },
  { schema: "public", table: "budget_authority" },
  { schema: "public", table: "rosetta" },
  { schema: "public", table: "ref_population_county" },
  { schema: "public", table: "subtier_agency" },
  { schema: "public", table: "bureau_title_lookup" },
  { schema: "public", table: "references_definition" },
  { schema: "public", table: "agency" },
  { schema: "public", table: "toptier_agency" },
  { schema: "public", table: "state_data" },
  { schema: "public", table: "ref_country_code" },
  { schema: "public", table: "ref_population_cong_district" },
  { schema: "public", table: "cgac" },
  { schema: "public", table: "frec" },
  { schema: "public", table: "dabs_submission_window_schedule" },
  { schema: "public", table: "disaster_emergency_fund_code" },
  { schema: "public", table: "overall_totals" },
  { schema: "public", table: "object_class" },
  { schema: "public", table: "award_category" },
];

interface IngestCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  schema?: string;
  table?: string;
  snapshot_date?: string;
}

export const usaspendingBulk = task({
  id: "usaspending-bulk",
  // Active-compute ceiling only — durable suspends consume no compute and are
  // not counted, so this covers the orchestration glue across all 51 tables.
  maxDuration: 21600,
  run: async (payload: { tables?: IngestTarget[] }) => {
    const tables = payload?.tables?.length ? payload.tables : DEFAULT_TABLES;

    logger.info("USAspending bulk ingest (Phase 2) starting", { tables: tables.length });
    const results: Array<{ table: string; rows: number }> = [];

    for (const t of tables) {
      const fqtn = `${t.schema}.${t.table}`;
      const functionName = t.giant ? "ingest_giant_table" : "ingest_table";

      // 1) Durable callback token — generous for giants (43 GiB gz → ~200 GiB text).
      const token = await wait.createToken({
        timeout: t.giant ? "4h" : "90m",
        tags: ["usaspending-bulk", fqtn],
      });

      // 2) Fire the Universal Dispatcher (202) — Modal runs the worker out of band.
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: "usaspending-bulk",
          function_name: functionName,
          kwargs: { schema: t.schema, table: t.table },
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`dispatcher ${res.status} for ${fqtn}: ${body.slice(0, 300)}`);
      }

      // 3) Suspend until the worker POSTs the callback. 4) Resolve from it.
      const out = await wait.forToken<IngestCallback>(token.id);
      if (!out.ok) throw new Error(`timed out before Modal callback for ${fqtn}`);
      if (out.output.status !== "success") {
        throw new Error(`Modal failed for ${fqtn}: ${JSON.stringify(out.output)}`);
      }

      logger.info("table ingested", { table: fqtn, rows: out.output.rows });
      results.push({ table: fqtn, rows: out.output.rows });
    }

    const rows = results.reduce((acc, r) => acc + r.rows, 0);
    logger.info("USAspending bulk ingest complete", { tables: results.length, rows });
    return { tables: results.length, rows, results };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
