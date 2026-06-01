import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending full-database bulk ingest, Phase 2 (Lance ingestion).
 *
 * A BOUNDED ingest of one point-in-time database dump (snapshot 2026-05-06). Phase 1
 * (exploding the 161 GiB pg_dump ZIP into per-table R2 landing objects) is run out of
 * band via the Modal `stage` entrypoint. This task fans the 51 landed analytical
 * tables out across the Universal Dispatcher into `ingest_table` / `ingest_giant_table`.
 *
 * Fan-out without unproven concurrent-waitpoint semantics: FIRE all dispatcher calls
 * up front (Modal runs every worker in parallel, server-side, durable), THEN await each
 * table's durable waitpoint token sequentially. Execution is fully parallel; only the
 * awaiting is ordered — wall-clock ≈ the slowest table, not the sum. Giants are fired
 * first so the long poles start earliest.
 *
 * Resilient: a per-table dispatch failure, timeout, or Modal error is recorded and the
 * run continues — one bad table never aborts the others. Per-table = per-dataset with
 * mode="overwrite", so any failed table is re-run in isolation (trigger with
 * { tables: [{schema, table, giant?}] }) without disturbing the successful datasets.
 *
 * The 2 raw.* source tables are staged but intentionally NOT ingested (scope decision).
 */

interface IngestTarget {
  schema: string;
  table: string;
  giant?: boolean;
}

// 51 analytical tables (Tiers 1-3 minus the 2 raw.* members), giants first. Mirrors
// TABLE_REGISTRY (ingest=true) in pipelines/usaspending/usaspending_bulk.py — the SoT.
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

interface TableResult {
  table: string;
  giant: boolean;
  status: "success" | "error" | "dispatch_failed" | "timeout";
  rows: number;
  error?: string;
}

export const usaspendingBulk = task({
  id: "usaspending-bulk",
  // Active-compute ceiling only — durable suspends consume no compute and are not
  // counted, so this covers the orchestration glue while workers run server-side.
  maxDuration: 28800,
  run: async (payload: { tables?: IngestTarget[] }) => {
    const tables = payload?.tables?.length ? payload.tables : DEFAULT_TABLES;
    logger.info("USAspending bulk ingest (Phase 2) starting", { tables: tables.length });

    const dispatcherUrl = requireEnv("MODAL_DISPATCHER_URL");
    const modalKey = requireEnv("MODAL_KEY");
    const modalSecret = requireEnv("MODAL_SECRET");

    // ── Pass 1: FIRE every dispatcher call up front → Modal runs all workers in
    //    parallel, server-side. Collect a durable waitpoint token per table.
    const pending: Array<{ t: IngestTarget; tokenId: string }> = [];
    const dispatchFailures: TableResult[] = [];

    for (const t of tables) {
      const fqtn = `${t.schema}.${t.table}`;
      const token = await wait.createToken({
        timeout: t.giant ? "5h" : "2h",
        tags: ["usaspending-bulk", fqtn],
      });
      try {
        const res = await fetch(dispatcherUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Modal-Key": modalKey,
            "Modal-Secret": modalSecret,
          },
          body: JSON.stringify({
            app_name: "usaspending-bulk",
            function_name: t.giant ? "ingest_giant_table" : "ingest_table",
            kwargs: { schema: t.schema, table: t.table },
            trigger_callback_url: token.url,
          }),
        });
        if (!res.ok) {
          const body = await res.text();
          dispatchFailures.push({
            table: fqtn, giant: !!t.giant, status: "dispatch_failed", rows: 0,
            error: `dispatcher ${res.status}: ${body.slice(0, 200)}`,
          });
          continue;
        }
        pending.push({ t, tokenId: token.id });
      } catch (err) {
        dispatchFailures.push({
          table: fqtn, giant: !!t.giant, status: "dispatch_failed", rows: 0,
          error: String(err).slice(0, 200),
        });
      }
    }
    logger.info("All workers dispatched; awaiting callbacks", {
      dispatched: pending.length, dispatch_failed: dispatchFailures.length,
    });

    // ── Pass 2: AWAIT each token. Work is already running in parallel; awaiting in
    //    order just collects results. One failure never aborts the rest.
    const results: TableResult[] = [...dispatchFailures];
    for (const { t, tokenId } of pending) {
      const fqtn = `${t.schema}.${t.table}`;
      try {
        const out = await wait.forToken<IngestCallback>(tokenId);
        if (!out.ok) {
          results.push({ table: fqtn, giant: !!t.giant, status: "timeout", rows: 0,
            error: "waitpoint timed out before Modal callback" });
        } else if (out.output.status !== "success") {
          results.push({ table: fqtn, giant: !!t.giant, status: "error", rows: out.output.rows ?? 0,
            error: JSON.stringify(out.output).slice(0, 200) });
        } else {
          results.push({ table: fqtn, giant: !!t.giant, status: "success", rows: out.output.rows });
          logger.info("table ingested", { table: fqtn, rows: out.output.rows, giant: !!t.giant });
        }
      } catch (err) {
        results.push({ table: fqtn, giant: !!t.giant, status: "error", rows: 0,
          error: String(err).slice(0, 200) });
      }
    }

    const succeeded = results.filter((r) => r.status === "success");
    const failed = results.filter((r) => r.status !== "success");
    const totalRows = succeeded.reduce((acc, r) => acc + r.rows, 0);
    const giants = results.filter((r) => r.giant);

    logger.info("USAspending bulk ingest complete", {
      total: results.length, succeeded: succeeded.length, failed: failed.length, totalRows,
    });
    if (failed.length) logger.warn("failed tables (re-run in isolation)", { failed });

    return {
      total: results.length,
      succeeded: succeeded.length,
      failed: failed.length,
      total_rows: totalRows,
      giants: giants.map((g) => ({ table: g.table, status: g.status, rows: g.rows })),
      failures: failed.map((f) => ({ table: f.table, status: f.status, error: f.error })),
      results,
    };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
