import { task, wait, logger } from "@trigger.dev/sdk";
import { enrichmentEnrichLinkedin } from "./enrichment_blitz";

/**
 * Control plane — enroll the active equipment-rental supply base into the Blitz
 * firmographic enrichment cycle (Atomic Workflow B, Directive 23).
 *
 * Two sequential hops, each a durable waitpoint (zero compute while suspended):
 *
 *   1. BUILD COHORT — dispatch the Modal cohort builder
 *      (pipelines/enrichment_blitz/cohort_equipment_rental.py::build_cohort) via the
 *      Universal Dispatcher. It resolves active US equip-rental firms (SAM NAICS
 *      bundle) → sam_master_domains → PDL → DISTINCT company_linkedin_url and drops
 *      a transport Parquet under the data-sink bucket, POSTing {r2_key, column,
 *      distinct_urls, …} back to the waitpoint.
 *
 *   2. ENROLL — trigger the EXISTING `enrichment-blitz-enrich-linkedin` task
 *      (Workflow B: company_linkedin_url → firmographics) on that Parquet cohort.
 *      Bulk backfill ⇒ default LOW priority so it yields to interactive GTM
 *      enrichment at the 5-RPS gateway (the LOW floor guarantees it is throttled,
 *      never starved). Results land in ops.task_runs → the firmographics-blitz
 *      materializer → the firmographics_blitz Lance system-of-record.
 *
 * THE CYCLE is the Workflow B TTL, not a loop here: firmo_ttl_days JIT-skips firms
 * already fresh, so re-invoking this task only re-hits stale or newly-registered
 * firms. MANUAL by design (no cron) so Blitz credit consumption is observed on the
 * first runs — mirrors src/trigger/exa_websets.ts / sba_foia_ingest.ts. Flip to
 * `schedules.task` with a monthly cron once consumption is observed.
 *
 * The two waitpoints are SEQUENTIAL (build → enroll), never concurrent, so this
 * never trips TASK_DID_CONCURRENT_WAIT.
 */

// The flat JSON body the Modal cohort builder POSTs to the waitpoint url.
interface CohortCallback {
  status: "success" | "error";
  feed: string;
  cohort_name: string;
  r2_key: string | null;
  column: string | null;
  distinct_urls: number;
  firms_total: number;
  firms_with_domain: number;
  firms_pdl_matched: number;
  firms_with_linkedin: number;
  error?: string | null;
}

interface EnrollPayload {
  // Bulk backfill default; the gateway floors LOW so it is throttled, never starved.
  priority?: "high" | "normal" | "low";
  firmoTtlDays?: number; // override the Workflow B freshness gate (default 180)
  negTtlDays?: number; // override the negative-cache window (default 30)
}

export const enrollEquipmentRentalFirmsFirmo = task({
  id: "enroll-equipment-rental-firms-firmo",
  // Parent suspends on two sequential waitpoints (cohort build, then the Workflow B
  // child run); zero compute while suspended. 2h ceiling covers a cold first run
  // (every firm an API call) at the 5-RPS gateway with wide margin.
  maxDuration: 7200,
  run: async (payload: EnrollPayload = {}, { ctx }) => {
    // ── 1) Build the cohort Parquet (Modal cohort builder via Universal Dispatcher) ──
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["enroll-rental-firms", "cohort-build"],
    });
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "enrichment-blitz-cohort-rental",
        function_name: "build_cohort",
        kwargs: { run_id: ctx.run.id },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`cohort dispatcher ${res.status}: ${body.slice(0, 300)}`);
    }

    const built = await wait.forToken<CohortCallback>(token.id);
    if (!built.ok) {
      throw new Error("cohort builder timed out before Modal callback");
    }
    const cohort = built.output;
    if (cohort.status !== "success" || !cohort.r2_key) {
      throw new Error(`cohort build failed: ${JSON.stringify(cohort)}`);
    }
    logger.info("equipment-rental LinkedIn cohort built", { ...cohort });

    if (cohort.distinct_urls === 0) {
      logger.warn("cohort empty — no PDL LinkedIn URLs resolved; nothing to enroll", { ...cohort });
      return { cohort, enrolled: 0 };
    }

    // ── 2) Enroll into the existing Workflow B firmo enrichment task ──
    const enrich = await enrichmentEnrichLinkedin.triggerAndWait({
      cohort: { r2_key: cohort.r2_key, column: cohort.column ?? "company_linkedin_url" },
      priority: payload.priority ?? "low",
      batchLabel: "equipment_rental_firms",
      firmoTtlDays: payload.firmoTtlDays,
      negTtlDays: payload.negTtlDays,
    });
    if (!enrich.ok) {
      throw new Error(`enrichment-blitz B failed: ${JSON.stringify(enrich.error)}`);
    }
    logger.info("equipment-rental firms enrolled in Blitz firmo enrichment", { ...enrich.output });
    return { cohort, enrichment: enrich.output };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
