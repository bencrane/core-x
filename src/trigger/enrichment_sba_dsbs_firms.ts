import { task, wait, logger } from "@trigger.dev/sdk";
import { enrichmentEnrichLinkedin } from "./enrichment_blitz";

/**
 * Control plane — enroll the SBA DSBS certified-firm base into the Blitz firmographic
 * enrichment cycle (Atomic Workflow B, Directive 23). Sibling of
 * src/trigger/enrichment_rental_firms.ts; identical two-hop shape, different supply.
 *
 *   1. BUILD COHORT — dispatch the Modal cohort builder
 *      (pipelines/enrichment_blitz/cohort_sba_dsbs_certified.py::build_cohort) via the
 *      Universal Dispatcher. It resolves every certified firm → candidate domain
 *      (normalized website ∪ sam_master_domains ∪ email suffix, non-generic) → PDL →
 *      DISTINCT company_linkedin_url, drops a transport Parquet, and POSTs the cohort
 *      stats back to the waitpoint.
 *
 *   2. ENROLL — trigger the EXISTING `enrichment-blitz-enrich-linkedin` task
 *      (Workflow B: company_linkedin_url → firmographics) on that Parquet cohort.
 *      Bulk backfill ⇒ default LOW priority so it yields to interactive GTM enrichment
 *      at the 5-RPS gateway. firmo_ttl_days JIT-skips firms already fresh in
 *      firmographics_blitz / ops.task_runs, so only not-yet-enriched firms spend credits.
 *      Results land in ops.task_runs → the firmographics-blitz materializer → the
 *      firmographics_blitz Lance system-of-record.
 *
 * MANUAL by design (no cron) so Blitz credit consumption is observed on the first runs.
 * Sequential waitpoints (build → enroll), never concurrent.
 */

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
  priority?: "high" | "normal" | "low";
  firmoTtlDays?: number; // override the Workflow B freshness gate (default 180)
  negTtlDays?: number; // override the negative-cache window (default 30)
}

export const enrollSbaDsbsCertifiedFirmsFirmo = task({
  id: "enroll-sba-dsbs-certified-firms-firmo",
  maxDuration: 7200,
  run: async (payload: EnrollPayload = {}, { ctx }) => {
    // ── 1) Build the cohort Parquet (Modal cohort builder via Universal Dispatcher) ──
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["enroll-sba-dsbs", "cohort-build"],
    });
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "enrichment-blitz-cohort-sba-dsbs",
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
    logger.info("sba-dsbs LinkedIn cohort built", { ...cohort });

    if (cohort.distinct_urls === 0) {
      logger.warn("cohort empty — no PDL LinkedIn URLs resolved; nothing to enroll", { ...cohort });
      return { cohort, enrolled: 0 };
    }

    // ── 2) Enroll into the existing Workflow B firmo enrichment task ──
    const enrich = await enrichmentEnrichLinkedin.triggerAndWait({
      cohort: { r2_key: cohort.r2_key, column: cohort.column ?? "company_linkedin_url" },
      priority: payload.priority ?? "low",
      batchLabel: "sba_dsbs_certified_firms",
      firmoTtlDays: payload.firmoTtlDays,
      negTtlDays: payload.negTtlDays,
    });
    if (!enrich.ok) {
      throw new Error(`enrichment-blitz B failed: ${JSON.stringify(enrich.error)}`);
    }
    logger.info("sba-dsbs certified firms enrolled in Blitz firmo enrichment", { ...enrich.output });
    return { cohort, enrichment: enrich.output };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
