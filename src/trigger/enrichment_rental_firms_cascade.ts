import { task, wait, logger } from "@trigger.dev/sdk";
import { enrichmentCascade } from "./enrichment_blitz";

/**
 * Control plane — enroll the equipment-rental firms the Workflow B path could not
 * reach into the Blitz CASCADE (Atomic Workflow C, Directive 23).
 *
 * SIBLING of src/trigger/enrichment_rental_firms.ts. That task enrolls the firms
 * that resolved to a PDL company LinkedIn URL into Workflow B. This task picks up
 * the GAP it leaves behind: equip-rental firms that HAVE a canonical SAM normalized
 * domain but did NOT match a non-generic PDL company (≈ firms_with_domain −
 * firms_pdl_matched), so they carry no LinkedIn URL and got no firmographics. They
 * are reachable through Workflow C, which resolves domain → company_linkedin_url →
 * firmographics inside the rate-governed gateway and needs no pre-resolved URL.
 *
 * Two sequential durable waitpoints (zero compute while suspended):
 *
 *   1. BUILD COHORT — dispatch the Modal cohort builder
 *      (pipelines/enrichment_blitz/cohort_equipment_rental_cascade.py::build_cohort)
 *      via the Universal Dispatcher. It resolves the gap firms → DISTINCT
 *      normalized_domain and drops a transport Parquet under the data-sink bucket,
 *      POSTing {r2_key, column, distinct_domains, …} back to the waitpoint.
 *
 *   2. ENROLL — trigger the EXISTING `enrichment-blitz-cascade` task (Workflow C:
 *      domain → linkedin → firmographics) on that Parquet cohort. Bulk backfill ⇒
 *      LOW priority so it yields to interactive GTM enrichment at the 5-RPS gateway
 *      (the LOW floor guarantees it is throttled, never starved). Results land in
 *      ops.task_runs → the firmographics-blitz materializer → the
 *      firmographics_blitz Lance system-of-record.
 *
 * COST NOTE: Workflow C spends MORE Blitz credits per firm than Workflow B (a
 * domain-resolve hop + company hops, vs. a single firmo hop on a known URL). Set
 * `previewOnly: true` to build + size the cohort and STOP before any enrichment
 * spend — the returned `cohort.distinct_domains` is the spend surface. The default
 * (previewOnly omitted) builds then enrolls.
 *
 * THE CYCLE is the Workflow C TTL, not a loop here: firmo_ttl_days JIT-skips firms
 * already fresh and neg_ttl_days skips recent misses, so re-invoking this task only
 * re-hits stale or newly-registered firms. MANUAL by design (no cron) so Blitz
 * credit consumption is observed on the first runs — mirrors
 * src/trigger/enrichment_rental_firms.ts. Flip to `schedules.task` with a monthly
 * cron once consumption is observed.
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
  distinct_domains: number;
  firms_total: number;
  firms_with_domain: number;
  firms_pdl_matched: number;
  firms_gap: number;
  error?: string | null;
}

interface EnrollPayload {
  // Build + size the cohort, then STOP before any Workflow C enrichment spend.
  previewOnly?: boolean;
  // Bulk backfill default; the gateway floors LOW so it is throttled, never starved.
  priority?: "high" | "normal" | "low";
  firmoTtlDays?: number; // override the Workflow C freshness gate (default 180)
  negTtlDays?: number; // override the negative-cache window (default 30)
}

export const enrollEquipmentRentalFirmsFirmoCascade = task({
  id: "enroll-equipment-rental-firms-firmo-cascade",
  // Parent suspends on two sequential waitpoints (cohort build, then the Workflow C
  // child run); zero compute while suspended. 2h ceiling covers a cold first run
  // (every firm a domain-resolve + company hop) at the 5-RPS gateway with margin.
  maxDuration: 7200,
  run: async (payload: EnrollPayload = {}, { ctx }) => {
    // ── 1) Build the cohort Parquet (Modal cohort builder via Universal Dispatcher) ──
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["enroll-rental-firms-cascade", "cohort-build"],
    });
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "enrichment-blitz-cohort-rental-cascade",
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
    logger.info("equipment-rental cascade (gap-domain) cohort built", { ...cohort });

    if (cohort.distinct_domains === 0) {
      logger.warn("cohort empty — no gap domains resolved; nothing to enroll", { ...cohort });
      return { cohort, enrolled: 0 };
    }

    // ── Preview gate — surface the spend surface and STOP before any Blitz spend ──
    if (payload.previewOnly) {
      logger.info("previewOnly — cohort built, skipping Workflow C enrollment (no spend)", {
        distinct_domains: cohort.distinct_domains,
        r2_key: cohort.r2_key,
      });
      return { cohort, enrolled: 0, previewOnly: true };
    }

    // ── 2) Enroll into the existing Workflow C cascade enrichment task ──
    const enrich = await enrichmentCascade.triggerAndWait({
      cohort: { r2_key: cohort.r2_key, column: cohort.column ?? "normalized_domain" },
      priority: payload.priority ?? "low",
      batchLabel: "equipment_rental_firms_cascade",
      firmoTtlDays: payload.firmoTtlDays,
      negTtlDays: payload.negTtlDays,
    });
    if (!enrich.ok) {
      throw new Error(`enrichment-blitz C failed: ${JSON.stringify(enrich.error)}`);
    }
    logger.info("equipment-rental gap firms enrolled in Blitz cascade enrichment", {
      ...enrich.output,
    });
    return { cohort, enrichment: enrich.output };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
