import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Parallel.ai ENRICHMENT (Directive 24, workflow 1 of 3).
 *
 * The gtm-agent's typed trigger for turning a resolved company audience into typed, cited
 * per-entity columns (segmentation workhorse). Mirrors exa_websets.ts EXACTLY:
 *   1. validate + clamp the payload (bad/oversized input never reaches Modal or Parallel,
 *      never bills) — the HARD pre-dispatch cost cap lives here,
 *   2. mint a waitpoint token (its `url` is a pre-signed callback — the callbackHash in the
 *      URL authenticates; no API key),
 *   3. POST the Universal Dispatcher (the ONLY Modal endpoint) targeting
 *      `parallel-enrich` / `enrich_companies` with the validated kwargs + that callback url,
 *   4. suspend on `wait.forToken` — checkpointed, zero compute, timeout-immune,
 *   5. resume on the worker's flat callback and resolve on the business status.
 *
 * Billing task: NO blind auto-retry (a retry reuses the same idempotencyKey and would risk
 * re-billing). A failed run is terminal; the operator re-triggers deliberately after
 * inspecting ops.parallel_runs.
 *
 * Tier ceiling = `core` (lite|base|core). `ultra` is rejected worker-side (needs the per-run
 * webhook path, deferred per §0/§9).
 */

interface ParallelEnrichPayload {
  /** The persisted spec id (ops.parallel_specs); also the per-spec dataset name. */
  specId: string;
  /** The corex.audience id whose source_sql resolves the company_id set (worker re-runs it). */
  audienceId: string;
  /** The PURE JSON-Schema data-column object (worker wraps it as {type:"json",json_schema:…}). */
  outputSchema: Record<string, unknown>;
  /** lite | base | core. Default core. ultra rejected worker-side. */
  processor?: "lite" | "base" | "core";
  /** >0 → small sample (test gate, rows land + inspectable); 0 → full run. Default 3 (test). */
  testLimit?: number;
  /** HARD pre-dispatch cap on companies dispatched. Clamps the resolved set. */
  maxRuns?: number;
  /** Optional advisory USD cap recorded on the ledger (worker counts; spend guard is maxRuns). */
  maxUsd?: number;
  /** Idempotency key (the launch tool sets f"{specId}:{audienceId}:{runKind}"). */
  idempotencyKey?: string;
}

// The flat body Modal POSTs to the waitpoint url becomes this run's output.
interface ParallelEnrichCallback {
  status: "success" | "partial" | "rejected" | "failed";
  run_id?: string;
  workflow?: string;
  spec_id?: string;
  group_id?: string;
  dataset_uri?: string;
  requested?: number;
  skipped_no_domain?: number;
  completed?: number;
  failed?: number;
  failed_company_ids?: string[];
  error?: string | null;
}

const ALLOWED_PROCESSORS = ["lite", "base", "core"];
const HARD_RUN_CAP = 1000; // §0 — ≤1000 runs/POST; the worker chunks, this clamps a test/launch.

export const parallelEnrich = task({
  id: "parallel-enrich",
  // The durable wait consumes no compute while suspended; match the worker's 60-min ceiling.
  maxDuration: 3600,
  // Billing task — no blind retry (idempotencyKey reuse would risk re-billing).
  retry: { maxAttempts: 1 },
  run: async (payload: ParallelEnrichPayload, { ctx }) => {
    // ── 1) Validate + clamp (control-plane gate) ───────────────────────────────────────
    const specId = (payload?.specId ?? "").trim();
    if (!/^[a-z0-9_]{3,64}$/.test(specId)) {
      throw new Error(`invalid specId ${JSON.stringify(specId)} — expect ^[a-z0-9_]{3,64}$`);
    }
    const audienceId = (payload?.audienceId ?? "").trim();
    if (!audienceId) {
      throw new Error("audienceId is required (the company_id audience to enrich)");
    }
    const outputSchema = payload?.outputSchema;
    if (!outputSchema || typeof outputSchema !== "object" || (outputSchema as any).type !== "object") {
      throw new Error("outputSchema must be a JSON-Schema object {type:'object',properties:…}");
    }
    const processor = payload?.processor ?? "core";
    if (!ALLOWED_PROCESSORS.includes(processor)) {
      throw new Error(
        `processor ${JSON.stringify(processor)} not allowed — enrichment caps at 'core' ` +
          `(ultra needs the per-run webhook path, deferred per §0/§9).`,
      );
    }
    // test gate: default 3 inline; 0 = full run.
    const testLimit = Math.max(0, Math.trunc(payload?.testLimit ?? 3));
    const runKind = testLimit > 0 ? "test" : "full";
    // HARD cost cap: maxRuns clamps the dispatched set. A test run is additionally capped to
    // testLimit; a full run is capped to maxRuns (or HARD_RUN_CAP if unset).
    const requestedCap = testLimit > 0 ? testLimit : (payload?.maxRuns ?? HARD_RUN_CAP);
    const maxRuns = Math.max(1, Math.min(requestedCap, HARD_RUN_CAP));
    const idempotencyKey = payload?.idempotencyKey ?? `${specId}:${audienceId}:${runKind}`;

    // Build the worker kwargs — keys MUST match enrich_companies' parameter names exactly.
    const kwargs: Record<string, unknown> = {
      run_id: ctx.run.id,
      spec_id: specId,
      audience_id: audienceId,
      output_schema: outputSchema,
      processor,
      run_kind: runKind,
      max_runs: maxRuns,
      idempotency_key: idempotencyKey,
      cost_cap: payload?.maxUsd ?? null,
    };

    logger.info("Parallel enrich starting", { specId, audienceId, processor, runKind, maxRuns });

    // ── 2) Durable callback token. token.url's callbackHash is the auth — no API key. ──
    const token = await wait.createToken({
      timeout: "1h",
      idempotencyKey,
      tags: ["parallel-enrich", specId, processor, "modal-dispatch"],
    });

    // ── 3) Fire the Universal Dispatcher (202) — Modal runs the worker out of band. ───
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "parallel-enrich",
        function_name: "enrich_companies",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // ── 4) Suspend until the worker POSTs the callback. 5) Resolve on business status. ─
    const result = await wait.forToken<ParallelEnrichCallback>(token.id);
    if (!result.ok) {
      throw new Error(`parallel enrich timed out before Modal callback (token ${token.id})`);
    }
    const out = result.output;
    // `failed` is a genuine error → surface it. `rejected` (guardrail) + `partial` (some runs
    // landed, some failed — completed rows persisted, failed ids on the ledger) are CLEAN terminals.
    if (out.status === "failed") {
      throw new Error(`parallel enrich failed in Modal: ${JSON.stringify(out)}`);
    }
    if (out.status === "rejected") {
      logger.warn("Parallel enrich rejected by guardrail", { error: out.error, ...out });
    } else if (out.status === "partial") {
      logger.warn("Parallel enrich partial — completed rows landed, failed ids on ledger", { ...out });
    } else {
      logger.info("Parallel enrich complete", { ...out });
    }
    return out;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
