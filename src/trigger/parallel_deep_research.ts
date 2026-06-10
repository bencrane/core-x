import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Parallel.ai DEEP RESEARCH (Directive 24, workflow 2 of 3).
 *
 * The gtm-agent's trigger for commissioning a cited long-form report (NOT typed columns).
 * Same durable-callback contract as exa_websets.ts:
 *   1. validate the payload (ultra rejected up front — needs the per-run webhook path),
 *   2. mint a waitpoint token,
 *   3. POST the Universal Dispatcher targeting `parallel-deep-research` / `deep_research`,
 *   4. suspend on `wait.forToken`,
 *   5. resume on the worker's flat callback, resolve on the business status.
 *
 * grain="topic"      → one report for `objective`, landed keyed run_id.
 * grain="per_entity" → one report per company in `audienceId`, landed keyed company_id.
 *
 * Tier: up to `pro`. `ultra` is REJECTED (hours-long; needs Parallel's per-run webhook →
 * trigger-token path, deferred per §0/§9). Billing task → no blind retry.
 */

interface ParallelDeepResearchPayload {
  /** Optional persisted spec id; defaults worker-side to research_<run>. */
  specId?: string;
  /** The research question / steering objective (required). */
  objective: string;
  /** "topic" (one standalone report) | "per_entity" (one per company). Default topic. */
  grain?: "topic" | "per_entity";
  /** Required when grain="per_entity": the company_id audience to research. */
  audienceId?: string;
  /** lite | base | core | pro. Default base. ultra REJECTED. */
  processor?: "lite" | "base" | "core" | "pro";
  /** "text" (long-form report) | "auto" (Parallel chooses). Default text. */
  outputType?: "text" | "auto";
  /** per_entity test gate: small sample; 0 = all. Default 3 (test). Ignored for topic. */
  testLimit?: number;
  /** HARD pre-dispatch cap on per_entity reports. */
  maxRuns?: number;
  /** Advisory USD cap recorded on the ledger. */
  maxUsd?: number;
  /** Idempotency key (the launch tool sets f"{specId}:{audienceId|topic}:{runKind}"). */
  idempotencyKey?: string;
}

interface ParallelDeepResearchCallback {
  status: "success" | "partial" | "rejected" | "failed";
  run_id?: string;
  workflow?: string;
  spec_id?: string;
  grain?: string;
  dataset_uri?: string;
  requested?: number;
  completed?: number;
  failed?: number;
  failed_company_ids?: string[];
  error?: string | null;
}

const ALLOWED_PROCESSORS = ["lite", "base", "core", "pro"];
const HARD_RUN_CAP = 1000;

export const parallelDeepResearch = task({
  id: "parallel-deep-research",
  maxDuration: 3600,
  retry: { maxAttempts: 1 },
  run: async (payload: ParallelDeepResearchPayload, { ctx }) => {
    // ── 1) Validate ────────────────────────────────────────────────────────────────────
    const objective = (payload?.objective ?? "").trim();
    if (objective.length < 8 || objective.length > 8000) {
      throw new Error("objective must be 8–8000 chars");
    }
    const grain = payload?.grain ?? "topic";
    if (!["topic", "per_entity"].includes(grain)) {
      throw new Error(`invalid grain ${JSON.stringify(grain)} — expect topic | per_entity`);
    }
    const processor = String(payload?.processor ?? "base");
    if (processor === "ultra") {
      throw new Error(
        "processor 'ultra' is not supported by deep-research v1 — it runs for hours and " +
          "requires Parallel's per-run webhook → trigger-token path (deferred per §0/§9). Use up to 'pro'.",
      );
    }
    if (!ALLOWED_PROCESSORS.includes(processor)) {
      throw new Error(`processor ${JSON.stringify(processor)} must be one of ${ALLOWED_PROCESSORS.join("|")}`);
    }
    const outputType = payload?.outputType ?? "text";
    if (!["text", "auto"].includes(outputType)) {
      throw new Error(`invalid outputType ${JSON.stringify(outputType)} — expect text | auto`);
    }

    let audienceId = (payload?.audienceId ?? "").trim();
    if (grain === "per_entity" && !audienceId) {
      throw new Error("grain=per_entity requires audienceId (the company_id audience to research)");
    }
    if (grain === "topic") audienceId = "";

    const testLimit = Math.max(0, Math.trunc(payload?.testLimit ?? 3));
    const runKind = grain === "per_entity" && testLimit > 0 ? "test" : "full";
    const requestedCap = testLimit > 0 ? testLimit : (payload?.maxRuns ?? HARD_RUN_CAP);
    const maxRuns = Math.max(1, Math.min(requestedCap, HARD_RUN_CAP));
    const specId = (payload?.specId ?? "").trim();
    // The waitpoint idempotency key is PROJECT-scoped (30-day TTL), not run-scoped. The fallback
    // MUST be run-unique — appending ctx.run.id (mirrors parallel_search.ts) — or every caller that
    // omits payload.idempotencyKey (the cal booking path; gtm-mcp topic-no-spec) collides on the
    // shared constant `research:topic:full` and resolves on the FIRST run's cached token. Callers
    // that pass payload.idempotencyKey keep their intentional same-spec dedup (the `??` only changes
    // the fallback — do NOT strip it).
    const idempotencyKey =
      payload?.idempotencyKey ?? `${specId || "research"}:${audienceId || "topic"}:${runKind}:${ctx.run.id}`;

    const kwargs: Record<string, unknown> = {
      run_id: ctx.run.id,
      spec_id: specId,
      objective,
      grain,
      audience_id: audienceId || null,
      processor,
      output_type: outputType,
      run_kind: runKind,
      max_runs: maxRuns,
      idempotency_key: idempotencyKey,
      cost_cap: payload?.maxUsd ?? null,
    };

    logger.info("Parallel deep research starting", { specId, grain, processor, runKind });

    // ── 2) Durable callback token ──────────────────────────────────────────────────────
    const token = await wait.createToken({
      timeout: "1h",
      idempotencyKey,
      tags: ["parallel-deep-research", grain, processor, "modal-dispatch"],
    });

    // ── 3) Fire the Universal Dispatcher (202) ─────────────────────────────────────────
    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "parallel-deep-research",
        function_name: "deep_research",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // ── 4) Suspend. 5) Resolve. ────────────────────────────────────────────────────────
    const result = await wait.forToken<ParallelDeepResearchCallback>(token.id);
    if (!result.ok) {
      throw new Error(`parallel deep research timed out before Modal callback (token ${token.id})`);
    }
    const out = result.output;
    if (out.status === "failed") {
      throw new Error(`parallel deep research failed in Modal: ${JSON.stringify(out)}`);
    }
    if (out.status === "rejected") {
      logger.warn("Parallel deep research rejected by guardrail", { error: out.error, ...out });
    } else if (out.status === "partial") {
      logger.warn("Parallel deep research partial — some reports landed", { ...out });
    } else {
      logger.info("Parallel deep research complete", { ...out });
    }
    return out;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
