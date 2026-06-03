import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Exa.ai Webset ingestion (Directive 22).
 * Contract: docs/reference/EXA_WEBSET_INGESTION_SPEC.md.
 *
 * Managed-Agent-facing entrypoint. Trigger.dev v4 owns the wait; Modal does the short
 * compute bursts (create webset → poll to idle → ingest → one callback). This task:
 *   1. validates + clamps the payload (bad input never reaches Modal or Exa, never burns credits),
 *   2. mints a waitpoint token (its `url` is a pre-signed HTTP callback — no API key),
 *   3. POSTs the Universal Dispatcher (the ONLY Modal endpoint) targeting
 *      `exa-webset-pipelines` / `ingest_exa_webset` with the validated kwargs + that callback url,
 *   4. suspends on `wait.forToken` — checkpointed, zero compute, timeout-immune,
 *   5. resumes on the worker's flat callback and resolves on the business status.
 *
 * No cron: the directive mandates manual invocation so credit consumption is observed on the
 * first runs. Flip to `schedules.task` only if/when an automated cadence is wanted.
 *
 * Minimal valid call (directive example): everything else is defaulted.
 *   { "webset_identifier": "osha_defense_firms",
 *     "search_prompt": "Top law firms specializing in OSHA defense and workplace safety compliance",
 *     "max_results_limit": 500 }
 */

const HARD_RESULT_CAP = 1000; // D4 — mirrors the worker clamp

interface ExaWebsetPayload {
  webset_identifier: string;
  search_prompt: string;
  max_results_limit?: number;
  entity_type?: "company" | "person";
  criteria?: string[];
  tier?: "precision" | "harvest";
  seed_urls?: string[];
  enrichments?: Array<Record<string, unknown>>;
  exclude_known_domains?: boolean;
  max_credits?: number;
  dry_run?: boolean;
}

// The flat JSON body Modal POSTs to the waitpoint url becomes this run's output.
interface ExaWebsetCallback {
  status: "success" | "timeout_partial" | "rejected" | "dry_run" | "failed";
  run_id?: string;
  exa_webset_id?: string;
  webset_label?: string;
  tier?: string;
  requested?: number;
  returned?: number;
  new_count?: number;
  known_count?: number;
  credits_estimated?: number;
  credits_actual?: number;
  usd_actual?: number;
  rejected_reason?: string | null;
  discovered_websets_uri?: string;
}

export const exaWebsetIngest = task({
  id: "exa-webset-ingest",
  // The durable wait consumes no compute while suspended; match the worker's 60-min ceiling.
  maxDuration: 3600,
  // Credit-spending task: NO blind auto-retry. A retry reuses the same externalId and would
  // re-reserve credits / risk a duplicate webset. A failed run is terminal; the operator
  // re-triggers deliberately after inspecting ops.exa_webset_runs.
  retry: { maxAttempts: 1 },
  run: async (payload: ExaWebsetPayload, { ctx }) => {
    // ── 1) Validate + clamp (control-plane gate; mirrors the spec §7 JSON Schema) ──────
    const identifier = (payload?.webset_identifier ?? "").trim();
    if (!/^[a-z0-9_]{3,64}$/.test(identifier)) {
      throw new Error(`invalid webset_identifier ${JSON.stringify(identifier)} — expect ^[a-z0-9_]{3,64}$`);
    }
    const prompt = (payload?.search_prompt ?? "").trim();
    if (prompt.length < 8 || prompt.length > 5000) {
      throw new Error("search_prompt must be 8–5000 chars");
    }
    const entityType = payload?.entity_type ?? "company";
    if (!["company", "person"].includes(entityType)) {
      throw new Error(`invalid entity_type ${JSON.stringify(entityType)}`);
    }
    const tier = payload?.tier ?? "precision";
    if (!["precision", "harvest"].includes(tier)) {
      throw new Error(`invalid tier ${JSON.stringify(tier)}`);
    }
    const count = Math.max(1, Math.min(Math.trunc(payload?.max_results_limit ?? 100), HARD_RESULT_CAP));

    // Build the worker kwargs — keys MUST match ingest_exa_webset's parameter names exactly
    // (the dispatcher spreads them as keyword args). run_id == this run's id (→ externalId).
    const kwargs: Record<string, unknown> = {
      run_id: ctx.run.id,
      webset_identifier: identifier,
      search_prompt: prompt,
      max_results_limit: count,
      entity_type: entityType,
      tier,
      criteria: (payload?.criteria ?? []).slice(0, 10),
      seed_urls: (payload?.seed_urls ?? []).slice(0, 50),
      enrichments: payload?.enrichments ?? [], // contact formats are stripped worker-side (D2)
      exclude_known_domains: payload?.exclude_known_domains ?? true,
      max_credits: payload?.max_credits ?? 5000, // clamped to the 5000 per-run ceiling worker-side (D1)
      dry_run: payload?.dry_run ?? false,
    };

    logger.info("Exa webset ingest starting", { identifier, tier, count, dryRun: kwargs.dry_run });

    // ── 2) Durable callback token. token.url's callbackHash is the auth — no API key. ──
    const token = await wait.createToken({
      timeout: "1h",
      tags: ["exa-webset", identifier, tier, "modal-dispatch"],
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
        app_name: "exa-webset-pipelines",
        function_name: "ingest_exa_webset",
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
    const result = await wait.forToken<ExaWebsetCallback>(token.id);
    if (!result.ok) {
      // result.ok === false ONLY on token timeout (no callback arrived).
      throw new Error(`Exa webset ingest timed out before Modal callback (token ${token.id})`);
    }

    const out = result.output;
    // `failed` is a genuine error → surface it. `rejected` (guardrail), `dry_run` (estimate),
    // and `timeout_partial` (bounded poll exhausted, items persisted) are CLEAN terminals.
    if (out.status === "failed") {
      throw new Error(`Exa webset ingest failed in Modal: ${JSON.stringify(out)}`);
    }
    if (out.status === "rejected") {
      logger.warn("Exa webset ingest rejected by guardrail", { reason: out.rejected_reason, ...out });
    } else if (out.status === "timeout_partial") {
      logger.warn("Exa webset poll budget exhausted; partial items persisted", { ...out });
    } else {
      logger.info("Exa webset ingest complete", { ...out });
    }
    return out;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
