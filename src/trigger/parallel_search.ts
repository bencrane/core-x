import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — Parallel.ai WEB SEARCH (Directive 24, workflow 3 of 3).
 *
 * The gtm-agent's trigger for synchronous evidence-gathering: Parallel's Search API
 * (POST /v1/search) returns ranked excerpts in ONE call — no run lifecycle, no groups, no
 * basis, no poll (§0). The worker is synchronous, but this task keeps the SAME
 * trigger→dispatcher→Modal→callback shape as the other two so the gtm-mcp surface is uniform:
 *   1. validate the payload,
 *   2. mint a waitpoint token,
 *   3. POST the Universal Dispatcher targeting `parallel-search` / `web_search`,
 *   4. suspend on `wait.forToken` (the worker returns fast — one HTTP call),
 *   5. resume on the worker's flat callback; the ranked excerpts come back INLINE.
 *
 * `persist=true` also lands the excerpts to s3://data-sink/active/parallel_search/.
 */

interface ParallelSearchPayload {
  /** Optional persisted spec id; defaults worker-side to search_<run>. */
  specId?: string;
  /** Natural-language search objective (objective and/or searchQueries required). */
  objective?: string;
  /** Up to 5 explicit search queries. */
  searchQueries?: string[];
  /** Parallel search mode. Default "advanced". */
  mode?: "advanced" | "base";
  /** Optional source policy {include_domains?, exclude_domains?, after_date?}. */
  sourcePolicy?: Record<string, unknown>;
  /** Also land the excerpts to Lance (default false → inline-only). */
  persist?: boolean;
  /** Idempotency key. */
  idempotencyKey?: string;
}

// The flat body Modal POSTs to the waitpoint url — excerpts come back INLINE here.
interface ParallelSearchCallback {
  status: "success" | "rejected" | "failed";
  run_id?: string;
  workflow?: string;
  spec_id?: string;
  objective?: string;
  search_queries?: string[];
  excerpt_count?: number;
  persisted?: number;
  dataset_uri?: string | null;
  excerpts?: Array<Record<string, unknown>>;
  error?: string | null;
}

export const parallelSearch = task({
  id: "parallel-search",
  // Search is synchronous (one /v1/search call) — a short cap is plenty; the durable wait
  // still consumes no compute while suspended.
  maxDuration: 600,
  // A search call may be cheap, but it still bills — keep it deliberate (no blind retry).
  retry: { maxAttempts: 1 },
  run: async (payload: ParallelSearchPayload, { ctx }) => {
    // ── 1) Validate ────────────────────────────────────────────────────────────────────
    const objective = (payload?.objective ?? "").trim();
    const searchQueries = (payload?.searchQueries ?? [])
      .map((q) => String(q).trim())
      .filter((q) => q.length > 0)
      .slice(0, 5); // §0 — ≤5 queries
    if (!objective && searchQueries.length === 0) {
      throw new Error("objective or searchQueries is required");
    }
    const mode = payload?.mode ?? "advanced";
    const specId = (payload?.specId ?? "").trim();
    const idempotencyKey = payload?.idempotencyKey ?? `${specId || "search"}:${ctx.run.id}`;

    const kwargs: Record<string, unknown> = {
      run_id: ctx.run.id,
      spec_id: specId,
      objective,
      search_queries: searchQueries,
      mode,
      source_policy: payload?.sourcePolicy ?? null,
      persist: payload?.persist ?? false,
      run_kind: payload?.persist ? "full" : "test",
      idempotency_key: idempotencyKey,
    };

    logger.info("Parallel web search starting", { specId, mode, queryCount: searchQueries.length });

    // ── 2) Durable callback token ──────────────────────────────────────────────────────
    const token = await wait.createToken({
      timeout: "10m",
      idempotencyKey,
      tags: ["parallel-search", mode, "modal-dispatch"],
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
        app_name: "parallel-search",
        function_name: "web_search",
        kwargs,
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    // ── 4) Suspend. 5) Resolve — excerpts come back inline in result.output. ───────────
    const result = await wait.forToken<ParallelSearchCallback>(token.id);
    if (!result.ok) {
      throw new Error(`parallel web search timed out before Modal callback (token ${token.id})`);
    }
    const out = result.output;
    if (out.status === "failed") {
      throw new Error(`parallel web search failed in Modal: ${JSON.stringify({ ...out, excerpts: undefined })}`);
    }
    if (out.status === "rejected") {
      logger.warn("Parallel web search rejected", { error: out.error });
    } else {
      logger.info("Parallel web search complete", {
        excerpt_count: out.excerpt_count,
        persisted: out.persisted,
      });
    }
    return out;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
