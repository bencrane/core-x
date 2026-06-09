import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending API FRESH (prime contract+IDV) daily APPEND top-up.
 *
 * Mints a Trigger.dev v4 waitpoint, POSTs the Universal Dispatcher to spawn the
 * `run_daily` Modal worker (append-only; NEVER overwrites the accumulating table),
 * suspends on `wait.forToken` (checkpointed, zero compute), and resumes on the
 * worker's flat terminal callback. Manually triggerable with `{ days }`; default 7.
 *
 * The prime `bulk_download/awards` pull runs on Modal precisely so each retry lands
 * on a fresh egress IP — the documented cure for USAspending's F5/IP throttle
 * (DIRECTIVE_33). No new endpoint or secret: the worker is resolved by name through
 * the single Universal Dispatcher (MODAL_DISPATCHER_URL + MODAL_KEY/MODAL_SECRET).
 */
interface FreshDailyCallback {
  status: "success" | "error";
  rows: number;
  feed: string;
  dataset_uri?: string;
  window_start?: string;
  window_end?: string;
  table_rows_after?: number;
  run_mode?: string;
}

export const usaspendingApiFreshDaily = task({
  id: "usaspending-api-fresh-daily",
  maxDuration: 3900, // suspended wait consumes no compute; token timeout bounds the window
  run: async (payload: { days?: number }, { ctx }) => {
    const days = payload?.days ?? 7;

    const token = await wait.createToken({
      timeout: "1h", // prime_awards backend is fast; a 10-day window finishes in minutes
      tags: ["usaspending-api-fresh", "modal-dispatch"],
    });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "usaspending-api-fresh",
        function_name: "run_daily",
        kwargs: { days },
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 500)}`);
    }

    logger.info("Dispatched usaspending_api_fresh daily → Modal; suspending on waitpoint", {
      tokenId: token.id,
      days,
      triggerRunId: ctx.run.id,
    });

    const result = await wait.forToken<FreshDailyCallback>(token.id);
    if (!result.ok) {
      throw new Error(
        `usaspending_api_fresh daily timed out before Modal callback (token ${token.id})`,
      );
    }
    if (result.output.status !== "success") {
      throw new Error(
        `usaspending_api_fresh daily failed in Modal: ${JSON.stringify(result.output)}`,
      );
    }

    logger.info("usaspending_api_fresh daily append complete", { ...result.output });
    return result.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
