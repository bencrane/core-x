import { task, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — SAM.gov Entity Registrations historical backfill.
 *
 * A BOUNDED backfill, not a schedule (no cron). One dispatch to the Modal
 * `run_backfill` worker, which stages every deduped landing extract into a local
 * Lance dataset and publishes it to s3://data-sink/active/entity_registrations/.
 * Suspends on a durable waitpoint token so the run consumes no compute and is
 * immune to HTTP timeouts while Modal works (~30-45 min).
 *
 * Trigger manually (dashboard Test, or MCP). The worker enumerates + dedups the
 * landing keys itself — no payload required.
 */

interface BackfillCallback {
  status: "success" | "error";
  rows: number;
  files: number;
  feed: string;
}

export const entityRegistrationsBackfill = task({
  id: "entity-registrations-backfill",
  // The durable wait consumes no compute while suspended; the worker runs in Modal.
  maxDuration: 8100,
  run: async () => {
    const token = await wait.createToken({ timeout: "2h", tags: ["entity-reg-backfill"] });

    const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Modal-Key": requireEnv("MODAL_KEY"),
        "Modal-Secret": requireEnv("MODAL_SECRET"),
      },
      body: JSON.stringify({
        app_name: "sam-gov-entity-pipelines",
        function_name: "run_backfill",
        kwargs: {},
        trigger_callback_url: token.url,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`dispatcher ${res.status}: ${body.slice(0, 400)}`);
    }

    logger.info("Backfill dispatched to Modal; suspending on waitpoint", { tokenId: token.id });

    const out = await wait.forToken<BackfillCallback>(token.id);
    if (!out.ok) throw new Error("entity backfill timed out before Modal callback");
    if (out.output.status !== "success") {
      throw new Error(`entity backfill failed in Modal: ${JSON.stringify(out.output)}`);
    }

    logger.info("Entity-registration backfill complete", { ...out.output });
    return out.output;
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
