import { schedules, wait, logger } from "@trigger.dev/sdk";

/**
 * Control plane — USAspending FRESH caches fragment compaction (weekly).
 *
 * Each daily APPEND writes +1 small Lance fragment; optimize_indices does NOT compact data fragments.
 * This weekly task dispatches the Modal `run_compaction` worker per target through the Universal
 * Dispatcher; the worker no-ops unless get_fragments() > 8, else optimize_indices → compact_files
 * (Lance-default target; remaps BTREE in-place) → cleanup_old_versions, asserting the row count is
 * invariant. Sun 09:00 UTC reduces (does not guarantee) overlap with the daily append; a concurrent-
 * append commit conflict is handled worker-side as a non-fatal no-op.
 *
 * Phase A = prime only. Phase B (after the subaward Modal port lands) = uncomment the subaward target.
 */
interface CompactionCallback {
  status: "success" | "error";
  run_mode: "compaction";
  feed: string;
  compacted: boolean;
  reason?: string;
  fragments_before: number;
  fragments_after: number;
  table_rows_after?: number;
}

export const usaspendingFreshCompaction = schedules.task({
  id: "usaspending-fresh-compaction",
  cron: { pattern: "0 9 * * 0", timezone: "UTC" }, // Sun 09:00 UTC — off-peak vs the daily append
  maxDuration: 5400,
  run: async (_payload, { ctx }) => {
    const targets = [
      { app_name: "usaspending-api-fresh", feed: "prime" },
      // Phase B (after the subaward Modal port lands), add:
      // { app_name: "usaspending-api-subaward-fresh", feed: "subaward" },
    ];

    const out: CompactionCallback[] = [];
    for (const t of targets) {
      const token = await wait.createToken({
        timeout: "1h",
        tags: ["usaspending-fresh-compaction", t.feed],
      });
      const res = await fetch(requireEnv("MODAL_DISPATCHER_URL"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Modal-Key": requireEnv("MODAL_KEY"),
          "Modal-Secret": requireEnv("MODAL_SECRET"),
        },
        body: JSON.stringify({
          app_name: t.app_name,
          function_name: "run_compaction",
          kwargs: {},
          trigger_callback_url: token.url,
        }),
      });
      if (!res.ok) {
        throw new Error(`dispatcher ${res.status} (${t.feed}): ${(await res.text()).slice(0, 300)}`);
      }

      const r = await wait.forToken<CompactionCallback>(token.id);
      if (!r.ok) throw new Error(`${t.feed} compaction timed out (token ${token.id})`);
      if (r.output.status !== "success") {
        throw new Error(`${t.feed} compaction failed: ${JSON.stringify(r.output)}`);
      }
      logger.info(`${t.feed} compaction`, { ...r.output });
      out.push(r.output);
    }
    return { compactions: out };
  },
});

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}
