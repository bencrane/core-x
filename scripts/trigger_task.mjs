#!/usr/bin/env node
/**
 * Generic Trigger.dev task launcher — reads a payload JSON file and triggers a deployed task.
 * The @trigger.dev/sdk offloads large payloads; TRIGGER_SECRET_KEY (env-scoped, tr_prod_ → prod)
 * selects the environment.
 *
 * Usage: doppler run -p core-x -c prd -- node scripts/trigger_task.mjs <taskId> <payload.json> [tag ...]
 */
import { tasks } from "@trigger.dev/sdk";
import { readFileSync } from "node:fs";

const [taskId, path, ...tags] = process.argv.slice(2);
if (!taskId || !path) {
  console.error("usage: node scripts/trigger_task.mjs <taskId> <payload.json> [tag ...]");
  process.exit(1);
}
if (!process.env.TRIGGER_SECRET_KEY) {
  console.error("TRIGGER_SECRET_KEY not set (source via doppler)");
  process.exit(1);
}

const payload = JSON.parse(readFileSync(path, "utf8"));
const n = Array.isArray(payload.contacts) ? payload.contacts.length : "?";
console.log(`env=${process.env.TRIGGER_SECRET_KEY.slice(0, 8)} task=${taskId} contacts=${n} batchLabel=${payload.batchLabel ?? "-"}`);

const handle = await tasks.trigger(taskId, payload, { tags: tags.slice(0, 5) });
console.log("RUN_ID " + handle.id);
console.log("DASHBOARD https://cloud.trigger.dev/projects/v3/proj_pakdcffjbeiwcixcoepb/runs/" + handle.id);
