import { task, logger } from "@trigger.dev/sdk";
import { callHqx } from "./lib/hqx-client";

/**
 * equipment-outreach-push — select an audience segment and push it into Clay.
 *
 * One run = one campaign batch:
 *   1. POST catalyst_api /api/v1/equipment-audience/select with the segment
 *      (server-side anti-join against ops.equipment_outreach_pushes — a person
 *      already pushed to this campaign is never selected again).
 *   2. POST each selected person to the Clay webhook-source table (one JSON
 *      object per request — Clay's webhook contract), small bounded concurrency.
 *   3. callHqx /internal/equipment-outreach/pushes to record ONLY the rows Clay
 *      accepted (the ledger write; idempotent ON CONFLICT DO NOTHING).
 *
 * Delivery-before-ledger ordering means a crash between 2 and 3 can re-push a
 * row to Clay on retry — Clay dedupes on person_key (the table's key column),
 * so the failure mode is a no-op upsert there, never a lost suppression row.
 *
 * Env (Trigger.dev dashboard): CATALYST_API_BASE_URL, CATALYST_API_TOKEN,
 * plus the hqx-client pair (EDGE_API_BASE_URL, TRIGGER_SHARED_SECRET).
 * The Clay webhook URL rides the payload — each campaign's table has its own.
 */

interface Segment {
  tiers?: string[];
  macro_regions?: string[];
  demo_regions?: string[];
  title_classes?: string[];
  email_status?: string[];
  source_planes?: string[];
  max_people_at_domain?: number;
}

interface Payload {
  campaignId: string;
  clayWebhookUrl: string;
  segment: Segment;
  batchLabel?: string;
  limit?: number;
}

interface AudiencePerson {
  person_key: string;
  macro_region: string | null;
  [k: string]: unknown;
}

interface SelectResponse {
  people: AudiencePerson[];
  meta: Record<string, unknown>;
}

const CLAY_CONCURRENCY = 5;

const requireEnv = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must be set in the Trigger.dev dashboard.`);
  return value;
};

async function selectBatch(payload: Payload): Promise<SelectResponse> {
  const base = requireEnv("CATALYST_API_BASE_URL").replace(/\/$/, "");
  const token = requireEnv("CATALYST_API_TOKEN");
  const resp = await fetch(`${base}/api/v1/equipment-audience/select`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      ...payload.segment,
      campaign_id: payload.campaignId,
      exclude_pushed: true,
      limit: payload.limit ?? 500,
    }),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`select failed: HTTP ${resp.status} — ${text.slice(0, 500)}`);
  }
  return JSON.parse(text) as SelectResponse;
}

async function postToClay(url: string, person: AudiencePerson): Promise<boolean> {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(person),
  });
  if (!resp.ok) {
    logger.warn("clay webhook rejected row", {
      personKey: person.person_key,
      status: resp.status,
    });
    return false;
  }
  return true;
}

export const equipmentOutreachPush = task({
  id: "equipment-outreach-push",
  retry: { maxAttempts: 3 },
  run: async (payload: Payload) => {
    if (!payload.campaignId?.trim()) throw new Error("campaignId is required");
    if (!/^https:\/\//.test(payload.clayWebhookUrl ?? "")) {
      throw new Error("clayWebhookUrl must be an https URL");
    }

    const selected = await selectBatch(payload);
    const people = selected.people ?? [];
    logger.info("segment selected", { ...selected.meta, campaignId: payload.campaignId });
    if (people.length === 0) {
      return { selected: 0, delivered: 0, ledgered: 0, meta: selected.meta };
    }

    // bounded-concurrency delivery; collect only the accepted rows
    const delivered: AudiencePerson[] = [];
    for (let i = 0; i < people.length; i += CLAY_CONCURRENCY) {
      const chunk = people.slice(i, i + CLAY_CONCURRENCY);
      const results = await Promise.all(
        chunk.map((p) => postToClay(payload.clayWebhookUrl, p)),
      );
      results.forEach((ok, j) => {
        if (ok) delivered.push(chunk[j]);
      });
    }

    let ledgered = 0;
    if (delivered.length > 0) {
      const res = await callHqx<{ inserted: number }>(
        "/internal/equipment-outreach/pushes",
        {
          campaign_id: payload.campaignId,
          batch_label: payload.batchLabel ?? null,
          rows: delivered.map((p) => ({
            person_key: p.person_key,
            macro_region: p.macro_region ?? null,
          })),
        },
      );
      ledgered = res.inserted;
    }

    const rejected = people.length - delivered.length;
    if (rejected > 0) {
      logger.warn("clay rejected rows — NOT ledgered, re-selectable next run", { rejected });
    }
    return {
      selected: people.length,
      delivered: delivered.length,
      ledgered,
      rejected,
      meta: selected.meta,
    };
  },
});
