# Proposals — Anvil e-sign + Resend

Per-client proposal/agreement flow: **hq-zone form → core-x → Anvil (PDF + e-sign) → Resend (delivery)**.

This is an on-demand **Trigger.dev v4 task** (the control plane), not a Modal gateway and
not a read-only `apps/` service. The Anvil + Resend keys live only here; hq-zone holds
just a Trigger key and calls `tasks.trigger("proposal-send", payload)`.

## Layout

| File | Role |
|---|---|
| [`types.ts`](types.ts) | `ProposalPayload` (the hq-zone form contract) + result/field types |
| [`render.ts`](render.ts) | payload → two HTML files (proposal body + fixed-geometry signature page) |
| [`anvil.ts`](anvil.ts) | `@anvilco/anvil` wrapper: key resolution, `createEtchPacket`, `generateEtchSignUrl`, download |
| [`email.ts`](email.ts) | Resend delivery of the proposal + signing link |
| [`send.ts`](send.ts) | `sendProposal()` — the pure orchestration both callers share |
| [`../trigger/proposal_send.ts`](../trigger/proposal_send.ts) | the Trigger.dev task wrapping `sendProposal` |
| [`../../scripts/proposal_smoke.ts`](../../scripts/proposal_smoke.ts) | local smoke runner (no Trigger server) |

## Test locally (dev key, free, no plan upgrade)

The repo is bound to Doppler `core-x/prd`, which holds `ANVIL_API_KEY_DEV` + `RESEND_API_KEY`.
With no `APP_ENV=production` / `ANVIL_API_KEY` set, every call runs in **test mode**
(`isTest:true`) — watermarked, non-quota, free on any Anvil plan.

```bash
# auth/health gate only
doppler run -- npx tsx scripts/proposal_smoke.ts --keycheck

# full path: create packet + mint sign URL + download the watermarked PDF to /tmp
doppler run -- npx tsx scripts/proposal_smoke.ts

# also send the email via Resend to yourself
doppler run -- npx tsx scripts/proposal_smoke.ts --to you@yourdomain.com
```

Trigger from a backend (what hq-zone does):

```ts
import { tasks } from "@trigger.dev/sdk";
await tasks.trigger("proposal-send", payload /* ProposalPayload */);
```

## Going to production

1. **Add the prod key**: put Anvil's production API key in Doppler `core-x/prd` as
   `ANVIL_API_KEY`, set `APP_ENV=production`. The resolver then uses the prod key with
   `isTest:false` (live, billed). `ANVIL_FORCE_TEST=1` overrides back to test if needed.
   Mirrors the `LOB_API_KEY`/`LOB_API_KEY_TEST` and `STRIPE_*_LIVE/_TEST` conventions.
2. **Plan**: production e-sign needs a paid Anvil tier; usage is metered (~$1.50 per Etch
   packet, $0.10 per PDF generate). The dev key only ever produces test docs.
3. **Deploy env**: the new keys are forwarded at deploy time via `syncEnvVars` in
   [`trigger.config.ts`](../../trigger.config.ts) — supply them with
   `doppler run -- npx trigger.dev deploy`.

## Two design notes that matter

**Embedded sign URLs expire in ≈2 hours.** The smoke runner emails the raw URL (fine for
immediate test). For production, email a link to an **hq-zone redirect route**
(`/proposals/:packetEid/sign`) that calls `generateEtchSignUrl` *fresh on click* and 302s
the client into the short-lived URL — the emailed link then never goes stale.

**Document source: HTML now, cast later.** `render.ts` generates the PDF from HTML
(`markup`), which needs no Anvil template and works on the dev plan today. Signature fields
are placed by absolute page-point rects on a dedicated signature page. For pixel-perfect
production placement, build an Anvil PDF template (cast) and swap `buildFiles()` in
`anvil.ts` to `files: [{ castEid }]` + `data.payloads` — the rest of the flow is unchanged.

## Phase 2 — signature completion (not wired)

Set a per-packet `webhookURL` (env `ANVIL_WEBHOOK_URL`) so Anvil's `etchPacketComplete`
fires when the client signs. The receiver verifies Anvil's `token` field (house
`*_WEBHOOK_SECRET` convention), then records state / triggers a downstream `proposal-signed`
task. Anvil webhook source IPs to allowlist: `35.233.165.3`, `34.148.239.131`.
