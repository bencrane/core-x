/**
 * Anvil e-sign client — the fleet's single egress to Anvil for proposals.
 *
 * Wraps @anvilco/anvil (official Node SDK) behind three calls the send flow needs:
 *   anvilKeyCheck()        — health gate (mirrors core/icypeas_gateway.py::key_check)
 *   createProposalPacket() — HTML files → createEtchPacket (embedded signer, no Anvil email)
 *   generateSignUrl()      — mint a (~2h) embedded signing URL for the client
 *   downloadProposalZip()  — pull the (watermarked, in test mode) PDFs for inspection
 *
 * KEY RESOLUTION. Dev/test uses ANVIL_API_KEY_DEV with isTest=true (watermarked,
 * non-quota — free on any plan). Production uses ANVIL_API_KEY with isTest=false,
 * gated on APP_ENV. This mirrors the LOB_API_KEY/LOB_API_KEY_TEST and
 * STRIPE_*_LIVE/_TEST conventions already in core-x/prd. Until the prod key exists
 * in Doppler, every call stays in test mode automatically.
 *
 * The dev key only ever produces test documents, so isTest tracks the key.
 */

import Anvil from "@anvilco/anvil";

import { PROPOSAL_BODY_FILE_ID, SIGNATURE_FILE_ID } from "./render";
import type { ProposalPayload, RenderedProposal, SendResult } from "./types";

export interface AnvilConfig {
  apiKey: string;
  isTest: boolean;
  mode: "dev" | "production";
  webhookUrl?: string;
}

export function resolveAnvilConfig(): AnvilConfig {
  const appEnv = (process.env.APP_ENV ?? "development").toLowerCase();
  const isProd = appEnv === "production" || appEnv === "prod";
  const prodKey = process.env.ANVIL_API_KEY;
  const devKey = process.env.ANVIL_API_KEY_DEV;

  const apiKey = isProd ? prodKey ?? devKey : devKey ?? prodKey;
  if (!apiKey) {
    throw new Error(
      "No Anvil API key — set ANVIL_API_KEY_DEV (dev) or ANVIL_API_KEY (prod) in the environment",
    );
  }

  // Test mode unless we are explicitly in production AND a real prod key is present.
  // ANVIL_FORCE_TEST=1 is an extra safety pin.
  const forceTest = process.env.ANVIL_FORCE_TEST === "1";
  const isTest = forceTest || !isProd || !prodKey;

  return {
    apiKey,
    isTest,
    mode: isProd && prodKey ? "production" : "dev",
    webhookUrl: process.env.ANVIL_WEBHOOK_URL,
  };
}

export function makeClient(cfg: AnvilConfig): Anvil {
  return new Anvil({ apiKey: cfg.apiKey });
}

/** GraphQL responses come back as { statusCode, data, errors }; data may be the
 * GraphQL envelope ({ data: {...} }) or already unwrapped depending on path.
 * Normalize to the inner data object and surface errors uniformly. */
function unwrap<T = any>(resp: any, op: string): T {
  const errors = resp?.errors ?? resp?.data?.errors;
  if (errors && (Array.isArray(errors) ? errors.length : true)) {
    const msg = Array.isArray(errors)
      ? errors.map((e: any) => e?.message ?? JSON.stringify(e)).join("; ")
      : JSON.stringify(errors);
    throw new Error(`Anvil ${op} failed: ${msg}`);
  }
  if (resp?.statusCode && resp.statusCode >= 400) {
    throw new Error(`Anvil ${op} HTTP ${resp.statusCode}: ${JSON.stringify(resp?.data).slice(0, 400)}`);
  }
  // data.data.<op> (raw envelope) → data.<op> (unwrapped) → data
  return (resp?.data?.data ?? resp?.data ?? resp) as T;
}

/** Health gate: confirm the key authenticates and report the org + mode. */
export async function anvilKeyCheck(cfg = resolveAnvilConfig()): Promise<{
  valid: boolean;
  mode: AnvilConfig["mode"];
  isTest: boolean;
  org?: { name: string; slug: string };
  error?: string;
}> {
  const client = makeClient(cfg);
  try {
    const resp = await client.requestGraphQL({
      query: `{ currentUser { name organizations { name slug } } }`,
      variables: {},
    });
    const data = unwrap<{ currentUser?: { organizations?: { name: string; slug: string }[] } }>(
      resp,
      "currentUser",
    );
    const org = data?.currentUser?.organizations?.[0];
    return { valid: Boolean(org), mode: cfg.mode, isTest: cfg.isTest, org };
  } catch (err) {
    return { valid: false, mode: cfg.mode, isTest: cfg.isTest, error: String(err) };
  }
}

/** Build the EtchFile[] (two merged files: body + signature page) from rendered HTML. */
function buildFiles(rendered: RenderedProposal) {
  return [
    {
      id: PROPOSAL_BODY_FILE_ID,
      title: rendered.packetName,
      filename: "proposal.pdf",
      markup: { html: rendered.body.html, css: rendered.body.css },
    },
    {
      id: SIGNATURE_FILE_ID,
      title: "Acceptance & signature",
      filename: "signature.pdf",
      markup: { html: rendered.signaturePage.html, css: rendered.signaturePage.css },
      fields: rendered.signaturePage.fields,
    },
  ];
}

export interface CreatedPacket {
  packetEid: string;
  documentGroupEid: string;
  signerEid: string;
  detailsURL?: string;
  raw: any;
}

/**
 * Create the e-sign packet: generate both PDFs from HTML, merge them, attach an
 * embedded signer with Anvil's own notification emails OFF (Resend owns delivery).
 */
export async function createProposalPacket(
  payload: ProposalPayload,
  rendered: RenderedProposal,
  cfg = resolveAnvilConfig(),
): Promise<CreatedPacket> {
  const client = makeClient(cfg);
  const webhookURL = payload.options?.webhookUrl ?? cfg.webhookUrl;

  const variables: Record<string, any> = {
    name: rendered.packetName,
    isDraft: false,
    isTest: cfg.isTest,
    mergePDFs: true,
    files: buildFiles(rendered),
    signers: [
      {
        id: "client",
        name: payload.client.name,
        email: payload.client.email,
        signerType: "embedded", // we mint the URL; Resend delivers it
        enableEmails: false, // suppress Anvil's signer notifications
        fields: rendered.signerFieldRefs,
      },
    ],
  };
  if (webhookURL) variables.webhookURL = webhookURL;

  const resp = await client.createEtchPacket({ variables });
  const data = unwrap<{ createEtchPacket?: any }>(resp, "createEtchPacket");
  const packet = data?.createEtchPacket ?? data;
  const signer = packet?.documentGroup?.signers?.[0];
  if (!packet?.eid || !signer?.eid) {
    throw new Error(`createEtchPacket returned no packet/signer eid: ${JSON.stringify(packet).slice(0, 400)}`);
  }
  return {
    packetEid: packet.eid,
    documentGroupEid: packet.documentGroup?.eid,
    signerEid: signer.eid,
    detailsURL: packet.detailsURL,
    raw: packet,
  };
}

/** Mint a short-lived (~2h) embedded signing URL for the client signer. */
export async function generateSignUrl(
  signerEid: string,
  clientUserId: string,
  cfg = resolveAnvilConfig(),
): Promise<string> {
  const client = makeClient(cfg);
  const resp: any = await client.generateEtchSignUrl({
    variables: { signerEid, clientUserId },
  });
  const errors = resp?.errors ?? resp?.data?.errors;
  if (errors) throw new Error(`Anvil generateEtchSignUrl failed: ${JSON.stringify(errors)}`);
  const url = resp?.url ?? resp?.data?.url ?? resp?.data?.data?.generateEtchSignUrl;
  if (!url) throw new Error(`generateEtchSignUrl returned no url: ${JSON.stringify(resp).slice(0, 300)}`);
  return url;
}

/** Download the packet's PDFs as a zip buffer (watermarked in test mode). */
export async function downloadProposalZip(
  documentGroupEid: string,
  cfg = resolveAnvilConfig(),
): Promise<Buffer> {
  const client = makeClient(cfg);
  const resp: any = await client.downloadDocuments(documentGroupEid, { dataType: "buffer" });
  const buf = resp?.data ?? resp?.response?.data ?? resp;
  if (!buf) throw new Error("downloadDocuments returned no data");
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

export type { SendResult };
