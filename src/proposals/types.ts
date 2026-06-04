/**
 * Proposal-send domain types — the contract between hq-zone (the form), the
 * renderer, the Anvil e-sign client, and the Resend mailer.
 *
 * This is the payload hq-zone POSTs (via `tasks.trigger("proposal-send", …)`)
 * and the standalone smoke runner feeds in. Keep it provider-agnostic: nothing
 * here references a specific business line or Anvil template — the renderer turns
 * it into a PDF, so swapping the document later (HTML → Anvil cast) never touches
 * this shape.
 */

/** A single priced row in the proposal pricing table. */
export interface LineItem {
  description: string;
  /** Defaults to 1 when omitted. */
  quantity?: number;
  /** Per-unit price in major currency units (e.g. dollars). */
  unitPrice?: number;
  /** Explicit line total; when omitted it is computed as quantity * unitPrice. */
  amount?: number;
}

/** The recipient who signs the proposal. */
export interface ProposalClient {
  name: string;
  email: string;
  company?: string;
  title?: string;
  /** Free-form address block, newline-separated; rendered verbatim. */
  address?: string;
}

/** The sending party (your side). Defaults are filled from env when omitted. */
export interface ProposalProvider {
  name: string;
  company: string;
  email: string;
  address?: string;
  website?: string;
}

/** The document body content. */
export interface ProposalBody {
  /** Headline, e.g. "Direct Mail Campaign — Proposal & Agreement". */
  title: string;
  /** Human reference number, e.g. "PRO-2026-0042". Optional. */
  number?: string;
  /** ISO date the proposal is issued. Defaults to today at render time. */
  dateISO?: string;
  /** ISO date the offer expires. Optional. */
  validUntilISO?: string;
  /** Opening paragraph(s). Plain text; newlines become paragraph breaks. */
  intro?: string;
  /** Bulleted scope-of-work items. */
  scopeItems?: string[];
  /** Priced line items; total is computed from these. */
  lineItems: LineItem[];
  /** ISO-4217 code for display only. Defaults to "USD". */
  currency?: string;
  /** Optional notes shown under the pricing table. */
  notes?: string;
  /** Terms & conditions, rendered as paragraphs above the signature block. */
  terms?: string;
}

/** The full request for one proposal send. */
export interface ProposalPayload {
  client: ProposalClient;
  proposal: ProposalBody;
  provider?: Partial<ProposalProvider>;
  options?: {
    /** Override the per-packet webhook URL (defaults to ANVIL_WEBHOOK_URL). */
    webhookUrl?: string;
    /** Skip the Resend email step (Anvil packet + sign URL still produced). */
    skipEmail?: boolean;
    /** Override the email recipient (defaults to client.email) — useful in test. */
    emailToOverride?: string;
    /** Stable id Anvil ties the embedded sign session to. Defaults to client email. */
    clientUserId?: string;
  };
}

// ── Anvil field geometry (PDF points, top-left origin, US Letter 612×792) ─────
export type AnvilFieldType =
  | "signature"
  | "signatureDate"
  | "initial"
  | "fullName"
  | "email";

export interface AnvilFieldRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface AnvilField {
  id: string;
  type: AnvilFieldType;
  pageNum: number;
  rect: AnvilFieldRect;
}

/** Output of the renderer — everything Anvil needs to build the packet files. */
export interface RenderedProposal {
  packetName: string;
  body: { html: string; css: string };
  signaturePage: { html: string; css: string; fields: AnvilField[] };
  /** Which rendered fields the client signer is assigned. */
  signerFieldRefs: { fileId: string; fieldId: string }[];
}

/** Terminal result of a send. */
export interface SendResult {
  ok: boolean;
  mode: "dev" | "production";
  isTest: boolean;
  packetEid?: string;
  documentGroupEid?: string;
  signerEid?: string;
  /** Short-lived (≈2h) embedded signing URL. */
  signUrl?: string;
  detailsURL?: string;
  email?: { sent: boolean; id?: string; to?: string; error?: string };
  error?: string;
}
