/**
 * Proposal renderer — ProposalPayload → Anvil-ready HTML/CSS files + field rects.
 *
 * Two files are produced and later merged (`mergePDFs: true`):
 *   1. proposalBody   — the variable-length proposal/agreement (no fields)
 *   2. signaturePage  — a fixed single page carrying the signature/date/name
 *      fields at known coordinates
 *
 * WHY TWO FILES. Anvil places signature fields by (pageNum, rect). On a
 * variable-length body the signature page number — and the content offset on it —
 * is unknowable ahead of time. Isolating the signature block on its own
 * fixed-geometry page makes `pageNum: 0` + absolute `pt` rects deterministic
 * regardless of how long the body runs. The signature page uses
 * `@page { size: Letter; margin: 0 }` + absolute positioning so the on-page rules
 * line up 1:1 with the field rects below (both are top-left-origin PDF points).
 *
 * Swapping to an Anvil PDF template (cast) later means replacing the file specs
 * in anvil.ts; this renderer and the geometry here are the HTML-generation path.
 */

import type {
  AnvilField,
  ProposalPayload,
  ProposalProvider,
  RenderedProposal,
} from "./types";

export const PROPOSAL_BODY_FILE_ID = "proposalBody";
export const SIGNATURE_FILE_ID = "signaturePage";

export const SIGNATURE_FIELDS = {
  signature: "clientSignature",
  date: "signDate",
  fullName: "clientFullName",
} as const;

// US Letter, PDF points. Signature-page field geometry (top-left origin).
const SIG_FIELDS: AnvilField[] = [
  { id: SIGNATURE_FIELDS.signature, type: "signature", pageNum: 0, rect: { x: 72, y: 300, width: 260, height: 34 } },
  { id: SIGNATURE_FIELDS.date, type: "signatureDate", pageNum: 0, rect: { x: 360, y: 300, width: 160, height: 34 } },
  { id: SIGNATURE_FIELDS.fullName, type: "fullName", pageNum: 0, rect: { x: 72, y: 392, width: 260, height: 28 } },
];

function esc(s: unknown): string {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** text → HTML paragraphs (blank-line separated), with single newlines as <br>. */
function paragraphs(text?: string): string {
  if (!text) return "";
  return text
    .split(/\n{2,}/)
    .map((p) => `<p>${esc(p).replace(/\n/g, "<br>")}</p>`)
    .join("\n");
}

function fmtMoney(amount: number, currency: string): string {
  try {
    return new Intl.NumberFormat("en-US", { style: "currency", currency }).format(amount);
  } catch {
    return `${currency} ${amount.toFixed(2)}`;
  }
}

function fmtDate(iso?: string): string {
  const d = iso ? new Date(iso) : new Date();
  if (Number.isNaN(d.getTime())) return esc(iso);
  return d.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
}

function lineTotal(item: ProposalPayload["proposal"]["lineItems"][number]): number {
  if (typeof item.amount === "number") return item.amount;
  const qty = item.quantity ?? 1;
  const unit = item.unitPrice ?? 0;
  return qty * unit;
}

export function resolveProvider(p?: Partial<ProposalProvider>): ProposalProvider {
  return {
    name: p?.name ?? process.env.PROPOSAL_PROVIDER_NAME ?? "",
    company: p?.company ?? process.env.PROPOSAL_PROVIDER_COMPANY ?? "Substrate",
    email: p?.email ?? process.env.PROPOSAL_PROVIDER_EMAIL ?? process.env.PROPOSAL_FROM_EMAIL ?? "",
    address: p?.address ?? process.env.PROPOSAL_PROVIDER_ADDRESS,
    website: p?.website ?? process.env.PROPOSAL_PROVIDER_WEBSITE,
  };
}

const BODY_CSS = `
@page { size: Letter; margin: 54pt; }
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; color: #1a1a1a; font-size: 10.5pt; line-height: 1.5; margin: 0; }
h1 { font-size: 20pt; margin: 0 0 2pt; letter-spacing: -0.4pt; }
h2 { font-size: 12pt; margin: 22pt 0 6pt; padding-bottom: 3pt; border-bottom: 1pt solid #e2e2e2; text-transform: uppercase; letter-spacing: 0.6pt; color: #555; }
p { margin: 0 0 8pt; }
.muted { color: #777; }
.header { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2pt solid #111; padding-bottom: 10pt; }
.brand { font-size: 13pt; font-weight: 700; }
.meta { text-align: right; font-size: 9pt; color: #555; line-height: 1.6; }
.parties { display: flex; justify-content: space-between; gap: 24pt; margin-top: 16pt; }
.party { font-size: 9.5pt; }
.party .label { text-transform: uppercase; letter-spacing: 0.6pt; color: #888; font-size: 8pt; margin-bottom: 3pt; }
.party .name { font-weight: 600; }
ul.scope { margin: 0; padding-left: 16pt; }
ul.scope li { margin: 0 0 4pt; }
table.pricing { width: 100%; border-collapse: collapse; margin-top: 4pt; font-size: 10pt; }
table.pricing th { text-align: left; border-bottom: 1.5pt solid #111; padding: 6pt 8pt; font-size: 8.5pt; text-transform: uppercase; letter-spacing: 0.5pt; color: #555; }
table.pricing th.num, table.pricing td.num { text-align: right; }
table.pricing td { padding: 7pt 8pt; border-bottom: 1pt solid #ececec; vertical-align: top; }
table.pricing tr.total td { border-bottom: none; border-top: 2pt solid #111; font-weight: 700; font-size: 11pt; padding-top: 9pt; }
.notes { font-size: 9pt; color: #555; margin-top: 8pt; }
`;

function renderBodyHtml(payload: ProposalPayload, provider: ProposalProvider): string {
  const { client, proposal } = payload;
  const currency = proposal.currency ?? "USD";
  const rows = proposal.lineItems
    .map((it) => {
      const qty = it.quantity ?? 1;
      const unit = it.unitPrice;
      return `<tr>
        <td>${esc(it.description)}</td>
        <td class="num">${qty}</td>
        <td class="num">${unit != null ? fmtMoney(unit, currency) : "&mdash;"}</td>
        <td class="num">${fmtMoney(lineTotal(it), currency)}</td>
      </tr>`;
    })
    .join("\n");
  const total = proposal.lineItems.reduce((s, it) => s + lineTotal(it), 0);

  const metaRows = [
    proposal.number ? `Proposal&nbsp;#: <strong>${esc(proposal.number)}</strong>` : "",
    `Date: ${fmtDate(proposal.dateISO)}`,
    proposal.validUntilISO ? `Valid&nbsp;until: ${fmtDate(proposal.validUntilISO)}` : "",
  ]
    .filter(Boolean)
    .join("<br>");

  return `<!doctype html><html><body>
  <div class="header">
    <div>
      <div class="brand">${esc(provider.company)}</div>
      ${provider.website ? `<div class="muted" style="font-size:9pt">${esc(provider.website)}</div>` : ""}
    </div>
    <div class="meta">${metaRows}</div>
  </div>

  <h1>${esc(proposal.title)}</h1>

  <div class="parties">
    <div class="party">
      <div class="label">Prepared for</div>
      <div class="name">${esc(client.name)}</div>
      ${client.title ? `<div>${esc(client.title)}</div>` : ""}
      ${client.company ? `<div>${esc(client.company)}</div>` : ""}
      ${client.address ? `<div class="muted">${esc(client.address).replace(/\n/g, "<br>")}</div>` : ""}
      <div class="muted">${esc(client.email)}</div>
    </div>
    <div class="party" style="text-align:right">
      <div class="label">Prepared by</div>
      ${provider.name ? `<div class="name">${esc(provider.name)}</div>` : ""}
      <div>${esc(provider.company)}</div>
      ${provider.address ? `<div class="muted">${esc(provider.address).replace(/\n/g, "<br>")}</div>` : ""}
      ${provider.email ? `<div class="muted">${esc(provider.email)}</div>` : ""}
    </div>
  </div>

  ${proposal.intro ? `<h2>Overview</h2>${paragraphs(proposal.intro)}` : ""}

  ${
    proposal.scopeItems && proposal.scopeItems.length
      ? `<h2>Scope of work</h2><ul class="scope">${proposal.scopeItems
          .map((s) => `<li>${esc(s)}</li>`)
          .join("")}</ul>`
      : ""
  }

  <h2>Pricing</h2>
  <table class="pricing">
    <thead><tr><th>Description</th><th class="num">Qty</th><th class="num">Unit</th><th class="num">Amount</th></tr></thead>
    <tbody>
      ${rows}
      <tr class="total"><td colspan="3">Total</td><td class="num">${fmtMoney(total, currency)}</td></tr>
    </tbody>
  </table>
  ${proposal.notes ? `<div class="notes">${paragraphs(proposal.notes)}</div>` : ""}

  ${proposal.terms ? `<h2>Terms &amp; conditions</h2>${paragraphs(proposal.terms)}` : ""}
  </body></html>`;
}

const SIG_CSS = `
@page { size: Letter; margin: 0; }
html, body { margin: 0; padding: 0; }
/* height < 792pt + overflow:hidden keeps the signature page to ONE page (no
   trailing blank). @page forces the page box to full Letter, so the absolute
   field rects below still map 1:1 to PDF points. */
body { font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; color: #1a1a1a; position: relative; width: 612pt; height: 640pt; overflow: hidden; page-break-after: avoid; page-break-inside: avoid; }
.abs { position: absolute; }
.heading { left: 72pt; top: 72pt; font-size: 16pt; font-weight: 700; letter-spacing: -0.3pt; }
.ack { left: 72pt; top: 108pt; width: 468pt; font-size: 10pt; line-height: 1.55; color: #333; }
.label { font-size: 8pt; text-transform: uppercase; letter-spacing: 0.6pt; color: #888; white-space: nowrap; }
.line { border-bottom: 1pt solid #111; }
`;

function renderSignatureHtml(payload: ProposalPayload, provider: ProposalProvider): string {
  const { client, proposal } = payload;
  const ack = `By signing below, ${esc(client.name)}${
    client.company ? ` on behalf of ${esc(client.company)}` : ""
  } agrees to the terms set out in this ${esc(proposal.title)} prepared by ${esc(
    provider.company,
  )}.`;
  // Absolute positions mirror SIG_FIELDS rects (labels above, ruled line below).
  return `<!doctype html><html><body>
  <div class="abs heading">Acceptance &amp; signature</div>
  <div class="abs ack">${ack}</div>

  <div class="abs label" style="left:72pt; top:286pt;">Authorized signature</div>
  <div class="abs line" style="left:72pt; top:336pt; width:260pt;"></div>

  <div class="abs label" style="left:360pt; top:286pt;">Date</div>
  <div class="abs line" style="left:360pt; top:336pt; width:160pt;"></div>

  <div class="abs label" style="left:72pt; top:378pt;">Printed name</div>
  <div class="abs line" style="left:72pt; top:424pt; width:260pt;"></div>

  <div class="abs" style="left:72pt; top:440pt; font-size:8pt; color:#aaa;">${esc(
    provider.company,
  )} &middot; ${esc(client.email)}</div>
  </body></html>`;
}

export function renderProposal(payload: ProposalPayload): RenderedProposal {
  const provider = resolveProvider(payload.provider);
  const packetName = `${payload.proposal.title} — ${
    payload.client.company || payload.client.name
  }`.slice(0, 200);

  return {
    packetName,
    body: { html: renderBodyHtml(payload, provider), css: BODY_CSS },
    signaturePage: {
      html: renderSignatureHtml(payload, provider),
      css: SIG_CSS,
      fields: SIG_FIELDS,
    },
    signerFieldRefs: [
      { fileId: SIGNATURE_FILE_ID, fieldId: SIGNATURE_FIELDS.signature },
      { fileId: SIGNATURE_FILE_ID, fieldId: SIGNATURE_FIELDS.date },
      { fileId: SIGNATURE_FILE_ID, fieldId: SIGNATURE_FIELDS.fullName },
    ],
  };
}
