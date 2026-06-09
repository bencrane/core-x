# Proposal Experience — Native React Presentation + Documenso for Signature

**Implementation spec / handoff.** Self-contained. Target: a web app (Next.js App Router
assumed; adapt as needed) where a prospect opens a link, reads a beautiful, web-native
proposal rendered in React, and signs it — with the legally-binding signature handled by
Documenso, **not** rebuilt in-house.

**Provenance of facts.** Every load-bearing claim is tagged:

- **[VERIFIED — OpenAPI v1/v2]** — confirmed against the live OpenAPI schemas fetched from
  `https://app.documenso.com/api/v1/openapi.json` and `/api/v2/openapi.json` (authenticated),
  and/or by issuing the live request against `app.documenso.com`.
- **[VERIFIED — docs]** — confirmed against `docs.documenso.com` or `documenso.com/pricing`
  with the source URL quoted inline.
- **[VERIFIED — SDK]** — confirmed against the installed `@documenso/embed-react` `.d.ts` files.
- **[INFERENCE]** / **[RECOMMENDATION]** — design guidance, not a Documenso contract.

Documenso Cloud is `https://app.documenso.com`. Self-host (AGPLv3) swaps the base URL.

---

## 0. The one fact that drives the whole architecture

**On Documenso Cloud there is NO public API to submit a signature, set a SIGNATURE field's
value, or otherwise "mark a recipient signed" from your own UI. The signature ceremony must
happen on a Documenso-rendered surface** (the embed iframe, the hosted `/sign/<token>` page,
or a direct-template link).

Proof, from the live schemas:

- **[VERIFIED — OpenAPI v1/v2]** The field-create and field-update request bodies
  (`POST /api/v1/documents/{id}/fields`, `PATCH /api/v1/documents/{id}/fields/{fieldId}`,
  `POST /api/v2/document/field/create`, `POST /api/v2/document/field/update`) accept only
  **placement + metadata**: `recipientId`, `type` (enum incl. `SIGNATURE`), `pageNumber`,
  `pageX/Y`, `pageWidth/Height`, and a `fieldMeta` object. The only writable `value` props
  inside `fieldMeta` belong to **TEXT / NUMBER / RADIO / CHECKBOX / DROPDOWN** fields (default
  text / option values). **There is no writable `signature`, `signatureImage`, `signedAt`, or
  `inserted` property anywhere in any request body.**
- **[VERIFIED — OpenAPI v2]** `POST /api/v2/document/recipient/update` accepts only
  `email, name, role, signingOrder, accessAuth, actionAuth` (+ ids). It has **no
  `signingStatus` and no `signedAt`** — you cannot flip a recipient to "signed" via the API.
- **[VERIFIED — OpenAPI v2]** The field/recipient **response** schemas DO contain `signature`,
  `inserted`, `customText`, `signedAt`, `signingStatus`, `signingUrl`, `token` — these are
  **read-only outputs**, surfaced only after the signer acts on a Documenso surface.
- **[VERIFIED — OpenAPI v1/v2]** Enumerating all ~13 v1 paths and all 85 v2 paths: the only
  signing-adjacent action endpoints are `POST /api/v2/embedding/create-presign-token` and
  `/verify-presign-token` (they mint/verify an *embedding session*, not a signature). **No
  `/sign`, `/complete`, `/recipient/{id}/sign`, or equivalent completion endpoint exists.**

**Verdict, stated precisely:**

| Capability | Cloud | How |
|---|---|---|
| **Theme / white-label** Documenso's signing surface (custom `css`, `cssVars`, dark-mode, drop branding) | **Yes** | **Platform plan** ($250/mo) — see §11 |
| **Replace the signing surface entirely** with your own UI (collect the signature in your DOM and POST it) | **No** | Not possible on Cloud at any plan |
| **Fully own / rebuild the signing UI** | Only by **self-hosting** | Documenso is AGPLv3; run your own instance |

This is by design: a public "here is a signature PNG, mark it signed" endpoint would let anyone
forge a completion and destroy the legal weight (ESIGN/UETA: identity, intent, consent,
timestamp, IP, tamper-evident seal + certificate) that is the entire value of an e-sign
provider. So: **you render the *reading* experience in React; Documenso renders the *signing*
action.** You theme that action to match your brand (Platform plan), but it is always
Documenso's surface.

---

## 1. Core principle

**Separate presentation from signature, and drive both from a single structured data source.**

- The proposal **content** is structured data (`{ contact, lineItems[], total, notes }`) —
  NOT a PDF. You render it as React for the web experience.
- The **legal signature** is owned by Documenso. You never build a signature pad, never store a
  signature image, never generate an audit trail. Documenso captures the signature, seals the
  document, and issues the signing certificate.
- The document Documenso signs is a **PDF generated from the same structured data**, so what the
  prospect reads (React) matches what they legally sign (PDF). One source of truth → two
  renderings.

Why the signature can't live in your UI: see §0. Capturing a PNG in your own pad and storing it
gives you a picture with no legal standing, and there is no API to register it with Documenso.

---

## 2. Components & responsibilities

| Layer | You build | Documenso owns |
|---|---|---|
| Proposal content | Structured data model + React renderer (the web-native proposal) | — |
| PDF | Generate a PDF from the same data (the legal document) | Stores / serves / seals it |
| Signature capture | Trigger it (embed or redirect) + listen for completion | Signature pad, capture, **sealing**, **certificate**, audit trail |
| State | Track proposal status (draft→sent→opened→signed→completed) from webhooks | Emits webhook events |
| Signed artifact | Pull + store the final signed PDF | Generates the sealed PDF + cert |

---

## 3. Canonical data model

```ts
// One source of truth. Renders to React (web) AND to PDF (legal doc).
interface Proposal {
  id: string;                       // your deal/proposal id (the URL slug)
  contact: {
    name: string;
    company: string;
    email: string;                  // becomes the Documenso recipient
    date: string;                   // ISO or display string
  };
  brand?: string;                   // wordmark / theme label
  lineItems: Array<{
    title: string;
    description: string;
    price: number;                  // store minor units (cents); display in USD
  }>;
  total: number;
  notes?: string;

  // Documenso linkage (populated when the doc is created):
  documensoDocumentId?: number;     // numeric document id (v1) — v2 also returns an envelopeId
  documensoEnvelopeId?: string;     // v2 envelope id (string, e.g. "envelope_…") if using v2
  signingToken?: string;            // recipient token — drives the embed / hosted URL
  status: ProposalStatus;           // see §8
  generatedAt?: string;
  signedPdfUrl?: string;            // your stored copy of the sealed PDF
}

type ProposalStatus =
  | 'draft' | 'sent' | 'opened' | 'signed' | 'completed' | 'rejected' | 'voided';
```

The frontend page only strictly needs `contact`, `lineItems`, `total`, `notes`,
`signingToken`, and `status`. Everything else is backend bookkeeping.

---

## 4. End-to-end flow

1. **Create proposal** (internal/admin action) → persist structured `Proposal` (status `draft`).
2. **Generate the legal PDF** from the proposal data (HTML→PDF; see §5.2).
3. **Create the Documenso document** from that PDF, with one recipient (the prospect) as a
   `SIGNER`, a `SIGNATURE` field placed, and **email delivery suppressed** (`sendEmail: false`
   — you're embedding, not letting Documenso email them). Capture the document id + recipient
   `token`. Status → `sent`.
4. **Send the prospect your link**: `https://yourapp.com/proposal/<id>` (your link, not
   Documenso's).
5. **Prospect opens the React page** → backend returns the structured proposal + `signingToken`.
   Page renders the proposal natively (dark, branded, web — no white PDF).
6. **Prospect clicks "Review & sign"** → the page mounts Documenso's `EmbedSignDocument` with
   the token (or redirects to `{host}/sign/<token>`). Documenso shows the PDF + captures the
   signature.
7. **Documenso fires webhooks** (`DOCUMENT_OPENED`, `DOCUMENT_SIGNED`, `DOCUMENT_COMPLETED`,
   …). Your webhook handler verifies + advances status.
8. **On completion** → pull the sealed signed PDF from Documenso, store its URL, mark
   `completed`, trigger downstream (Stripe checkout, CRM update, etc.).

---

## 5. Backend spec

All Documenso REST calls are **server-side only** — the API key never touches the browser.

### 5.1 Proposal data API
`GET /api/proposal/:id` → returns the structured `Proposal` (the frontend's data source):

```jsonc
{
  "id": "deal_123",
  "brand": "Revenue Activation",
  "contact": { "name": "Sarah Johnson", "company": "Acme Corporation",
               "email": "sarah@acme.io", "date": "2026-02-09" },
  "lineItems": [
    { "title": "Sales Pipeline Optimization", "price": 750000,
      "description": "Comprehensive audit and optimization..." },
    { "title": "Outbound Campaign Setup", "price": 500000,
      "description": "Design and launch of targeted outbound..." }
  ],
  "total": 1250000,
  "notes": "Q1 2026 consulting engagement.",
  "signingToken": "phn-xxxxxxxxxxxxx",
  "status": "sent"
}
```
Return `signingToken` only to the intended recipient context. The token is a bearer credential
for signing — treat the proposal URL as sensitive (unguessable id, optionally an auth gate).

### 5.2 PDF generation (the legal document)
Render the **same** proposal data to HTML, then to PDF. Options: DocRaptor (Prince),
Puppeteer/Playwright, or `@react-pdf/renderer`. The PDF need not match the React styling
pixel-for-pixel, but its **content** must (line items, prices, total, terms). This PDF is the
artifact Documenso signs.

### 5.3 Create the Documenso document

Both v1 and v2 are live on Documenso Cloud (see §10). **v1 is functional but officially
deprecated**; **v2 is current**. Two viable creation strategies:

**(A) Generate-PDF-then-upload** — best when line items / lengths vary per deal.

*v1 path* — **[VERIFIED — OpenAPI v1]**:
1. `POST /api/v1/documents` with
   `{ title, recipients: [{ name, email, role: "SIGNER" }] }`.
   → **Response: `{ uploadUrl, documentId, externalId, recipients: [{ recipientId, name,
   email, token, role, signingOrder, signingUrl }] }`.** The `uploadUrl` is a presigned S3
   PUT URL; the recipient `token` (and a ready-made `signingUrl`) are returned right here.
2. `PUT <uploadUrl>` the PDF bytes (`Content-Type: application/pdf`).
3. `POST /api/v1/documents/{id}/fields` → place the `SIGNATURE` field for the recipient:
   `{ recipientId, type: "SIGNATURE", pageNumber, pageX, pageY, pageWidth, pageHeight }`.
   (`type` enum: `SIGNATURE, FREE_SIGNATURE, INITIALS, NAME, EMAIL, DATE, TEXT, NUMBER, RADIO,
   CHECKBOX, DROPDOWN`.)
4. `POST /api/v1/documents/{id}/send` with `{ sendEmail: false }` — **[VERIFIED — OpenAPI v1]**
   the body's `sendEmail` description reads: *"Whether to send an email to the recipients asking
   them to action the document. If you disable this, you will need to manually distribute the
   document to the recipients using the generated signing links."* (Also accepts
   `sendCompletionEmails`.) This is exactly what you want — you distribute via your own
   proposal URL.

*v2 path* — **[VERIFIED — OpenAPI v2]** (current; singular resources):
1. `POST /api/v2/document/create` — **`multipart/form-data`** with two parts: `payload` (JSON
   metadata incl. `title`, `recipients`) and `file` (the PDF). One request, no separate
   presigned-URL round-trip. → Response: `{ id, envelopeId }`.
   *Alternative:* `POST /api/v2/document/create/beta` — **JSON** body
   (`{ title, recipients, meta, visibility, … }`) that returns `{ document, uploadUrl }` (the
   v1-style two-step presigned upload), if you prefer to PUT the bytes separately.
2. `POST /api/v2/document/field/create` (or `…/field/create-many`) →
   `{ documentId, field: { recipientId, type: "SIGNATURE", page, positionX, positionY, … } }`.
3. `POST /api/v2/document/distribute` with `{ documentId, meta }` — this is v2's "send".
   Suppress emailed signing links via `meta` and distribute through your own URL.

**(B) Documenso Template + prefill** — best when the proposal layout is fixed and only data
varies. Author a template once in Documenso's UI (place the `SIGNATURE` field on the role), then
per-deal instantiate it:
- *v1* — **[VERIFIED — OpenAPI v1]** `POST /api/v1/templates/{templateId}/create-document` or
  `…/generate-document`.
- *v2* — **[VERIFIED — OpenAPI v2]** `POST /api/v2/template/use` (or `/api/v2/envelope/use`).

> **There is no plan gate that hides v2.** v2 is fully available on this Cloud account (85
> paths, verified live). The earlier belief that "v2 returns 404" came from calling
> **`/api/v2/documents` (plural), which does not exist** — v2 uses **singular** resources
> (`/document`, `/template`, `/envelope`) with action suffixes (`/create`, `/distribute`,
> `/get-many`). Choose v1 vs v2 on ergonomics, not availability; prefer **v2** for new work
> since v1 is deprecated.

### 5.4 Token storage
Persist the document id (+ `envelopeId` if v2) and `signingToken` on the proposal row. The token
is stable for a recipient until the document is completed / voided.

### 5.5 Webhook handler
`POST /api/webhooks/documenso` — Documenso calls this on signing events.

**[VERIFIED — docs]** Events (configurable per webhook), per
`docs.documenso.com/developers/webhooks`: document **created, sent, opened, signed, completed,
rejected, cancelled** (plus template **created, updated, deleted, used**). The example payload
shows the `event` field as **all-caps-underscore**, e.g. `"event": "DOCUMENT_COMPLETED"` — i.e.
`DOCUMENT_CREATED`, `DOCUMENT_SENT`, `DOCUMENT_OPENED`, `DOCUMENT_SIGNED`, `DOCUMENT_COMPLETED`,
`DOCUMENT_REJECTED`, `DOCUMENT_CANCELLED`.

Payload shape (representative):
`{ event, payload: { id, externalId, status, recipients: [{ email, role, token, signingStatus,
signedAt }], … }, createdAt }`.

**Verification — [VERIFIED — docs]** (`docs.documenso.com/developers/webhooks/verification`):
Documenso sends the configured secret **verbatim** in the **`X-Documenso-Secret`** header. It is
a **shared secret, NOT an HMAC signature.** The docs state: *"Always use constant-time string
comparison to prevent timing attacks"* and *"Standard equality operators (`===` or `==`) can leak
information about the secret through response time variations"* — recommending Node
`crypto.timingSafeEqual()` / Python `hmac.compare_digest()`. Compare the header against your
`DOCUMENSO_WEBHOOK_SECRET`; reject with 401 on mismatch.

**Idempotency:** key off `document id + event`; signing events can be delivered more than once.
Make status transitions monotonic (don't regress `completed` → `signed`).

**On `DOCUMENT_COMPLETED`:** mark `completed`, pull the signed PDF (§5.6), fire downstream effects.

### 5.6 Read document / retrieve signed PDF
- **[VERIFIED — OpenAPI v1 + live]** `GET /api/v1/documents/{id}` → top-level
  `{ id, externalId, userId, teamId, title, status, createdAt, completedAt, recipients[],
  fields[] }`. The **live** recipient objects include `token, signedAt, readStatus,
  signingStatus, sendStatus, signingUrl` (the published OpenAPI schema under-specifies the
  recipient item, but the live response carries these fields). Use this to (re)read the recipient
  `token` and status. The canonical first source of the token is still the **create** response
  (§5.3).
- **[VERIFIED — OpenAPI v1]** `GET /api/v1/documents/{id}/download` → **returns JSON
  `{ downloadUrl }`, not the raw PDF bytes.** Then GET that `downloadUrl` to fetch the sealed,
  signed PDF. Accepts a `downloadOriginalDocument` query param (true = original upload, false/
  omitted = signed/sealed version). Store your own copy.
- **[VERIFIED — OpenAPI v2]** `GET /api/v2/document/{documentId}/download` (and
  `…/download-beta`, which returns `{ downloadUrl, filename, contentType }`); `version` query
  param selects original vs signed.

---

## 6. Frontend spec

### 6.1 Native React proposal renderer
A data-driven component that renders the `Proposal` as web-native UI (your design system — dark
theme, branded). No iframe, no PDF for the reading experience. Sections: brand wordmark, title +
"Prepared for", contact meta, scope-of-work line-item cards (title / description / price), total
banner, notes, and a "Review & sign" CTA.

> Tip: if your CSS pipeline is fragile, inline styles guarantee the look renders regardless of
> build config. Otherwise use your design system.

### 6.2 Signature step — `@documenso/embed-react`

**[VERIFIED — SDK]** Installed versions observed: **`0.6.1`** (current; also the version pinned by
the official embed demo and the latest on npm) and an older **`0.4.0`**. **Use `^0.6.1`.** The
two differ in the `EmbedSignDocument` prop surface (0.6.1 adds `language`, `email`, `lockEmail`)
and in the exported V2 envelope-editor components.

**[VERIFIED — SDK] `EmbedSignDocument` props (`@documenso/embed-react@0.6.1`,
`dist/sign-document.d.ts`):**
```ts
type EmbedSignDocumentProps = {
  token: string;                 // recipient signing token (required)
  host?: string;                 // e.g. "https://app.documenso.com" or your self-host URL
  className?: string;
  css?: string;                  // raw CSS injected into the signing surface (Platform plan)
  cssVars?: CssVars & Record<string, string>;   // themed variables (Platform plan) — see below
  darkModeDisabled?: boolean;    // (Platform plan)
  language?: string;             // 0.6.1+
  name?: string;                 // prefill signer name
  lockName?: boolean;
  email?: string;                // 0.6.1+
  lockEmail?: boolean;           // 0.6.1+
  allowDocumentRejection?: boolean;
  additionalProps?: Record<string, string | number | boolean>;
  onDocumentReady?: () => void;
  onDocumentCompleted?: (d: { token: string; documentId: number; recipientId: number }) => void;
  onDocumentError?: (error: string) => void;
  onDocumentRejected?: (d: { token: string; documentId: number; recipientId: number; reason: string }) => void;
};
```

**[VERIFIED — SDK] Other exports** (`dist/index.d.ts`): `EmbedDirectTemplate` (public fill-then-
sign link; props include `email`, `lockEmail`, `externalId`, `onFieldSigned`/`onFieldUnsigned`),
plus V2 envelope-editor embeds `EmbedCreateEnvelopeV2` / `EmbedUpdateEnvelopeV2` and the
`unstable_*` create/update builders (`EmbedCreateDocument`, `EmbedCreateTemplate`,
`EmbedUpdateDocument`, `EmbedMultiSignDocument`, …). For pre-created, per-recipient proposals,
**`EmbedSignDocument` is the right component.** The create/update builders authenticate via an
**embedding presign token** (§9), not the recipient token.

**Theming (`cssVars`) — [VERIFIED — SDK] keys** (`dist/css-vars.d.ts`):
`background, foreground, muted, mutedForeground, popover, popoverForeground, card, cardBorder,
cardBorderTint, cardForeground, fieldCard, fieldCardBorder, fieldCardForeground, widget,
widgetForeground, border, input, primary, primaryForeground, secondary, secondaryForeground,
accent, accentForeground, destructive, destructiveForeground, ring, radius, warning`. (The live
docs also list `envelopeEditorBackground`; `cssVars` is typed `CssVars & Record<string, string>`,
so unknown string keys pass through.)

> **[VERIFIED — docs] `cssVars` / `css` / `darkModeDisabled` require the Platform plan.**
> `docs.documenso.com/developers/embedding/css-variables`: *"Custom CSS and CSS variables are
> available on the Platform Plan."* `docs.documenso.com/developers/embedding`: the Platform Plan
> adds *"Custom CSS and styling variables, Dark mode controls, Removal of Documenso branding."*
> On Teams (the minimum embedding tier), embedding works but the surface keeps Documenso's stock
> styling/branding. See §11.

> **Caveat:** even fully themed, the **document pages themselves remain the rendered PDF** (a
> white sheet). `cssVars`/`css` style the signing **chrome/widgets** around it, not the PDF
> content. To avoid the white-PDF feel, keep the *reading* experience in your React renderer
> (§6.1) and treat the embed purely as the *signature action*.

```tsx
'use client';
import { EmbedSignDocument } from '@documenso/embed-react';

function SignatureStep({ token, onDone }: { token: string; onDone: () => void }) {
  return (
    <EmbedSignDocument
      token={token}
      host="https://app.documenso.com"
      darkModeDisabled={false}
      // cssVars/css below are honored only on the Platform plan:
      cssVars={{
        background: '#0a0a0f', foreground: '#e7e9ee',
        card: '#0d0d12', cardForeground: '#e7e9ee', cardBorder: 'rgba(255,255,255,0.1)',
        primary: '#5b8cff', primaryForeground: '#ffffff',
        accent: '#5b8cff', radius: '0.75rem',
      }}
      onDocumentCompleted={() => onDone()}
      onDocumentError={(e) => console.error('sign error', e)}
    />
  );
}
```

### 6.3 Completion handling
- `onDocumentCompleted` → optimistic UI ("Signed — thank you"), then `router.refresh()` to
  re-pull status. **Do not** treat the client callback as authoritative for business logic — it
  is client-side and can be missed/spoofed; the **webhook** (§5.5) is the source of truth.
- Three ways to surface the signature step, in increasing nativeness:
  1. **Inline embed** behind a "Review & sign" button (iframe in your page).
  2. **Themed inline embed** via `cssVars`/`css` (chrome matches your UI — **Platform plan**).
  3. **Full-screen redirect** to `{host}/sign/{token}` (cleanest hand-off, no nested iframe).

### 6.4 Embed vs. hosted-token — concrete

The most important integration decision, and the easiest to confuse, because **both approaches
use the exact same recipient `token`.** "Embed vs token" is NOT two credentials — it is **how you
deliver Documenso's signing surface and how you learn it completed.** The embed's iframe URL *is*
a token URL: `/embed/sign/{token}`.

- **Embed approach** = render Documenso's signing surface **inside your page** as an iframe, via
  `@documenso/embed-react`'s `EmbedSignDocument`, pointed at `{host}/embed/sign/{token}`.
- **Hosted-token approach** = take the same `token` and send the prospect to Documenso's
  **hosted full-page** signing experience at `{host}/sign/{token}` (redirect or link). No embed
  package, no iframe.

| | **Embed** (`EmbedSignDocument`) | **Hosted token** (`/sign/{token}`) |
|---|---|---|
| URL it hits | `{host}/embed/sign/{token}` (iframe) | `{host}/sign/{token}` (full page) |
| Client dependency | `@documenso/embed-react` (required) | **none** — just a URL |
| Where signing happens | **Inside your app** (your domain, your layout) | Documenso's hosted page (their domain / new tab) |
| Theming control | `cssVars`/`css`/`darkModeDisabled` on the chrome — **Platform plan only** | **None** — Documenso's stock UI/branding |
| Completion signal | Real-time JS callbacks via postMessage **and** webhook | **Webhook only** (+ optional post-sign redirect URL) |
| Prefill / lock signer | `name`/`lockName` (+ `email`/`lockEmail` in 0.6.1) | Not via URL (set on the document) |
| Prospect leaves your app | No | Yes (full-page) or new tab |
| Iframe constraints | Third-party-storage/cookie rules, CSP `frame-ancestors`, mobile iframe quirks | **None** — maximally robust |
| Plan requirement | **Embedding requires Teams plan and above**; white-label themed embed requires **Platform** | Standard recipient page — works on any plan that can create the document |
| Best for | Keeping the prospect in a branded, in-app flow | Maximum reliability, mobile, zero-dependency hand-off |

> **[VERIFIED — docs]** Embedding gating (`docs.documenso.com/developers/embedding`):
> *"Embedding is available on Teams Plan and above, as well as for Early Adopters within a
> team."* So the embed iframe is **not** free/unconditional — your Documenso account must be on
> **Teams or higher** to embed at all, and on **Platform** for `cssVars`/`css`/dark-mode/
> branding removal. The hosted `/sign/{token}` page is the standard recipient experience and does
> not require an embedding tier to exist (you still need an API-capable plan to *create* the
> document — see §11).

#### Concrete — embed
```tsx
'use client';
import { EmbedSignDocument } from '@documenso/embed-react';

<EmbedSignDocument
  token={token}                          // recipient token, e.g. "phn-xxxx"
  host="https://app.documenso.com"       // MUST match the instance the doc was created on
  cssVars={{ background: '#0a0a0f', card: '#0d0d12', primary: '#5b8cff', radius: '0.75rem' }}
  onDocumentCompleted={() => router.refresh()}   // client signal (NOT authoritative)
  onDocumentError={(e) => console.error(e)}
/>
```

#### Concrete — hosted token
```tsx
// No package. The token is the whole integration.
const signUrl = `${process.env.NEXT_PUBLIC_DOCUMENSO_APP_URL}/sign/${token}`;
// full-page:  window.location.href = signUrl;
// or a link:
<a href={signUrl} target="_blank" rel="noopener noreferrer">Review &amp; sign →</a>
```
Hosted mode gives **no client callback** — your **webhook** (§5.5) is the *only* way to learn the
document was signed/completed.

#### They compose — recommended production pattern [RECOMMENDATION]
Use the embed; fall back to the hosted token URL if the iframe fails to load (blocked
third-party storage, CSP, mobile):
```tsx
const [embedError, setEmbedError] = useState(false);
return embedError
  ? <a href={`${host}/sign/${token}`} target="_blank" rel="noopener noreferrer">Open signing in a new tab →</a>
  : <EmbedSignDocument token={token} host={host}
      onDocumentError={() => setEmbedError(true)}
      onDocumentCompleted={onDone} />;
```

#### Three precise clarifications
1. **Don't hand-roll a raw iframe to `/embed/sign/{token}` casually.** You *can* — the package
   itself just builds `{host}/embed/sign/{token}#<base64(JSON config)>` and listens for
   `postMessage` actions (`document-ready`, `document-completed`, `document-error`) — but the
   package handles the config-encoding and the postMessage handshake for you. If you roll your
   own (as Documenso's own embed demo does, to set `scrolling="no"`), you must replicate both. If
   you want zero dependencies, prefer the **hosted** redirect over a bare iframe.
2. **Webhook is authoritative in both modes.** Business logic (mark `completed`, charge,
   provision) keys off the **webhook**, never the client signal.
3. **Neither mode turns the document into HTML.** In both, the thing being signed is the PDF; the
   embed only themes the *surrounding* chrome (Platform plan). The web-native *reading*
   experience is your React renderer (§6.1).

---

## 7. Status state machine

```
draft ──create+send──▶ sent ──DOCUMENT_OPENED──▶ opened
  │                      │                          │
  │                      └──────────────┬───────────┘
  │                                     ▼
  │                          DOCUMENT_SIGNED ──▶ signed
  │                                     │
  │                          DOCUMENT_COMPLETED ──▶ completed ──▶ (pull signed PDF, downstream)
  │
  ├── DOCUMENT_REJECTED ──▶ rejected
  └── DOCUMENT_CANCELLED ──▶ voided
```
Single-signer proposals: `signed` and `completed` often coincide. Transitions are driven by
**webhooks** (authoritative), never by the client embed callback alone.

---

## 8. Documenso integration reference (verified facts)

- **Instance:** Documenso Cloud `https://app.documenso.com` (or your self-host base URL).
- **[VERIFIED — docs] Base URLs:** v1 → `https://app.documenso.com` (paths under `/api/v1/…`);
  v2 → `https://app.documenso.com/api/v2`. (`docs.documenso.com/developers/public-api` lists the
  v2 base and `Authorization: api_xxxx` auth.)
- **[VERIFIED — OpenAPI] API versions:** **Both v1 and v2 are live** on this Cloud account. v1
  has **13** paths; v2 has **85** paths. **v1 is deprecated** — its own OpenAPI `info` reads
  *"API V1 is deprecated, but will continue to be supported."* Prefer **v2** for new work.
- **[VERIFIED — OpenAPI/live] Auth:** `Authorization: <API_KEY>` (the raw key as the header
  value; key format `api_…`). Store as `DOCUMENSO_API_KEY`. **Server-side only.**
- **[VERIFIED — OpenAPI v1] v1 surface (full):** `GET/POST /api/v1/documents`;
  `GET/DELETE /api/v1/documents/{id}`; `GET …/{id}/download`; `POST …/{id}/fields`;
  `PATCH/DELETE …/{id}/fields/{fieldId}`; `POST …/{id}/recipients`;
  `PATCH/DELETE …/{id}/recipients/{recipientId}`; `POST …/{id}/send`; `POST …/{id}/resend`;
  `GET/POST /api/v1/templates`; `GET/DELETE /api/v1/templates/{id}`;
  `POST /api/v1/templates/{templateId}/create-document`;
  `POST /api/v1/templates/{templateId}/generate-document`.
- **[VERIFIED — OpenAPI v2] v2 surface uses SINGULAR resources** with action suffixes:
  `/api/v2/document/create` (multipart) and `…/create/beta` (JSON), `…/distribute`,
  `…/get-many`, `…/{documentId}`, `…/{documentId}/download`, `…/field/create|update|delete`,
  `…/recipient/create|update|delete`; parallel `/template/*` and `/envelope/*` trees;
  `/template/use`, `/envelope/use`; `/embedding/create-presign-token`,
  `/embedding/verify-presign-token`. **`/api/v2/documents` (plural) does not exist (404).**
- **[VERIFIED — live] Recipient signing URLs:** `{host}/sign/{token}` (hosted full page) and
  `{host}/embed/sign/{token}` (embeddable) are the recipient signing surfaces. The embed package
  also builds `/embed/direct/{token}` for direct templates.
- **[VERIFIED — OpenAPI v2] Embedding presign token** (§9): `create-presign-token` →
  `{ token, expiresAt, expiresIn }` (default `expiresIn` 60 min, max 10080); `verify-presign-
  token` → `{ success }`. Authorizes the create/update embed components without exposing the API
  key to the browser.
- **Webhook auth:** `X-Documenso-Secret` header = your configured secret verbatim (shared
  secret, **not** HMAC); constant-time compare (§5.5).

Required env: `DOCUMENSO_API_KEY`, `DOCUMENSO_API_URL` (default `https://app.documenso.com`),
`DOCUMENSO_WEBHOOK_SECRET`. For the browser embed you may also expose
`NEXT_PUBLIC_DOCUMENSO_APP_URL` (the host) — a public URL, not the key.

> **Host consistency:** the `host` you pass to `EmbedSignDocument` MUST be the same Documenso
> instance the document was created on. A doc created on self-host but embedded against
> `app.documenso.com` (or vice-versa) will not resolve.

---

## 9. Embedding presign tokens (for create/update embed components)

**[VERIFIED — OpenAPI v2 + docs]** Distinct from the recipient signing token. If you use the
*authoring* embed components (`EmbedCreateDocumentV1`, `unstable_EmbedCreateEnvelope`, etc.) to
let a user build/edit a document in your UI, you do **not** ship your API key to the browser.
Instead:
1. Server: `POST /api/v2/embedding/create-presign-token` with `{ expiresIn?, scope? }` →
   `{ token, expiresAt, expiresIn }` (short-lived; default 60 min).
2. Browser: pass that presign token to the authoring component.
3. Documenso verifies it server-side via `/api/v2/embedding/verify-presign-token`.

The plain **`EmbedSignDocument` / hosted `/sign/{token}` signing flow does NOT need a presign
token** — the recipient `token` alone authorizes signing. Presign tokens are only for the
authoring/create-and-edit embeds.

---

## 10. Plan gating & pricing (this changes what you can build)

**[VERIFIED — docs] `documenso.com/pricing`:**

| Plan | Price | API access | Embedded signing | White-label (custom css/cssVars, dark-mode, branding removal) |
|---|---|---|---|---|
| **Free** | $0/mo | — | — | — |
| **Individual** | **$25/mo** ($300/yr) | "API Access for Personal Use" | — | — |
| **Teams** | **$40/mo** ($480/yr) | "API Access for Automation" | **Embedded Signing** | — |
| **Platform** | **$250/mo** ($3,000/yr) | "Unlimited API access" | **Embedded Signing (Whitelabel)** | **Yes** |

Implications for this build:
- **Creating documents via the API** requires at least **Individual ($25)**. Free has no API.
- **Embedding the signing iframe** (`EmbedSignDocument`) requires **Teams ($40) and above** —
  `docs.documenso.com/developers/embedding`: *"Embedding is available on Teams Plan and above…"*
- **Theming/white-labeling the embed** (`cssVars`, `css`, `darkModeDisabled`, removing Documenso
  branding) requires **Platform ($250)** —
  `docs.documenso.com/developers/embedding/css-variables`: *"Custom CSS and CSS variables are
  available on the Platform Plan."*
- The reason the embed renders fully themed on the test account is that **the account is on the
  Platform plan** — not because embedding/theming is free. Budget for Platform if the
  dark-branded, in-app signing surface is a requirement.
- **Self-hosting** (AGPLv3) removes the plan gates entirely and is the only way to *replace*
  (not merely theme) the signing UI — at the cost of running and legally maintaining your own
  e-sign infrastructure.

---

## 11. Gotchas & constraints

- **You cannot collect the signature in your own UI on Cloud.** No API accepts a signature value
  / flips a recipient to signed (§0). The signature happens on Documenso's surface; you theme it
  (Platform) but can't replace it without self-hosting.
- **The PDF stays a PDF.** `cssVars`/`css` theme the embed chrome, not the document page. The
  "web-native" feel comes from your React renderer being the thing the prospect *reads*; the
  embed is only the *sign* action.
- **Content parity:** generate the signed PDF from the **same** structured data as the React view
  so the legal artifact matches what they read.
- **v1 is deprecated; v2 is current.** Both work on Cloud. v2 resources are **singular**
  (`/document`, not `/documents`) — plural paths 404.
- **API key is server-only.** All Documenso REST calls happen on your backend. The browser only
  ever sees the recipient `token` and the public `host`. Authoring embeds use a presign token,
  not the key (§9).
- **Webhook is the source of truth**, not `onDocumentCompleted` (client callback is spoofable).
  Verify with the `X-Documenso-Secret` shared secret and a constant-time compare.
- **Email suppression:** create/send with `sendEmail: false` (v1) / distribute via your URL (v2)
  so Documenso doesn't email the prospect a competing signing link.
- **`/download` returns a `{ downloadUrl }`, not bytes.** Fetch the URL to get the sealed PDF.
- **Token sensitivity:** the proposal URL exposes a signing token to whoever has the link; use
  unguessable ids and consider an auth/identity gate for higher-value deals.
- **Embedding tier:** the embed iframe requires the account to be on **Teams+**; theming requires
  **Platform**. The hosted `/sign/{token}` page is the fallback that always works.

---

## 12. Open decisions for the implementing team

1. **PDF generation engine** (DocRaptor vs Puppeteer vs react-pdf) and where it runs.
2. **API version:** v2 (current, singular, multipart create) vs v1 (deprecated but simple
   presigned-upload). Prefer v2 for new work.
3. **Create path:** generate-and-upload (A) vs Documenso template + prefill (B) — depends on how
   dynamic the layout is.
4. **Signature surface:** inline embed (Teams+) vs themed embed (Platform) vs full-screen
   hosted redirect (any plan).
5. **Plan tier:** Individual ($25, API only) vs Teams ($40, embed) vs Platform ($250, themed/
   white-label embed) vs self-host (full UI ownership). This is a budget/UX decision, not an
   afterthought.
6. **Auth on the proposal URL:** open unguessable link vs identity-gated.
7. **Downstream on completion:** Stripe checkout? CRM/deal-stage update? counter-signature?

---

### Reference implementations & sources
- **Live OpenAPI:** `https://app.documenso.com/api/v1/openapi.json` (13 paths, v1 deprecated),
  `https://app.documenso.com/api/v2/openapi.json` (85 paths, current).
- **Official docs:** `docs.documenso.com/developers/public-api`, `/developers/embedding`,
  `/developers/embedding/react`, `/developers/embedding/css-variables`,
  `/developers/webhooks`, `/developers/webhooks/verification`; pricing at `documenso.com/pricing`.
- **SDKs:** `@documenso/embed-react@^0.6.1` (React embed components);
  `@documenso/sdk-typescript` (server REST SDK).
- **Working embed reference:** a Vite + `@documenso/embed-react@^0.6.1` demo that hand-rolls the
  iframe (`{host}/embed/sign/{token}#<base64 config>`), pushes a dark `cssVars` + raw `css`
  theme, and consumes the `document-ready`/`document-completed`/`document-error` postMessage
  protocol — a faithful model for a themed, in-app signing surface.
