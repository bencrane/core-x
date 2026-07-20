# DOCUMENSO_STALE_REFERENCES — canonical stale-reference audit

| | |
|---|---|
| **Audit date** | 2026-07-20 |
| **Main SHA audited** | `5a0db63` (`feat(edge): documenso plane moves to gc schema; rsh-era config tables excised (#1255)`) |
| **Scope** | `apps/edge_api/**/*.py`, `docs/reference/DOCUMENSO_ARCHITECTURE/` (12 files), `docs/reference/PROPOSAL_EXPERIENCE_REACT_DOCUMENSO_SPEC.md`, `apps/edge_api/main.py`, `apps/edge_api/src/migrate.py`, `scripts/{documenso_push_templates,render_ao_preview,rs_capital_origination_generate}.py` |
| **Mode** | READ-ONLY. No source edited; this file is the sole artifact. |

## Dropped objects (any mention is stale by definition)

Tables dropped from the HQX Postgres and expected deleted from the codebase:

- `business.documenso_envelopes`
- `business.documenso_webhook_events`
- `business.documenso_template_document_prefill_configs`
- `business.documenso_template_defaults`
- `business.documenso_templates`
- `business.engagement_documenso_template_mappings`
- `business.documenso_template_configs`
- `business.documenso_envelopes_orphan`

Removed routers/modules: `documenso_templates_v1`, `engagement_mappings_v1`, `documenso_prefill_configs_v1`, `documenso_template_defaults_v1`.

Retired concepts: the "mirror default" / `is_default` picker; the prefill-config editor; the geometric field lock (lock/editable now matched by field **LABEL**); the `locked`/`editable` rule vocabulary (replaced by the Documenso terms **Required** / **Read-Only**); handlebar-token baked values for government-contracted content (v3 is underscore-blank).

Operative replacements (all HQX Postgres, `gc` schema): `gc.documenso_envelopes`, `gc.documenso_webhook_events`, `gc.documenso_template_field_rules`, `gc.global_agreement_archetypes`, `gc.global_agreement_archetype_versions`. `business.agreements` is the one legitimate survivor and is **not** flagged here.

## Summary of counts

**S1 is NOT zero.** The `.py` sweep proper is clean — every executable SQL statement in `apps/edge_api/**/*.py` targets `gc.*`, no removed router is imported by `main.py`, and the four leftover support-module directories (`src/documenso_template_defaults/`, `documenso_prefill_configs/`, `documenso_templates/`, `engagement_mappings/`) contain only stale `__pycache__` with no `.py` source. **But two live-code paths still execute against dropped tables:** (1) `apps/edge_api/src/migrate.py` auto-applies every `sql/*.sql` on boot, and `sql/` still holds the CREATE/ALTER DDL for eight dropped-table objects — under the migration's stated end-state this **fails the next default-config boot** at the first `ALTER` against a missing table (and silently *resurrects* three dropped tables before it gets there); (2) `scripts/rs_capital_origination_generate.py` runs `SELECT`/`UPDATE`/`INSERT` against `business.documenso_templates`, erroring whenever the script is invoked. **S1 total: 2 code paths, 11 executable references.** S2 (misleading docs/comments presenting retired behavior as current): 11 `.py` docstring/comment sites plus the `DOCUMENSO_ARCHITECTURE/` corpus, which predates the migration and is stale as a body (docs 06, 07, 10, 11, 12 are wholesale pre-migration; 00, 02, 03, 04, 05, 09 carry dropped-table names for the raw webhook landing). S3 (accurate historical mentions): 4 `.py` sites plus past-tense doc passages. The SPEC file and two of the three scripts (`documenso_push_templates.py`, `render_ao_preview.py`) are **clean** — zero stale references.

---

## S1 — live code referencing a dropped table/module (would error at runtime)

> Expected count was zero. It is **not** zero. Both paths below are reached through in-scope `.py` (`migrate.py`, a `scripts/` file); the `.sql` files themselves are the payload those `.py` paths execute. This depends on the migration's stated DROP having been applied to the live DB — a read-only code audit cannot confirm prod schema, but under the directive's stated end-state the failures below are the direct consequence.

### S1.1 — `migrate.py` boot DDL apply re-runs dropped-table DDL (deploy-breaking)

`apps/edge_api/src/migrate.py:61` discovers DDL by unfiltered sorted glob — `return sorted(SQL_DIR.glob("*.sql"))` — and `run_migrations()` applies **every** file on boot in filename order, re-raising on the first failure to "fail the boot loudly" (`apps/edge_api/src/migrate.py:64-96`, esp. the `except … raise` at `:92-94`). Gated only by `EDGE_API_SKIP_DB_MIGRATE` (`:67`); the default is apply-on-boot. `sql/` still contains DDL for eight dropped-table objects. Filename-order boot behavior:

**Silently RESURRECT the dropped table (`CREATE TABLE IF NOT EXISTS` — succeeds, recreates the table empty, undoing the DROP):**

| file:line | object recreated |
|---|---|
| `apps/edge_api/sql/documenso_envelopes.sql:37` | `business.documenso_envelopes` |
| `apps/edge_api/sql/documenso_envelopes.sql:71` | `business.documenso_template_document_prefill_configs` |
| `apps/edge_api/sql/documenso_template_defaults.sql:20` | `business.documenso_template_defaults` |
| `apps/edge_api/sql/documenso_webhook_events.sql:26` | `business.documenso_webhook_events` |

**Hard ERROR on boot (`ALTER TABLE` on a dropped table — `ADD COLUMN IF NOT EXISTS` guards the column, not the table; `DO` blocks probe `information_schema` for a column but still issue a bare `ALTER` against the missing relation):**

| file:line | statement | blast radius |
|---|---|---|
| `apps/edge_api/sql/documenso_templates_is_default.sql:9` | `ALTER TABLE business.documenso_templates ADD COLUMN IF NOT EXISTS is_default …` | **First hard failure in filename order.** `relation "business.documenso_templates" does not exist` → `migrate.py` re-raises → FastAPI lifespan fails boot → the deploy never serves traffic. |
| `apps/edge_api/sql/documenso_templates_recipients_to_response.sql:19,27` | `ALTER TABLE business.documenso_templates …` inside a `DO` block | Same relation-missing error (the `ELSIF NOT EXISTS` branch fires `ADD COLUMN` on the absent table). |
| `apps/edge_api/sql/engagement_archetypes.sql:49,58,64,69` | `ALTER TABLE business.documenso_templates ADD COLUMN … / ADD CONSTRAINT … / CREATE INDEX … / UPDATE …` | Relation-missing error (also references `business.engagement_archetypes`). |
| `apps/edge_api/sql/engagement_mappings_demand_side_partner_type.sql:19` | `ALTER TABLE business.engagement_documenso_template_mappings ADD COLUMN IF NOT EXISTS …` | `relation "business.engagement_documenso_template_mappings" does not exist`. |
| `apps/edge_api/sql/global_input_content_variants.sql:38,46` | `ALTER TABLE business.documenso_templates …` | Relation-missing error. |

Because `migrate.py` halts on the first failure, `documenso_templates_is_default.sql` is the boot-stopper; the resurrection files (alphabetically earlier) run first and recreate three dropped tables, and the remaining `ALTER` files are latent failures that would surface if the earlier one were removed. Adjacent lower-risk carriers of the same defect (no error in prod only because the table already exists so `CREATE TABLE IF NOT EXISTS` is a no-op that skips FK re-validation): `apps/edge_api/sql/deal_details.sql:28` (`default_template_uuid … REFERENCES business.documenso_templates (id)`) and `apps/edge_api/sql/engagement_mappings_template_uuid_rename.sql:29` (guarded `DO`-block rename — no-op, no error).

### S1.2 — `rs_capital_origination_generate.py` executes SQL against a dropped table

`scripts/rs_capital_origination_generate.py` is a manual one-shot origination script (errors when invoked, not on service boot):

| file:line | statement |
|---|---|
| `scripts/rs_capital_origination_generate.py:105` | `cur.execute("SELECT organization_id FROM business.documenso_templates WHERE documenso_template_id='14423'")` |
| `scripts/rs_capital_origination_generate.py:194` | `cur.execute("UPDATE business.documenso_templates SET status='archived', updated_at=now() …")` |
| `scripts/rs_capital_origination_generate.py:199` | `cur.execute("INSERT INTO business.documenso_templates …")` |

Each raises `relation "business.documenso_templates" does not exist` the next time the script is run against the migrated DB.

---

## S2 — misleading docs/comments (retired behavior described as current)

### S2.a — `apps/edge_api/**/*.py` docstrings & comments

Every one of these is a comment/docstring; the executable code beside them already targets `gc.*`. The stale text names a dropped table as a current entity.

| file:line | snippet (≤2 lines) | current truth |
|---|---|---|
| `apps/edge_api/main.py:318` | `# … stores every delivery verbatim in business.documenso_webhook_events.` | The raw landing table is `gc.documenso_webhook_events` (the route writes it via `documenso_webhooks/queries.py:24`). |
| `apps/edge_api/src/migrate.py:26` | ``…cross-file FKs reference upstream-owned tables (``business.organizations``, ``business.documenso_templates``) that already exist in prod`` | `business.documenso_templates` is dropped; it is no longer an upstream FK target. |
| `apps/edge_api/src/routers/documenso_envelopes_v1.py:10` | `Re-grab NEVER writes business.documenso_template_configs.` | `business.documenso_template_configs` no longer exists; the invariant references a dropped table. |
| `apps/edge_api/src/documenso_projection/__init__.py:7` | `The projector NEVER writes business.documenso_template_configs.` | Dropped table; projector writes only `gc.documenso_envelopes`. |
| `apps/edge_api/src/documenso_projection/projector.py:12` | `* NEVER write business.documenso_template_configs.` | Dropped table. |
| `apps/edge_api/src/documenso_projection/queries.py:6` | `it MUST NEVER touch business.documenso_template_configs (operator/app-owned).` | Dropped table; the "(operator/app-owned)" parenthetical presents it as a live table. |
| `apps/edge_api/src/documenso_projection/resync.py:11` | `NEVER writes business.documenso_template_configs.` | Dropped table. |
| `apps/edge_api/src/services/documenso_client.py:195` | ``business.documenso_templates`` stores only the numeric template id, and ``/envelope/use``…` | Dropped table; template-id provenance is now `gc.documenso_envelopes` / caller-supplied plan. |
| `apps/edge_api/src/services/documenso_client.py:530` | ``business.documenso_templates`` stores the numeric id as text; tolerate a prefixed handle…` | Dropped table (same provenance correction). |
| `apps/edge_api/src/deals/originate.py:10` | ``documenso_template_document_prefill_configs.field_settings`` (defaults + read_only)` — named as a current originate input | The originate resolver sources defaults/read-only from `gc.documenso_template_field_rules` (`deals/queries.py:235`); the prefill-config table is dropped. |
| `apps/edge_api/src/deals/queries.py:122` | ``is_default`` is retired (always false) with business.documenso_template_defaults.` | Correct that `is_default` is retired, but names the dropped table as the vehicle; the code returns `false AS is_default` off `gc.documenso_envelopes`. (Borderline S2/S3 — the retirement framing is accurate; the dropped-table name is stale.) |

### S2.b — `docs/reference/DOCUMENSO_ARCHITECTURE/` (predates the migration; stale as a body)

These 12 files were written before `#1255` and describe the pre-migration world — the `business.*` tables, the removed routers, the `is_default` picker, the prefill-config editor, and the `locked`/`editable` vocabulary — as current. Docs **06, 07, 10, 11, 12** are wholesale pre-migration; docs **00, 02, 03, 04, 05, 09** carry the dropped `business.documenso_webhook_events` name for the (still-live) raw webhook landing. Per-file anchors:

**`00-ORIENTATION.md`** — `business.documenso_webhook_events` presented as SoR: `:147`, `:188`, `:353`. Removed router `engagement_mappings_v1.py` listed as ACTIVE: `:156`. → Landing table is `gc.documenso_webhook_events`; `engagement_mappings_v1` is deleted.

**`02-FLOW-through-docraptor.md`** — `business.documenso_webhook_events`: `:221`, `:226`, `:456`, `:464`. → `gc.documenso_webhook_events`.

**`03-FLOW-direct-to-documenso.md`** — `business.documenso_webhook_events`: `:174`. `readOnly field lock` / derived-field locking vocabulary: `:32`. → `gc.documenso_webhook_events`; lock is matched by LABEL and expressed as Documenso Read-Only.

**`04-DOCUMENSO-INTEGRATION.md`** — `business.documenso_webhook_events`: `:37`, `:443`, `:447`, `:489`, `:629`, `:637`, `:697`. `business.documenso_templates`: `:315`. Removed router `engagement_mappings_v1.py`: `:385`. `editable-vs-locked` vocabulary: `:202`. → tables now `gc.*`; router removed; Required/Read-Only vocabulary.

**`05-PAYMENTS.md`** — `business.documenso_webhook_events` as the offline sign-state source: `:132`, `:221`. (`:318` `run_migrations` boot apply description is accurate.) → `gc.documenso_webhook_events`.

**`06-ENGAGEMENT-DOCS-AND-TEMPLATES.md`** — wholesale stale. Declares the templates layer "ACTIVE in the live flow" (`:9`). `business.documenso_templates`: `:55`, `:115`, `:117`, `:120`, `:202`, `:223`, and the `..._mappings` table `:65`, `:74`, `:75`, `:117`, `:223`. Removed router `engagement_mappings_v1.py`: `:61`, `:63`, `:89`. → both tables dropped; `engagement_mappings_v1` removed; the whole "engagement templates/mappings" surface is retired.

**`07-DATA-STORES.md`** — `business.documenso_templates` catalogued as a live store: `:63`, `:120`, `:202`, `:315`, `:327`, `:379`. `business.documenso_webhook_events`: `:85`, `:315`. → dropped / now `gc.*`.

**`10-TEMPLATE-ITERATION-RUNBOOK.md`** — wholesale stale. `business.documenso_templates` register/select steps: `:37`, `:100`, `:158`. `business.engagement_documenso_template_mappings`: `:38`, `:157`. `business.documenso_webhook_events`: `:40`. Retired `default_field_values` / `editable_field_labels` / `locked` vocabulary and "lock/editable matched by label vs editor checkbox": `:9`, `:45`, `:46`, `:65`, `:69`, `:70`, `:101`, `:116`, `:117`, `:118`, `:128`, `:129`, `:155`, `:156`. → tables dropped; the entire template-iteration runbook targets a removed registry; lock vocabulary is Required/Read-Only, matched by LABEL.

**`11-ENVELOPE-MIRROR-AND-PREFILL-CONFIG.md`** — most stale doc. `business.documenso_envelopes`: `:9`, `:25`, `:78`, `:81`, `:397`, `:443`, `:453`. `business.documenso_template_document_prefill_configs`: `:10`, `:27`, `:113`, `:115`. `business.documenso_templates` (legacy registry): `:63`, `:154`, `:396`. `business.documenso_envelopes_orphan` + `business.documenso_template_configs` "still exist": `:406`, `:407`. Removed routers `documenso_prefill_configs_v1` (`:67`, `:141`, `:243`, `:244`, `:305`) and `documenso_templates_v1` (`:245`, `:246`). Prefill-config editor + `is_default` picker + `locked`/`editable` vocabulary: `:4`, `:6`, `:20`, `:21`, `:72`, `:160`, `:162`, `:283`, `:285`, `:300`, `:336`, `:386`. → mirror is `gc.documenso_envelopes`; field rules are `gc.documenso_template_field_rules`; prefill-config editor, `is_default` picker, and both routers are removed; Required/Read-Only by label.

**`12-DEAL-DOCUMENT-CONFIG-AND-ORIGINATE.md`** — wholesale stale. `business.documenso_template_defaults`: `:7`, `:28`, `:115`, `:245`, `:252`, `:411`. `business.documenso_envelopes`: `:12`, `:192`, `:251`, `:348`, `:449`, `:460`. `business.documenso_template_document_prefill_configs`: `:13`, `:170`, `:299`, `:470`. `business.documenso_templates` (legacy registry): `:21`, `:36`, `:396`, `:398`. `business.documenso_template_configs`: `:408`, `:411`. Removed routers `documenso_template_defaults_v1` (`:244`, `:245`) and `documenso_templates_v1` (`:399`). `is_default` picker + prefill-config + `locked`/`editable` resolver vocabulary: `:11`, `:29`, `:127`, `:134`, `:170`, `:176`–`:178`, `:207`–`:221`, `:231`, `:281`, `:299`–`:302`, `:326`, `:340`–`:346`, `:388`, `:392`. → defaults store and `is_default` picker retired; resolution is `gc.documenso_template_field_rules` (default + read_only + required) via `deals/originate.py`; both routers removed.

### S2.c — out of primary scope, noted for completeness

The following are **not** `.py` under `apps/edge_api/` and were outside the enumerated sweep, but carry heavy stale references and are recorded here so the canonical file is not silently incomplete:

- `apps/edge_api/sql/*.sql` — beyond the S1.1 executable defects, the DDL comments describe the dropped tables as current design (e.g. `documenso_envelopes.sql:4,7,11,31`; `documenso_template_defaults.sql:5,6,9,15`; `deal_details.sql:15,21,35,36`; `deal_document_configs.sql:19`; `engagement_archetypes.sql:2`; `close_call_sync.sql:3`).
- `apps/edge_api/content/government-contracted/docraptor-to-documenso-template/prepaid-introductions/v1/global_engagement_content/README.md:21,22,100` — `business.documenso_templates` + `business.engagement_documenso_template_mappings` described as the live registration targets.

---

## S3 — historical mentions (accurate as history; no action)

These correctly frame the dropped objects as retired/legacy; they read as history, not as current behavior.

- `apps/edge_api/src/deals/queries.py:53` — "the mirror-default concept is retired with `business.documenso_template_defaults`".
- `apps/edge_api/src/deals/queries.py:211` — comment: originate inputs come from the new world, "never the legacy documenso_templates".
- `apps/edge_api/src/deals/originate.py:11` — "…recipients) — never the legacy documenso_templates."
- `docs/reference/DOCUMENSO_ARCHITECTURE/02-FLOW-through-docraptor.md:14` — "This WAS the engagement-agreement + e-signature pathway…" (past tense).
- `docs/reference/DOCUMENSO_ARCHITECTURE/02-FLOW-through-docraptor.md:464` — "`POST /api/v1/proposals/webhook` is REMOVED, not deprecated."
- `docs/reference/DOCUMENSO_ARCHITECTURE/11-ENVELOPE-MIRROR-AND-PREFILL-CONFIG.md:67` — changelog entry: "rename `documenso_template_configs` → `documenso_template_document_prefill_configs`" (dated commit history).
- `docs/reference/DOCUMENSO_ARCHITECTURE/12-DEAL-DOCUMENT-CONFIG-AND-ORIGINATE.md:71` — changelog entry: "retire the 3 legacy resolvers" (dated commit history).

---

## Clean surfaces (explicit zero)

- `docs/reference/PROPOSAL_EXPERIENCE_REACT_DOCUMENSO_SPEC.md` — **no** dropped-table, removed-module, or retired-concept references. Its "template + prefill" and "prefill signer name" usages (`:229`, `:321`, `:621`) describe native Documenso template prefill, not the retired `documenso_template_document_prefill_configs` editor.
- `scripts/documenso_push_templates.py` — **zero** stale references.
- `scripts/render_ao_preview.py` — **zero** stale references.
- `apps/edge_api/**/*.py` executable code — **zero** SQL statements against a dropped table (all target `gc.*`); `main.py` imports **no** removed router; the leftover `src/{documenso_template_defaults,documenso_prefill_configs,documenso_templates,engagement_mappings}/` directories hold only `__pycache__` (no `.py` source).
