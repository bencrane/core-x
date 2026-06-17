"""Operator settings — the per-operator cockpit configuration the BFF resolves at originate.

``public.operator_settings`` (one row per operator, keyed by the Supabase ``auth_user_id``) carries
the originate pathway: ``render_mode`` (through-docraptor | direct-to-documenso) and, when direct,
``direct_to_documenso_lane`` (envelope-distribute | prefill-document-from-template).

Lives in the SAME ``HQX_DB_URL_POOLED`` Postgres as ``business.*``; edge_api is now its SOLE gateway.
Previously read/written by the platform-api BFF directly via its Supabase service-role client — that
direct-DB reach is RETIRED in favor of this service-token surface, so the BFF becomes a pass-through
(platform-app -> platform-api -> edge_api). DDL system-of-record: ``apps/edge_api/sql/operator_settings.sql``.
"""
