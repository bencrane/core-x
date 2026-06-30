"""Originate-time RESOLVER (model B) — turns a deal's stored document config into the inputs
``documenso_client.create_document_from_template`` needs. PURE functions over the query result; no I/O.

Model B: per template field LABEL, value = the deal's ``field_values[label]`` (operator override) ELSE
the prefill config's ``default_document_field_value``. Labels the operator marked ``read_only`` in the
config are LOCKED on the derived document; everything else prefilled stays editable for the prospect.
The prospect recipient is the one the template's value (TEXT/NUMBER) fields are assigned to.

Inputs come from the NEW world only — ``deal_document_configs.field_values`` (overrides),
``documenso_template_document_prefill_configs.field_settings`` (defaults + read_only), and the template's
verbatim ``documenso_envelopes.documenso_response`` (recipients) — never the legacy documenso_templates.
"""
from __future__ import annotations

from collections import Counter
from typing import Any


def resolve_field_values(field_settings: dict, deal_field_values: dict) -> dict[str, str]:
    """Per label, override (the deal's ``field_values``, if non-empty) ELSE the config default. Values
    are coerced to strings (Documenso prefill wants strings); empty-after-coercion labels are skipped
    (no value to prefill). The merge is the keyset of BOTH config defaults and deal overrides, so a
    config-default-only label still prefills and a deal-override-only label still applies."""
    config_defaults = {
        label: fs.get("default_document_field_value")
        for label, fs in (field_settings or {}).items()
        if isinstance(fs, dict)
    }
    overrides = deal_field_values or {}
    out: dict[str, str] = {}
    for label in set(config_defaults) | set(overrides):
        ov = overrides.get(label)
        ov = str(ov).strip() if ov is not None else ""
        df = config_defaults.get(label)
        df = str(df).strip() if df is not None else ""
        value = ov or df
        if value:
            out[label] = value
    return out


def locked_labels(field_settings: dict) -> set[str]:
    """Labels the operator marked ``read_only: true`` in the prefill config — LOCKED on the derived
    document (operator terms the signer can't change). Everything else stays editable."""
    return {
        label
        for label, fs in (field_settings or {}).items()
        if isinstance(fs, dict) and fs.get("read_only") is True
    }


def derive_prospect_recipient_id(template_response: dict[str, Any] | None) -> int | None:
    """The recipient the prospect binds to = the one the template's labelled TEXT/NUMBER (value) fields
    are assigned to — the most-common ``recipientId`` among them (a template's value fields cluster on the
    prospect slot; SIGNATURE/DATE carry no label). ``None`` when undeterminable, so the caller lets
    ``create_document_from_template`` fall back to its placeholder heuristic.

    ASSUMES value fields cluster on ONE prospect recipient (true for the current single-prospect
    templates). If a template ever puts a LARGER cluster of labelled value fields on a SECOND recipient
    (e.g. an operator slot added via "Add Myself"), most-common would bind the wrong slot — persist an
    explicit prospect recipient id on the config/mirror and read it instead at that point."""
    fields = (template_response or {}).get("fields") or []
    counts: Counter[int] = Counter()
    for f in fields:
        if not isinstance(f, dict):
            continue
        if str(f.get("type") or "").upper() not in ("TEXT", "NUMBER"):
            continue
        if not (f.get("fieldMeta") or {}).get("label"):
            continue
        rid = f.get("recipientId")
        if rid is None:
            continue
        try:
            counts[int(rid)] += 1
        except (TypeError, ValueError):
            continue
    return counts.most_common(1)[0][0] if counts else None
