"""DMaaS — Direct Mail as a Service action wrappers (stubs).

The forward action surface for autonomous GTM agents: turn an audience segment
(built via audience.py) into physical mail. Fulfillment is Lob-backed
(``LOB_API_KEY`` + ``DMAAS_MCP_BEARER_TOKEN`` are provisioned in the service env),
but per the directive these are STUBS — the Lob calls are not yet wired.

Contract discipline: a stub must not lie. Every wrapper validates and echoes its
inputs, then returns ``{"status": "not_implemented", ...}`` so a calling agent
gets an unambiguous machine-readable signal — never a fabricated success or a
silent no-op. Wiring the real provider later is a fill-in behind these exact
signatures; the tool schema the agent sees does not change.
"""

from __future__ import annotations

from typing import Any

_PROVIDER = "lob"


def _staged(action: str, **echo: Any) -> dict[str, Any]:
    """Uniform stub envelope — the action is accepted and validated, fulfillment
    is not yet wired to the provider."""
    return {
        "status": "not_implemented",
        "provider": _PROVIDER,
        "action": action,
        "request": echo,
        "detail": (
            f"DMaaS '{action}' is a stub: inputs validated, "
            f"{_PROVIDER} fulfillment not yet wired."
        ),
    }


def create_direct_mail_campaign(
    name: str,
    audience_query: str,
    creative_template_id: str,
    mail_class: str = "usps_first_class",
) -> dict[str, Any]:
    """Define a direct-mail campaign over an audience segment. ``audience_query``
    is the ANSI SQL (see ``execute_audience_query``) whose rows resolve the
    recipient list; ``creative_template_id`` is the Lob template to render per
    recipient; ``mail_class`` is the postal service tier. STUB — returns a staged
    envelope; nothing is enqueued.
    """
    if not name.strip() or not audience_query.strip() or not creative_template_id.strip():
        raise ValueError("name, audience_query, and creative_template_id are required")
    return _staged(
        "create_direct_mail_campaign",
        name=name,
        audience_query=audience_query,
        creative_template_id=creative_template_id,
        mail_class=mail_class,
    )


def send_postcard(
    to_address: dict[str, str],
    from_address: dict[str, str],
    front_template_id: str,
    back_template_id: str,
    size: str = "4x6",
) -> dict[str, Any]:
    """Fulfill a single postcard to one recipient. ``to_address`` /
    ``from_address`` are mailing addresses (``name, address_line1,
    address_line2?, address_city, address_state, address_zip, address_country?``);
    ``size`` is a Lob postcard size (``4x6`` / ``6x9`` / ``6x11``). STUB — returns
    a staged envelope; no mail is produced.
    """
    for label, addr in (("to_address", to_address), ("from_address", from_address)):
        missing = {"name", "address_line1", "address_city", "address_state", "address_zip"} - set(addr or {})
        if missing:
            raise ValueError(f"{label} missing required fields: {sorted(missing)}")
    return _staged(
        "send_postcard",
        to_address=to_address,
        from_address=from_address,
        front_template_id=front_template_id,
        back_template_id=back_template_id,
        size=size,
    )


def send_letter(
    to_address: dict[str, str],
    from_address: dict[str, str],
    template_id: str,
    color: bool = True,
    double_sided: bool = True,
) -> dict[str, Any]:
    """Fulfill a single letter to one recipient. Addresses follow the same shape
    as ``send_postcard``; ``template_id`` is the Lob letter template; ``color`` /
    ``double_sided`` control print options. STUB — returns a staged envelope; no
    mail is produced.
    """
    for label, addr in (("to_address", to_address), ("from_address", from_address)):
        missing = {"name", "address_line1", "address_city", "address_state", "address_zip"} - set(addr or {})
        if missing:
            raise ValueError(f"{label} missing required fields: {sorted(missing)}")
    return _staged(
        "send_letter",
        to_address=to_address,
        from_address=from_address,
        template_id=template_id,
        color=color,
        double_sided=double_sided,
    )


def get_fulfillment_status(piece_id: str) -> dict[str, Any]:
    """Check the delivery status of a previously-sent mail piece by its provider
    id. STUB — returns a staged envelope; no provider lookup is performed.
    """
    if not piece_id.strip():
        raise ValueError("piece_id is required")
    return _staged("get_fulfillment_status", piece_id=piece_id)


def register(mcp) -> None:
    """Mount the DMaaS action wrappers onto the FastMCP server."""
    for fn in (
        create_direct_mail_campaign,
        send_postcard,
        send_letter,
        get_fulfillment_status,
    ):
        mcp.add_tool(fn)
