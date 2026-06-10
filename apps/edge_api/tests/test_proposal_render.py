"""Unit tests for dynamic-value rendering into the agreement PDF (DocRaptor HTML).

No network. Pins the contract that the operator's locked-in STRUCTURED values flow into the
rendered document:

  1. ``success_fee_markdown_table`` renders the schedule's tiers + rates (edited rates included).
  2. ``substitute_markdown_tokens`` swaps ``{{success_fee_table}}`` for that table in the markdown.
  3. End-to-end: a published-template body with ``{{success_fee_table}}`` + ``{{monthly_fee}}`` /
     ``{{billing_cadence}}`` / ``{{total}}`` / ``{{duration}}`` renders to HTML carrying the edited
     fee, cadence phrase, total, term, and a tier table with the edited rate — i.e. nothing the
     operator changes is hardcoded.
"""
from __future__ import annotations

import datetime as dt

from apps.edge_api.src.proposals import template_render
from apps.edge_api.src.proposals.models import Proposal

_SCHEDULE = [
    {"tier": "First $1,000,000 of Contract Value", "rate": "6.0%"},
    {"tier": "All Contract Value Exceeding $4,000,000", "rate": "1.5%"},
]


def _proposal() -> Proposal:
    return Proposal(
        ref="rs_x",
        template_id="active-operators-growth-agreement",
        client_name="Lone Star Hazmat",
        client_signer_name="Richard Lenius",
        client_title="President",
        client_email="r@lonestarhazmat.com",
        effective_date=dt.date(2026, 6, 10),
        monthly_fee_cents=4_000_000,
        duration_months=9,
        billing_cadence="monthly",
        success_fee_schedule=_SCHEDULE,
        quarterly_total_cents=36_000_000,
        rs_signer_name="Active Operators",
        status="draft",
    )


def test_success_fee_markdown_table_renders_tiers_and_rates() -> None:
    table = template_render.success_fee_markdown_table(_SCHEDULE)
    assert "| First $1,000,000 of Contract Value | 6.0% |" in table
    assert "| All Contract Value Exceeding $4,000,000 | 1.5% |" in table
    assert table.splitlines()[1].strip().startswith("| :---")  # GFM alignment row


def test_success_fee_markdown_table_empty() -> None:
    assert template_render.success_fee_markdown_table([]) == ""


def test_success_fee_table_neutralizes_newlines_and_pipes() -> None:
    # A control char or pipe in a cell must NOT break the single-line GFM row (legal-doc integrity).
    hostile = [{"tier": "First 1M\n\n## INJECTED", "rate": "5.0% | extra\r\nrow"}]
    table = template_render.success_fee_markdown_table(hostile)
    body_lines = table.splitlines()[2:]  # skip header + alignment row
    assert len(body_lines) == 1  # still exactly one GFM row — no split on the embedded newlines
    # The hostile content survives only as inert cell TEXT: the rendered HTML stays one well-formed
    # table (header + a single data row), with no escaped heading/paragraph leaking out of the table.
    html = template_render.markdown_to_html(table)
    assert html.count("<tr>") == 2  # header + the one data row
    assert "<h2" not in html
    assert html.count("<table>") == 1


def test_substitute_markdown_tokens_swaps_block_token() -> None:
    src = "## Fees\n\n{{success_fee_table}}\n\nEnd."
    out = template_render.substitute_markdown_tokens(src, _proposal())
    assert "{{success_fee_table}}" not in out
    assert "| First $1,000,000 of Contract Value | 6.0% |" in out


def test_full_render_carries_every_dynamic_value() -> None:
    p = _proposal()
    body = (
        "## 3. Fees\n\n"
        "Fee of {{monthly_fee}} per month, payable {{billing_cadence}} "
        "({{total}} in aggregate).\n\n"
        "## 4. Success Fees\n\n{{success_fee_table}}\n\n"
        "## 6. Term\n\nInitial committed term of {{duration}} months.\n"
    )
    md = template_render.substitute_markdown_tokens(body, p)
    html = template_render.render_template_html(md, apply_brand=True)
    out = template_render.substitute_tokens(html, template_render.proposal_token_values(p))
    # Inline scalars (edited):
    assert "$40,000" in out  # monthly fee
    assert "monthly in advance" in out  # cadence phrase for 'monthly'
    assert "$360,000" in out  # total = 40k × 9
    assert "9 months" in out  # term
    # The structured tier table (edited rate), rendered as a real HTML table:
    assert "<table" in out
    assert "6.0%" in out
    assert "1.5%" in out
    # And nothing left as a literal unfilled block token:
    assert "{{success_fee_table}}" not in out
