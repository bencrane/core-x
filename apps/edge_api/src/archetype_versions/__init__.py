"""Engagement-template render — STANDALONE path for the docraptor-to-documenso *template* family.

A self-contained surface with NO ties to the engagement-doc (document-only) pathway: its own content
catalog, its own ``__STYLESHEET__`` assembly (NO token substitution — the HTML is a static body),
its own DocRaptor live render, and its own R2 namespace. It renders a repo-resident template to a
clean PDF and hands back a short-lived presigned URL; it does NOT touch Documenso. The operator
takes the PDF and affixes Documenso fields in the editor themselves.

Content tree (under ``apps/edge_api/content/active-operators/``):

    <path>/<archetype>/<version>/global_engagement_content/{manifest.json, *.html, styles/*.css}

e.g. ``docraptor-to-documenso-template/term-only/v1/global_engagement_content/``. The operator selects
(path, archetype, version) in Settings; the render returns the plain-style PDF by default.
"""
