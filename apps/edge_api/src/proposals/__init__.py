"""Engagement template-authoring support.

The surviving support modules for the template-authoring surface (``proposal_templates_v1``):

  models          — the canonical Proposal data model + fee helpers (consumed by template_render)
  template_models — the authored-template data shapes
  template_queries — business.global_engagement_content persistence (psycopg)
  template_render — markdown → branded HTML, token extraction/substitution
  signing_anchors — the ``[[CLIENT_SIGNATURE]]`` / ``[[CLIENT_DATE]]`` marker constants
"""
