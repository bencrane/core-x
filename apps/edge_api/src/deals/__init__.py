"""Deals — the operator pipeline entity (``business.deals`` + ``business.deal_details``).

  queries   — business.deals read access (the Applications/Research list)
  models    — Deal / DealSummary projection

Replaces the booking->opportunity projection as the cockpit's list + Application
detail data source. Read-only here; mutations (booking->deal upsert) land later.
"""
