"""Deals — the operator pipeline entity (``business.deals`` + ``business.deal_details``).

  queries     — business.deals read access (the Applications/Research list)
  models      — Deal / DealSummary projection
  materialize — the booking → deal producer (account+contact+deal+deal_contacts)

Replaces the booking->opportunity projection as the cockpit's list + Application detail
data source AND as the booking producer: the cal webhook's deal-materialize task projects
each new booking into one deal per account (advancing last_booking_id).
"""
