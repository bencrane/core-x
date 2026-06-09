"""Bookings — the cal.com-derived booking surface (read-only, Phase 1).

``corex.bookings`` is the normalized form of a cal.com BOOKING_CREATED event
(``public.cal_raw_events`` → consumer → here). This package exposes the operator
list the Pipeline tab renders; enrichment + origination attach in later phases.
"""
