"""cal.com webhook surface — RAW CAPTURE (Phase 1).

``POST /webhooks/cal`` verifies cal.com's HMAC signature and lands the VERBATIM
payload into ``public.cal_raw_events`` (the append-only raw SoR). That is the whole
job for now: capture the real payload off a live booking.

Normalizing it into ``corex.bookings`` — created / cancelled / rescheduled — is a
SEPARATE, later step wired against the ACTUAL captured shape, not guessed ahead of
seeing it.
"""
