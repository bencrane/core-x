"""Ops watchdog — the fleet's only scheduled failure detector.

One small read-only checker (2026-07-23 modal-durability-closure directive §2). Before
this file existed, NOTHING noticed a dead build, a stale serving artifact, or a
silently-failed ingest — every failure was caught by a human happening to look
(recon: ~/Desktop/hq/sessions/2026-07-23-sidecar-rebuild-recon.md §2.5).

Control plane: Trigger.dev owns cadence (``src/trigger/ops_watchdog.ts``, hourly) and
POSTs the Universal Dispatcher, which ``spawn()``s ``check`` on this DEPLOYED app —
``modal.Cron`` is forbidden in core-x (docs/reference/03_modal_compute.md §1).

Four check classes, all read-only:

1. **Hung sidecar build** — ``ops.query_sidecar_runs`` row with ``started_at`` but no
   ``completed_at`` older than ``hung_build_minutes`` (default 90). NOTE: the builder
   writes its row terminally today, so this check is structurally silent until the
   builder's start-row lands (tracked as a §5 rider of the same directive); it is
   implemented to the directive's spec and arms itself the moment such rows exist.
2. **Stale serving artifact** — ``/healthz`` ``built_at`` older than
   ``healthz_max_age_days`` (default 7), or serving unreachable/not-ready after
   3 bounded retries (a deploy window can 502 transiently; retries absorb it).
3. **Error terminal rows** — any failure-classed ``status`` in the in-scope
   ``ops.*_runs`` ledgers recorded within ``error_window_hours`` (default 2 — sized
   to the hourly cadence so one failure alerts once or twice, not forever).
4. **Cadence expectations** — feeds whose ACTIVE Trigger schedule implies a cadence
   with no success row within 2x that cadence. Every Trigger schedule was parked on
   2026-07-19 (free-plan 10-schedule cap), so ``CADENCE_EXPECTATIONS`` is empty
   today; populate it when schedules are unparked.

Alerts land via ``core.ops_alert.alert()`` -> ``OPS_ALERT_WEBHOOK`` (Telegram
``sendMessage``, ``ops-alerts`` Modal secret). One aggregated message per run; a
quiet run sends nothing. Alerts are the ONLY output — no ledger writes, no R2.

Manual smoke (short, idempotent, read-only — safe as a SYNC run):

    modal deploy pipelines/ops_watchdog/watchdog.py
    modal run pipelines/ops_watchdog/watchdog.py

Durable manual fire (the fleet-doctrine form):

    python3 -c "import modal; fc = modal.Function.from_name('ops-watchdog','check').spawn(); print(fc.object_id)"
"""

from __future__ import annotations

import os

import modal

SIDECAR_HEALTHZ_URL = os.environ.get(
    "SIDECAR_HEALTHZ_URL", "https://query-sidecar-api.onrender.com/healthz"
)

# In-scope ops.*_runs ledgers (business-critical feeds: SAM/DSBS registrant plane,
# USAspending, UCC/SoS + crosswalk hub, resolution, GTM, reference, sidecar).
IN_SCOPE_RUNS_TABLES = [
    "query_sidecar_runs",
    # sam_gov
    "sam_opps_canonical_runs",
    "sam_opps_archived_runs",
    "sam_entity_registration_runs",
    "sam_extraction_runs",
    "sam_master_runs",
    "sam_normalized_entities_runs",
    "sam_pocs_runs",
    "sam_attachment_manifest_runs",
    "sam_attachment_download_runs",
    "sam_attachment_gtm_scope_runs",
    "sam_wage_determination_runs",
    "sam_labor_poc_people_runs",
    # sba
    "crosswalk_dsbs_sam_runs",
    "dsbs_pocs_runs",
    "dsbs_poc_people_runs",
    "dsbs_poc_linkedin_resolve_runs",
    "sba_foia_runs",
    # usaspending
    "usaspending_table_runs",
    "usaspending_archive_runs",
    "usaspending_fpds_canonical_runs",
    "usaspending_award_canonical_runs",
    "usaspending_fpds_mod_delta_runs",
    "usaspending_fpds_prime_award_state_runs",
    "usaspending_fpds_entity_version_runs",
    "usaspending_subaward_canonical_runs",
    "usaspending_api_fresh_runs",
    "usaspending_api_award_fresh_runs",
    "usaspending_api_subaward_fresh_runs",
    "usaspending_award_search_delta_runs",
    "usaspending_award_search_api_landing_runs",
    "ffata_exec_comp_runs",
    "contractor_award_summary_runs",
    "govcon_prime_trajectories_runs",
    # ucc / sos + crosswalk hub
    "ca_ucc_runs",
    "co_ucc_runs",
    "co_ucc_companions_runs",
    "ca_sos_runs",
    "co_sos_entity_runs",
    "fl_sos_runs",
    "ny_sos_corporation_runs",
    "sos_normalized_runs",
    # resolution
    "crosswalk_hmda_gleif_runs",
    "crosswalk_sam_usaspending_runs",
    "crosswalk_sec_adv_ucc_runs",
    "crosswalk_sos_sam_runs",
    "crosswalk_ucc_sos_runs",
    "federal_spine_index_runs",
    "entity_award_lines_gold_runs",
    "sam_ucc_debtor_overlap_runs",
    # gtm
    "gtm_sam_entities_runs",
    "gtm_sam_people_runs",
    "gtm_sam_person_titles_runs",
    "gtm_sam_person_identity_runs",
    "gtm_fpds_entity_signal_events_runs",
    "blitz_hydration_runs",
    # reference
    "industry_cost_structure_runs",
    "labor_share_runs",
    "naics_psc_labor_profile_runs",
    "sca_soc_crosswalk_runs",
]

# feed -> (runs_table, cadence_hours). Populate ONLY from ACTIVE Trigger schedules.
# All 28 Trigger schedules were parked 2026-07-19 (free-plan 10-schedule cap), so no
# feed carries a cadence expectation today. When a schedule is unparked, add its row
# here in the same PR — e.g. "sam_opps_bulk": ("sam_opps_canonical_runs", 24).
CADENCE_EXPECTATIONS: dict[str, tuple[str, int]] = {}

# Failure-classed terminal statuses across the fleet's heterogeneous ledgers
# (error / *_failed / stalled / rejected). 'partial' and 'dry_run' are not failures.
FAILURE_STATUS_REGEX = r"(error|fail|stall|reject)"

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("requests>=2.32", "psycopg[binary]>=3.2")
    .add_local_python_source("core")
)

app = modal.App("ops-watchdog", image=image)


def _check_hung_sidecar_build(cur, hung_build_minutes: int) -> list[str]:
    cur.execute(
        """
        SELECT id, started_at, tiers, launch_mode
        FROM ops.query_sidecar_runs
        WHERE started_at IS NOT NULL
          AND completed_at IS NULL
          AND started_at < now() - make_interval(mins => %s)
        ORDER BY started_at
        """,
        (hung_build_minutes,),
    )
    return [
        f"HUNG BUILD: query_sidecar_runs id={rid} started {started} "
        f"(tiers={tiers}, launch_mode={mode}) has no completed_at after "
        f"{hung_build_minutes} min"
        for rid, started, tiers, mode in cur.fetchall()
    ]


def _check_serving_artifact(healthz_max_age_days: int) -> list[str]:
    import time
    from datetime import datetime, timezone

    import requests

    last_err: str = ""
    for attempt in range(3):
        try:
            resp = requests.get(SIDECAR_HEALTHZ_URL, timeout=15)
            resp.raise_for_status()
            body = resp.json()
            break
        except Exception as exc:  # noqa: BLE001 — any transport/parse failure retries
            last_err = f"{type(exc).__name__}: {exc}"
            time.sleep(5 * (attempt + 1))
    else:
        return [f"SERVING UNREACHABLE: {SIDECAR_HEALTHZ_URL} failed 3x — {last_err}"]

    findings = []
    if not body.get("ready"):
        findings.append(f"SERVING NOT READY: /healthz={body}")
    built_at = body.get("built_at")
    if built_at:
        stamp = datetime.fromisoformat(built_at)
        age_days = (datetime.now(timezone.utc) - stamp).total_seconds() / 86400
        if age_days > healthz_max_age_days:
            findings.append(
                f"STALE ARTIFACT: serving {body.get('artifact')} built "
                f"{age_days:.1f} days ago (threshold {healthz_max_age_days}d)"
            )
    else:
        findings.append(f"STALE ARTIFACT CHECK IMPOSSIBLE: /healthz has no built_at ({body})")
    return findings


def _check_error_rows(cur, error_window_hours: int) -> list[str]:
    cur.execute(
        """
        SELECT c.table_name,
               bool_or(c.column_name = 'recorded_at')  AS has_recorded,
               bool_or(c.column_name = 'completed_at') AS has_completed,
               bool_or(c.column_name = 'status')       AS has_status
        FROM information_schema.columns c
        WHERE c.table_schema = 'ops' AND c.table_name = ANY(%s)
        GROUP BY c.table_name
        """,
        (IN_SCOPE_RUNS_TABLES,),
    )
    findings = []
    for table, has_recorded, has_completed, has_status in cur.fetchall():
        if not has_status or not (has_recorded or has_completed):
            continue
        ts_col = "recorded_at" if has_recorded else "completed_at"
        cur.execute(
            f"""
            SELECT status, count(*), max({ts_col})::text
            FROM ops."{table}"
            WHERE status ~* %s
              AND {ts_col} >= now() - make_interval(hours => %s)
            GROUP BY status
            """,
            (FAILURE_STATUS_REGEX, error_window_hours),
        )
        for status, n, latest in cur.fetchall():
            findings.append(
                f"ERROR ROWS: ops.{table} has {n} '{status}' row(s) in the last "
                f"{error_window_hours}h (latest {latest})"
            )
    return findings


def _check_cadence(cur) -> list[str]:
    findings = []
    for feed, (table, cadence_hours) in CADENCE_EXPECTATIONS.items():
        cur.execute(
            f"""
            SELECT max(completed_at)::text
            FROM ops."{table}"
            WHERE status = 'success'
              AND completed_at >= now() - make_interval(hours => %s)
            """,
            (2 * cadence_hours,),
        )
        (latest,) = cur.fetchone()
        if latest is None:
            findings.append(
                f"CADENCE MISS: {feed} has no success row in ops.{table} within "
                f"2x its {cadence_hours}h cadence"
            )
    return findings


@app.function(
    secrets=[
        modal.Secret.from_name("hqx-postgres"),
        modal.Secret.from_name("ops-alerts"),
    ],
    timeout=60 * 5,
    memory=512,
    cpu=0.5,
    retries=0,
    max_containers=1,
)
def check(
    trigger_callback_url: str | None = None,
    hung_build_minutes: int = 90,
    healthz_max_age_days: int = 7,
    error_window_hours: int = 2,
) -> dict:
    """Run all four checks; alert once, aggregated, only if something is wrong."""
    import psycopg

    from core.ops_alert import alert

    findings: list[str] = []
    checks_failed: list[str] = []

    findings += _check_serving_artifact(healthz_max_age_days)

    try:
        with psycopg.connect(os.environ["HQX_DB_URL_POOLED"], connect_timeout=20) as conn:
            with conn.cursor() as cur:
                findings += _check_hung_sidecar_build(cur, hung_build_minutes)
                findings += _check_error_rows(cur, error_window_hours)
                findings += _check_cadence(cur)
    except Exception as exc:  # noqa: BLE001 — the watchdog itself failing is an alert
        checks_failed.append(f"WATCHDOG DEGRADED: ledger checks failed — {type(exc).__name__}: {exc}")

    all_findings = findings + checks_failed
    for f in all_findings:
        print(f"[watchdog] {f}")
    if not all_findings:
        print("[watchdog] all checks green")

    if all_findings:
        msg = f"[ops-watchdog] {len(all_findings)} finding(s):\n" + "\n".join(
            f"- {f}" for f in all_findings
        )
        alert(msg[:3500])

    result = {
        "status": "success",
        "feed": "ops_watchdog",
        "findings": len(all_findings),
        "detail": all_findings,
    }

    if trigger_callback_url:
        try:
            import requests

            requests.post(trigger_callback_url, json={"status": "success", "rows": len(all_findings), "feed": "ops_watchdog"}, timeout=15)
        except Exception as exc:  # noqa: BLE001 — callback is best-effort
            print(f"WARN: trigger callback failed: {exc}")

    return result


@app.local_entrypoint()
def main() -> None:
    # Short, idempotent, read-only — the leave-class manual smoke path.
    print(check.remote())
