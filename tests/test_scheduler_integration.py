"""Live scheduler end-to-end: real VAPI outbound, real ``daily_coach_tracking`` writes, real recall polls.

No mocks for ``process_concept`` or ``process_recalls_for_concept`` — APScheduler invokes the same code paths as production.

**Enable:** ``RUN_SCHEDULER_TRACKING_LIVE_TEST=1`` plus full ``.env`` (Mongo, Supabase per concept, OpenAI, VAPI, assistants).

**Defaults in test** (override via env before pytest):

- ``SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS=120`` — first daily batch ~2 minutes after scheduler starts  
- ``RECALL_POLL_INTERVAL_SECONDS=60`` — recall poll interval (minimum 5 in ``main.py``)  
- Two recall cycles are waited **after** the daily window: ``2 * RECALL_POLL_INTERVAL_SECONDS`` (+ buffer)

Optional scope (not fake recall — only fewer concepts):

- ``SCHEDULER_TEST_CONCEPT_IDS=people_manager``

Recall behaviour:

- ``RECALL_POLL_IGNORE_LONDON_HOURS=1`` — required for most off-hours test runs  
- ``RECALL_MAX_CALL_ATTEMPTS=4`` — allows initial dial + up to three recall dials (tune for “two recall attempts”)

Run with ``pytest -s`` so step prints appear in the terminal.

Example:

  ``RUN_SCHEDULER_TRACKING_LIVE_TEST=1 pytest tests/test_scheduler_integration.py -s``
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import pytest

import concepts as concepts_mod


def _today_run_date() -> str:
    from dates_london import london_today_date

    return london_today_date().isoformat()


def _log(msg: str) -> None:
    print(f"[scheduler-tracking-live] {msg}", flush=True)


def _sleep_progress(total_s: int, label: str, chunk_s: int = 30) -> None:
    elapsed = 0
    while elapsed < total_s:
        step = min(chunk_s, total_s - elapsed)
        time.sleep(step)
        elapsed += step
        _log(f"{label} ... {elapsed}s / {total_s}s")


def _fetch_tracking_rows(concept_id: str, run_date: Optional[str] = None) -> List[Dict[str, Any]]:
    from workflow_engine import ConceptWorkflow, SharedServiceConfig, resolve_concept_from_env

    run_date = run_date or _today_run_date()
    rc = resolve_concept_from_env(concept_id)
    wf = ConceptWorkflow(rc, SharedServiceConfig())
    try:
        tbl = wf._tracking_table()
        r = (
            wf.supabase.table(tbl)
            .select("*")
            .eq("run_date", run_date)
            .eq("concept", concept_id)
            .execute()
        )
        return list(getattr(r, "data", None) or [])
    finally:
        wf.close()


def _print_tracking_snapshot(step_name: str, concept_ids: List[str], run_date: str) -> Dict[str, List[Dict[str, Any]]]:
    _log(f"--- {step_name} | run_date={run_date} ---")
    out: Dict[str, List[Dict[str, Any]]] = {}
    for cid in concept_ids:
        rows = _fetch_tracking_rows(cid, run_date)
        out[cid] = rows
        _log(f"concept={cid!r} table=daily_coach_tracking row_count={len(rows)}")
        for i, row in enumerate(rows):
            brief = {
                "id": row.get("id"),
                "customer_number": row.get("customer_number"),
                "advisor_name": row.get("advisor_name"),
                "called_count": row.get("called_count"),
                "final_status": row.get("final_status"),
                "current_vapi_call_id": row.get("current_vapi_call_id"),
                "last_classification_reason": row.get("last_classification_reason"),
                "last_call_at": row.get("last_call_at"),
            }
            _log(f"  row[{i}] {json.dumps(brief, default=str)}")
        if len(rows) == 0:
            _log(f"  (no rows for {cid})")
    return out


def _tracking_live_enabled() -> bool:
    return os.environ.get("RUN_SCHEDULER_TRACKING_LIVE_TEST", "").strip() == "1"


@pytest.mark.integration
@pytest.mark.scheduler
@pytest.mark.skipif(
    not _tracking_live_enabled(),
    reason="Set RUN_SCHEDULER_TRACKING_LIVE_TEST=1 for live scheduler + coaching track test.",
)
def test_live_scheduler_real_daily_then_two_recall_cycles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real scheduler: delayed daily batch, wait, snapshot DB, wait 2 recall intervals, snapshot DB.

    Uses **no** patches on ``process_concept`` / ``process_recalls_for_concept`` — only env + optional concept scope.
    """
    # --- Configure (defaults: ~2 min first daily, 60s recall interval, 2 recall cycles after daily) ---
    first_delay_s = int(
        (
            os.environ.get("SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS")
            or os.environ.get("SCHEDULER_FIRST_DELAY_SECONDS")
            or "120"
        ).strip()
        or "120"
    )
    recall_iv_s = max(
        5,
        int(
            (
                os.environ.get("RECALL_POLL_INTERVAL_SECONDS")
                or os.environ.get("SCHEDULER_RECALL_INTERVAL_SECONDS")
                or "60"
            ).strip()
            or "60"
        ),
    )
    post_daily_buffer_s = int((os.environ.get("SCHEDULER_POST_DAILY_BUFFER_SECONDS") or "15").strip() or "15")
    recall_cycles = int((os.environ.get("SCHEDULER_RECALL_CYCLES") or "2").strip() or "2")

    monkeypatch.setenv("ENABLE_SCHEDULER", "1")
    monkeypatch.setenv("SCHEDULER_SKIP_DAILY_CRON", "1")
    monkeypatch.setenv("SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS", str(first_delay_s))
    monkeypatch.setenv("RECALL_POLL_INTERVAL_SECONDS", str(recall_iv_s))
    monkeypatch.setenv("RECALL_POLL_IGNORE_LONDON_HOURS", "1")
    monkeypatch.setenv("RECALL_MAX_CALL_ATTEMPTS", (os.environ.get("RECALL_MAX_CALL_ATTEMPTS") or "4").strip() or "4")

    narrow = (os.environ.get("SCHEDULER_TEST_CONCEPT_IDS") or "").strip()
    orig_list = concepts_mod.list_concept_ids

    def _list_concepts() -> List[str]:
        if narrow:
            out = [x.strip() for x in narrow.split(",") if x.strip()]
            unknown = [x for x in out if x not in orig_list()]
            if unknown:
                raise pytest.UsageError(f"Unknown SCHEDULER_TEST_CONCEPT_IDS: {unknown}")
            return out
        return orig_list()

    monkeypatch.setattr(concepts_mod, "list_concept_ids", _list_concepts)

    concept_ids = _list_concepts()
    run_date = _today_run_date()

    wait_after_daily_s = first_delay_s + post_daily_buffer_s
    wait_recall_phase_s = recall_cycles * recall_iv_s + post_daily_buffer_s

    _log("=== Configuration ===")
    _log(f"concepts={concept_ids} (narrow via SCHEDULER_TEST_CONCEPT_IDS if set)")
    _log(f"SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS={first_delay_s} (~first daily batch after start)")
    _log(f"RECALL_POLL_INTERVAL_SECONDS={recall_iv_s}")
    _log(f"Plan: start app -> wait {wait_after_daily_s}s (daily + buffer) -> print tracking ->")
    _log(f"        wait {wait_recall_phase_s}s ({recall_cycles} recall intervals + buffer) -> print tracking again")
    _log("Run pytest with -s to see this output.")

    if "main" in sys.modules:
        del sys.modules["main"]
    import main as main_mod

    importlib.reload(main_mod)

    _log("=== Step 1: Start FastAPI lifespan (APScheduler starts; jobs are real, not mocked) ===")
    from fastapi.testclient import TestClient

    with TestClient(main_mod.app):
        _log("=== Step 2: Wait for delayed daily batch (process_concept per concept, recall_tracking on) ===")
        _sleep_progress(wait_after_daily_s, "waiting for daily batch window")

        snap_a = _print_tracking_snapshot("Step 3 - daily_coach_tracking after daily batch window", concept_ids, run_date)

        _log("=== Step 4: Wait for recall poll intervals (real GET VAPI + classifier + Supabase updates) ===")
        _sleep_progress(wait_recall_phase_s, "waiting for recall poll cycles")

        snap_b = _print_tracking_snapshot(
            "Step 5 - daily_coach_tracking after recall phase",
            concept_ids,
            run_date,
        )

    _log("=== Step 6: Summary ===")
    total_rows_a = sum(len(v) for v in snap_a.values())
    total_rows_b = sum(len(v) for v in snap_b.values())
    _log(f"total rows after daily window: {total_rows_a}")
    _log(f"total rows after recall phase: {total_rows_b}")

    if total_rows_a == 0:
        pytest.skip(
            "No rows in daily_coach_tracking after daily batch — no successful outbound VAPI with tracking "
            "(advisors skipped or failed). Check advisors have yesterday activity + mapped phone + run outcomes."
        )

    changed = False
    for cid in concept_ids:
        before = {str(r.get("id")): r for r in snap_a.get(cid, [])}
        after = {str(r.get("id")): r for r in snap_b.get(cid, [])}
        for rid, row_b in after.items():
            row_a = before.get(rid)
            if row_a is None:
                changed = True
                continue
            if int(row_b.get("called_count") or 0) != int(row_a.get("called_count") or 0):
                changed = True
            if str(row_b.get("final_status") or "") != str(row_a.get("final_status") or ""):
                changed = True
            if str(row_b.get("last_classification_reason") or "") != str(
                row_a.get("last_classification_reason") or ""
            ):
                changed = True
            if str(row_b.get("current_vapi_call_id") or "") != str(row_a.get("current_vapi_call_id") or ""):
                changed = True

    if not changed:
        _log(
            "WARNING: No detectable row changes after recall phase — recall may have found no rows, "
            "classifier returned real_conversation on first poll, or polls had nothing to update."
        )
    else:
        _log("OK: At least one coaching track row changed between snapshots (recall path touched Supabase).")
