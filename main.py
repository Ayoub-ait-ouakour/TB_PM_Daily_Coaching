"""FastAPI orchestration service: multi-tenant concepts.

Execution modes:
- **Scheduler** (optional): `ENABLE_SCHEDULER=1` — daily job at `DAILY_CRON` in `SCHEDULER_TZ` (default 09:30 Europe/London)
  runs **all concepts** for London yesterday; recall poll at `RECALL_POLL_CRON` or `RECALL_POLL_INTERVAL_SECONDS`.
  Optional **one-shot** daily batch: `SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS` (e.g. 120) schedules `_scheduled_daily` once after that many seconds.
  Tests: `SCHEDULER_SKIP_DAILY_CRON=1` registers only the delayed job (no repeating cron).
- **POST /run-all**: same as scheduler batch — all concepts, all Mongo advisors matching each concept.
- **POST /run/{concept}**: one concept, all matching Mongo advisors.
- **POST /run/{concept}/advisors**: one concept, only Mongo `users._id` values listed in the JSON body.

Optional APScheduler is started in a **FastAPI lifespan** context (not deprecated `on_event` hooks).

"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, AsyncGenerator, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from concepts import list_concept_ids

_log_level = getattr(logging, (os.environ.get("LOG_LEVEL") or "INFO").strip().upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("advisor-api")
logger.setLevel(_log_level)

from workflow_engine import default_run_date, process_concept, process_recalls_for_concept

# Align workflow_engine logger after import (workflow also reads LOG_LEVEL on load).
workflow_engine_logger = logging.getLogger("advisor-orchestration")
workflow_engine_logger.setLevel(_log_level)

_runs_lock = threading.Lock()
# In-memory run registry (per task). With multiple ECS tasks, prefer ADVISOR_OUTREACH_AUDIT_TABLE + batch_run_id.
_RUNS: Dict[str, Dict[str, Any]] = {}


class RunAdvisorsSubsetBody(BaseModel):
    """Mongo `users` document `_id` values (24-char hex ObjectId strings)."""

    mongo_user_ids: List[str] = Field(..., min_length=1)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_run_record(concept_ids: List[str], run_date: str) -> str:
    run_id = str(uuid.uuid4())
    with _runs_lock:
        _RUNS[run_id] = {
            "run_id": run_id,
            "status": "running",
            "concepts": concept_ids,
            "run_date": run_date,
            "started_at": _utc_now_iso(),
            "finished_at": None,
            "results": {},
            "error": None,
        }
    return run_id


def _finish_run(run_id: str, results: Dict[str, Any], error: Optional[str] = None) -> None:
    with _runs_lock:
        if run_id not in _RUNS:
            return
        _RUNS[run_id]["status"] = "failed" if error else "completed"
        _RUNS[run_id]["finished_at"] = _utc_now_iso()
        _RUNS[run_id]["results"] = results
        _RUNS[run_id]["error"] = error


def _scheduled_daily() -> None:
    run_date = default_run_date()
    for cid in list_concept_ids():
        try:
            _, metrics = process_concept(cid, run_date=run_date, enable_recall_tracking=True)
            logger.info("Scheduled run complete %s: %s", cid, metrics)
        except Exception:  # noqa: BLE001
            logger.exception("Scheduled run failed for %s", cid)


def _scheduled_recall_poll() -> None:
    for cid in list_concept_ids():
        try:
            stats = process_recalls_for_concept(cid)
            logger.info("Recall poll complete %s: %s", cid, stats)
        except Exception:  # noqa: BLE001
            logger.exception("Recall poll failed for %s", cid)


_scheduler: Optional[BackgroundScheduler] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start optional APScheduler on startup; stop it on application shutdown."""
    global _scheduler
    _scheduler = None
    if os.environ.get("ENABLE_SCHEDULER", "").strip().lower() in ("1", "true", "yes"):
        tz_name = os.environ.get("SCHEDULER_TZ", "Europe/London")
        tzinfo = ZoneInfo(tz_name)
        skip_daily_cron = os.environ.get("SCHEDULER_SKIP_DAILY_CRON", "").strip().lower() in ("1", "true", "yes")
        cron = os.environ.get("DAILY_CRON", "30 9 * * *")
        parts = cron.split()

        _scheduler = BackgroundScheduler(
            timezone=tz_name,
            job_defaults={
                # Allow execution shortly after scheduled time (clock skew, VM pause, tests with fake clocks).
                "misfire_grace_time": int(os.environ.get("APSCHEDULER_MISFIRE_GRACE_SECONDS", "3600")),
                "coalesce": True,
                "max_instances": 1,
            },
        )

        if not skip_daily_cron and len(parts) == 5:
            minute, hour, day, month, dow = parts
            _scheduler.add_job(
                _scheduled_daily,
                CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow, timezone=tz_name),
                id="daily_all_concepts",
                replace_existing=True,
            )
        elif not skip_daily_cron:
            logger.warning("Invalid DAILY_CRON %r; expected 5 fields — daily cron job not registered", cron)

        first_delay_raw = (os.environ.get("SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS") or "").strip()
        if first_delay_raw:
            try:
                delay_sec = max(1, int(first_delay_raw))
            except ValueError:
                delay_sec = 0
                logger.warning("Invalid SCHEDULER_DAILY_FIRST_RUN_DELAY_SECONDS %r", first_delay_raw)
            if delay_sec:
                run_at = datetime.now(tzinfo) + timedelta(seconds=delay_sec)
                _scheduler.add_job(
                    _scheduled_daily,
                    DateTrigger(run_date=run_at),
                    id="daily_all_concepts_first_delay",
                    replace_existing=True,
                )
                logger.info(
                    "Scheduled one-shot daily batch at %s (%s) in %ss",
                    run_at.isoformat(),
                    tz_name,
                    delay_sec,
                )

        recall_interval_raw = (os.environ.get("RECALL_POLL_INTERVAL_SECONDS") or "").strip()
        recall_parts: List[str] = []
        if recall_interval_raw:
            try:
                recall_sec = max(5, int(recall_interval_raw))
            except ValueError:
                recall_sec = 0
                logger.warning("Invalid RECALL_POLL_INTERVAL_SECONDS %r", recall_interval_raw)
            if recall_sec:
                _scheduler.add_job(
                    _scheduled_recall_poll,
                    IntervalTrigger(seconds=recall_sec, timezone=tz_name),
                    id="recall_poll",
                    replace_existing=True,
                )
                recall_parts = [f"interval={recall_sec}s"]
        else:
            recall_cron = (os.environ.get("RECALL_POLL_CRON") or "*/30 * * * *").strip()
            recall_parts = recall_cron.split()
            if len(recall_parts) != 5:
                logger.warning(
                    "Invalid RECALL_POLL_CRON %r (expected 5 cron fields); using */30 * * * *",
                    recall_cron,
                )
                recall_parts = "*/30 * * * *".split()
            rm, rh, rd, rmon, rdow = recall_parts
            _scheduler.add_job(
                _scheduled_recall_poll,
                CronTrigger(minute=rm, hour=rh, day=rd, month=rmon, day_of_week=rdow, timezone=tz_name),
                id="recall_poll",
                replace_existing=True,
            )

        _scheduler.start()
        logger.info(
            "APScheduler started: daily_cron=%s skip_daily_cron=%s recall=%s tz=%s",
            cron,
            skip_daily_cron,
            " ".join(recall_parts),
            tz_name,
        )
    try:
        yield
    finally:
        if _scheduler is not None:
            _scheduler.shutdown(wait=False)
            _scheduler = None


app = FastAPI(title="Advisor outreach orchestration", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> Dict[str, Any]:
    with _runs_lock:
        if run_id not in _RUNS:
            raise HTTPException(status_code=404, detail="Unknown run_id (in-memory registry is per task).")
        return dict(_RUNS[run_id])


@app.post("/run/{concept}")
def run_concept(concept: str) -> Dict[str, Any]:
    if concept not in list_concept_ids():
        raise HTTPException(status_code=404, detail=f"Unknown concept. Valid: {list_concept_ids()}")

    run_date = default_run_date()
    run_id = _new_run_record([concept], run_date)
    try:
        _batch_id, metrics = process_concept(
            concept,
            run_date=run_date,
            batch_run_id=run_id,
            enable_recall_tracking=False,
        )
        _finish_run(run_id, {concept: {"batch_run_id": _batch_id, "metrics": metrics}})
    except Exception as exc:  # noqa: BLE001
        logger.exception("Run failed for %s", concept)
        _finish_run(run_id, {}, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    with _runs_lock:
        return dict(_RUNS[run_id])


@app.post("/run/{concept}/advisors")
def run_concept_advisors(concept: str, body: RunAdvisorsSubsetBody) -> Dict[str, Any]:
    """Run a single concept for only the given Mongo advisor ids (`userId` = `users._id`)."""
    if concept not in list_concept_ids():
        raise HTTPException(status_code=404, detail=f"Unknown concept. Valid: {list_concept_ids()}")

    run_date = default_run_date()
    run_id = _new_run_record([concept], run_date)
    try:
        _batch_id, metrics = process_concept(
            concept,
            run_date=run_date,
            batch_run_id=run_id,
            mongo_user_ids=body.mongo_user_ids,
            enable_recall_tracking=False,
        )
        _finish_run(run_id, {concept: {"batch_run_id": _batch_id, "metrics": metrics}})
    except Exception as exc:  # noqa: BLE001
        logger.exception("Run failed for %s (subset)", concept)
        _finish_run(run_id, {}, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    with _runs_lock:
        return dict(_RUNS[run_id])


@app.post("/run-all")
def run_all() -> Dict[str, Any]:
    concepts = list_concept_ids()
    run_date = default_run_date()
    run_id = _new_run_record(concepts, run_date)
    results: Dict[str, Any] = {}
    try:
        for cid in concepts:
            batch_id, metrics = process_concept(
                cid,
                run_date=run_date,
                batch_run_id=f"{run_id}:{cid}",
                enable_recall_tracking=True,
            )
            results[cid] = {"batch_run_id": batch_id, "metrics": metrics}
        _finish_run(run_id, results)
    except Exception as exc:  # noqa: BLE001
        logger.exception("run-all failed")
        _finish_run(run_id, results, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    with _runs_lock:
        return dict(_RUNS[run_id])
