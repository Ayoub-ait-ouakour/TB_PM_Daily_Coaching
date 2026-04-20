"""
CLI batch runner (same pipeline as FastAPI `main.py`).

Examples:
  python -m advisor_daily_workflow
  python -m advisor_daily_workflow --concept people_manager
  python -m advisor_daily_workflow --all-concepts
  python -m advisor_daily_workflow --concept t_and_b --mongo-user-ids 507f1f77bcf86cd799439011,507f191e810c19729de860ea

Prefer `uvicorn main:app` for HTTP scheduling and `/run`, `/run-all`, `/run/{concept}/advisors`.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from concepts import list_concept_ids
from workflow_engine import default_run_date, process_concept

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("advisor-daily-workflow-cli")


def _bridge_legacy_env_to_people_manager() -> None:
    if not os.environ.get("VAPI_PM_ASSISTANT_ID") and os.environ.get("VAPI_ASSISTANT_ID"):
        os.environ["VAPI_PM_ASSISTANT_ID"] = os.environ["VAPI_ASSISTANT_ID"]


def _load_env_file() -> None:
    """Load project .env for direct CLI runs."""
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", override=False)


def main() -> None:
    _load_env_file()
    parser = argparse.ArgumentParser(description="Advisor outreach — Mongo advisors → Supabase → VAPI")
    parser.add_argument(
        "--concept",
        default="people_manager",
        help="Concept id: people_manager | t_and_b",
    )
    parser.add_argument(
        "--mongo-user-ids",
        default=None,
        metavar="IDS",
        help="Comma-separated Mongo users._id hex strings (subset run for one concept)",
    )
    parser.add_argument(
        "--all-concepts",
        action="store_true",
        help="Run every concept with full Mongo advisor lists",
    )
    args = parser.parse_args()

    _bridge_legacy_env_to_people_manager()
    run_date = default_run_date()

    if args.all_concepts:
        for cid in list_concept_ids():
            batch_id, metrics = process_concept(cid, run_date=run_date, enable_recall_tracking=True)
            logger.info("[%s] batch_run_id=%s metrics=%s", cid, batch_id, metrics)
        return

    if args.mongo_user_ids:
        ids = [x.strip() for x in args.mongo_user_ids.split(",") if x.strip()]
        if not ids:
            parser.error("--mongo-user-ids requires at least one id")
        batch_id, metrics = process_concept(
            args.concept,
            run_date=run_date,
            mongo_user_ids=ids,
            enable_recall_tracking=False,
        )
        logger.info("Subset run batch_run_id=%s metrics=%s", batch_id, metrics)
        return

    logger.info("CLI run %s for date %s (London yesterday unless OVERRIDE_RUN_DATE)", args.concept, run_date)
    batch_id, metrics = process_concept(args.concept, run_date=run_date, enable_recall_tracking=False)
    logger.info("Done batch_run_id=%s metrics=%s", batch_id, metrics)


if __name__ == "__main__":
    main()
