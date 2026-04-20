"""Live Mongo + Supabase integration: advisors → Supabase mapping → calls & meetings.

Requires:
  RUN_DATA_INTEGRATION_TESTS=1
  MONGO_URI (or MONGO_URL), MONGO_DB_NAME (or MONGO_DB)
  Concept-specific Supabase (see workflow_engine.resolve_supabase_credentials_for_concept):
    people_manager: PM_SUPABASE_* or SUPABASE_*
    t_and_b: TB_SUPABASE_* or SUPABASE_*

Optional:
  OVERRIDE_RUN_DATE=YYYY-MM-DD — same as production workflow (dates_london / workflow_engine.default_run_date)
  DATA_FETCH_TEST_MAX_ADVISORS=N — cap per concept (default 25)
  DATA_FETCH_TEST_CONCEPTS=people_manager,t_and_b — subset of concepts (default: all from concepts.list_concept_ids)
  DATA_FETCH_TEST_PRINT=1 — print fetched summary to stdout (default 1). Set 0 to silence.
  DATA_FETCH_TEST_PRINT_FULL_CALLS=1 — include full call documents in print (can be large).
  DATA_FETCH_TEST_PRINT_FULL_NAC_COACHING=1 — include full latest NAC / coaching Mongo documents.
  DATA_FETCH_TEST_DAILY_PAYLOAD_ACTIVITY=any — when to build/print daily_payload (same gate as production):
    any (default): yesterday calls > 0 OR meetings > 0
    both: calls > 0 AND meetings > 0

Run pytest with -s (or --capture=no) so prints appear in the terminal.

OpenAI/VAPI assistant env vars are not required for this read-only test; placeholders are injected if unset.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import pytest

from concepts import list_concept_ids
from dates_london import yesterday_london_iso
from workflow_engine import ConceptWorkflow, SharedServiceConfig, resolve_concept_from_env


pytestmark = pytest.mark.integration


def _enabled() -> bool:
    return os.environ.get("RUN_DATA_INTEGRATION_TESTS", "").strip() == "1"


def _max_advisors() -> int:
    raw = os.environ.get("DATA_FETCH_TEST_MAX_ADVISORS", "").strip()
    if not raw:
        return 25
    try:
        return max(1, int(raw))
    except ValueError:
        return 25


def _run_date() -> str:
    override = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip()
    return override if override else yesterday_london_iso()


def _env_flag(name: str, default: str = "1") -> bool:
    v = (os.environ.get(name) or default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _log_print(msg: str) -> None:
    if _env_flag("DATA_FETCH_TEST_PRINT", "1"):
        print(msg, flush=True)


def _truncate(s: Any, limit: int = 240) -> str:
    t = "" if s is None else str(s).strip()
    if len(t) <= limit:
        return t
    return t[: limit - 3] + "..."


def _has_yesterday_activity(calls_count: int, meetings_n: int) -> bool:
    """Match process_single_advisor gate: skip VAPI only when both are zero."""
    mode = (os.environ.get("DATA_FETCH_TEST_DAILY_PAYLOAD_ACTIVITY") or "any").strip().lower()
    if mode in ("both", "and", "all"):
        return calls_count > 0 and meetings_n > 0
    return calls_count > 0 or meetings_n > 0


@pytest.fixture
def dummy_vapi_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid requiring real LLM/VAPI credentials for read-only fetch tests."""
    if not os.environ.get("OPENAI_API_KEY"):
        monkeypatch.setenv("OPENAI_API_KEY", "integration-test-placeholder-openai")
    if not os.environ.get("VAPI_API_KEY"):
        monkeypatch.setenv("VAPI_API_KEY", "integration-test-placeholder-vapi")


@pytest.fixture
def workflow(dummy_vapi_openai: None, monkeypatch: pytest.MonkeyPatch, concept_id: str) -> ConceptWorkflow:
    from concepts import get_concept_definition

    raw = get_concept_definition(concept_id)
    assistant_env = str(raw["vapi_assistant_id_env"])
    if not os.environ.get(assistant_env):
        monkeypatch.setenv(assistant_env, "00000000-0000-4000-8000-000000000001")

    concept = resolve_concept_from_env(concept_id)
    shared = SharedServiceConfig()
    cw = ConceptWorkflow(concept, shared)
    cw._active_run_date = _run_date()
    yield cw
    cw.close()


def pytest_generate_tests(metafunc: Any) -> None:
    if "concept_id" not in metafunc.fixturenames:
        return
    known = frozenset(list_concept_ids())
    concepts = os.environ.get("DATA_FETCH_TEST_CONCEPTS", "").strip()
    if concepts:
        ids_list = [x.strip() for x in concepts.split(",") if x.strip()]
    else:
        ids_list = list(known)
    unknown = [x for x in ids_list if x not in known]
    if unknown:
        raise pytest.UsageError(f"Unknown DATA_FETCH_TEST_CONCEPTS entries (not in concepts): {unknown}")
    metafunc.parametrize("concept_id", ids_list)


@pytest.mark.skipif(not _enabled(), reason="Set RUN_DATA_INTEGRATION_TESTS=1 to run live Mongo/Supabase fetch tests.")
def test_advisors_mapped_supabase_then_calls_and_meetings(workflow: ConceptWorkflow, concept_id: str) -> None:
    """Mongo advisors → Supabase map → calls/meetings/NAC/coaching → daily_payload when yesterday has activity."""
    run_date = _run_date()
    cap = _max_advisors()
    calls_date_col = workflow.concept.calls_date_col
    print_full_calls = _env_flag("DATA_FETCH_TEST_PRINT_FULL_CALLS", "0")
    print_full_nc = _env_flag("DATA_FETCH_TEST_PRINT_FULL_NAC_COACHING", "0")
    c = workflow.concept

    mongo_advisors = workflow.get_advisors_from_mongo()
    assert isinstance(mongo_advisors, list)

    mapped = workflow.map_advisors_to_supabase_phone(mongo_advisors)
    assert isinstance(mapped, list)
    sample = mapped[:cap]

    _log_print("")
    _log_print(f"========== DATA FETCH CHECK [{concept_id}] run_date={run_date} ==========")
    _log_print(f"Mongo advisors (advisor_query): {len(mongo_advisors)}")
    _log_print(f"Mapped with valid Supabase row + E.164 phone: {len(mapped)}")
    _log_print(f"Printing up to {cap} mapped advisors (DATA_FETCH_TEST_MAX_ADVISORS)")
    _log_print("")

    for advisor in sample:
        assert advisor.mongo_user_id
        assert advisor.supabase_advisor_id
        assert advisor.e164_phone.startswith("+"), advisor.e164_phone

        row = workflow.fetch_supabase_user_row(
            email=advisor.email,
            mongo_user_id_str=advisor.mongo_user_id,
        )
        assert row is not None
        sid = str(row.get(workflow.concept.supabase_advisor_id_col, "")).strip()
        assert sid == advisor.supabase_advisor_id

        uid = advisor.peoplemanager_id or advisor.mongo_user_id
        yesterday_calls: List[Dict[str, Any]] = workflow.fetch_yesterday_customer_calls_from_mongo(uid, run_date)
        assert isinstance(yesterday_calls, list)
        for doc in yesterday_calls:
            assert doc.get("feedbackType") == "call"

        meetings_n = workflow.fetch_meetings_yesterday_count_from_supabase(advisor.supabase_advisor_id, run_date)
        assert isinstance(meetings_n, int)
        assert meetings_n >= 0

        nac_row: Optional[Dict[str, Any]] = workflow.fetch_latest_nac_from_mongo(uid)
        coaching_row: Optional[Dict[str, Any]] = workflow.fetch_latest_coaching_from_mongo(uid)
        assert nac_row is None or isinstance(nac_row, dict)
        assert coaching_row is None or isinstance(coaching_row, dict)

        phone_col = workflow.concept.supabase_phone_col
        email_col = workflow.concept.supabase_email_col
        _log_print(f"--- advisor: {advisor.advisor_name!r} ---")
        _log_print(f"  mongo_user_id:       {advisor.mongo_user_id}")
        _log_print(f"  email (Mongo norm): {advisor.email!r}")
        _log_print(f"  e164_phone:          {advisor.e164_phone}")
        _log_print(f"  supabase_advisor_id: {advisor.supabase_advisor_id}")
        _log_print(
            f"  Supabase row ({email_col}, {phone_col}): "
            f"{row.get(email_col)!r} | raw {phone_col}={row.get(phone_col)!r}"
        )
        _log_print(f"  yesterday_calls ({workflow.concept.mongo_calls_collection}): {len(yesterday_calls)}")
        for i, doc in enumerate(yesterday_calls[:5]):
            if print_full_calls:
                try:
                    _log_print(f"    call[{i}] {json.dumps(doc, default=str, indent=2)}")
                except Exception:
                    _log_print(f"    call[{i}] {doc!r}")
            else:
                brief = {
                    "_id": doc.get("_id"),
                    calls_date_col: doc.get(calls_date_col),
                    "feedbackType": doc.get("feedbackType"),
                    "userId": doc.get(workflow.concept.calls_user_id_col),
                }
                _log_print(f"    call[{i}] {json.dumps(brief, default=str)}")
        if len(yesterday_calls) > 5:
            _log_print(f"    ... and {len(yesterday_calls) - 5} more calls (omit or set DATA_FETCH_TEST_PRINT_FULL_CALLS=1)")
        _log_print(f"  meetings_yesterday ({workflow.concept.supabase_meetings_table}): {meetings_n}")

        _log_print(f"  latest_nac ({c.mongo_nac_collection}) order_by={c.nac_order_col}: {'found' if nac_row else 'none'}")
        if nac_row:
            if print_full_nc:
                try:
                    _log_print(f"    {json.dumps(nac_row, default=str, indent=2)}")
                except Exception:
                    _log_print(f"    {nac_row!r}")
            else:
                brief_nac = {
                    "_id": nac_row.get("_id"),
                    c.nac_date_col: nac_row.get(c.nac_date_col),
                    c.nac_text_col: _truncate(nac_row.get(c.nac_text_col)),
                }
                _log_print(f"    {json.dumps(brief_nac, default=str)}")

        _log_print(
            f"  latest_coaching ({c.mongo_coaching_collection}) order_by={c.coaching_order_col}: "
            f"{'found' if coaching_row else 'none'}"
        )
        if coaching_row:
            if print_full_nc:
                try:
                    _log_print(f"    {json.dumps(coaching_row, default=str, indent=2)}")
                except Exception:
                    _log_print(f"    {coaching_row!r}")
            else:
                brief_co = {
                    "_id": coaching_row.get("_id"),
                    c.coaching_order_col: coaching_row.get(c.coaching_order_col),
                    c.coaching_summary_col: _truncate(coaching_row.get(c.coaching_summary_col)),
                }
                _log_print(f"    {json.dumps(brief_co, default=str)}")

        calls_count = len(yesterday_calls)
        if _has_yesterday_activity(calls_count, meetings_n):
            daily_payload = workflow.build_daily_payload(
                advisor,
                run_date,
                calls_count,
                meetings_n,
                nac_row,
                coaching_row,
            )
            assert isinstance(daily_payload, dict)
            assert daily_payload.get("Calls yesterday") == calls_count
            assert daily_payload.get("Meetings yesterday") == meetings_n
            assert daily_payload.get("Advisor Name") == advisor.advisor_name
            _log_print("  daily_payload (build_daily_payload — would send to VAPI variableValues):")
            try:
                _log_print(json.dumps(daily_payload, default=str, indent=2))
            except Exception:
                _log_print(f"    {daily_payload!r}")
        else:
            _log_print(
                "  daily_payload: skipped (no yesterday activity per DATA_FETCH_TEST_DAILY_PAYLOAD_ACTIVITY; "
                f"calls={calls_count}, meetings={meetings_n})"
            )

        _log_print("")

    _log_print(f"========== END [{concept_id}] ==========")
    _log_print("")
