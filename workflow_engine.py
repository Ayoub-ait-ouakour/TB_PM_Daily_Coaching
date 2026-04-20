"""Shared pipeline: Mongo advisors (full docs) → Supabase phone + advisor id → Mongo calls + Supabase meetings → NAC/coaching → VAPI."""

from __future__ import annotations

import base64
import json
import logging
import os
import uuid
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import date, datetime
from zoneinfo import ZoneInfo

import phonenumbers
import requests
from bson import ObjectId
from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient
from supabase import Client, create_client
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from concepts import ResolvedConcept, get_concept_definition
from dates_london import london_today_date, yesterday_london_iso, yesterday_london_utc_bounds

logger = logging.getLogger("advisor-orchestration")
LONDON = ZoneInfo("Europe/London")

# Load project .env for direct module/CLI usage (does not override already-exported env vars).
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)


def apply_log_level_from_env(root_logger: Optional[logging.Logger] = None) -> None:
    """Set levels from LOG_LEVEL (DEBUG, INFO, WARNING, ERROR). Applies to advisor-orchestration and optional root."""
    raw = (os.environ.get("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, raw, logging.INFO)
    logger.setLevel(level)
    if root_logger is not None:
        root_logger.setLevel(level)
    # APScheduler is noisy at INFO during tests; quiet unless DEBUG.
    sched = logging.getLogger("apscheduler")
    sched.setLevel(logging.DEBUG if level <= logging.DEBUG else logging.WARNING)


apply_log_level_from_env()


def _recall_max_call_attempts() -> int:
    """0 = unlimited. When > 0, no further recall dials once called_count >= this (stops recall pool)."""
    raw = (os.environ.get("RECALL_MAX_CALL_ATTEMPTS") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _recall_poll_ignore_london_hours() -> bool:
    """If true, recall polls run outside Mon–Fri 09:00–17:00 London (for staging/integration tests)."""
    return os.environ.get("RECALL_POLL_IGNORE_LONDON_HOURS", "").strip().lower() in ("1", "true", "yes", "on")


def _coerce_daily_payload_from_tracking(val: Any) -> Dict[str, Any]:
    """Supabase/Postgres JSON may return as dict or as a serialized string; VAPI expects a JSON object."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str) and val.strip():
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def yesterday_utc_date() -> str:
    """Deprecated for scheduling; use yesterday_london_iso(). Kept for tests/import compatibility."""
    return yesterday_london_iso()


def _normalize_mongo_uri(raw: str) -> str:
    """Fix common .env typo: MONGO_URI=MONGO_URL=mongodb+srv://..."""
    uri = (raw or "").strip()
    if uri.startswith("MONGO_URL="):
        uri = uri[len("MONGO_URL=") :].strip()
    return uri


def _shared_mongo_uri_and_db() -> Tuple[str, str]:
    uri = _normalize_mongo_uri(
        os.environ.get("MONGO_URI")
        or os.environ.get("MONGO_URL")
        or os.environ.get("PM_MONGO_URI")
        or os.environ.get("TB_MONGO_URI")
        or ""
    )
    if not uri:
        raise RuntimeError(
            "Set MONGO_URI or MONGO_URL (shared MongoDB for all concepts). "
            "Legacy: PM_MONGO_URI / TB_MONGO_URI also work."
        )
    db_name = (
        os.environ.get("MONGO_DB_NAME")
        or os.environ.get("MONGO_DB")
        or os.environ.get("TB_MONGO_DB")
        or os.environ.get("PM_MONGO_DB")
        or ""
    ).strip()
    if not db_name:
        raise RuntimeError(
            "Set MONGO_DB_NAME or MONGO_DB (shared DB name). Legacy: TB_MONGO_DB / PM_MONGO_DB also work."
        )
    return uri, db_name


def default_run_date() -> str:
    """London previous working day, unless OVERRIDE_RUN_DATE=YYYY-MM-DD (manual/debug)."""
    override = (os.environ.get("OVERRIDE_RUN_DATE") or "").strip()
    if override:
        return override
    return yesterday_london_iso()


_NAC_FEEDBACK_TEXT_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("areasForImprovement", "reportText"),
    ("AreasForImprovement", "reportText"),
    ("dailynacfeedback", "areasForImprovement", "reportText"),
    ("KeyCoachingRecommendations", "reportText"),
    ("keyCoachingRecommendations", "reportText"),
    ("KeyCoachingRecommendations", "report_text"),
    ("key_coaching_recommendations", "report_text"),
)

_COACHING_INSIGHT_KEYS: Tuple[str, ...] = (
    "strengths",
    "improvements",
    "engagement_level",
    "productivity_signal",
    "follow_up_recommended",
)

# Keys on dailynacfeedbacks rows that are not feedback sections (Mongo metadata / linkage).
_NAC_FEEDBACK_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "_id",
        "id",
        "userId",
        "user_id",
        "email",
        "date",
        "Date",
        "receivedAt",
        "createdAt",
        "updatedAt",
        "timestamp",
        "feedbackType",
        "__v",
        "v",
        # Sometimes the whole nested blob lives here; we recurse into it separately.
        "dailynacfeedback",
        "dailynacfeedbacks",
    }
)


def _report_text_from_feedback_block(block: Any) -> Optional[str]:
    """Extract text from {'reportText': ...} or legacy snake_case variants."""
    if not isinstance(block, dict):
        return None
    for rk in ("reportText", "report_text", "ReportText"):
        if rk in block and block[rk] is not None:
            s = str(block[rk]).strip()
            return s
    return None


def _feedback_sections_from_mapping(
    row: Dict[str, Any],
    *,
    skip_keys: frozenset[str],
) -> Dict[str, Dict[str, str]]:
    """Collect MeetingConversionRate / TopPerformanceGaps-style { reportText } sections."""
    out: Dict[str, Dict[str, str]] = {}
    for key, val in row.items():
        if key in skip_keys:
            continue
        text = _report_text_from_feedback_block(val)
        if text is None:
            continue
        out[key] = {"reportText": text}
    return out


def _get_nested(doc: Dict[str, Any], *keys: str) -> Any:
    cur: Any = doc
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _feedback_summary_object_from_nac_row(concept_text_col: str, row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Structured Feedback Summary for daily_payload: all NAC sections as { Name: { reportText } }.

    Matches dailynacfeedbacks documents where each category (e.g. MeetingConversionRate,
    TopPerformanceGaps) is an object with ``reportText``. Mongo field order is preserved.
    """
    skip = frozenset(_NAC_FEEDBACK_METADATA_KEYS | {concept_text_col})
    out = _feedback_sections_from_mapping(row, skip_keys=skip)

    if not out:
        for nested_key in ("dailynacfeedback", "dailynacfeedbacks"):
            inner = row.get(nested_key)
            if isinstance(inner, dict):
                inner_skip = frozenset(_NAC_FEEDBACK_METADATA_KEYS)
                out = _feedback_sections_from_mapping(inner, skip_keys=inner_skip)
                if out:
                    break

    if out:
        return out

    # Legacy: older rows used fixed camelCase sections or a single text column.
    legacy_three = ("areasForImprovement", "wordsOfEncouragement", "dailyPerformanceOverview")

    def _block_text(section: str) -> str:
        candidates: Tuple[Tuple[str, ...], ...] = (
            (section, "reportText"),
            (section[0].upper() + section[1:] if section else section, "reportText"),
        )
        for path in candidates:
            val = _get_nested(row, *path)
            if val is not None and str(val).strip():
                return str(val).strip()
        return ""

    legacy_out: Dict[str, Dict[str, str]] = {}
    for k in legacy_three:
        text = _block_text(k)
        legacy_out[k] = {"reportText": text}

    if any(v.get("reportText") for v in legacy_out.values()):
        return legacy_out

    fallback = ""
    for path in _NAC_FEEDBACK_TEXT_PATHS:
        val = _get_nested(row, *path)
        if val is not None and str(val).strip():
            fallback = str(val).strip()
            break
    if not fallback:
        legacy = row.get(concept_text_col)
        if legacy is not None and str(legacy).strip():
            fallback = str(legacy).strip()
    if fallback:
        return {"areasForImprovement": {"reportText": fallback}}
    return {}


def _stringify_coaching_insight_value(val: Any) -> Union[str, Dict[str, Any], List[Any]]:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return val
    s = str(val).strip()
    return s


def _memory_object_from_coaching_row(row: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Memory payload: coaching_insights only (from daily_users_coaching_vapi)."""
    raw = row.get("coaching_insights")
    if not isinstance(raw, dict):
        raw = {}
    insights: Dict[str, Any] = {}
    for k in _COACHING_INSIGHT_KEYS:
        insights[k] = _stringify_coaching_insight_value(raw.get(k))
    if not any(v not in ("", {}, []) for v in insights.values()):
        legacy = row.get("previous_call_summary") or row.get("call_summary")
        if legacy is not None and str(legacy).strip():
            insights["strengths"] = str(legacy).strip()
    return {"coaching_insights": insights}


def _compute_performance_tier(calls_yesterday: int, meetings_yesterday: int) -> str:
    """Derived from yesterday's call/meeting counts (not stored in Mongo)."""
    performance_tier = "INTERMEDIATE"
    if calls_yesterday <= 1 and meetings_yesterday <= 0:
        performance_tier = "LOW"
    if calls_yesterday >= 5 or meetings_yesterday >= 2:
        performance_tier = "HIGH"
    return performance_tier


def _coaching_context_label(concept_id: str) -> str:
    if concept_id == "people_manager":
        return "CRM PM"
    if concept_id == "t_and_b":
        return "CRM TB"
    return concept_id


def _first_env(*keys: str) -> str:
    for k in keys:
        v = os.environ.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    raise RuntimeError(f"Missing required env (tried): {keys}")


def _jwt_role_claim(supabase_key: str) -> Optional[str]:
    """Decode Supabase JWT role claim (anon | authenticated | service_role). Key may be invalid JWT."""
    parts = supabase_key.strip().split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        r = payload.get("role")
        return str(r).strip() if r else None
    except Exception:
        return None


def warn_if_supabase_key_wrong_role(concept_id: str, supabase_key: str) -> None:
    """Mis-pasted anon key hits PostgREST as anon and often gets permission denied on writes."""
    role = _jwt_role_claim(supabase_key)
    if role and role != "service_role":
        logger.warning(
            "[supabase] concept=%s JWT role is '%s' (expected 'service_role'). "
            "Writes to daily_coach_tracking may fail; use the service_role key from Supabase Settings -> API.",
            concept_id,
            role,
        )


def resolve_supabase_credentials_for_concept(concept_id: str) -> Tuple[str, str]:
    """PM/T&B-specific Supabase projects; falls back to SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY."""
    if concept_id == "people_manager":
        url = _first_env("PM_SUPABASE_URL", "SUPABASE_URL")
        key = _first_env("PM_SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_ROLE_KEY")
    elif concept_id == "t_and_b":
        url = _first_env("TB_SUPABASE_URL", "SUPABASE_URL")
        key = _first_env("TB_SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_ROLE_KEY")
    else:
        url = _first_env("SUPABASE_URL")
        key = _first_env("SUPABASE_SERVICE_ROLE_KEY")
    return url, key


def resolve_concept_from_env(concept_id: str) -> ResolvedConcept:
    raw = get_concept_definition(concept_id)

    def _req(name: str) -> str:
        v = os.environ.get(name)
        if not v:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return v

    def _opt(env_key: str) -> Optional[str]:
        v = os.environ.get(env_key)
        return v.strip() if v and str(v).strip() else None

    phone_id_env = raw.get("vapi_phone_number_id_env")
    vapi_phone_number_id = _opt(str(phone_id_env)) if phone_id_env else None

    mongo_uri, mongo_db_name = _shared_mongo_uri_and_db()
    supabase_url, supabase_key = resolve_supabase_credentials_for_concept(concept_id)
    warn_if_supabase_key_wrong_role(concept_id, supabase_key)

    return ResolvedConcept(
        concept_id=concept_id,
        mongo_uri=mongo_uri,
        mongo_db_name=mongo_db_name,
        mongo_users_collection=raw["mongo_users_collection"],
        mongo_calls_collection=raw["mongo_calls_collection"],
        mongo_nac_collection=raw["mongo_nac_collection"],
        mongo_coaching_collection=raw["mongo_coaching_collection"],
        advisor_query=dict(raw["advisor_query"]),
        supabase_lookup_mode=raw["supabase_lookup_mode"],
        supabase_user_table=raw["supabase_user_table"],
        supabase_email_col=raw["supabase_email_col"],
        supabase_phone_col=raw["supabase_phone_col"],
        supabase_user_role_col=raw["supabase_user_role_col"],
        supabase_user_role_value=raw["supabase_user_role_value"],
        supabase_peoplemanager_id_col=raw["supabase_peoplemanager_id_col"],
        calls_user_id_col=raw["calls_user_id_col"],
        calls_date_col=raw["calls_date_col"],
        nac_user_id_col=raw["nac_user_id_col"],
        nac_date_col=raw["nac_date_col"],
        nac_text_col=raw["nac_text_col"],
        nac_order_col=raw["nac_order_col"],
        coaching_user_id_col=raw["coaching_user_id_col"],
        coaching_summary_col=raw["coaching_summary_col"],
        coaching_order_col=raw["coaching_order_col"],
        supabase_advisor_id_col=raw["supabase_advisor_id_col"],
        supabase_meetings_table=raw["supabase_meetings_table"],
        supabase_meetings_advisor_id_col=raw["supabase_meetings_advisor_id_col"],
        supabase_meetings_date_col=raw["supabase_meetings_date_col"],
        meetings_date_match_mode=raw["meetings_date_match_mode"],
        supabase_url=supabase_url,
        supabase_service_role_key=supabase_key,
        vapi_assistant_id=_req(raw["vapi_assistant_id_env"]),
        vapi_phone_number_id=vapi_phone_number_id,
    )


@dataclass
class AdvisorRecord:
    mongo_user_id: str
    advisor_name: str
    email: str
    e164_phone: str
    # Canonical linkage id = str(Mongo users document _id); matches Supabase userId & users.peoplemanager_id (PM).
    peoplemanager_id: Optional[str]
    # Supabase users row primary key (or configured column); joins meetings.advisor_id.
    supabase_advisor_id: str
    # Full Mongo users document (all fields) after serialization (_id as str).
    mongo_document: Dict[str, Any]


class SharedServiceConfig:
    """Env shared across all concepts (Supabase URL/key are per-concept on ResolvedConcept)."""

    def __init__(self) -> None:
        self.openai_api_key = os.environ["OPENAI_API_KEY"]
        self.openai_model = os.environ.get("OPENAI_MODEL", "gpt-4o")
        self.vapi_api_key = os.environ["VAPI_API_KEY"]
        self.vapi_base_url = os.environ.get("VAPI_BASE_URL", "https://api.vapi.ai")
        _phone_id = os.environ.get("VAPI_PHONE_NUMBER_ID", "").strip()
        self.vapi_phone_number_id = _phone_id or None
        self.default_region = os.environ.get("PHONE_DEFAULT_REGION", "US")
        self.vapi_max_concurrent = max(1, int(os.environ.get("VAPI_MAX_CONCURRENT_PER_CONCEPT", "3")))
        self.daily_coach_tracking_table = "daily_coach_tracking"


class ConceptWorkflow:
    def __init__(self, concept: ResolvedConcept, shared: SharedServiceConfig) -> None:
        self.concept = concept
        self.shared = shared
        self.mongo = MongoClient(concept.mongo_uri)
        self.mongo_db = self.mongo[concept.mongo_db_name]
        self._active_run_date: Optional[str] = None
        self._logs_root = Path(__file__).resolve().parent / "logs"
        # Thread-local HTTP/Supabase: Session is not shared safely across threads.
        self._tls = threading.local()

    @property
    def supabase(self) -> Client:
        client = getattr(self._tls, "supabase", None)
        if client is None:
            client = create_client(
                self.concept.supabase_url,
                self.concept.supabase_service_role_key,
            )
            self._tls.supabase = client
        return client

    @property
    def http(self) -> requests.Session:
        session = getattr(self._tls, "http", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "Authorization": f"Bearer {self.shared.vapi_api_key}",
                    "Content-Type": "application/json",
                }
            )
            self._tls.http = session
        return session

    @property
    def openai(self) -> OpenAI:
        client = getattr(self._tls, "openai", None)
        if client is None:
            client = OpenAI(api_key=self.shared.openai_api_key)
            self._tls.openai = client
        return client

    def close(self) -> None:
        self.mongo.close()

    def _concept_log_path(self, kind: str) -> Path:
        run_date = self._active_run_date or london_today_date().isoformat()
        concept_dir = self._logs_root / self.concept.concept_id
        concept_dir.mkdir(parents=True, exist_ok=True)
        return concept_dir / f"{run_date}_{kind}.log"

    def _write_concept_log(self, kind: str, message: str) -> None:
        ts = datetime.now(LONDON).isoformat()
        path = self._concept_log_path(kind)
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{ts} | {message}\n")

    def get_advisors_from_mongo(self, mongo_user_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Advisors matching concept `advisor_query`; optional filter by Mongo `users._id` (hex or str)."""
        collection = self.mongo_db[self.concept.mongo_users_collection]
        query: Dict[str, Any] = dict(self.concept.advisor_query)
        if mongo_user_ids:
            ids_in: List[Any] = []
            for raw in mongo_user_ids:
                s = (raw or "").strip()
                if not s:
                    continue
                if len(s) == 24 and ObjectId.is_valid(s):
                    ids_in.append(ObjectId(s))
                else:
                    ids_in.append(s)
            if not ids_in:
                return []
            query["_id"] = {"$in": ids_in}
        return [self._serialize_mongo_doc(doc) for doc in collection.find(query)]

    def _mongo_advisor_id_str(self, doc: Dict[str, Any]) -> Optional[str]:
        """Mongo users._id — same value as userId / peoplemanager_id keys elsewhere."""
        oid = doc.get("_id")
        if oid is None:
            return None
        s = str(oid).strip()
        return s or None

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_supabase_user_row(self, *, email: str, mongo_user_id_str: str) -> Optional[Dict[str, Any]]:
        """
        Fetch one advisor row from Supabase `users` using concept lookup mode.

        Query is always an AND operation:
          - role column must match configured advisor role
          - lookup key matches either email (TB) or peoplemanager_id (PM)
        """
        c = self.concept
        cols = f"{c.supabase_email_col},{c.supabase_phone_col},{c.supabase_advisor_id_col}"
        q = self.supabase.table(c.supabase_user_table).select(cols).eq(
            c.supabase_user_role_col, c.supabase_user_role_value
        )
        mode = (c.supabase_lookup_mode or "").strip().lower()
        if mode == "email":
            q = q.eq(c.supabase_email_col, email)
        elif mode == "peoplemanager_id":
            q = q.eq(c.supabase_peoplemanager_id_col, mongo_user_id_str)
        else:
            raise RuntimeError(f"Unknown supabase_lookup_mode: {c.supabase_lookup_mode}")
        response = q.limit(1).execute()
        if not response.data:
            return None
        return dict(response.data[0])

    def fetch_phone_from_supabase_by_email(self, email: str) -> Optional[str]:
        row = self.fetch_supabase_user_row(email=email, mongo_user_id_str="")
        return row.get(self.concept.supabase_phone_col) if row else None

    def fetch_phone_from_supabase_by_peoplemanager_id(self, mongo_user_id_str: str) -> Optional[str]:
        row = self.fetch_supabase_user_row(email="", mongo_user_id_str=mongo_user_id_str)
        return row.get(self.concept.supabase_phone_col) if row else None

    def map_advisors_to_supabase_phone(
        self, mongo_advisors: List[Dict[str, Any]]
    ) -> List[AdvisorRecord]:
        valid: List[AdvisorRecord] = []
        mode = self.concept.supabase_lookup_mode
        c = self.concept
        for advisor in mongo_advisors:
            email = (advisor.get("email") or "").strip().lower()
            name = (advisor.get("name") or "Unknown advisor").strip()
            mongo_id_str = self._mongo_advisor_id_str(advisor)
            if not mongo_id_str:
                logger.warning("Skipping advisor %s: missing Mongo _id", email or "?")
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor email={email or '?'} reason=missing_mongo_id",
                )
                continue

            if mode == "email" and not email:
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor mongo_user_id={mongo_id_str} reason=missing_email_for_email_lookup",
                )
                continue
            row = self.fetch_supabase_user_row(email=email, mongo_user_id_str=mongo_id_str)

            if not row:
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor name={name} email={email or '?'} mongo_user_id={mongo_id_str} reason=no_supabase_user_match",
                )
                continue
            phone = row.get(c.supabase_phone_col)
            advisor_id_raw = row.get(c.supabase_advisor_id_col)
            if not phone:
                logger.info(
                    "Skipping advisor %s: no phone in Supabase users row",
                    email or mongo_id_str,
                )
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor name={name} email={email or '?'} mongo_user_id={mongo_id_str} reason=missing_supabase_phone",
                )
                continue
            if advisor_id_raw is None or str(advisor_id_raw).strip() == "":
                logger.warning(
                    "Skipping advisor %s: Supabase row missing %s",
                    email or mongo_id_str,
                    c.supabase_advisor_id_col,
                )
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor name={name} email={email or '?'} mongo_user_id={mongo_id_str} reason=missing_supabase_advisor_id",
                )
                continue

            e164_phone = self.to_e164(phone)
            if not e164_phone:
                logger.warning("Skipping advisor %s, invalid phone: %s", email or mongo_id_str, phone)
                self._write_concept_log(
                    "mapping",
                    f"skipped advisor name={name} email={email or '?'} mongo_user_id={mongo_id_str} reason=invalid_phone raw_phone={phone}",
                )
                continue

            mongo_doc = dict(advisor)
            valid.append(
                AdvisorRecord(
                    mongo_user_id=mongo_id_str,
                    advisor_name=name,
                    email=email,
                    e164_phone=e164_phone,
                    peoplemanager_id=mongo_id_str,
                    supabase_advisor_id=str(advisor_id_raw).strip(),
                    mongo_document=mongo_doc,
                )
            )
            logger.info(
                "Advisor mapped [%s] name=%s email=%s mongo_user_id=%s supabase_advisor_id=%s e164_phone=%s",
                c.concept_id,
                name,
                email,
                mongo_id_str,
                str(advisor_id_raw).strip(),
                e164_phone,
            )
            self._write_concept_log(
                "mapping",
                f"mapped advisor name={name} email={email} mongo_user_id={mongo_id_str} supabase_advisor_id={str(advisor_id_raw).strip()} e164_phone={e164_phone}",
            )
        return valid

    def to_e164(self, raw_phone: str) -> Optional[str]:
        compact = str(raw_phone or "").strip()
        compact = "".join(compact.split())
        if not compact:
            return None
        if compact.startswith("+"):
            return compact

        digits = "".join(ch for ch in compact if ch.isdigit())
        if not digits:
            return None

        if digits.startswith("0") and len(digits) >= 10:
            return f"+44{digits[1:]}"

        return f"+{digits}"

    def _serialize_mongo_doc(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(doc)
        if "_id" in out:
            out["_id"] = str(out["_id"])
        return out

    def _mongo_user_id_match_filter(self, field_name: str, user_id: str) -> Dict[str, Any]:
        """Match rows by advisor id whether stored as string or ObjectId.

        Combines:
        - Compass-style ``$expr: { $eq: [ { $toString: \"$userId\" }, \"...\" ] }``
        - Plain equality on string (index-friendly)
        - Plain equality on ObjectId when ``user_id`` is 24-char hex

        Using ``$or`` avoids silent failures when only ``$expr`` or only equality applies.
        """
        uid = (user_id or "").strip()
        path = "$" + field_name
        clauses: List[Dict[str, Any]] = [
            {"$expr": {"$eq": [{"$toString": path}, uid]}},
            {field_name: uid},
        ]
        if len(uid) == 24 and ObjectId.is_valid(uid):
            clauses.append({field_name: ObjectId(uid)})
        return {"$or": clauses}

    def _calls_date_range_yesterday(self, run_date: str) -> Dict[str, Any]:
        """Run date as UTC half-open interval [lo, hi) on BSON/datetime `date` field."""
        lo, hi = yesterday_london_utc_bounds(run_date)
        return {"$gte": lo, "$lt": hi}

    def _run_date_weekday_name(self, run_date: str) -> str:
        return date.fromisoformat(run_date).strftime("%A")

    def _run_date_display(self, run_date: str) -> str:
        d = date.fromisoformat(run_date)
        return f"{d.strftime('%A')} {d.strftime('%d-%m-%Y')}"

    def _advisor_supabase_user_id(self, advisor: AdvisorRecord) -> Optional[str]:
        """Mongo userId on related collections = str(Mongo users._id)."""
        return advisor.peoplemanager_id

    def fetch_yesterday_customer_calls_from_mongo(
        self, user_id: str, run_date: str
    ) -> List[Dict[str, Any]]:
        """All call rows for userId with `calls_date_col` in [start, end) of run date in Europe/London."""
        c = self.concept
        coll = self.mongo_db[c.mongo_calls_collection]
        date_range = self._calls_date_range_yesterday(run_date)
        configured_date_col = c.calls_date_col
        candidate_date_cols = [configured_date_col]
        # Real datasets may use "Date" while config/tests use "date".
        if configured_date_col.lower() == "date":
            candidate_date_cols.extend(["Date", "date"])
        candidate_date_cols = list(dict.fromkeys(candidate_date_cols))
        q: Dict[str, Any] = {
            "$and": [
                self._mongo_user_id_match_filter(c.calls_user_id_col, user_id),
                {"feedbackType": "call"},
                {"$or": [{col: date_range} for col in candidate_date_cols]},
            ]
        }
        docs = list(coll.find(q))
        return [self._serialize_mongo_doc(d) for d in docs]

    def last_customer_call_yesterday(
        self, yesterday_calls: List[Dict[str, Any]], run_date: str
    ) -> Optional[Dict[str, Any]]:
        """Most recent call row among yesterday's rows (date field, then finer timestamps if tied)."""
        if not yesterday_calls:
            return None
        c = self.concept
        date_key = c.calls_date_col

        def _sort_key(row: Dict[str, Any]) -> Tuple[Any, Any]:
            d = row.get(date_key)
            tie = (
                row.get("call_datetime")
                or row.get("startedAt")
                or row.get("receivedAt")
                or row.get("endedAt")
                or ""
            )
            return (d, tie)

        return max(yesterday_calls, key=_sort_key)

    def fetch_latest_nac_from_mongo(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Latest nacfeedbacks row for userId (by nac_order_col), no date filter."""
        c = self.concept
        coll = self.mongo_db[c.mongo_nac_collection]
        q = self._mongo_user_id_match_filter(c.nac_user_id_col, user_id)
        cur = coll.find(q).sort(c.nac_order_col, -1).limit(1)
        docs = list(cur)
        if not docs:
            return None
        return self._serialize_mongo_doc(docs[0])

    def fetch_latest_coaching_from_mongo(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Latest coaching row for userId (Mongo users._id); uses previous_call_summary column."""
        c = self.concept
        coll = self.mongo_db[c.mongo_coaching_collection]
        q = self._mongo_user_id_match_filter(c.coaching_user_id_col, user_id)
        cur = coll.find(q).sort(c.coaching_order_col, -1).limit(1)
        docs = list(cur)
        if not docs:
            return None
        return self._serialize_mongo_doc(docs[0])

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_meetings_yesterday_count_from_supabase(self, advisor_supabase_id: str, run_date: str) -> int:
        """Meetings linked to Supabase advisor id with meeting_date in London run_date (range or calendar day)."""
        c = self.concept
        mode = (c.meetings_date_match_mode or "utc_bounds").strip().lower()
        if mode == "iso_day":
            q = (
                self.supabase.table(c.supabase_meetings_table)
                .select("*", count="exact")
                .eq(c.supabase_meetings_advisor_id_col, advisor_supabase_id)
                .eq(c.supabase_meetings_date_col, run_date)
            )
        else:
            lo, hi = yesterday_london_utc_bounds(run_date)
            q = (
                self.supabase.table(c.supabase_meetings_table)
                .select("*", count="exact")
                .eq(c.supabase_meetings_advisor_id_col, advisor_supabase_id)
                .gte(c.supabase_meetings_date_col, lo.isoformat())
                .lt(c.supabase_meetings_date_col, hi.isoformat())
            )
        response = q.execute()
        cnt = getattr(response, "count", None)
        if cnt is not None:
            return int(cnt)
        data = getattr(response, "data", None) or []
        return len(data)

    def build_daily_payload(
        self,
        advisor: AdvisorRecord,
        run_date: str,
        calls_yesterday: int,
        meetings_yesterday: int,
        last_nac_feedback_row: Optional[Dict[str, Any]],
        last_coaching_row: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Structured fields for VAPI assistantOverrides.variableValues.daily_payload (display labels)."""
        c = self.concept
        nac_payload = last_nac_feedback_row or {}
        coaching_payload = last_coaching_row or {}
        feedback_summary = _feedback_summary_object_from_nac_row(c.nac_text_col, nac_payload)
        memory = _memory_object_from_coaching_row(coaching_payload)
        calls_n = calls_yesterday
        meetings_n = meetings_yesterday
        tier = _compute_performance_tier(calls_n, meetings_n)
        return {
            "repport_date": self._run_date_display(run_date),
            "Advisor Name": advisor.advisor_name,
            "Performance Tier": tier,
            "Calls yesterday": calls_n,
            "Meetings yesterday": meetings_n,
            "Feedback Summary": feedback_summary,
            "Coaching Context": _coaching_context_label(c.concept_id),
            "Memory": memory,
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def call_vapi_advisor(
        self, advisor: AdvisorRecord, daily_payload: Dict[str, Any]
    ) -> Tuple[int, str, Optional[str]]:
        endpoint = f"{self.shared.vapi_base_url}/call"
        phone_number_id = self.concept.vapi_phone_number_id or self.shared.vapi_phone_number_id
        body: Dict[str, Any] = {
            "assistantId": self.concept.vapi_assistant_id,
            "phoneNumberId": phone_number_id,
            "customer": {"number": advisor.e164_phone},
            "assistantOverrides": {
                "variableValues": {
                    "advisor_name": advisor.advisor_name,
                    "daily_payload": daily_payload,
                },
            },
        }
        if phone_number_id:
            body["phoneNumberId"] = phone_number_id
        logger.info(
            "[VAPI] outbound dial concept=%s advisor=%s e164=%s POST %s assistant_id=%s phone_number_id=%s",
            self.concept.concept_id,
            advisor.advisor_name,
            advisor.e164_phone,
            endpoint,
            self.concept.vapi_assistant_id,
            phone_number_id or "(none)",
        )
        logger.debug("[VAPI] outbound dial body assistantOverrides.variableValues keys=%s", list(body.keys()))
        response = self.http.post(endpoint, json=body, timeout=30)
        vapi_id: Optional[str] = None
        try:
            payload = response.json()
            if isinstance(payload, dict):
                vapi_id = payload.get("id") or payload.get("callId")
        except Exception:
            pass
        if response.status_code >= 400:
            logger.error(
                "[VAPI] HTTP error concept=%s advisor=%s status=%s response=%s",
                self.concept.concept_id,
                advisor.email,
                response.status_code,
                (response.text or "")[:1200],
            )
            raise RuntimeError(
                f"VAPI call failed for {advisor.email}: {response.status_code} {response.text}"
            )
        logger.info(
            "[VAPI] success concept=%s advisor=%s status=%s vapi_call_id=%s",
            self.concept.concept_id,
            advisor.email,
            response.status_code,
            vapi_id or "(none)",
        )
        if response.status_code < 400 and not vapi_id:
            logger.warning(
                "[VAPI] response OK but no call id in JSON concept=%s advisor=%s raw_preview=%s",
                self.concept.concept_id,
                advisor.email,
                (response.text or "")[:500],
            )
        return response.status_code, response.text, vapi_id

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def call_vapi_number(
        self, customer_number: str, advisor_name: str, daily_payload: Dict[str, Any]
    ) -> Tuple[int, str, Optional[str]]:
        endpoint = f"{self.shared.vapi_base_url}/call"
        phone_number_id = self.concept.vapi_phone_number_id or self.shared.vapi_phone_number_id
        body: Dict[str, Any] = {
            "assistantId": self.concept.vapi_assistant_id,
            "phoneNumberId": phone_number_id,
            "customer": {"number": customer_number},
            "assistantOverrides": {
                "variableValues": {
                    "advisor_name": advisor_name,
                    "daily_payload": daily_payload,
                },
            },
        }
        if phone_number_id:
            body["phoneNumberId"] = phone_number_id
        logger.info(
            "[VAPI] recall dial concept=%s customer=%s POST %s assistant_id=%s phone_number_id=%s",
            self.concept.concept_id,
            customer_number,
            endpoint,
            self.concept.vapi_assistant_id,
            phone_number_id or "(none)",
        )
        response = self.http.post(endpoint, json=body, timeout=30)
        vapi_id: Optional[str] = None
        try:
            payload = response.json()
            if isinstance(payload, dict):
                vapi_id = payload.get("id") or payload.get("callId")
        except Exception:
            pass
        if response.status_code >= 400:
            logger.error(
                "[VAPI] recall dial HTTP error concept=%s customer=%s status=%s response=%s",
                self.concept.concept_id,
                customer_number,
                response.status_code,
                (response.text or "")[:1200],
            )
            raise RuntimeError(
                f"VAPI call failed for {customer_number}: {response.status_code} {response.text}"
            )
        logger.info(
            "[VAPI] recall dial success concept=%s status=%s vapi_call_id=%s",
            self.concept.concept_id,
            response.status_code,
            vapi_id or "(none)",
        )
        return response.status_code, response.text, vapi_id

    def _tracking_table(self) -> str:
        return self.shared.daily_coach_tracking_table

    def _tracking_run_date_today(self) -> str:
        return london_today_date().isoformat()

    def _is_london_working_hours(self) -> bool:
        now = datetime.now(LONDON)
        if now.weekday() >= 5:
            return False
        return 9 <= now.hour < 17

    def _upsert_tracking_row(
        self,
        *,
        concept: str,
        customer_number: str,
        advisor_name: str,
        daily_payload: Dict[str, Any],
        vapi_call_id: str,
        called_count: int,
        final_status: str,
        last_classification_reason: Optional[str],
        run_date: Optional[str] = None,
    ) -> None:
        run_date_value = run_date or self._tracking_run_date_today()
        tbl = self._tracking_table()
        q = (
            self.supabase.table(tbl)
            .select("id,called_count")
            .eq("run_date", run_date_value)
            .eq("concept", concept)
            .eq("customer_number", customer_number)
            .limit(1)
            .execute()
        )
        rows = list(getattr(q, "data", None) or [])
        if rows:
            row_id = rows[0]["id"]
            self.supabase.table(tbl).update(
                {
                    "advisor_name": advisor_name,
                    "daily_paylaod": daily_payload,
                    "current_vapi_call_id": vapi_call_id,
                    "called_count": called_count,
                    "last_call_at": datetime.now().astimezone().isoformat(),
                    "last_classification_reason": last_classification_reason,
                    "final_status": final_status,
                }
            ).eq("id", row_id).execute()
            return
        ins = (
            self.supabase.table(tbl)
            .insert(
                {
                    "run_date": run_date_value,
                    "concept": concept,
                    "customer_number": customer_number,
                    "advisor_name": advisor_name,
                    "daily_paylaod": daily_payload,
                    "current_vapi_call_id": vapi_call_id,
                    "called_count": called_count,
                    "last_call_at": datetime.now().astimezone().isoformat(),
                    "last_classification_reason": last_classification_reason,
                    "final_status": final_status,
                }
            )
            .execute()
        )
        err = getattr(ins, "error", None)
        if err:
            logger.error("[tracking] Supabase insert returned error key: %s", err)

    def _save_initial_tracking_row(
        self,
        advisor: AdvisorRecord,
        daily_payload: Dict[str, Any],
        vapi_call_id: Optional[str],
        enable_recall_tracking: bool,
    ) -> None:
        if not enable_recall_tracking:
            return
        if not vapi_call_id:
            logger.warning(
                "[tracking] skip daily_coach_tracking row (no vapi id after dial) concept=%s email=%s advisor=%s",
                self.concept.concept_id,
                advisor.email,
                advisor.advisor_name,
            )
            self._write_concept_log(
                "tracking",
                f"skipped row: vapi_call_id empty after outbound dial advisor={advisor.advisor_name}",
            )
            return
        try:
            self._upsert_tracking_row(
                concept=self.concept.concept_id,
                customer_number=advisor.e164_phone,
                advisor_name=advisor.advisor_name,
                daily_payload=daily_payload,
                vapi_call_id=vapi_call_id,
                called_count=1,
                final_status="recall",
                last_classification_reason="initial_call_created",
            )
        except Exception as exc:
            err_txt = str(exc).lower()
            hint = ""
            if "permission denied" in err_txt or "42501" in err_txt:
                hint = (
                    " Fix: run scripts/grant_daily_coach_tracking.sql in the T&B Supabase SQL Editor, "
                    "and ensure TB_SUPABASE_SERVICE_ROLE_KEY is the service_role JWT (not anon)."
                )
            logger.exception(
                "[tracking] Supabase upsert FAILED concept=%s table=%s advisor=%s%s",
                self.concept.concept_id,
                self._tracking_table(),
                advisor.email,
                hint,
            )
            self._write_concept_log(
                "tracking",
                f"supabase upsert FAILED: {type(exc).__name__}: {str(exc)[:800]}{hint}",
            )
            raise
        logger.info(
            "[tracking] row upserted concept=%s advisor=%s vapi_call_id=%s table=%s",
            self.concept.concept_id,
            advisor.email,
            vapi_call_id,
            self._tracking_table(),
        )

    def fetch_tracking_rows_for_today_recall(self) -> List[Dict[str, Any]]:
        run_date_value = self._tracking_run_date_today()
        tbl = self._tracking_table()
        r = (
            self.supabase.table(tbl)
            .select("*")
            .eq("run_date", run_date_value)
            .eq("concept", self.concept.concept_id)
            .eq("final_status", "recall")
            .execute()
        )
        return list(getattr(r, "data", None) or [])

    def fetch_vapi_call_by_id(self, call_id: str) -> Dict[str, Any]:
        endpoint = f"{self.shared.vapi_base_url}/call/{call_id}"
        response = self.http.get(endpoint, timeout=30)
        if response.status_code >= 400:
            raise RuntimeError(f"Failed VAPI call fetch {call_id}: {response.status_code} {response.text}")
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _extract_transcript_and_ended_reason(self, call_payload: Dict[str, Any]) -> Tuple[str, str]:
        body = call_payload.get("body") if isinstance(call_payload.get("body"), dict) else {}
        ended_reason = str(
            body.get("endedReason")
            or call_payload.get("endedReason")
            or call_payload.get("endReason")
            or call_payload.get("statusReason")
            or ""
        ).strip()
        transcript = None
        artifact = body.get("artifact")
        if isinstance(artifact, dict):
            transcript = artifact.get("transcript")
        if transcript is None:
            transcript = call_payload.get("transcript")
        if isinstance(transcript, str):
            transcript_text = transcript.strip()
        elif isinstance(transcript, list):
            transcript_text = "\n".join(str(x) for x in transcript if x is not None).strip()
        else:
            transcript_text = ""
        return transcript_text, ended_reason

    def classify_call_outcome(self, transcript_text: str, ended_reason: str) -> Tuple[str, str]:
        prompt = (
            "Classify call outcome into one label: real_conversation, voicemail, no_response.\n"
            "Return strict JSON with keys label and reason.\n"
            "real_conversation means advisor answered and interacted with assistant.\n"
            f"ended_reason: {ended_reason}\n"
            f"transcript: {transcript_text[:8000]}"
        )
        resp = self.openai.chat.completions.create(
            model=self.shared.openai_model,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a strict classifier."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        content = (resp.choices[0].message.content or "").strip()
        try:
            obj = json.loads(content)
            label = str(obj.get("label", "")).strip().lower()
            reason = str(obj.get("reason", "")).strip()
        except Exception:
            label = ""
            reason = content[:500]
        if label not in {"real_conversation", "voicemail", "no_response"}:
            label = "no_response"
            reason = reason or "Fallback classification due to malformed model output."
        return label, reason

    def process_recalls_for_today(self) -> Dict[str, int]:
        stats = {
            "checked": 0,
            "responded": 0,
            "recalled": 0,
            "skipped_outside_hours": 0,
            "skipped_max_attempts": 0,
            "failed": 0,
        }
        if not _recall_poll_ignore_london_hours() and not self._is_london_working_hours():
            stats["skipped_outside_hours"] += 1
            logger.info("Recall poll skipped outside London working hours for concept=%s", self.concept.concept_id)
            self._write_concept_log(
                "recall",
                "daily_coach_tracking recall poll skipped: outside London working hours",
            )
            return stats
        rows = self.fetch_tracking_rows_for_today_recall()
        self._write_concept_log(
            "recall",
            f"daily_coach_tracking rows fetched for recall={len(rows)}",
        )
        for row in rows:
            stats["checked"] += 1
            try:
                call_id = str(row.get("current_vapi_call_id") or "").strip()
                if not call_id:
                    self._write_concept_log(
                        "recall",
                        f"skipped row_id={row.get('id')} reason=missing_current_vapi_call_id",
                    )
                    continue
                logger.info(
                    "[recall] processing row concept=%s row_id=%s vapi_call_id=%s advisor=%s",
                    self.concept.concept_id,
                    row.get("id"),
                    call_id,
                    row.get("advisor_name"),
                )
                payload = self.fetch_vapi_call_by_id(call_id)
                transcript_text, ended_reason = self._extract_transcript_and_ended_reason(payload)
                logger.debug(
                    "[recall] vapi GET payload keys=%s transcript_len=%s ended_reason=%s",
                    list(payload.keys()) if isinstance(payload, dict) else type(payload),
                    len(transcript_text),
                    ended_reason[:200] if ended_reason else "",
                )
                label, reason = self.classify_call_outcome(transcript_text, ended_reason)
                logger.info(
                    "[recall] classification concept=%s label=%s reason_preview=%s",
                    self.concept.concept_id,
                    label,
                    (reason[:200] + "…") if len(reason) > 200 else reason,
                )
                advisor_name = str(row.get("advisor_name") or "Advisor").strip() or "Advisor"
                customer_number = str(row.get("customer_number") or "").strip()
                normalized_number = self.to_e164(customer_number) or customer_number
                if label == "real_conversation":
                    self.supabase.table(self._tracking_table()).update(
                        {
                            "final_status": "responded",
                            "last_classification_reason": reason,
                            "last_call_at": datetime.now().astimezone().isoformat(),
                        }
                    ).eq("id", row["id"]).execute()
                    self._write_concept_log(
                        "recall",
                        f"advisor={advisor_name} classification={label} reason={reason} normalized_number={normalized_number} new_vapi_call_id=",
                    )
                    stats["responded"] += 1
                    continue
                max_attempts = _recall_max_call_attempts()
                cc0 = int(row.get("called_count") or 0)
                if max_attempts > 0 and cc0 >= max_attempts:
                    stats["skipped_max_attempts"] += 1
                    self._write_concept_log(
                        "recall",
                        f"skipped row_id={row.get('id')} reason=max_attempts called_count={cc0} max={max_attempts}",
                    )
                    continue
                daily_payload = _coerce_daily_payload_from_tracking(row.get("daily_paylaod"))
                _s, _t, new_call_id = self.call_vapi_number(customer_number, advisor_name, daily_payload)
                called_count = int(row.get("called_count") or 0) + 1
                if new_call_id:
                    self.supabase.table(self._tracking_table()).update(
                        {
                            "current_vapi_call_id": new_call_id,
                            "called_count": called_count,
                            "last_call_at": datetime.now().astimezone().isoformat(),
                            "last_classification_reason": reason,
                            "final_status": "recall",
                        }
                    ).eq("id", row["id"]).execute()
                    self._write_concept_log(
                        "recall",
                        f"advisor={advisor_name} classification={label} reason={reason} normalized_number={normalized_number} new_vapi_call_id={new_call_id}",
                    )
                    stats["recalled"] += 1
            except Exception as exc:
                logger.exception("Recall poll failed row for concept=%s row_id=%s", self.concept.concept_id, row.get("id"))
                err_short = str(exc).strip().replace("\n", " ")
                if len(err_short) > 800:
                    err_short = err_short[:797] + "..."
                self._write_concept_log(
                    "recall",
                    f"failed row_id={row.get('id')} advisor={row.get('advisor_name') or ''} error={err_short}",
                )
                stats["failed"] += 1
        return stats

    def process_single_advisor(
        self,
        advisor: AdvisorRecord,
        run_date: str,
        batch_run_id: str,
        enable_recall_tracking: bool = False,
    ) -> str:
        """Returns outcome: success | skipped | failed."""
        try:
            uid = self._advisor_supabase_user_id(advisor)
            if not uid:
                logger.info(
                    "Skipping advisor %s: missing Mongo advisor id (needed for Supabase userId joins)",
                    advisor.email,
                )
                return "skipped"

            yesterday_calls = self.fetch_yesterday_customer_calls_from_mongo(uid, run_date)
            calls_count = len(yesterday_calls)
            meetings_count = self.fetch_meetings_yesterday_count_from_supabase(
                advisor.supabase_advisor_id, run_date
            )
            logger.info(
                "Advisor activity [%s] name=%s email=%s phone=%s calls=%s meetings=%s run_date=%s",
                self.concept.concept_id,
                advisor.advisor_name,
                advisor.email,
                advisor.e164_phone,
                calls_count,
                meetings_count,
                run_date,
            )
            self._write_concept_log(
                "activity",
                f"advisor={advisor.advisor_name} email={advisor.email} phone={advisor.e164_phone} calls={calls_count} meetings={meetings_count} run_date={run_date}",
            )

            if calls_count == 0 and meetings_count == 0:
                logger.info(
                    "Skipping advisor %s: no calls or meetings yesterday (Mongo calls=%s Supabase meetings=%s)",
                    advisor.email,
                    calls_count,
                    meetings_count,
                )
                return "skipped"

            nac_row = self.fetch_latest_nac_from_mongo(uid)
            coaching = self.fetch_latest_coaching_from_mongo(uid)

            daily_payload = self.build_daily_payload(
                advisor,
                run_date,
                calls_count,
                meetings_count,
                nac_row,
                coaching,
            )
            logger.info(
                "Daily payload prepared [%s] name=%s email=%s payload=%s",
                self.concept.concept_id,
                advisor.advisor_name,
                advisor.email,
                daily_payload,
            )
            self._write_concept_log(
                "vapi",
                f"prepared advisor={advisor.advisor_name} email={advisor.email} phone={advisor.e164_phone} payload={daily_payload}",
            )
            _status, _text, vapi_id = self.call_vapi_advisor(advisor, daily_payload)
            self._save_initial_tracking_row(advisor, daily_payload, vapi_id, enable_recall_tracking)
            logger.info(
                "VAPI call queued [%s] name=%s email=%s phone=%s calls=%s meetings=%s vapi_call_id=%s",
                self.concept.concept_id,
                advisor.advisor_name,
                advisor.email,
                advisor.e164_phone,
                calls_count,
                meetings_count,
                vapi_id or "",
            )
            self._write_concept_log(
                "vapi",
                f"created advisor={advisor.advisor_name} email={advisor.email} phone={advisor.e164_phone} calls={calls_count} meetings={meetings_count} vapi_call_id={vapi_id or ''}",
            )
            return "success"
        except Exception as exc:  # noqa: BLE001 — top-level per-advisor boundary
            logger.exception("Advisor failed: %s", advisor.email)
            return "failed"

    def run(
        self,
        run_date: str,
        batch_run_id: str,
        mongo_user_ids: Optional[List[str]] = None,
        enable_recall_tracking: bool = False,
        metrics_cb: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, int]:
        counts = {"processed": 0, "success": 0, "skipped": 0, "failed": 0}
        self._active_run_date = run_date

        advisors = self.get_advisors_from_mongo(mongo_user_ids)
        logger.info("[%s] Mongo advisors found: %d", self.concept.concept_id, len(advisors))
        self._write_concept_log("mapping", f"mongo_advisors_found={len(advisors)}")

        mapped = self.map_advisors_to_supabase_phone(advisors)
        logger.info("[%s] Advisors with valid phone numbers: %d", self.concept.concept_id, len(mapped))
        self._write_concept_log("mapping", f"mapped_with_valid_phone={len(mapped)}")

        max_workers = self.shared.vapi_max_concurrent
        lock = threading.Lock()

        def _run_one(adv: AdvisorRecord) -> str:
            return self.process_single_advisor(
                adv,
                run_date,
                batch_run_id,
                enable_recall_tracking=enable_recall_tracking,
            )

        def _record(out: str) -> None:
            with lock:
                counts["processed"] += 1
                counts[out] = counts.get(out, 0) + 1
            if metrics_cb:
                metrics_cb(out)

        if max_workers <= 1:
            for adv in mapped:
                _record(_run_one(adv))
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(_run_one, adv) for adv in mapped]
                for fut in as_completed(futures):
                    _record(fut.result())

        self._active_run_date = None
        return counts


def process_concept(
    concept_id: str,
    run_date: Optional[str] = None,
    batch_run_id: Optional[str] = None,
    mongo_user_ids: Optional[List[str]] = None,
    enable_recall_tracking: bool = False,
    metrics_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[str, Dict[str, int]]:
    """Run pipeline for one concept. Optional `mongo_user_ids` restricts Mongo users._id (hex strings)."""
    rid = batch_run_id or str(uuid.uuid4())
    date_s = run_date or default_run_date()
    concept = resolve_concept_from_env(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    try:
        metrics = wf.run(
            date_s,
            rid,
            mongo_user_ids=mongo_user_ids,
            enable_recall_tracking=enable_recall_tracking,
            metrics_cb=metrics_cb,
        )
        return rid, metrics
    finally:
        wf.close()


def process_recalls_for_concept(concept_id: str) -> Dict[str, int]:
    concept = resolve_concept_from_env(concept_id)
    shared = SharedServiceConfig()
    wf = ConceptWorkflow(concept, shared)
    try:
        return wf.process_recalls_for_today()
    finally:
        wf.close()
