"""Multi-tenant concept definitions: shared Mongo DB; per-concept Supabase phone rules + VAPI only."""

from dataclasses import dataclass
from typing import Any, Dict, Optional

# Shared Mongo collections (same DB for all concepts; link userId = str(Mongo users._id))
_SHARED_MONGO = {
    "mongo_calls_collection": "customernacfeedbacks",
    # Daily NAC / coaching narrative (same DB). Rows may include KeyCoachingRecommendations.reportText.
    "mongo_nac_collection": "dailynacfeedbacks",
    "mongo_coaching_collection": "daily_users_coaching_vapi",
    "calls_user_id_col": "userId",
    "calls_date_col": "date",
    "nac_user_id_col": "userId",
    "nac_date_col": "date",
    "nac_text_col": "nac_feedback",
    "nac_order_col": "date",
    "coaching_user_id_col": "userId",
    "coaching_summary_col": "previous_call_summary",
    "coaching_order_col": "date",
}

# Supabase `users`: only rows with this role are treated as advisors when resolving phone/id.
_SHARED_SUPABASE_USER_ROLE = {
    "supabase_user_role_col": "role",
    "supabase_user_role_value": "advisor",
}

# Supabase meetings (per concept project): count rows for meetings_yesterday.
_SHARED_MEETINGS = {
    "supabase_advisor_id_col": "id",
    "supabase_meetings_table": "meetings",
    "supabase_meetings_advisor_id_col": "advisor_id",
    "supabase_meetings_date_col": "created_at",
    # "utc_bounds": meeting_date in [London yesterday start, today start) as UTC ISO. "iso_day": equality to run_date string.
    "meetings_date_match_mode": "utc_bounds",
}

# Keys are URL path segments, e.g. POST /run/people_manager
CONCEPTS: Dict[str, Dict[str, Any]] = {
    "people_manager": {
        **_SHARED_MONGO,
        **_SHARED_SUPABASE_USER_ROLE,
        **_SHARED_MEETINGS,
        "mongo_users_collection": "users",
        "advisor_query": {"role": "advisor"},
        # Phone: Supabase users.peoplemanager_id = str(Mongo users._id)
        "supabase_lookup_mode": "peoplemanager_id",
        "supabase_user_table": "users",
        "supabase_email_col": "email",
        "supabase_phone_col": "phone_number",
        "supabase_advisor_id_col": "id",
        "supabase_peoplemanager_id_col": "peoplemanager_id",
        "vapi_assistant_id_env": "VAPI_PM_ASSISTANT_ID",
        "vapi_phone_number_id_env": "VAPI_PM_PHONE_NUMBER_ID",
        "supabase_user_id_col": "user_id",
    },
    "t_and_b": {
        **_SHARED_MONGO,
        **_SHARED_SUPABASE_USER_ROLE,
        **_SHARED_MEETINGS,
        "mongo_users_collection": "users",
        "advisor_query": {"role": "advisor"},
        # Phone: Supabase users row matched by email
        "supabase_lookup_mode": "email",
        "supabase_user_table": "users",
        "supabase_email_col": "email",
        "supabase_phone_col": "phone_number",
        "supabase_advisor_id_col": "id",
        "supabase_peoplemanager_id_col": "peoplemanager_id",
        "vapi_assistant_id_env": "VAPI_TB_ASSISTANT_ID",
        "vapi_phone_number_id_env": "VAPI_TB_PHONE_NUMBER_ID",
    },
}


@dataclass(frozen=True)
class ResolvedConcept:
    concept_id: str
    mongo_uri: str
    mongo_db_name: str
    mongo_users_collection: str
    mongo_calls_collection: str
    mongo_nac_collection: str
    mongo_coaching_collection: str
    advisor_query: Dict[str, Any]
    supabase_lookup_mode: str
    supabase_user_table: str
    supabase_email_col: str
    supabase_phone_col: str
    supabase_user_role_col: str
    supabase_user_role_value: str
    supabase_peoplemanager_id_col: str
    calls_user_id_col: str
    calls_date_col: str
    nac_user_id_col: str
    nac_date_col: str
    nac_text_col: str
    nac_order_col: str
    coaching_user_id_col: str
    coaching_summary_col: str
    coaching_order_col: str
    supabase_advisor_id_col: str
    supabase_meetings_table: str
    supabase_meetings_advisor_id_col: str
    supabase_meetings_date_col: str
    meetings_date_match_mode: str
    supabase_url: str
    supabase_service_role_key: str
    vapi_assistant_id: str
    vapi_phone_number_id: Optional[str]


def list_concept_ids() -> list[str]:
    return list(CONCEPTS.keys())


def get_concept_definition(concept_id: str) -> Dict[str, Any]:
    if concept_id not in CONCEPTS:
        raise KeyError(f"Unknown concept: {concept_id}")
    return CONCEPTS[concept_id]
