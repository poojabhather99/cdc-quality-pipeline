"""Row-level data-quality rules. Each rule returns an error code or None."""
from __future__ import annotations

import re
from datetime import date

ALLOWED_STATUS = {"scheduled", "completed", "missed"}
ALLOWED_TYPES = {"antenatal", "postnatal", "immunisation", "follow_up", "screening"}
REQUIRED = ("visit_id", "worker_id", "village_id", "visit_date", "visit_type", "status", "duration_min")
WORKER_RE = re.compile(r"^W\d{3}$")
MAX_DURATION_MIN = 480


def validate_row(row: dict | None, as_of: date) -> list[str]:
    """Return a list of error codes; an empty list means the row is valid."""
    if not isinstance(row, dict):
        return ["missing_payload"]
    errors = [f"missing_{col}" for col in REQUIRED if row.get(col) in (None, "")]

    if row.get("worker_id") and not WORKER_RE.match(str(row["worker_id"])):
        errors.append("bad_worker_format")
    if row.get("status") and row["status"] not in ALLOWED_STATUS:
        errors.append("bad_status")
    if row.get("visit_type") and row["visit_type"] not in ALLOWED_TYPES:
        errors.append("bad_visit_type")

    if row.get("visit_date"):
        try:
            if date.fromisoformat(str(row["visit_date"])) > as_of:
                errors.append("future_visit_date")
        except ValueError:
            errors.append("bad_date")

    duration = row.get("duration_min")
    if duration is not None:
        if not isinstance(duration, int | float) or isinstance(duration, bool):
            errors.append("bad_duration_type")
        elif duration < 0 or duration > MAX_DURATION_MIN:
            errors.append("duration_out_of_range")
    return errors


def validate_event(event: dict, as_of: date) -> list[str]:
    """Validate a change event: structure first, then the row payload that matters for its op."""
    op = event.get("op")
    if op not in {"c", "u", "d", "r"}:
        return ["bad_op"]
    if not isinstance(event.get("ts_ms"), int):
        return ["bad_timestamp"]
    if op == "d":
        before = event.get("before")
        return [] if isinstance(before, dict) and before.get("visit_id") is not None else ["delete_missing_key"]
    return validate_row(event.get("after"), as_of)
