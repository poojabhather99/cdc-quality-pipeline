"""Bronze, silver (+ quarantine) and gold layers."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from cdcpipe.quality import REQUIRED, validate_event


def load_bronze(path: Path) -> pd.DataFrame:
    """Bronze: every raw event exactly as received, plus its arrival order. No filtering."""
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return pd.DataFrame({"arrival_seq": range(len(events)), "event": events})


def build_silver(bronze: pd.DataFrame, as_of: date) -> dict[str, pd.DataFrame]:
    """Validate, deduplicate and apply change events.

    Returns the current-state table, the quarantine table and a stats frame. Events are ordered by
    source timestamp (not arrival), so late and out-of-order events still land correctly.
    """
    valid_rows, quarantine_rows = [], []
    for seq, event in zip(bronze["arrival_seq"], bronze["event"], strict=True):
        errors = validate_event(event, as_of)
        if errors:
            quarantine_rows.append({"arrival_seq": seq, "errors": ",".join(errors), "event": json.dumps(event)})
            continue
        payload = event["before"] if event["op"] == "d" else event["after"]
        valid_rows.append({"arrival_seq": seq, "op": event["op"], "ts_ms": event["ts_ms"],
                           "visit_id": payload["visit_id"], "payload": payload})

    valid = pd.DataFrame(valid_rows, columns=["arrival_seq", "op", "ts_ms", "visit_id", "payload"])
    quarantine = pd.DataFrame(quarantine_rows, columns=["arrival_seq", "errors", "event"])

    # exact duplicate deliveries: same key, op, timestamp and payload
    valid["fingerprint"] = valid["payload"].map(lambda p: json.dumps(p, sort_keys=True))
    deduped = valid.drop_duplicates(subset=["visit_id", "op", "ts_ms", "fingerprint"], keep="first")
    duplicates = len(valid) - len(deduped)

    # latest event per visit wins; a final delete removes the row
    latest = deduped.sort_values(["visit_id", "ts_ms", "arrival_seq"]).groupby("visit_id").tail(1)
    live = latest[latest["op"] != "d"]
    state = pd.DataFrame(list(live["payload"])).reindex(columns=list(REQUIRED)).sort_values("visit_id")
    state = state.reset_index(drop=True)

    stats = pd.DataFrame([{
        "raw_events": len(bronze), "quarantined": len(quarantine), "duplicates_dropped": duplicates,
        "events_applied": len(deduped), "visits_total": deduped["visit_id"].nunique(),
        "visits_deleted": int((latest["op"] == "d").sum()), "visits_live": len(state),
    }])
    return {"state": state, "quarantine": quarantine, "stats": stats}


GOLD_SQL = """
SELECT
    worker_id,
    strftime(CAST(visit_date AS DATE), '%Y-%m') AS month,
    COUNT(*)                                                         AS visits,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)            AS completed,
    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN status IN ('completed','missed') THEN 1 ELSE 0 END), 0), 3)
                                                                     AS completion_rate,
    ROUND(AVG(CASE WHEN status = 'completed' THEN duration_min END), 1) AS avg_duration_min
FROM state
GROUP BY worker_id, month
ORDER BY worker_id, month
"""


def build_gold(state: pd.DataFrame) -> pd.DataFrame:
    """Gold: monthly KPIs per worker. Completion rate excludes still-scheduled visits."""
    con = duckdb.connect()
    con.register("state", state)
    return con.execute(GOLD_SQL).df()
