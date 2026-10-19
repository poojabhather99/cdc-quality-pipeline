"""Generate synthetic Debezium-style change events for a `visits` table.

The stream deliberately contains the problems real CDC feeds have: out-of-order arrival,
duplicate deliveries, deletes, schema drift and corrupted records.
"""
from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 1, 1)
VISIT_TYPES = ["antenatal", "postnatal", "immunisation", "follow_up", "screening"]
BASE_TS = 1_767_225_600_000  # 2026-01-01T00:00:00Z in epoch milliseconds


def _row(rng: random.Random, visit_id: int) -> dict:
    return {
        "visit_id": visit_id,
        "worker_id": f"W{rng.randint(1, 25):03d}",
        "village_id": f"V{rng.randint(1, 12):02d}",
        "visit_date": (START + timedelta(days=rng.randint(0, 180))).isoformat(),
        "visit_type": rng.choice(VISIT_TYPES),
        "status": "scheduled",
        "duration_min": 0,
    }


def _event(op: str, ts_ms: int, before: dict | None, after: dict | None) -> dict:
    return {"op": op, "ts_ms": ts_ms, "source": {"table": "visits"}, "before": before, "after": after}


def _corrupt(rng: random.Random, row: dict) -> dict:
    bad = dict(row)
    kind = rng.choice(["null_worker", "bad_date", "negative_duration", "bad_status", "bad_worker_format"])
    if kind == "null_worker":
        bad["worker_id"] = None
    elif kind == "bad_date":
        bad["visit_date"] = "31/13/2026"
    elif kind == "negative_duration":
        bad["duration_min"] = -15
    elif kind == "bad_status":
        bad["status"] = "unknown"
    else:
        bad["worker_id"] = "worker-7"
    return bad


def generate_events(n_visits: int = 300, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    for visit_id in range(1, n_visits + 1):
        ts = BASE_TS + visit_id * 60_000
        row = _row(rng, visit_id)
        created = _corrupt(rng, row) if rng.random() < 0.04 else row
        events.append(_event("c", ts, None, created))

        if rng.random() < 0.6:  # visit takes place: status and duration are updated
            done = dict(row, status=rng.choice(["completed", "completed", "completed", "missed"]))
            done["duration_min"] = rng.randint(10, 90) if done["status"] == "completed" else 0
            events.append(_event("u", ts + 3_600_000, row, done))
            row = done
        if rng.random() < 0.08:  # record removed at source
            events.append(_event("d", ts + 7_200_000, row, None))

    # schema drift: a new column appears on some later events
    for ev in rng.sample(events, k=len(events) // 20):
        if ev["after"]:
            ev["after"]["supervisor_note"] = "reviewed"

    # duplicate deliveries (at-least-once semantics)
    events += [json.loads(json.dumps(ev)) for ev in rng.sample(events, k=len(events) // 25)]

    # out-of-order arrival: shuffle a window of events
    rng.shuffle(events)
    return events


def write_events(events: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


if __name__ == "__main__":
    out = Path("data/raw/events.jsonl")
    write_events(generate_events(), out)
    print(f"Wrote {out}")
