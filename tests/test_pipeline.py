from datetime import date

import pandas as pd
import pytest

from cdcpipe.generate import generate_events
from cdcpipe.layers import build_gold, build_silver
from cdcpipe.quality import validate_event, validate_row
from cdcpipe.run import AS_OF, reconcile

GOOD = {"visit_id": 1, "worker_id": "W001", "village_id": "V01", "visit_date": "2026-03-01",
        "visit_type": "antenatal", "status": "completed", "duration_min": 30}


def ev(op, ts, before=None, after=None):
    return {"op": op, "ts_ms": ts, "source": {"table": "visits"}, "before": before, "after": after}


def bronze(events):
    return pd.DataFrame({"arrival_seq": range(len(events)), "event": events})


def test_valid_row_has_no_errors():
    assert validate_row(GOOD, AS_OF) == []


@pytest.mark.parametrize("change,code", [
    ({"worker_id": None}, "missing_worker_id"),
    ({"worker_id": "worker-7"}, "bad_worker_format"),
    ({"visit_date": "31/13/2026"}, "bad_date"),
    ({"visit_date": "2026-12-01"}, "future_visit_date"),
    ({"duration_min": -5}, "duration_out_of_range"),
    ({"duration_min": 9999}, "duration_out_of_range"),
    ({"duration_min": "30"}, "bad_duration_type"),
    ({"status": "unknown"}, "bad_status"),
    ({"visit_type": "x"}, "bad_visit_type"),
])
def test_each_rule_fires(change, code):
    assert code in validate_row({**GOOD, **change}, AS_OF)


def test_event_structure_checks():
    assert validate_event({"op": "x", "ts_ms": 1}, AS_OF) == ["bad_op"]
    assert validate_event({"op": "c", "ts_ms": "1"}, AS_OF) == ["bad_timestamp"]
    assert validate_event(ev("d", 1, before={}), AS_OF) == ["delete_missing_key"]
    assert validate_event(ev("d", 1, before={"visit_id": 1}), AS_OF) == []


def test_out_of_order_update_still_wins():
    created, updated = {**GOOD, "status": "scheduled", "duration_min": 0}, GOOD
    # the update arrives BEFORE the create but has the later source timestamp
    out = build_silver(bronze([ev("u", 200, created, updated), ev("c", 100, None, created)]), AS_OF)
    assert out["state"].loc[0, "status"] == "completed"


def test_delete_removes_visit_but_late_create_does_not_resurrect_it():
    events = [ev("d", 300, before=GOOD), ev("c", 100, None, GOOD)]
    out = build_silver(bronze(events), AS_OF)
    assert out["state"].empty and out["stats"].loc[0, "visits_deleted"] == 1


def test_duplicates_are_dropped_and_counted():
    events = [ev("c", 100, None, GOOD), ev("c", 100, None, GOOD)]
    out = build_silver(bronze(events), AS_OF)
    assert len(out["state"]) == 1 and out["stats"].loc[0, "duplicates_dropped"] == 1


def test_corrupted_events_are_quarantined_with_reasons():
    events = [ev("c", 100, None, {**GOOD, "status": "unknown"}), ev("c", 101, None, {**GOOD, "visit_id": 2})]
    out = build_silver(bronze(events), AS_OF)
    assert list(out["quarantine"]["errors"]) == ["bad_status"] and len(out["state"]) == 1


def test_schema_drift_extra_column_is_tolerated_and_dropped():
    out = build_silver(bronze([ev("c", 100, None, {**GOOD, "supervisor_note": "x"})]), AS_OF)
    assert "supervisor_note" not in out["state"].columns and len(out["state"]) == 1


def test_gold_kpis_hand_computed():
    rows = [
        {**GOOD, "visit_id": 1, "status": "completed", "duration_min": 20},
        {**GOOD, "visit_id": 2, "status": "completed", "duration_min": 40},
        {**GOOD, "visit_id": 3, "status": "missed", "duration_min": 0},
        {**GOOD, "visit_id": 4, "status": "scheduled", "duration_min": 0},
    ]
    gold = build_gold(pd.DataFrame(rows))
    r = gold.iloc[0]
    assert (r["visits"], r["completed"]) == (4, 2)
    assert r["completion_rate"] == pytest.approx(2 / 3, abs=1e-3)  # scheduled visits excluded
    assert r["avg_duration_min"] == 30.0


def test_generator_is_deterministic_and_dirty():
    a, b = generate_events(100, seed=1), generate_events(100, seed=1)
    assert a == b
    out = build_silver(bronze(a), date(2026, 6, 30))
    stats = out["stats"].iloc[0]
    assert stats["quarantined"] > 0 and stats["duplicates_dropped"] > 0 and stats["visits_deleted"] > 0


def test_end_to_end_reconciliation_holds():
    out = build_silver(bronze(generate_events(300, seed=7)), AS_OF)
    reconcile(out["stats"].iloc[0].to_dict())


def test_reconcile_detects_a_leak():
    with pytest.raises(AssertionError):
        reconcile({"raw_events": 10, "quarantined": 1, "duplicates_dropped": 1, "events_applied": 7})
