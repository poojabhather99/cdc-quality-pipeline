"""Run the full pipeline.  Usage: python -m cdcpipe.run"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from cdcpipe.generate import generate_events, write_events
from cdcpipe.layers import build_gold, build_silver, load_bronze

AS_OF = date(2026, 6, 30)


def reconcile(stats: dict) -> None:
    """Every raw event must be accounted for: quarantined, a dropped duplicate, or applied."""
    accounted = stats["quarantined"] + stats["duplicates_dropped"] + stats["events_applied"]
    if accounted != stats["raw_events"]:
        raise AssertionError(f"reconciliation failed: {accounted} accounted vs {stats['raw_events']} raw")


def main() -> None:
    parser = argparse.ArgumentParser(description="CDC bronze -> silver -> gold pipeline demo")
    parser.add_argument("--out", type=Path, default=Path("data"))
    parser.add_argument("--visits", type=int, default=300)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    raw = args.out / "raw" / "events.jsonl"
    write_events(generate_events(args.visits, args.seed), raw)

    layers = build_silver(load_bronze(raw), AS_OF)
    stats = layers["stats"].iloc[0].to_dict()
    reconcile(stats)
    gold = build_gold(layers["state"])

    (args.out / "silver").mkdir(parents=True, exist_ok=True)
    (args.out / "gold").mkdir(parents=True, exist_ok=True)
    layers["state"].to_csv(args.out / "silver" / "visits.csv", index=False)
    layers["quarantine"].to_csv(args.out / "silver" / "quarantine.csv", index=False)
    gold.to_csv(args.out / "gold" / "worker_monthly_kpis.csv", index=False)

    reasons = layers["quarantine"]["errors"].str.split(",").explode().value_counts().to_dict()
    print(json.dumps({"stats": {k: int(v) for k, v in stats.items()}, "quarantine_reasons": reasons,
                      "gold_rows": len(gold)}, indent=2))


if __name__ == "__main__":
    main()
