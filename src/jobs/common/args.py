from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class JobArgs:
    start_ts: str
    end_ts: str
    run_id: str


def parse_job_args() -> JobArgs:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start-ts",
        required=True,
        help="Inclusive window start timestamp. Example: 2024-01-01T00:00:00",
    )
    parser.add_argument(
        "--end-ts",
        required=True,
        help="Exclusive window end timestamp. Example: 2024-01-02T00:00:00",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Pipeline run id. Usually passed from Airflow dag_run.run_id.",
    )

    args = parser.parse_args()

    return JobArgs(
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        run_id=args.run_id,
    )