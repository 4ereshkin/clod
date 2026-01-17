from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleSpec,
    ScheduleIntervalSpec,
)

from point_cloud.workflows.reconcile_ingest_workflow import ReconcileIngestWorkflow, ReconcileIngestParams


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule-id", default="ingest-reconcile-schedule")
    parser.add_argument("--interval-minutes", type=int, default=15)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")

    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            ReconcileIngestWorkflow.run,
            ReconcileIngestParams(limit=args.limit),
            id=f"{args.schedule_id}-workflow",
            task_queue="point-cloud-task-queue",
        ),
        spec=ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(minutes=args.interval_minutes))],
        ),
    )

    try:
        await client.create_schedule(args.schedule_id, schedule)
        print(f"Created schedule '{args.schedule_id}'")
    except Exception as exc:
        print(f"Failed to create schedule '{args.schedule_id}': {exc}")


if __name__ == "__main__":
    asyncio.run(main())
