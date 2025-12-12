"""
Temporal worker responsible for orchestrating point cloud workflows.

This script connects to a Temporal server running at ``localhost:7233``,
registers both workflow and activity implementations on a common task
queue and starts polling for tasks.  It is designed to be invoked as
a standalone program.

Example::

    # Start the Temporal server (if not already running)
    docker run --rm -it -p 7233:7233 temporalio/auto-setup:1.23

    # Run the worker
    python -m point_cloud.temporal.workers.worker_orchestrator

The worker will run indefinitely until terminated.  Multiple workers
can be started to increase throughput or to separate different kinds
of activities onto distinct queues.
"""

import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from clod.point_cloud.temporal.workflows.mls_pipeline_workflow import MlsPipelineWorkflow
from clod.point_cloud.temporal.activities import (
    load_metadata_for_file,
    reproject_file,
    insert_file_into_db,
    convert_to_tileset,
)


async def main() -> None:
    """Entry point for running the orchestration worker."""
    # Connect to the Temporal server.  The address can be made
    # configurable via environment variables or CLI arguments.
    client = await Client.connect("localhost:7233")

    # Create a worker that subscribes to the task queue.  Register all
    # workflows and activities that this worker should handle.  Adjust
    # ``max_concurrent_*`` parameters to tune throughput.
    worker = Worker(
        client,
        task_queue="point-cloud-task-queue",
        workflows=[MlsPipelineWorkflow],
        activities=[
            load_metadata_for_file,
            reproject_file,
            insert_file_into_db,
            convert_to_tileset,
        ],
    )

    print("[Temporal] Worker started on task queue 'point-cloud-task-queue'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())