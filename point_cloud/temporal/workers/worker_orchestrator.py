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

from point_cloud.temporal.workflows.registration_solver_workflow import RegistrationSolverWorkflow
from point_cloud.temporal.activities.registration_icp_activities import refine_edges_with_icp
from point_cloud.temporal.activities.preprocess_activities import preprocess_point_cloud

from point_cloud.temporal.activities.registration_activities import (
    collect_registration_graph,
    solve_pose_graph,
    persist_pose_graph_solution,)

from point_cloud.temporal.activities.export_activities import (
    export_merged_laz,
)

from point_cloud.temporal.activities.pipe_activities import resolve_crs_to_pdal_srs
from point_cloud.temporal.activities.pipe_activities import (
    load_ingest_manifest,
    reproject_scan_to_target_crs,
    build_registration_anchors,
    propose_registration_edges,
    propose_registration_edges_for_dataset,
    compute_icp_edge,
)
from point_cloud.temporal.workflows.mls_new import MlsPipelineWorkflow
from point_cloud.temporal.workflows.ingest_workflow import IngestWorkflow
from point_cloud.temporal.activities import (
    create_scan,
    ensure_company,
    ensure_crs,
    ensure_dataset,
    ensure_dataset_version,
    upload_raw_artifact,
    create_ingest_run,
    process_ingest_run,
    get_scan,
    list_raw_artifacts,
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
        workflows=[MlsPipelineWorkflow, IngestWorkflow, RegistrationSolverWorkflow],
        activities=[
            # ingest
            ensure_company,
            ensure_crs,
            ensure_dataset,
            ensure_dataset_version,
            create_scan,
            upload_raw_artifact,
            create_ingest_run,
            process_ingest_run,
            get_scan,
            list_raw_artifacts,

            # pipeline / registration prep
            load_ingest_manifest,
            reproject_scan_to_target_crs,
            build_registration_anchors,
            propose_registration_edges,
            resolve_crs_to_pdal_srs,

            # registration solver
            collect_registration_graph,
            solve_pose_graph,
            persist_pose_graph_solution,
            compute_icp_edge,
            propose_registration_edges_for_dataset,

            refine_edges_with_icp,
            export_merged_laz,

            preprocess_point_cloud
        ],
    )

    print("[Temporal] Worker started on task queue 'point-cloud-task-queue'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())