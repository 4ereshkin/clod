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

from point_cloud.workflows.registration_solver_workflow import (
    RegistrationSolverWorkflow,
)
from point_cloud.workflows.prod_reg_workflow import ProdRegistrationWorkflow
from point_cloud.workflows.preprocess_workflow import PreprocessPipeline
from point_cloud.workflows.reproject_workflow import ReprojectWorkflow
from point_cloud.workflows.download_workflow import DownloadWorkflow
from point_cloud.workflows.profiling_workflow import ProfilingWorkflow
from point_cloud.workflows.full_pipeline_workflow import (
    FullPipelineWorkflow,
    FullPipelineWorkflowLegacy,
)
from point_cloud.workflows.cluster_workflow import ClusterPipeline
from point_cloud.activities.registration_icp_activities import refine_edges_with_icp
from point_cloud.activities.preprocess_activities import (
    list_scans_by_dataset_version,
    preprocess_point_cloud,
)
from point_cloud.activities.prod_reg_activities import (
    prod_build_registration_anchors,
    prod_collect_registration_graph,
    prod_persist_pose_graph_solution,
    prod_propose_registration_edges,
    prod_register_pair,
    prod_solve_pose_graph,
)

from point_cloud.activities.registration_activities import (
    collect_registration_graph,
    solve_pose_graph,
    persist_pose_graph_solution,)

from point_cloud.activities.export_activities import (
    export_merged_laz,
)

from point_cloud.activities.pipe_activities import resolve_crs_to_pdal_srs
from point_cloud.activities.pipe_activities import (
    load_ingest_manifest,
    reproject_scan_to_target_crs,
    build_registration_anchors,
    propose_registration_edges,
    propose_registration_edges_for_dataset,
    compute_icp_edge,
)
from point_cloud.workflows.mls_new import MlsPipelineWorkflow
from point_cloud.workflows.ingest_workflow import IngestWorkflow
from point_cloud.activities.download_activities import download_from_s3
from point_cloud.activities.profiling_activities import (
    point_cloud_meta,
    read_cloud_hexbin,
    extract_hexbin_fields,
    upload_hexbin,
    upload_profiling_manifest,
)
from point_cloud.activities import (
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
from point_cloud.activities.cluster_activities import (
    extract_scale_offset,
    split_into_tiles,
    split_ground_offground,
    cluster_tile,
    crop_buffer,
    merge_tiles,
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
        workflows=[
            MlsPipelineWorkflow,
            IngestWorkflow,
            RegistrationSolverWorkflow,
            PreprocessPipeline,
            ReprojectWorkflow,
            ProdRegistrationWorkflow,
            DownloadWorkflow,
            ProfilingWorkflow,
            ClusterPipeline,
            FullPipelineWorkflow,
            *([FullPipelineWorkflowLegacy] if FullPipelineWorkflowLegacy else []),
        ],
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
            download_from_s3,

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

            list_scans_by_dataset_version,
            preprocess_point_cloud,

            # prod registration
            prod_build_registration_anchors,
            prod_propose_registration_edges,
            prod_register_pair,
            prod_collect_registration_graph,
            prod_solve_pose_graph,
            prod_persist_pose_graph_solution,

            # profiling
            point_cloud_meta,
            read_cloud_hexbin,
            extract_hexbin_fields,
            upload_hexbin,
            upload_profiling_manifest,

            # cluster
            extract_scale_offset,
            split_into_tiles,
            split_ground_offground,
            cluster_tile,
            crop_buffer,
            merge_tiles,
        ],
    )

    print("[Temporal] Worker started on task queue 'point-cloud-task-queue'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
