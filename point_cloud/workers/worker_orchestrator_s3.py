"""
Temporal worker responsible for the S3-backed full pipeline workflow.
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from point_cloud.workflows.full_pipeline_s3_workflow import FullPipelineS3Workflow
from point_cloud.activities.ingest_activities import (
    create_scan,
    ensure_company,
    ensure_crs,
    ensure_dataset,
    ensure_dataset_version,
    create_ingest_run,
    process_ingest_run,
)
from point_cloud.activities.s3_ingest_activities import register_raw_artifact_from_s3
from point_cloud.workflows.preprocess_workflow import PreprocessPipeline
from point_cloud.workflows.reproject_workflow import ReprojectWorkflow
from point_cloud.workflows.profiling_workflow import ProfilingWorkflow
from point_cloud.workflows.registration_solver_workflow import RegistrationSolverWorkflow
from point_cloud.workflows.prod_reg_workflow import ProdRegistrationWorkflow
from point_cloud.activities.preprocess_activities import (
    list_scans_by_dataset_version,
    preprocess_point_cloud,
)
from point_cloud.activities.pipe_activities import (
    load_ingest_manifest,
    reproject_scan_to_target_crs,
    resolve_crs_to_pdal_srs,
)
from point_cloud.activities.registration_activities import (
    collect_registration_graph,
    solve_pose_graph,
    persist_pose_graph_solution,
)
from point_cloud.activities.registration_icp_activities import refine_edges_with_icp
from point_cloud.activities.export_activities import export_merged_laz
from point_cloud.activities.prod_reg_activities import (
    prod_build_registration_anchors,
    prod_collect_registration_graph,
    prod_persist_pose_graph_solution,
    prod_propose_registration_edges,
    prod_register_pair,
    prod_solve_pose_graph,
)
from point_cloud.activities.profiling_activities import (
    point_cloud_meta,
    read_cloud_hexbin,
    extract_hexbin_fields,
    upload_hexbin,
    upload_profiling_manifest,
)


async def main() -> None:
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="point-cloud-task-queue",
        workflows=[
            FullPipelineS3Workflow,
            PreprocessPipeline,
            ReprojectWorkflow,
            ProfilingWorkflow,
            RegistrationSolverWorkflow,
            ProdRegistrationWorkflow,
        ],
        activities=[
            ensure_company,
            ensure_crs,
            ensure_dataset,
            ensure_dataset_version,
            create_scan,
            register_raw_artifact_from_s3,
            create_ingest_run,
            process_ingest_run,

            load_ingest_manifest,
            reproject_scan_to_target_crs,
            resolve_crs_to_pdal_srs,

            collect_registration_graph,
            solve_pose_graph,
            persist_pose_graph_solution,
            refine_edges_with_icp,
            export_merged_laz,

            list_scans_by_dataset_version,
            preprocess_point_cloud,

            prod_build_registration_anchors,
            prod_propose_registration_edges,
            prod_register_pair,
            prod_collect_registration_graph,
            prod_solve_pose_graph,
            prod_persist_pose_graph_solution,

            point_cloud_meta,
            read_cloud_hexbin,
            extract_hexbin_fields,
            upload_hexbin,
            upload_profiling_manifest,
        ],
    )

    print("[Temporal] S3 worker started on task queue 'point-cloud-task-queue'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
