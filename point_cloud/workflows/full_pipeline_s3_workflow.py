from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from point_cloud.workflows.profiling_workflow import ProfilingWorkflowParams
from point_cloud.workflows.preprocess_workflow import PreprocessPipelineParams
from point_cloud.workflows.registration_solver_workflow import RegistrationSolverParams
from point_cloud.workflows.reproject_workflow import ReprojectWorkflowParams
from point_cloud.workflows.prod_reg_workflow import ProdRegistrationWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class FullPipelineS3Scan:
    cloud_s3_key: str
    path_s3_key: str


@dataclass
class FullPipelineS3Params:
    company_id: str
    dataset_name: str
    source_srs: str
    target_srs: str
    bump_version: bool = False
    schema_version: str = "1.1.0"
    force: bool = False
    scans: Optional[List[FullPipelineS3Scan]] = None
    s3_bucket: Optional[str] = None
    profiling_cloud_dir: str = "point_cloud/tmp/profiling"
    profiling_geojson_dir: str = "point_cloud/tmp/hexbin"
    preprocessing_voxel_size_m: float = 0.10
    preprocessing_mean_k: int = 20
    preprocessing_multiplier: float = 2.0
    use_prod_registration: bool = False


class _FullPipelineS3WorkflowBase:
    def __init__(self) -> None:
        self._stage = "init"
        self._scan_ids: list[str] = []
        self._dataset_version_id: Optional[str] = None

    @workflow.query
    def progress(self) -> dict:
        return {
            "stage": self._stage,
            "scan_ids": self._scan_ids,
            "dataset_version_id": self._dataset_version_id,
        }

    async def _run_full_pipeline_s3(self, params: FullPipelineS3Params) -> Dict[str, Any]:
        scans = params.scans or []
        if not scans:
            raise ApplicationError("Full pipeline requires at least one scan with S3 keys")

        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_once = RetryPolicy(maximum_attempts=1)

        self._stage = "ensure_company"
        await workflow.execute_activity(
            "ensure_company",
            args=[params.company_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        crs_id = params.source_srs
        self._stage = "ensure_crs"
        if crs_id.upper().startswith("EPSG:"):
            try:
                epsg = int(crs_id.split(":", 1)[1])
            except ValueError:
                epsg = None
            await workflow.execute_activity(
                "ensure_crs",
                args=[crs_id, crs_id, 0, epsg, "m", "x_east,y_north,z_up"],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

        self._stage = "ensure_dataset"
        dataset_id = await workflow.execute_activity(
            "ensure_dataset",
            args=[params.company_id, crs_id, params.dataset_name],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        self._stage = "ensure_dataset_version"
        dataset_version = await workflow.execute_activity(
            "ensure_dataset_version",
            args=[dataset_id, params.bump_version],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )
        self._dataset_version_id = dataset_version["id"]

        ingest_results = []
        for idx, scan in enumerate(scans, start=1):
            if not scan.cloud_s3_key or not scan.path_s3_key:
                raise ApplicationError("Each scan must include cloud and path S3 keys")

            self._stage = f"ingest:{idx}/{len(scans)}"
            scan_id = await workflow.execute_activity(
                "create_scan",
                args=[params.company_id, self._dataset_version_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )
            self._scan_ids.append(scan_id)

            await workflow.execute_activity(
                "register_raw_artifact_from_s3",
                args=[
                    params.company_id,
                    self._dataset_version_id,
                    scan_id,
                    "raw.point_cloud",
                    scan.cloud_s3_key,
                    params.s3_bucket,
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )
            await workflow.execute_activity(
                "register_raw_artifact_from_s3",
                args=[
                    params.company_id,
                    self._dataset_version_id,
                    scan_id,
                    "raw.trajectory",
                    scan.path_s3_key,
                    params.s3_bucket,
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

            ingest_run_id = await workflow.execute_activity(
                "create_ingest_run",
                args=[params.company_id, scan_id, params.schema_version, params.force],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )
            process_result = await workflow.execute_activity(
                "process_ingest_run",
                args=[ingest_run_id],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_fast,
            )
            ingest_results.append(
                {
                    "scan_id": scan_id,
                    "ingest_run_id": ingest_run_id,
                    "process_result": process_result,
                }
            )

        self._stage = "profiling"
        profiling_results = []
        for scan_id in self._scan_ids:
            profiling_params = ProfilingWorkflowParams(
                scan_id=scan_id,
                cloud_path=params.profiling_cloud_dir,
                geojson_dst=params.profiling_geojson_dir,
            )
            profiling_res = await workflow.execute_child_workflow(
                f"{VERSION}-profiling",
                profiling_params,
                task_queue="point-cloud-task-queue",
                retry_policy=rp_once,
            )
            profiling_results.append(profiling_res)

        self._stage = "reproject"
        reproject_params = ReprojectWorkflowParams(
            company_id=params.company_id,
            dataset_version_id=self._dataset_version_id,
            schema_version=params.schema_version,
            scan_ids=self._scan_ids,
            in_srs=params.source_srs,
            out_srs=params.target_srs,
        )
        reproject_result = await workflow.execute_child_workflow(
            f"{VERSION}-reproject",
            reproject_params,
            task_queue="point-cloud-task-queue",
            retry_policy=rp_once,
        )

        self._stage = "preprocess"
        preprocess_params = PreprocessPipelineParams(
            company_id=params.company_id,
            dataset_version_id=self._dataset_version_id,
            schema_version=params.schema_version,
            scan_ids=self._scan_ids,
            input_kind="derived.reprojected_point_cloud",
            output_kind="derived.preprocessed_point_cloud",
            voxel_size_m=params.preprocessing_voxel_size_m,
            mean_k=params.preprocessing_mean_k,
            multiplier=params.preprocessing_multiplier,
        )
        preprocess_result = await workflow.execute_child_workflow(
            f"{VERSION}-preprocessing_workflow",
            preprocess_params,
            task_queue="point-cloud-task-queue",
            retry_policy=rp_once,
        )

        if params.use_prod_registration:
            self._stage = "prod-registration"
            registration_params = ProdRegistrationWorkflowParams(
                company_id=params.company_id,
                dataset_version_id=self._dataset_version_id,
                schema_version=params.schema_version,
                force=params.force,
            )
            registration_result = await workflow.execute_child_workflow(
                f"{VERSION}-registration",
                registration_params,
                task_queue="point-cloud-task-queue",
                retry_policy=rp_once,
            )
        else:
            self._stage = "registration"
            registration_params = RegistrationSolverParams(
                company_id=params.company_id,
                dataset_version_id=self._dataset_version_id,
                schema_version=params.schema_version,
                force=params.force,
            )
            registration_result = await workflow.execute_child_workflow(
                f"{VERSION}-registration-solver",
                registration_params,
                task_queue="point-cloud-task-queue",
                retry_policy=rp_once,
            )

        self._stage = "done"
        return {
            "scan_ids": self._scan_ids,
            "dataset_version_id": self._dataset_version_id,
            "ingest_results": ingest_results,
            "profiling_results": profiling_results,
            "reproject_result": reproject_result,
            "preprocess_result": preprocess_result,
            "registration_result": registration_result,
        }


@workflow.defn(name=f"{VERSION}-full-pipeline-s3")
class FullPipelineS3Workflow(_FullPipelineS3WorkflowBase):
    @workflow.run
    async def run(self, params: FullPipelineS3Params) -> Dict[str, Any]:
        return await self._run_full_pipeline_s3(params)
