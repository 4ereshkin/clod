from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from point_cloud.workflows.cluster_workflow import ClusterHeuristicsParams, ClusterPipelineParams
from point_cloud.workflows.ingest_workflow import IngestWorkflowParams
from point_cloud.workflows.profiling_workflow import ProfilingWorkflowParams
from point_cloud.workflows.preprocess_workflow import PreprocessPipelineParams
from point_cloud.workflows.registration_solver_workflow import RegistrationSolverParams
from point_cloud.workflows.reproject_workflow import ReprojectWorkflowParams
from point_cloud.workflows.prod_reg_workflow import ProdRegistrationWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]
LEGACY_VERSION = os.environ.get("WORKFLOW_VERSION_LEGACY")
if not LEGACY_VERSION and VERSION.startswith("MVP") and VERSION != "MVP":
    LEGACY_VERSION = "MVP"


@dataclass
class FullPipelineScan:
    artifacts: List[Dict[str, str]]


@dataclass
class FullPipelineParams:
    company_id: str
    dataset_name: str
    dataset_crs_id: str
    target_srs: str
    bump_version: bool = False
    schema_version: str = "1.1.0"
    force: bool = False
    scans: Optional[List[FullPipelineScan]] = None
    profiling_cloud_dir: str = "point_cloud/tmp/profiling"
    profiling_geojson_dir: str = "point_cloud/tmp/hexbin"
    preprocessing_voxel_size_m: float = 0.10
    preprocessing_mean_k: int = 20
    preprocessing_multiplier: float = 2.0
    use_prod_registration: bool = False
    run_clustering: bool = False
    cluster_dst_dir: str = "point_cloud/tmp/cluster"
    cluster_tile_size: float = 50.0
    cluster_splitter_buffer: float = 3.0
    cluster_csf_params: Dict[str, Any] = field(default_factory=dict)
    cluster_params: ClusterHeuristicsParams = field(default_factory=ClusterHeuristicsParams)


class _FullPipelineWorkflowBase:
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

    async def _run_full_pipeline(self, params: FullPipelineParams) -> Dict[str, Any]:
        scans = params.scans or []
        if not scans:
            raise ApplicationError("Full pipeline requires at least one scan with artifacts")

        ingest_results = []
        rp_once = RetryPolicy(maximum_attempts=1)

        for idx, scan in enumerate(scans, start=1):
            self._stage = f"ingest:{idx}/{len(scans)}"
            ingest_params = IngestWorkflowParams(
                company_id=params.company_id,
                dataset_name=params.dataset_name,
                bump_version=params.bump_version,
                crs_id=params.dataset_crs_id,
                schema_version=params.schema_version,
                force=params.force,
                artifacts=scan.artifacts,
            )

            ingest_res = await workflow.execute_child_workflow(
                f"{VERSION}-ingest",
                ingest_params,
                task_queue="point-cloud-task-queue",
                retry_policy=rp_once,
            )
            ingest_results.append(ingest_res)
            scan_id = ingest_res["scan_id"]
            dataset_version_id = ingest_res["dataset_version_id"]
            if self._dataset_version_id is None:
                self._dataset_version_id = dataset_version_id
            elif dataset_version_id != self._dataset_version_id:
                raise ApplicationError(
                    "Ingest produced different dataset versions for the same pipeline"
                )
            self._scan_ids.append(scan_id)

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
            in_crs_id=params.dataset_crs_id,
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

        cluster_result = None
        if params.run_clustering:
            self._stage = "cluster"
            cluster_params = ClusterPipelineParams(
                dataset_version_id=self._dataset_version_id,
                schema_version=params.schema_version,
                dst_dir=params.cluster_dst_dir,
                tile_size=params.cluster_tile_size,
                splitter_buffer=params.cluster_splitter_buffer,
                csf_params=params.cluster_csf_params,
                cluster_params=params.cluster_params,
            )
            cluster_result = await workflow.execute_child_workflow(
                f"{VERSION}_cluster",
                cluster_params,
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
            "cluster_result": cluster_result,
        }


@workflow.defn(name=f"{VERSION}-full-pipeline")
class FullPipelineWorkflow(_FullPipelineWorkflowBase):
    @workflow.run
    async def run(self, params: FullPipelineParams) -> Dict[str, Any]:
        return await self._run_full_pipeline(params)


FullPipelineWorkflowLegacy = None
if LEGACY_VERSION and LEGACY_VERSION != VERSION:

    @workflow.defn(name=f"{LEGACY_VERSION}-full-pipeline")
    class FullPipelineWorkflowLegacy(_FullPipelineWorkflowBase):
        @workflow.run
        async def run(self, params: FullPipelineParams) -> Dict[str, Any]:
            return await self._run_full_pipeline(params)
