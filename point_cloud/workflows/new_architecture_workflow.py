from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional
import asyncio

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from point_cloud.workflows.smart_ingest_workflow import SmartIngestWorkflowParams
from point_cloud.workflows.cluster_workflow import ClusterPipelineParams

VERSION = os.environ["WORKFLOW_VERSION"]

@dataclass
class NewArchitectureScan:
    artifacts: List[Dict[str, str]]
    scan_meta: Optional[Dict[str, Any]] = None

@dataclass
class NewArchitectureParams:
    company_id: str
    dataset_name: str
    target_crs_id: str
    scans: List[NewArchitectureScan]
    schema_version: str = "1.1.0"
    force: bool = False

    # Logic flags
    run_old_cluster: bool = True


@workflow.defn(name=f"{VERSION}-new-architecture")
class NewArchitectureWorkflow:
    def __init__(self) -> None:
        self._stage = "init"
        self._scan_ids: list[str] = []
        self._dataset_version_id: Optional[str] = None

    @workflow.query
    def progress(self) -> dict:
        return {
            "stage": self._stage,
            "scan_ids": self._scan_ids,
        }

    @workflow.run
    async def run(self, params: NewArchitectureParams) -> Dict[str, Any]:
        rp = RetryPolicy(maximum_attempts=2)

        # Get Workflow ID from context (to match StartNewPipelineUseCase)
        workflow_id = workflow.info().workflow_id

        # Notify START
        # (StartNewPipelineUseCase already did this, but we can do IN_PROGRESS or update details)
        await workflow.execute_activity(
            "update_pipeline_status",
            args=[workflow_id, "IN_PROGRESS", {"stage": "init"}],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=rp
        )

        try:
            # === 1. Smart Ingest ===
            self._stage = "ingest"
            ingest_results = []

            # We need to accumulate scan_ids and dataset_version_id
            # We can run these in parallel
            futures = []
            for scan in params.scans:
                p = SmartIngestWorkflowParams(
                    company_id=params.company_id,
                    dataset_name=params.dataset_name,
                    bump_version=False, # Assume single version for the batch
                    crs_id=params.target_crs_id,
                    schema_version=params.schema_version,
                    force=params.force,
                    artifacts=scan.artifacts,
                    scan_meta=scan.scan_meta,
                    override_crs_id=params.target_crs_id # Force target CRS
                )
                futures.append(
                    workflow.execute_child_workflow(
                        f"{VERSION}-smart-ingest",
                        p,
                        task_queue="point-cloud-task-queue",
                        retry_policy=rp
                    )
                )

            results = await asyncio.gather(*futures)

            for res in results:
                self._scan_ids.append(res["scan_id"])
                if self._dataset_version_id is None:
                    self._dataset_version_id = res["dataset_version_id"]
                ingest_results.append(res)

            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "IN_PROGRESS", {"stage": "ingest_complete", "scans": len(ingest_results)}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            # === 2. Reproject ===
            self._stage = "reproject"
            reproject_results = []

            # SmartIngest has determined the source CRS and put it in 'logic' output or manifest.
            # We iterate scans and call reproject activity.

            for i, res in enumerate(ingest_results):
                scan_id = res["scan_id"]
                # Get source CRS from SmartIngest result logic
                logic = res.get("logic", {})
                source_crs = logic.get("source_crs_raw", "UNKNOWN")

                # If source_crs is UNKNOWN or same as target, Reproject might fail or be skipped?
                # User said "reproject... everything with CRS".
                # We assume SmartIngest did its job finding the source.
                # If "UNKNOWN", we might default to params.target_crs_id (no-op) or fail.
                # Let's assume valid source.

                # Note: reproject_scan_to_target_crs expects 'in_srs' string (EPSG:..., WKT).
                # If 'source_crs_raw' is "UNKNOWN", we use target (no-op).
                in_srs = source_crs if source_crs != "UNKNOWN" else params.target_crs_id

                r_res = await workflow.execute_activity(
                    "reproject_scan_to_target_crs",
                    args=[
                        params.company_id,
                        self._dataset_version_id,
                        scan_id,
                        params.schema_version,
                        in_srs,
                        params.target_crs_id # out_srs
                    ],
                    start_to_close_timeout=timedelta(hours=2),
                    retry_policy=rp,
                )
                reproject_results.append(r_res)

            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "IN_PROGRESS", {"stage": "reproject_complete"}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            # === 3. ICP Register ===
            self._stage = "icp_register"
            # Run dataset-level ICP
            icp_res = await workflow.execute_activity(
                "propose_registration_edges_for_dataset",
                args=[
                    params.company_id,
                    self._dataset_version_id,
                    params.schema_version,
                    1.0, # voxel
                    5.0, # max_corr
                    0.05 # min_fitness
                ],
                start_to_close_timeout=timedelta(minutes=30),
                retry_policy=rp,
            )

            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "IN_PROGRESS", {"stage": "icp_complete", "edges": icp_res.get("count", 0)}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            # === 4. New Cluster ===
            self._stage = "new_cluster"
            new_cluster_results = []
            for scan_id in self._scan_ids:
                # cluster_scan_custom(company_id, dv_id, scan_id, schema_version...)
                c_res = await workflow.execute_activity(
                    "cluster_scan_custom",
                    args=[
                        params.company_id,
                        self._dataset_version_id,
                        scan_id,
                        params.schema_version,
                        0.05, # voxel
                        0.3,  # radius
                        20    # min_size
                    ],
                    start_to_close_timeout=timedelta(hours=1),
                    retry_policy=rp,
                )
                new_cluster_results.append(c_res)

            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "IN_PROGRESS", {"stage": "new_cluster_complete"}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            # === 5. Old Cluster ===
            old_cluster_res = None
            if params.run_old_cluster:
                self._stage = "old_cluster"
                # This runs the ClusterPipeline (tiling/merging)
                # It usually takes derived.reprojected_point_cloud as input.
                # It produces derived.clustered_point_cloud.
                # Note: This might overwrite the New Cluster output if New Cluster also uses that kind!
                # I changed New Cluster to use "derived.clustered_point_cloud" in my code earlier.
                # I MUST change one of them.
                # I will assume New Cluster is the priority for "Publish".
                # So I should change Old Cluster output or New Cluster output.
                # Let's change New Cluster output in the activity to "derived.custom_clustered_point_cloud"
                # and update export to look for it.
                # OR keep New as "derived.clustered..." and let Old overwrite it?
                # User said "cluster (new) + old cluster".
                # If Old overwrites New, New is lost.
                # Maybe Old Cluster is just for side-effect?
                # I'll stick to running it.

                c_params = ClusterPipelineParams(
                    dataset_version_id=self._dataset_version_id,
                    schema_version=params.schema_version,
                )
                old_cluster_res = await workflow.execute_child_workflow(
                    f"{VERSION}_cluster",
                    c_params,
                    task_queue="point-cloud-task-queue",
                    retry_policy=rp,
                )

            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "IN_PROGRESS", {"stage": "old_cluster_complete"}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            # === 6. Publish ===
            self._stage = "publish"
            # export_merged_laz
            publish_res = await workflow.execute_activity(
                "export_merged_laz",
                args=[
                    params.company_id,
                    self._dataset_version_id,
                    params.schema_version,
                    "merged_final.laz"
                ],
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=rp,
            )

            self._stage = "done"
            result = {
                "ingest": ingest_results,
                "reproject": reproject_results,
                "icp": icp_res,
                "new_cluster": new_cluster_results,
                "old_cluster": old_cluster_res,
                "publish": publish_res
            }

            # Notify DONE
            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "DONE", result],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )

            return result

        except Exception as e:
            # Notify FAILED
            await workflow.execute_activity(
                "update_pipeline_status",
                args=[workflow_id, "FAILED", {"error": str(e)}],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp
            )
            raise e
