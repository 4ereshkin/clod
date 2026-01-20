"""
Workflow for ingesting point cloud data using the new database structure.

This workflow coordinates the ingestion process:
1. Ensure company/CRS/dataset exist
2. Create a scan
3. Upload raw artifacts (point cloud, trajectory, control points) to S3
4. Create an ingest run
5. Process the ingest run (build manifest)
"""

from __future__ import annotations

import os
from datetime import timedelta
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class IngestWorkflowParams:
    company_id: str
    dataset_name: str
    bump_version: bool
    crs_id: Optional[str] = None
    crs_epsg: Optional[int] = None
    crs_name: Optional[str] = None
    crs_zone_degree: int = 0
    crs_units: str = "m"
    crs_axis_order: str = "x_east,y_north,z_up"
    schema_version: str = "1.1.0"
    force: bool = False
    artifacts: Optional[List[Dict[str, str]]] = None
    scan_meta: Optional[Dict[str, Any]] = None


@workflow.defn(name=f"{VERSION}-ingest")
class IngestWorkflow:
    def __init__(self) -> None:
        self._stage: str = "Initializing"
        self._scan_id: Optional[str] = None
        self._errors: Dict[str, str] = {}
        self._artifacts: List[Dict[str, str]] = []
        self._upload_results: List[Dict[str, Any]] = []
        self._manifest_key: Optional[str] = None
        self._manifest_bucket: Optional[str] = None

    @workflow.query
    def progress(self) -> dict:
        return {
            'stage': self._stage,
            'scan_id': self._scan_id,
            'errors': self._errors,
        }

    @workflow.query
    def ingested_artifacts(self) -> Dict[str, Any]:
        artifacts = list(self._upload_results)
        if self._manifest_key and self._manifest_bucket:
            artifacts.append(
                {
                    "kind": "derived.ingest_manifest",
                    "bucket": self._manifest_bucket,
                    "key": self._manifest_key,
                }
            )
        return {"artifacts": artifacts}

    @workflow.signal
    async def add_raw_artifacts(
        self,
        artifacts: List[Dict[str, str]],
    ) -> None:
        """Signal to add raw artifacts for upload (optional, can also be provided in params)."""
        if isinstance(artifacts, list):
            self._artifacts.extend(artifacts)

    @workflow.run
    async def run(self, params: IngestWorkflowParams) -> Dict[str, Any]:
        """
        Run the ingest workflow.

        Parameters
        ----------
        params:
            Workflow parameters including company, dataset, and scan info
        """
        # 1. Ensure company exists
        self._stage = "Ensuring company exists"
        await workflow.execute_activity(
            "ensure_company",
            args=[params.company_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 2. Ensure CRS if provided
        crs_id = params.crs_id
        if params.crs_epsg is not None and not crs_id:
            crs_id = f"EPSG:{params.crs_epsg}"

        if crs_id:
            epsg = params.crs_epsg
            if epsg is None and crs_id.upper().startswith("EPSG:"):
                try:
                    epsg = int(crs_id.split(":", 1)[1])
                except ValueError:
                    epsg = None
            self._stage = "Ensuring CRS exists"
            await workflow.execute_activity(
                "ensure_crs",
                args=[
                    crs_id,
                    params.crs_name or crs_id,
                    params.crs_zone_degree,
                    epsg,
                    params.crs_units,
                    params.crs_axis_order,
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

        # 3. Ensure dataset exists + resolve dataset_id (ULID)
        self._stage = "Ensuring dataset exists"
        dataset_id = await workflow.execute_activity(
            "ensure_dataset",
            args=[params.company_id, crs_id, params.dataset_name],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 3.1 Ensure dataset version (active or bump)
        self._stage = "Ensuring dataset version"
        dv = await workflow.execute_activity(
            "ensure_dataset_version",
            args=[dataset_id, params.bump_version],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        dataset_version_id = dv["id"]

        # 4. Create scan (on dataset_version)
        self._stage = "Creating scan"
        scan_id = await workflow.execute_activity(
            "create_scan",
            args=[params.company_id, dataset_version_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        self._scan_id = scan_id

        if params.scan_meta:
            self._stage = "Updating scan metadata"
            await workflow.execute_activity(
                "update_scan_meta",
                args=[scan_id, params.scan_meta],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

        # 5. Get artifacts from params or wait for signal
        self._stage = "Preparing artifacts"
        artifacts = params.artifacts or []
        
        # If no artifacts in params, wait for signal
        if not artifacts:
            self._stage = "Waiting for raw artifacts"
            await workflow.wait_condition(lambda: len(self._artifacts) > 0)
            artifacts = self._artifacts

        # 6. Upload raw artifacts
        self._stage = "Uploading raw artifacts"
        upload_results = []
        cloud_uploaded = False
        for artifact in artifacts:
            try:
                result = await workflow.execute_activity(
                    "upload_raw_artifact",
                    args=[
                        params.company_id,
                        dataset_version_id,
                        scan_id,
                        artifact.get('kind', 'raw.point_cloud'),
                        artifact.get('local_file_path'),
                        artifact.get('filename'),
                    ],
                    start_to_close_timeout=timedelta(hours=1),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )
                upload_results.append(result)
                if artifact.get('kind', 'raw.point_cloud') == 'raw.point_cloud':
                    cloud_uploaded = True
            except Exception as e:
                error_msg = f"Failed to upload artifact {artifact.get('local_file_path')}: {e}"
                self._errors[artifact.get('local_file_path', 'unknown')] = error_msg
                # Если это point_cloud и загрузка не удалась, прерываем workflow
                if artifact.get('kind') == 'raw.point_cloud':
                    raise ApplicationError(f"Failed to upload required point cloud artifact: {error_msg}")

        # Проверяем, что point_cloud был загружен
        if not cloud_uploaded:
            raise RuntimeError("raw.point_cloud artifact is required but was not uploaded successfully")
        self._upload_results = upload_results

        # Небольшая задержка для гарантии коммита транзакций БД
        await workflow.sleep(timedelta(seconds=0.5))

        # 7. Create ingest run
        self._stage = "Creating ingest run"
        run_id = await workflow.execute_activity(
            "create_ingest_run",
            args=[
                params.company_id,
                scan_id,
                params.schema_version,
                params.force,
            ],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 8. Process ingest run
        self._stage = "Processing ingest run"
        process_result = await workflow.execute_activity(
            "process_ingest_run",
            args=[run_id],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        self._manifest_key = process_result.get("manifest_key")
        self._manifest_bucket = process_result.get("manifest_bucket")

        self._stage = "Completed"

        return {
            "scan_id": scan_id,
            'dataset_id': dataset_id,
            'dataset_version_id': dataset_version_id,
            "ingest_run_id": run_id,
            "upload_results": upload_results,
            "process_result": process_result,
            "errors": self._errors,
        }
