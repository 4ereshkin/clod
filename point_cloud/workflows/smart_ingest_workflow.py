from __future__ import annotations

import os
from datetime import timedelta
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from .download_workflow import DownloadWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]

@dataclass
class SmartIngestWorkflowParams:
    company_id: str
    dataset_name: str
    bump_version: bool
    # CRS params (Target CRS)
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

    # Extra logic
    override_crs_id: Optional[str] = None  # Explicit override if different from dataset's crs_id


@workflow.defn(name=f"{VERSION}-smart-ingest")
class SmartIngestWorkflow:
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

    @workflow.signal
    async def add_raw_artifacts(
        self,
        artifacts: List[Dict[str, str]],
    ) -> None:
        if isinstance(artifacts, list):
            self._artifacts.extend(artifacts)

    @workflow.run
    async def run(self, params: SmartIngestWorkflowParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_long = RetryPolicy(maximum_attempts=2)

        # === PART 1: Standard Ingest ===

        # 1. Ensure company
        self._stage = "Ensuring company exists"
        await workflow.execute_activity(
            "ensure_company",
            args=[params.company_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        # 2. Ensure CRS (Target)
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
                retry_policy=rp_fast,
            )

        # 3. Ensure dataset
        self._stage = "Ensuring dataset exists"
        dataset_id = await workflow.execute_activity(
            "ensure_dataset",
            args=[params.company_id, crs_id, params.dataset_name],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        # 3.1 Ensure dataset version
        self._stage = "Ensuring dataset version"
        dv = await workflow.execute_activity(
            "ensure_dataset_version",
            args=[dataset_id, params.bump_version],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )
        dataset_version_id = dv["id"]

        # 4. Create scan
        self._stage = "Creating scan"
        scan_id = await workflow.execute_activity(
            "create_scan",
            args=[params.company_id, dataset_version_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )
        self._scan_id = scan_id

        if params.scan_meta:
            self._stage = "Updating scan metadata"
            await workflow.execute_activity(
                "update_scan_meta",
                args=[scan_id, params.scan_meta],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

        # 5. Prepare artifacts
        self._stage = "Preparing artifacts"
        artifacts = params.artifacts or []
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
                    retry_policy=rp_long,
                )
                upload_results.append(result)
                if artifact.get('kind', 'raw.point_cloud') == 'raw.point_cloud':
                    cloud_uploaded = True
            except Exception as e:
                error_msg = f"Failed to upload artifact {artifact.get('local_file_path')}: {e}"
                self._errors[artifact.get('local_file_path', 'unknown')] = error_msg
                if artifact.get('kind') == 'raw.point_cloud':
                    raise ApplicationError(f"Failed to upload required point cloud artifact: {error_msg}")

        if not cloud_uploaded:
            raise RuntimeError("raw.point_cloud artifact is required")
        self._upload_results = upload_results

        await workflow.sleep(timedelta(seconds=0.5))

        # 7. Create ingest run
        self._stage = "Creating ingest run"
        run_id = await workflow.execute_activity(
            "create_ingest_run",
            args=[params.company_id, scan_id, params.schema_version, params.force],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        # 8. Process ingest run
        self._stage = "Processing ingest run"
        process_result = await workflow.execute_activity(
            "process_ingest_run",
            args=[run_id],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=rp_long,
        )
        self._manifest_key = process_result.get("manifest_key")
        self._manifest_bucket = process_result.get("manifest_bucket")


        # === PART 2: Profiling ===

        self._stage = "Downloading for profiling"
        # Download raw.point_cloud to a temp location for profiling
        # Using child workflow to handle download logic
        # We need a temporary directory path, assuming worker handles it relative or absolute
        profiling_dir = f"/tmp/profiling/{scan_id}"

        files_by_kind = await workflow.execute_child_workflow(
            f"{VERSION}-download",
            DownloadWorkflowParams(
                scan_id=scan_id,
                dst_dir=profiling_dir,
                kinds=["raw.point_cloud"],
            ),
        )
        cloud_file = files_by_kind["raw.point_cloud"]
        geojson_dst = f"{profiling_dir}/{scan_id}.geojson"

        self._stage = "Profiling (Meta)"
        meta = await workflow.execute_activity(
            "point_cloud_meta",
            args=[cloud_file, geojson_dst],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_fast,
        )

        self._stage = "Profiling (Hexbin)"
        geojson = await workflow.execute_activity(
            "read_cloud_hexbin",
            args=[geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )

        hexbin_fields = await workflow.execute_activity(
            "extract_hexbin_fields",
            args=[geojson],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )

        self._stage = "Uploading Profiling Artifacts"
        upload_info = await workflow.execute_activity(
            "upload_hexbin",
            args=[scan_id, geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )

        manifest_info = await workflow.execute_activity(
            "upload_profiling_manifest",
            args=[scan_id, meta, hexbin_fields, upload_info],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )


        # === PART 3: Logic & Manifest Update ===

        self._stage = "Computing Logic"

        # 1. Count scans
        scan_count = await workflow.execute_activity(
            "count_scans_in_dataset_version",
            args=[dataset_version_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        # 2. Determine Flags
        # Logic: < 5 -> NO, >= 5 -> YES
        georeference = "NO" if scan_count < 5 else "YES"

        # Logic: CRS Override
        # If Point Cloud has embedded SRS, we still override with External (Choice without choice).
        # We assume 'crs_id' passed in params is the External/Target CRS.
        target_crs_id = params.override_crs_id or crs_id

        # Extract intrinsic SRS from 'meta' (PDAL metadata)
        # PDAL usually puts SRS in 'srs' key (wkt or projjson)
        intrinsic_srs = meta.get("srs", {}).get("wkt") or meta.get("srs", {}).get("json") or "UNKNOWN"

        logic_update = {
            "georeference": georeference,
            "reproject": "YES",  # Always reproject to ensure consistency with Target CRS
            "target_crs_id": target_crs_id,
            "source_crs_raw": intrinsic_srs,
            "profiling_scan_count": scan_count
        }

        # 3. Update Manifest
        self._stage = "Updating Manifest"
        await workflow.execute_activity(
            "update_ingest_manifest_with_logic",
            args=[scan_id, logic_update, params.schema_version],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        self._stage = "Completed"

        return {
            "scan_id": scan_id,
            "dataset_version_id": dataset_version_id,
            "ingest_run_id": run_id,
            "profiling": manifest_info,
            "logic": logic_update,
            "status": "SUCCEEDED"
        }
