import os
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class TriggerArtifactDTO(BaseModel):
    s3_key: str
    etag: str

class TriggerScanDTO(BaseModel):
    control_point: Optional[Dict[str, TriggerArtifactDTO]] = None
    trajectory: Optional[Dict[str, TriggerArtifactDTO]] = None
    point_cloud: Optional[Dict[str, TriggerArtifactDTO]] = None
    # Add other possible fields as optional

class TriggerVersionDTO(BaseModel):
    message_version: str
    pipeline_version: str

class TriggerPayloadDTO(BaseModel):
    workflow_id: str
    scenario: str
    version: TriggerVersionDTO
    dataset: Dict[str, TriggerScanDTO]

    # Extra fields for mapping purposes (not in original spec, but needed for internal logic)
    company_id: Optional[str] = "test_company"
    dataset_name: Optional[str] = None
    target_crs_id: Optional[str] = "EPSG:32641"
    run_old_cluster: bool = True

class PipelineScanDTO(BaseModel):
    artifacts: List[Dict[str, str]]
    scan_meta: Optional[Dict[str, Any]] = None

class StartPipelineDTO(BaseModel):
    company_id: str
    dataset_name: str
    target_crs_id: str
    scans: List[PipelineScanDTO]
    schema_version: str = "1.1.0"
    force: bool = False
    run_old_cluster: bool = True

def _clean_key(path: str) -> str:
    # Handle windows paths on linux env
    parts = path.replace("\\", "/").split("/")
    return parts[-1]

def map_trigger_to_start_dto(trigger: TriggerPayloadDTO) -> StartPipelineDTO:
    scans = []

    for scan_id, scan_data in trigger.dataset.items():
        artifacts = []

        if scan_data.point_cloud:
            for key, artifact in scan_data.point_cloud.items():
                local_path = artifact.s3_key
                filename = _clean_key(local_path)
                artifacts.append({
                    "kind": "raw.point_cloud",
                    "bucket": "default",
                    "key": filename,
                    "local_file_path": local_path,
                    "etag": artifact.etag
                })

        if scan_data.control_point:
            for key, artifact in scan_data.control_point.items():
                local_path = artifact.s3_key
                filename = _clean_key(local_path)
                artifacts.append({
                    "kind": "raw.control_point",
                    "bucket": "default",
                    "key": filename,
                    "local_file_path": local_path,
                    "etag": artifact.etag
                })

        if scan_data.trajectory:
            for key, artifact in scan_data.trajectory.items():
                local_path = artifact.s3_key
                filename = _clean_key(local_path)
                artifacts.append({
                    "kind": "raw.trajectory",
                    "bucket": "default",
                    "key": filename,
                    "local_file_path": local_path,
                    "etag": artifact.etag
                })

        scans.append(PipelineScanDTO(
            artifacts=artifacts,
            scan_meta={"scan_id": scan_id}
        ))

    comp_id = trigger.company_id or "test_company"
    ds_name = trigger.dataset_name or f"dataset_{trigger.workflow_id}"
    crs = trigger.target_crs_id or "EPSG:32641"

    return StartPipelineDTO(
        company_id=comp_id,
        dataset_name=ds_name,
        target_crs_id=crs,
        scans=scans,
        run_old_cluster=trigger.run_old_cluster
    )
