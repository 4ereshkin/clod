from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any, List, Optional
import os

from temporalio import workflow
from temporalio.common import RetryPolicy

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class PreprocessPipelineParams:
    company_id: str
    dataset_version_id: str
    schema_version: str = "1.1.0"
    scan_ids: Optional[List[str]] = None
    input_kind: str = "derived.reprojected_point_cloud"
    output_kind: str = "derived.preprocessed_point_cloud"
    voxel_size_m: float = 0.10
    mean_k: int = 20
    multiplier: float = 2.0


@workflow.defn(name=f"{VERSION}-preprocessing_workflow")
class PreprocessPipeline:
    def __init__(self) -> None:
        self._stage: str = "Initializing"
        self._processed: list[str] = []

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage, "processed": self._processed}

    @workflow.run
    async def run(self, params: PreprocessPipelineParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)

        self._stage = "resolve_scans"
        scan_ids = list(params.scan_ids or [])
        if not scan_ids:
            scan_ids = await workflow.execute_activity(
                "list_scans_by_dataset_version",
                args=[params.dataset_version_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

        results = []
        self._stage = "preprocess_scans"
        for scan_id in scan_ids:
            result = await workflow.execute_activity(
                "preprocess_point_cloud",
                args=[
                    params.company_id,
                    params.dataset_version_id,
                    scan_id,
                    params.schema_version,
                    params.input_kind,
                    params.output_kind,
                    params.voxel_size_m,
                    params.mean_k,
                    params.multiplier,
                ],
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=rp_fast,
            )
            self._processed.append(scan_id)
            results.append(result)

        self._stage = "done"
        return {
            "scan_ids": scan_ids,
            "processed": self._processed,
            "results": results,
        }
