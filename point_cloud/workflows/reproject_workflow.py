from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional
import os

from temporalio import workflow
from temporalio.common import RetryPolicy

VERSION = os.getenv("WORKFLOW_VERSION", "MVP")


@dataclass
class ReprojectWorkflowParams:
    company_id: str
    dataset_version_id: str
    schema_version: str = "1.1.0"
    scan_ids: Optional[List[str]] = None
    in_crs_id: Optional[str] = None
    out_crs_id: Optional[str] = None
    in_srs: Optional[str] = None
    out_srs: Optional[str] = None


@workflow.defn(name=f"{VERSION}-reproject")
class ReprojectWorkflow:
    def __init__(self) -> None:
        self._stage: str = "Initializing"
        self._processed: list[str] = []

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage, "processed": self._processed}

    @workflow.run
    async def run(self, params: ReprojectWorkflowParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)

        self._stage = "resolve_srs"
        in_srs = params.in_srs
        if not in_srs and params.in_crs_id:
            in_srs = await workflow.execute_activity(
                "resolve_crs_to_pdal_srs",
                args=[params.in_crs_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

        out_srs = params.out_srs
        if not out_srs and params.out_crs_id:
            out_srs = await workflow.execute_activity(
                "resolve_crs_to_pdal_srs",
                args=[params.out_crs_id],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=rp_fast,
            )

        if not in_srs or not out_srs:
            raise ValueError("Both input and output SRS must be provided or resolvable")

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
        self._stage = "reproject_scans"
        for scan_id in scan_ids:
            result = await workflow.execute_activity(
                "reproject_scan_to_target_crs",
                args=[
                    params.company_id,
                    params.dataset_version_id,
                    scan_id,
                    params.schema_version,
                    in_srs,
                    out_srs,
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
            "in_srs": in_srs,
            "out_srs": out_srs,
        }
