from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict

from temporalio import workflow
from temporalio.common import RetryPolicy

from .download_workflow import DownloadWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class ProfilingWorkflowParams:
    scan_id: str
    cloud_path: str
    geojson_dst: str


@workflow.defn(name=f'{VERSION}-profiling')
class ProfilingWorkflow:
    def __init__(self) -> None:
        self._stage = 'Initialize'

    @workflow.query
    async def progress(self) -> dict:
        return {
            'stage': self._stage,
        }

    @workflow.run
    async def run(self, params: ProfilingWorkflowParams) -> Dict[str, object]:
        self._stage = 'Downloading file'

        files_by_kind: Dict[str, str] = await workflow.execute_child_workflow(
            f"{VERSION}-download",
            DownloadWorkflowParams(
                scan_id=params.scan_id,
                dst_dir=params.cloud_path,
                kinds=["raw.point_cloud"],
            ),
        )

        cloud_file = files_by_kind["raw.point_cloud"]

        self._stage = 'Profiling files'

        meta = await workflow.execute_activity(
            "point_cloud_meta",
            args=[cloud_file, params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=1),
            ),
        )

        self._stage = 'Reading hexbin GeoJSON'
        geojson = await workflow.execute_activity(
            "read_cloud_hexbin",
            args=[params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=20),
            ),
        )

        self._stage = 'Extracting hexbin GeoJSON fields'
        hexbin_fields = await workflow.execute_activity(
            "extract_hexbin_fields",
            args=[geojson],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=20),
            ),
        )

        self._stage = 'Uploading hexbin GeoJSON'
        upload_info = await workflow.execute_activity(
            "upload_hexbin",
            args=[params.scan_id, params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=1),
            ),
        )

        self._stage = 'Uploading profiling manifest'
        manifest_info = await workflow.execute_activity(
            "upload_profiling_manifest",
            args=[params.scan_id, meta, hexbin_fields, upload_info],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=1),
            ),
        )

        self._stage = 'Aggregating results'
        return {
            "scan_id": params.scan_id,
            "cloud_file": cloud_file,
            "geojson_dst": params.geojson_dst,
            "meta": meta,
            "hexbin_fields": hexbin_fields,
            "upload_info": upload_info,
            "manifest_info": manifest_info,
        }
