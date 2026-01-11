from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional
import yaml

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store

with open(r'D:\1_prod\point_cloud\config.yaml', 'r') as f:
    VERSION = yaml.safe_load(f.read())['VERSION_INFO']['WORKFLOW_VERSION']

@dataclass
class DownloadWorkflowParams:
    scan_id: str
    dst_dir: str
    kinds: Optional[List[str]] = None


@workflow.defn(name=f"{VERSION}-download")
class DownloadWorkflow:
    def __init__(self):
        self._stage = 'Initialize'
        self._scan_id: Optional[str] = None

    @workflow.query
    def progress(self) -> dict:
        return {
            'stage': self._stage,
            'scan_id': self._scan_id,
        }

    @workflow.run
    async def run(self, params: DownloadWorkflowParams):
        self._scan_id = params.scan_id
        self._stage = 'Looking for raw artifacts for scan_id in DB'

        raw_arts = await workflow.execute_activity(
            'list_raw_artifacts',
            args=[params.scan_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )

        if params.kinds:
            wanted = set(params.kinds)
            raw_arts = [art for art in raw_arts if art['kind'] in wanted]

        if not raw_arts:
            raise ApplicationError(
                f'No raw artifacts for scan {params.scan_id}',
                non_retryable=True
            )

        self._stage = 'Download artifacts'
        results: Dict[str, str] = {}
        for art in raw_arts:
            local_path = await workflow.execute_activity(
                "download_from_s3",
                args=[art["bucket"], art["key"], params.dst_dir],
                start_to_close_timeout=timedelta(hours=1),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )
            kind = art["kind"]
            if kind in results:
                raise ApplicationError(
                    f"Duplicate raw artifact kind '{kind}' for scan {params.scan_id}",
                    non_retryable=True,
                )
            results[kind] = local_path

        self._stage = 'Workflow is done'
        return results