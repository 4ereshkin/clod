from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class DownloadWorkflowParams:
    scan_id: str
    dst_dir: str
    kinds: Optional[List[str]] = None


@workflow.defn(name=f"{VERSION}-download")
class DownloadWorkflow:
    def __init__(self) -> None:
        self._stage = 'Initialize'
        self._scan_id: Optional[str] = None
        self._total_artifacts = 0
        self._downloaded_artifacts = 0
        self._current_kind: Optional[str] = None

    @workflow.query
    async def progress(self) -> dict:
        return {
            'stage': self._stage,
            'scan_id': self._scan_id,
            'total_artifacts': self._total_artifacts,
            'downloaded_artifacts': self._downloaded_artifacts,
            'current_kind': self._current_kind,
        }

    @workflow.run
    async def run(self, params: DownloadWorkflowParams) -> Dict[str, str]:
        self._scan_id = params.scan_id
        self._stage = 'Looking for raw artifacts for scan_id in DB'

        raw_arts = await workflow.execute_activity(
            'list_raw_artifacts',
            args=[params.scan_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=5,
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=10),
            ),
        )

        if params.kinds:
            wanted = set(params.kinds)
            raw_arts = [art for art in raw_arts if art['kind'] in wanted]

        if not raw_arts:
            raise ApplicationError(
                f'No raw artifacts for scan {params.scan_id}',
                non_retryable=True,
            )

        kinds_seen = set()
        for art in raw_arts:
            kind = art["kind"]
            if kind in kinds_seen:
                raise ApplicationError(
                    f"Duplicate raw artifact kind '{kind}' for scan {params.scan_id}",
                    non_retryable=True,
                )
            kinds_seen.add(kind)

        self._total_artifacts = len(raw_arts)
        self._downloaded_artifacts = 0
        self._stage = 'Download artifacts'

        results: Dict[str, str] = {}
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(3)

        async def download_artifact(art: Dict[str, str]) -> None:
            kind = art["kind"]
            async with semaphore:
                async with lock:
                    self._current_kind = kind
                local_path = await workflow.execute_activity(
                    "download_from_s3",
                    args=[art["bucket"], art["key"], params.dst_dir],
                    start_to_close_timeout=timedelta(hours=1),
                    retry_policy=RetryPolicy(
                        maximum_attempts=4,
                        initial_interval=timedelta(seconds=2),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=30),
                    ),
                )
            async with lock:
                results[kind] = local_path
                self._downloaded_artifacts += 1

        await asyncio.gather(*(download_artifact(art) for art in raw_arts))

        self._stage = 'Workflow is done'
        self._current_kind = None
        return results
