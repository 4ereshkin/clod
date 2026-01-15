from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any, List, Optional
import os

from temporalio import workflow
from temporalio.common import RetryPolicy

from point_cloud.workflows.download_workflow import DownloadWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class ClusterPipelineParams:
    company_id: str
    dataset_name: str


@workflow.defn
class ClusterPipeline:
    def __init__(self):
        self._stage = 'Init'

    @workflow.query
    async def progress(self):
        return {
            'stage': self._stage,
        }

    @workflow.run
    async def run(self, params: ClusterPipelineParams):

        self._stage = 'Download merged point cloud'

        files_by_kind: Dict[str, str] = await workflow.execute_child_workflow(
            f"{VERSION}-download",
            DownloadWorkflowParams(
                scan_id=params.scan_id,
                dst_dir=params.cloud_path,
                kinds=["raw.point_cloud"],
            ),
        )