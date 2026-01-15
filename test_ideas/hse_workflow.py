from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import timedelta
from typing import Dict, Any, List, Optional
import os

from temporalio import workflow
from temporalio.common import RetryPolicy

from point_cloud.workflows.download_workflow import DownloadWorkflowParams

VERSION = os.environ["WORKFLOW_VERSION"]
SCHEMA_VERSION = os.environ['SCHEMA_VERSION']


@dataclass
class ClusterPipelineParams:
    dataset_version_id: str
    schema_version: str = SCHEMA_VERSION
    dst_dir: str = f'point_cloud/tmp/cluster/'


@workflow.defn(name=f'{VERSION}_cluster')
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

        merged_cloud = workflow.execute_activity('download_dataset_version_artifact',
                                        args=[params.dataset_version_id,
                                              'derived.merged_point_cloud',
                                              params.schema_version,
                                              f'{params.dst_dir}/{params.dataset_version_id}/'
                                              ],)

        hasattr()
