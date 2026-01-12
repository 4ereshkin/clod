from __future__ import annotations

from dataclasses import dataclass
import os

from typing import Optional, List, Dict, Any

from temporalio import workflow
from temporalio.common import RetryPolicy

VERSION = os.environ["WORKFLOW_VERSION"]


@dataclass
class PreprocessPipelineParams:
    company_id: str
    dataset_version_id: str
    schema_version: str = '1.1.0'

@workflow.defn(name=f'{VERSION}-preprocessing_workflow')
class PreprocessPipeline:
    def __init__(self) -> None:
        self._stage: str = "Initializing"

    @workflow.query
    def progress(self) -> dict:
        return {'stage': self._stage}

    @workflow.run
    async def run(self, params: PreprocessPipelineParams) -> Dict[str, Any]:

        self._stage = 'shift_cloud'
        shift = await workflow.execute_activity(
            'shift_pointcloud',
            args=[params.company_id, params.dataset_version_id, params.schema_version],
        )
