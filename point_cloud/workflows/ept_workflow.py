import os
from datetime import timedelta

from typing import Optional, Dict, Any
from dataclasses import dataclass

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

WORKFLOW_VERSION = os.getenv("WORKFLOW_VERSION", "MVP")
SCHEMA_VERSION = os.getenv("WORKFLOW_VERSION", "MVP")


@dataclass
class EptWorkflowParams:
    company_id: str
    dataset_name: str
    crs_id: str
    scan_id: str
    schema_version: str = SCHEMA_VERSION


@workflow.defn
class EptWorkflow:
    def __init__(self) -> None:
        self._stage: str = 'Initializing'
        self._scan_id: Optional[str] = None

    @workflow.query
    async def progress(self) -> dict:
        return {
            'stage': self._stage,
            'scan_id': self._scan_id
        }

    @workflow.run
    async def run(self, params: EptWorkflowParams) -> Dict[str, Any]:
        self._stage = 'Ensuring company exists'
        # TODO: может общие для всех воркфлоу проверки вынести в какую-то общую функцию?
        await workflow.execute_activity(
            'ensure_company',
            args=[params.company_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._stage = 'Ensuring dataset exists'
        if not params.crs_id:
            raise ApplicationError('crs_id is required, not found in params for workflow')

        dataset_id = await workflow.execute_activity(
            'ensure_dataset',
            args=[params.company_id, params.crs_id, params.dataset_name],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._stage = 'Ensuring dataset version'
        dv = await workflow.execute_activity(
            'ensure_dataset_version',
            args=[dataset_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        dataset_version_id = dv['id']

        # TODO: Максим из будущего, я знаю ты это прочтёшь, но посмотри и законспектируй этот крутой видос https://www.youtube.com/watch?v=vBD4jzv0oJ0
