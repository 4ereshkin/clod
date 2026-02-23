from typing import Any
from temporalio.client import Client

from temporalio.service import RPCError


class TemporalAdapter:
    def __init__(self, client: Client):
        self.client = client


    async def start_workflow(self, *, workflow_name: str, workflow_id: str,
                             task_queue: str, payload: dict[str, Any]) -> None:

        await self.client.start_workflow(
            workflow=workflow_name,
            id=workflow_id,
            task_queue=task_queue,
            arg=payload
        )

    async def query_workflow(self, *, workflow_id: str, query_name: str) -> dict[str, Any]:
        handle = self.client.get_workflow_handle(workflow_id)
        return await handle.query(query_name)

    async def wait_result(self, *, workflow_id: str) -> dict[str, Any]:
        handle = self.client.get_workflow_handle(workflow_id)
        return await handle.result()