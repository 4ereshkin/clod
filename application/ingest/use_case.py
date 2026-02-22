from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Protocol

from application.ingest.contracts import ScenarioResult, StartIngestCommand, WorkflowStatus
from application.ingest.mappers import to_result_objects, to_status_event
from application.ingest.scenario_resolver import resolve_scenario


class TemporalGateway(Protocol):
    async def start_workflow(self, *, workflow_name: str,
                             workflow_id: str, task_queue: str,
                             payload: dict[str, Any]) -> None:
        ...
    async def query_workflow(self, *, workflow_id: str,
                             query_name: str) -> dict[str, Any]:
        ...
    async def wait_result(self, *, workflow_id: str,) -> dict[str, Any]:
        ...


class StatusStore(Protocol):
    async def set_status(self, *, workflow_id: str, status: str,
                         payload: dict[str, Any]) -> None:
        ...


class EventPublisher(Protocol):
    async def publish_status(self, event: Any) -> None:
        ...
    async def publish_completed(self, event: Any) -> None:
        ...
    async def publish_failed(self, event: Any) -> None:
        ...


@dataclass
class StartIngestUseCase:
    temporal: TemporalGateway
    status_store: StatusStore
    publisher: EventPublisher

    async def execute(self, command: StartIngestCommand) -> ScenarioResult:
        try:
            spec = resolve_scenario(scenario=command.scenario, pipeline_version=command.pipeline_version)
        except ValueError:
            await self._push_status(command=command, status=WorkflowStatus.FAILED)
            raise

        await self._push_status(command=command, status=WorkflowStatus.RESOLVED_SCENARIO,
                                details={"workflow_name": spec.workflow_name})

        payload = {
            "workflow_id": command.workflow_id,
            "scenario": command.scenario,
            "message_version": command.message_version,
            "pipeline_version": command.pipeline_version,
            "dataset": command.dataset,
        }

        await self._push_status(command=command, status=WorkflowStatus.STARTING)
        await self.temporal.start_workflow(
            workflow_name=spec.workflow_name,
            workflow_id=command.workflow_id,
            task_queue=spec.task_queue,
            payload=payload
        )

        progress = await self.temporal.query_workflow(workflow_id=command.workflow_id, query_name=spec.query_name)
        await self._push_status(command=command, status=WorkflowStatus.RUNNING, details=progress)

        raw_result = await self.temporal.wait_result(workflow_id=command.workflow_id)
        outputs = to_result_objects(raw_result.get("outputs", []))

        result = ScenarioResult(
            workflow_id=command.workflow_id,
            scenario=command.scenario,
            status=WorkflowStatus.COMPLETED,
            outputs=outputs,
            details=raw_result,
            timestamp=time.time(),

        )

        await self._push_status(command=command, status=WorkflowStatus.COMPLETED, details={"outputs": [o.__dict__ for o in outputs]})
        await self.publisher.publish_completed(result)
        return result

    async def _push_status(self, *, command: StartIngestCommand, status: WorkflowStatus,
                           details: dict[str, Any] | None = None) -> None:
        event = to_status_event(
            workflow_id=command.workflow_id,
            scenario=command.scenario,
            status=status,
            details=details,
        )
        await self.status_store.set_status(
            workflow_id=command.workflow_id,
            status=status.value,
            payload=event.model_dump(),
        )
        await self.publisher.publish_status(event)
