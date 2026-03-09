import time
from dataclasses import dataclass
from typing import Any

from temporalio.exceptions import ApplicationError

from application.common.contracts import (
    WorkflowStatus, ErrorCode, FailedEvent, StatusEvent
)
from application.common.interfaces import (
    TemporalGateway, StatusStore, EventPublisher
)

from application.common.contracts import BaseStartCommand, ScenarioResult
from application.common.scenario_resolver import resolve_scenario

from application.common.mappers import to_result_objects, to_status_event


@dataclass
class StartUseCase:
    temporal: TemporalGateway
    status_store: StatusStore
    publisher: EventPublisher

    async def execute(self, command: BaseStartCommand) -> ScenarioResult:
        try:
            spec = resolve_scenario(scenario=command.scenario, pipeline_version=command.pipeline_version)
        except ValueError as err:
            await self._push_status(command=command, status=WorkflowStatus.FAILED, details={"error": str(err)})
            raise

        await self._push_status(command=command, status=WorkflowStatus.RESOLVED_SCENARIO,
                                details={"workflow_name": spec.workflow_name})

        payload = command.to_temporal_payload()

        await self._push_status(command=command, status=WorkflowStatus.STARTING, details={"payload": payload})

        try:
            await self.temporal.start_workflow(
                workflow_name=spec.workflow_name,
                workflow_id=command.workflow_id,
                task_queue=spec.task_queue,
                payload=payload
            )
        except ApplicationError as err:
            await self.status_store.set_status(
                workflow_id=command.workflow_id,
                status=WorkflowStatus.FAILED.value,
                payload={"error": str(err)},
            )

            failed_event = FailedEvent(
                workflow_id=command.workflow_id,
                scenario=command.scenario,
                error_code=ErrorCode.TEMPORAL_START_ERROR,
                error_message=str(err),
                retryable=True,
            )
            await self.publisher.publish_failed(failed_event)
            raise

        progress = await self.temporal.query_workflow(workflow_id=command.workflow_id, query_name=spec.query_name)
        await self._push_status(command=command, status=WorkflowStatus.RUNNING, details=progress)

        raw_result = {}
        try:
            raw_result = await self.temporal.wait_result(workflow_id=command.workflow_id)
        except ApplicationError as err:
            failed_event = FailedEvent(
                workflow_id=command.workflow_id,
                scenario=command.scenario,
                error_code=ErrorCode.TEMPORAL_EXECUTION_ERROR,
                error_message=str(err),
                retryable=True,
            )
            await self.publisher.publish_failed(failed_event)
            raise

        outputs = to_result_objects(raw_result.get("outputs", []))

        result = ScenarioResult(
            workflow_id=command.workflow_id,
            scenario=command.scenario,
            status=WorkflowStatus.COMPLETED,
            outputs=outputs,
            details=raw_result,
            timestamp=time.time(),
        )

        await self._push_status(command=command, status=WorkflowStatus.COMPLETED,
                                details={"outputs": [o.model_dump() for o in outputs]})
        await self.publisher.publish_completed(result)
        return result

    async def _push_status(self, *, command: BaseStartCommand, status: WorkflowStatus,
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
            payload=event.model_dump(mode='json'),
        )
        await self.publisher.publish_status(event)