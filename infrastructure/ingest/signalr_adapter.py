from typing import Any
from signalrcore.hub_connection_builder import BaseHubConnection

from infrastructure.common.signalr import SignalREventPublisher as CommonSignalREventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult
from interfaces.ingest.mappers import to_completed_event, to_failed_event, to_status_dto

class SignalREventPublisher(CommonSignalREventPublisher):
    def __init__(self, client: BaseHubConnection):
        super().__init__(
            client=client,
            status_method='IngestStatus',
            completed_method='IngestCompleted',
            failed_method='IngestFailed'
        )

    def _serialize_status(self, event: StatusEvent) -> dict[str, Any]:
        dto = to_status_dto(event)
        return dto.model_dump(mode='json')

    def _serialize_completed(self, result: ScenarioResult) -> dict[str, Any]:
        dto = to_completed_event(result)
        return dto.model_dump(mode='json')

    def _serialize_failed(self, event: FailedEvent) -> dict[str, Any]:
        dto = to_failed_event(event)
        return dto.model_dump(mode='json')
