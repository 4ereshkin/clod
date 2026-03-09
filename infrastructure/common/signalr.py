import asyncio

from typing import Any

from signalrcore.hub_connection_builder import BaseHubConnection

from application.common.interfaces import EventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult
from interfaces.ingest.mappers import to_status_dto, to_completed_event, to_failed_event


class SignalREventPublisher:
    def __init__(self,
                 client: BaseHubConnection,
                 status_method: str,
                 completed_method: str,
                 failed_method: str) -> None:
        self.client = client
        self.status_method = status_method
        self.completed_method = completed_method
        self.failed_method = failed_method

    async def publish_status(self, event: StatusEvent) -> None:
        dto = to_status_dto(event)
        payload = dto.model_dump(mode='json')

        await asyncio.to_thread(self.client.send, self.status_method, [payload])


    async def publish_completed(self, result: ScenarioResult) -> None:
        dto = to_completed_event(result)
        payload = dto.model_dump(mode='json')

        await asyncio.to_thread(self.client.send, self.completed_method, [payload])

    async def publish_failed(self, event: FailedEvent) -> None:
        dto = to_failed_event(event)
        payload = dto.model_dump(mode='json')

        await asyncio.to_thread(self.client.send, self.failed_method, [payload])