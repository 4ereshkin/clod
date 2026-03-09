import asyncio

from typing import Any

from signalrcore.hub_connection_builder import BaseHubConnection

from application.common.interfaces import EventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult


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

    def _serialize_status(self, event: StatusEvent) -> dict[str, Any]:
        raise NotImplementedError

    def _serialize_completed(self, result: ScenarioResult) -> dict[str, Any]:
        raise NotImplementedError

    def _serialize_failed(self, event: FailedEvent) -> dict[str, Any]:
        raise NotImplementedError

    async def publish_status(self, event: StatusEvent) -> None:
        payload = self._serialize_status(event)

        await asyncio.to_thread(self.client.send, self.status_method, [payload])


    async def publish_completed(self, result: ScenarioResult) -> None:
        payload = self._serialize_completed(result)

        await asyncio.to_thread(self.client.send, self.completed_method, [payload])

    async def publish_failed(self, event: FailedEvent) -> None:
        payload = self._serialize_failed(event)

        await asyncio.to_thread(self.client.send, self.failed_method, [payload])