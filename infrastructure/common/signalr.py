import asyncio
from typing import Any
from signalrcore.hub_connection_builder import BaseHubConnection
from application.common.interfaces import EventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult


class SignalREventPublisher(EventPublisher):
    def __init__(self, client: BaseHubConnection) -> None:
        self.client = client

    def _get_method_name(self, scenario: str, event_type: str) -> str:
        # Превратит ("ingest", "Status") -> "RecieveIngestStatus"
        # Превратит ("registration", "Completed") -> "RecieveRegistrationCompleted"
        return f"Recieve{scenario.capitalize()}{event_type}"

    async def publish_status(self, event: StatusEvent) -> None:
        payload = event.model_dump(mode='json')
        method = self._get_method_name(event.scenario, "Status")
        await asyncio.to_thread(self.client.send, method, [payload])

    async def publish_completed(self, result: ScenarioResult) -> None:
        payload = result.model_dump(mode='json')
        method = self._get_method_name(result.scenario, "Completed")
        await asyncio.to_thread(self.client.send, method, [payload])

    async def publish_failed(self, event: FailedEvent) -> None:
        payload = event.model_dump(mode='json')
        method = self._get_method_name(event.scenario, "Failed")
        await asyncio.to_thread(self.client.send, method, [payload])