from typing import Any
from aio_pika import Message, DeliveryMode, Exchange

from application.common.interfaces import EventPublisher
from application.ingest.contracts import ScenarioResult
from application.common.contracts import StatusEvent, FailedEvent

from interfaces.ingest.mappers import to_completed_event, to_failed_event, to_status_dto


class RabbitEventPublisher:
    def __init__(self, exchange: Exchange, status_key: str, completed_key: str, failed_key: str) -> None:
        self.exchange = exchange
        self.status_key = status_key
        self.completed_key = completed_key
        self.failed_key = failed_key

    async def publish_status(self, event: StatusEvent) -> None:
        dto = to_status_dto(event)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type=self.status_key)

        await self.exchange.publish(message, routing_key=self.status_key)


    async def publish_completed(self, result: ScenarioResult) -> None:
        dto = to_completed_event(result)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=result.workflow_id,
                          content_type="application/json",
                          type=self.completed_key)

        await self.exchange.publish(message, routing_key=self.completed_key)


    async def publish_failed(self, event: FailedEvent) -> None:
        dto = to_failed_event(event)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type=self.failed_key)

        await self.exchange.publish(message, routing_key=self.failed_key)
