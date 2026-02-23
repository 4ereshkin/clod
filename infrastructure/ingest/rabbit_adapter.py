from typing import Any
from aio_pika import Message, DeliveryMode, Exchange

from application.ingest.use_case import EventPublisher
from application.ingest.contracts import ScenarioResult, StatusEvent, FailedEvent

from interfaces.ingest.mappers import to_completed_event, to_failed_event, to_status_dto


class RabbitEventPublisher:
    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange

    async def publish_status(self, event: StatusEvent) -> None:
        dto = to_status_dto(event)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type='ingest.status')

        await self.exchange.publish(message, routing_key='ingest.status')


    async def publish_completed(self, result: ScenarioResult) -> None:
        dto = to_completed_event(result)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=result.workflow_id,
                          content_type="application/json",
                          type='ingest.complete')

        await self.exchange.publish(message, routing_key='ingest.complete')


    async def publish_failed(self, event: FailedEvent) -> None:
        dto = to_failed_event(event)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type='ingest.failed')

        await self.exchange.publish(message, routing_key='ingest.failed')