from typing import Any
from aio_pika.pool import Pool
from aio_pika import Message, DeliveryMode
from aio_pika.abc import AbstractChannel


from application.common.interfaces import EventPublisher

from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult
from interfaces.ingest.mappers import to_completed_event, to_failed_event, to_status_dto


class RabbitEventPublisher:
    def __init__(self, channel_pool: Pool[AbstractChannel], exchange_name: str,
                 status_key: str, completed_key: str, failed_key: str) -> None:
        self.channel_pool = channel_pool
        self.exchange_name = exchange_name
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

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.status_key)


    async def publish_completed(self, result: ScenarioResult) -> None:
        dto = to_completed_event(result)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=result.workflow_id,
                          content_type="application/json",
                          type=self.completed_key)

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.completed_key)


    async def publish_failed(self, event: FailedEvent) -> None:
        dto = to_failed_event(event)
        body = dto.model_dump_json().encode()

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type=self.failed_key)

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.failed_key)
