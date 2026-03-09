from typing import Any
from aio_pika.pool import Pool
from aio_pika import Message, DeliveryMode
from aio_pika.abc import AbstractChannel


from application.common.interfaces import EventPublisher

from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult


class RabbitEventPublisher:
    def __init__(self, channel_pool: Pool[AbstractChannel], exchange_name: str,
                 status_key: str, completed_key: str, failed_key: str) -> None:
        self.channel_pool = channel_pool
        self.exchange_name = exchange_name
        self.status_key = status_key
        self.completed_key = completed_key
        self.failed_key = failed_key

    def _serialize_status(self, event: StatusEvent) -> bytes:
        raise NotImplementedError

    def _serialize_completed(self, result: ScenarioResult) -> bytes:
        raise NotImplementedError

    def _serialize_failed(self, event: FailedEvent) -> bytes:
        raise NotImplementedError

    async def publish_status(self, event: StatusEvent) -> None:
        body = self._serialize_status(event)

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type=self.status_key)

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.status_key)


    async def publish_completed(self, result: ScenarioResult) -> None:
        body = self._serialize_completed(result)

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=result.workflow_id,
                          content_type="application/json",
                          type=self.completed_key)

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.completed_key)


    async def publish_failed(self, event: FailedEvent) -> None:
        body = self._serialize_failed(event)

        message = Message(body=body,
                          delivery_mode=DeliveryMode.PERSISTENT,
                          correlation_id=event.workflow_id,
                          content_type="application/json",
                          type=self.failed_key)

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name)
            await exchange.publish(message, routing_key=self.failed_key)
