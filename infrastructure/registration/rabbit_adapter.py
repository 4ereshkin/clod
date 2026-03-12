from aio_pika.abc import AbstractChannel
from aio_pika.pool import Pool

from infrastructure.common.rabbit import RabbitEventPublisher as CommonRabbitEventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult
from interfaces.registration.mappers import to_status_dto, to_completed_event, to_failed_event


class RabbitEventPublisher(CommonRabbitEventPublisher):
    def __init__(self, channel_pool: Pool[AbstractChannel]):
        # Один обменник на все пайплайны
        super().__init__(channel_pool=channel_pool, exchange_name='pipeline_events')

    def _serialize_status(self, event: StatusEvent) -> bytes:
        # Для простоты можно использовать напрямую Pydantic модель (она универсальна)
        return event.model_dump_json().encode()

    def _serialize_completed(self, result: ScenarioResult) -> bytes:
        return result.model_dump_json().encode()

    def _serialize_failed(self, event: FailedEvent) -> bytes:
        return event.model_dump_json().encode()