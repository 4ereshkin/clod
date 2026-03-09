from aio_pika.abc import AbstractChannel
from aio_pika.pool import Pool

from infrastructure.common.rabbit import RabbitEventPublisher as CommonRabbitEventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult
from interfaces.ingest.mappers import to_completed_event, to_failed_event, to_status_dto

class RabbitEventPublisher(CommonRabbitEventPublisher):
    def __init__(self, channel_pool: Pool[AbstractChannel]): # Передаем пул
        super().__init__(
            channel_pool=channel_pool,
            exchange_name='ingest', # Имя эксченджа зашито для ingest
            status_key='ingest.status',
            completed_key='ingest.complete',
            failed_key='ingest.failed'
        )

    def _serialize_status(self, event: StatusEvent) -> bytes:
        dto = to_status_dto(event)
        return dto.model_dump_json().encode()

    def _serialize_completed(self, result: ScenarioResult) -> bytes:
        dto = to_completed_event(result)
        return dto.model_dump_json().encode()

    def _serialize_failed(self, event: FailedEvent) -> bytes:
        dto = to_failed_event(event)
        return dto.model_dump_json().encode()