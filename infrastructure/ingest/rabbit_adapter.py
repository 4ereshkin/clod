from aio_pika.abc import AbstractChannel
from aio_pika.pool import Pool

from infrastructure.common.rabbit import RabbitEventPublisher as CommonRabbitEventPublisher

class RabbitEventPublisher(CommonRabbitEventPublisher):
    def __init__(self, channel_pool: Pool[AbstractChannel]): # Передаем пул
        super().__init__(
            channel_pool=channel_pool,
            exchange_name='ingest', # Имя эксченджа зашито для ingest
            status_key='ingest.status',
            completed_key='ingest.complete',
            failed_key='ingest.failed'
        )