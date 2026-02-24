from infrastructure.common.rabbit import RabbitEventPublisher as CommonRabbitEventPublisher

class RabbitEventPublisher(CommonRabbitEventPublisher):
    def __init__(self, exchange):
        super().__init__(
            exchange,
            status_key='ingest.status',
            completed_key='ingest.complete',
            failed_key='ingest.failed'
        )
