from infrastructure.common.rabbit import RabbitEventPublisher as CommonRabbitEventPublisher
from application.common.contracts import StatusEvent, FailedEvent, ScenarioResult

from interfaces.registration.mappers import to_status_dto, to_completed_event, to_failed_event