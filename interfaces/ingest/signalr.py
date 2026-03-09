import asyncio
import logging
from typing import Any

from signalrcore.hub_connection_builder import BaseHubConnection

from application.common.use_case import StartUseCase
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command

logger = logging.getLogger(__name__)


class IngestSignalRController:
    def __init__(self, use_case: StartUseCase, client: BaseHubConnection, method_name: str):
        self.use_case = use_case
        self.client = client
        self.method_name = method_name
        self._loop = asyncio.get_running_loop() # Сохраняем текущий Event Loop


    def start_listening(self) -> None:
        self.client.on(self.method_name, self._handle_message)
        logger.info(f'Subscribed to SignalR method: {self.method_name}')


    def _handle_message(self, args: list[Any]) -> None:
        if not args:
            logger.error('Received an empty message')
            return

        payload = args[0]

        try:
            dto = IngestStartMessageDTO.model_validate(payload)

            command = to_start_command(dto)

            future = asyncio.run_coroutine_threadsafe(
                self.use_case.execute(command),
                self._loop)

            future.add_done_callback(self._task_done_callback)

        except Exception as e:
            logger.exception(f'Failed to process SignalR message: {e}')

    def _task_done_callback(self, future):
        try:
            exc = future.exception()
            if exc:
                logger.error(f'Background task failed: {exc}', exc_info=exc)
        except Exception as e:
            logger.error(f'Failed to retrieve background task exception: {e}')