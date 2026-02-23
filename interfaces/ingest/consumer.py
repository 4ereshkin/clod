from aio_pika import IncomingMessage
from application.ingest.use_case import StartIngestUseCase
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command


class IngestConsumer:
    def __init__(self, use_case: StartIngestUseCase):
        self.use_case = use_case


    async def process_message(self, message: IncomingMessage):
        async with message.process():
            dto = IngestStartMessageDTO.model_validate_json(message.body)
            command = to_start_command(dto)

            await self.use_case.execute(command)

