import asyncio
import logging

from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection, AbstractChannel, AbstractIncomingMessage
from dishka import make_async_container
from signalrcore.hub.base_hub_connection import BaseHubConnection

from application.common.use_case import StartUseCase
from infrastructure.providers import InfrastructureProvider, ApplicationProvider
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command
from legacy_env_vars import Settings
from interfaces.ingest.signalr import IngestSignalRController

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    container = make_async_container(
        InfrastructureProvider(),
        ApplicationProvider()
    )

    try:
        start_uc = await container.get(StartUseCase)
        config = await container.get(Settings)

        if config.event_transport == "signalr":
            logger.info("Starting SignalR listening...")
            hub_connection = await container.get(BaseHubConnection)

            signalr_controller = IngestSignalRController(
                use_case=start_uc,
                client=hub_connection,
                method_name="StartIngest"
            )
            signalr_controller.start_listening()

        else:
            logger.info("Starting RabbitMQ listening...")
            rabbit_conn = await container.get(AbstractRobustConnection)
            channel: AbstractChannel = await rabbit_conn.channel()

            ingest_queue = await channel.declare_queue('ingest.start', durable=True)

            async def process_ingest(message: AbstractIncomingMessage):
                async with message.process():
                    try:
                        if message.body is None:
                            logger.error('Received empty message body')
                            return

                        payload = IngestStartMessageDTO.model_validate_json(message.body)
                        command = to_start_command(payload)
                        logger.info(f"Starting ingest workflow {command.workflow_id}")

                        await start_uc.execute(command)

                    except Exception as e:
                        logger.error(f"Failed to process ingest: {e}", exc_info=True)

            await ingest_queue.consume(process_ingest)

        logger.info("Worker started. Waiting for messages...")
        await asyncio.Future()

    finally:
        logger.info("Closing container and connections...")
        await container.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped.")