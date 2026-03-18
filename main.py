import asyncio
import logging

from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection, AbstractChannel, AbstractIncomingMessage
from dishka import make_async_container
from signalrcore.hub.base_hub_connection import BaseHubConnection

from application.common.use_case import StartUseCase
from infrastructure.providers import InfrastructureProvider, ApplicationProvider
from infrastructure.logging import setup_logging, correlation_id_var

from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command

from interfaces.registration.dto import RegistrationStartMessageDTO
from interfaces.registration.mappers import to_registration_start_command

from application.common.config import AppSettings
from interfaces.ingest.signalr import IngestSignalRController
from interfaces.registration.signalr import RegistrationSignalRController

setup_logging()
logger = logging.getLogger(__name__)


async def main():
    container = make_async_container(
        InfrastructureProvider(),
        ApplicationProvider()
    )

    try:
        start_uc = await container.get(StartUseCase)
        config = await container.get(AppSettings)

        if config.event_transport == "signalr":
            logger.info("Starting SignalR listening...")
            hub_connection = await container.get(BaseHubConnection)

            ingest_controller = IngestSignalRController(
                use_case=start_uc,
                client=hub_connection,
                method_name="StartIngest"
            )
            ingest_controller.start_listening()

            registration_controller = RegistrationSignalRController(
                use_case=start_uc,
                client=hub_connection,
                method_name="StartRegistration"
            )
            registration_controller.start_listening()

        else:
            logger.info("Starting RabbitMQ listening...")
            rabbit_conn = await container.get(AbstractRobustConnection)
            channel: AbstractChannel = await rabbit_conn.channel()

            ingest_queue = await channel.declare_queue('ingest.start', durable=True)

            reg_queue = await channel.declare_queue('registration.start', durable=True)

            async def process_ingest(message: AbstractIncomingMessage):
                async with message.process():
                    cid = message.correlation_id or ""
                    token = correlation_id_var.set(cid)
                    try:
                        if message.body is None:
                            logger.error('Received empty message body')
                            return

                        payload = IngestStartMessageDTO.model_validate_json(message.body)
                        if not cid:
                            correlation_id_var.set(payload.workflow_id)
                        command = to_start_command(payload)
                        logger.info(f"Starting ingest workflow {command.workflow_id}")

                        await start_uc.execute(command)

                    except Exception as e:
                        logger.error(f"Failed to process ingest: {e}", exc_info=True)
                    finally:
                        correlation_id_var.reset(token)

            async def process_registration(message: AbstractIncomingMessage):
                async with message.process():
                    cid = message.correlation_id or ""
                    token = correlation_id_var.set(cid)
                    try:
                        if not message.body:
                            return

                        payload = RegistrationStartMessageDTO.model_validate_json(message.body)
                        if not cid:
                            correlation_id_var.set(payload.workflow_id)
                        command = to_registration_start_command(payload)
                        logger.info(f"Starting registration workflow {command.workflow_id}")

                        await start_uc.execute(command)

                    except Exception as e:
                        logger.error(f"Failed to process registration: {e}", exc_info=True)
                    finally:
                        correlation_id_var.reset(token)

            await ingest_queue.consume(process_ingest)
            await reg_queue.consume(process_registration)

        logger.info("Listener started. Waiting for messages...")
        await asyncio.Future()

    finally:
        logger.info("Closing container and connections...")
        await container.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped.")