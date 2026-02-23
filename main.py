import asyncio
import logging
from contextlib import AsyncExitStack

from aio_pika import connect_robust, IncomingMessage
from redis.asyncio import from_url
from temporalio.client import Client

from application.ingest.use_case import StartIngestUseCase
from env_vars import settings
from infrastructure.ingest.keydb_adapter import KeyDbStatusStore
from infrastructure.ingest.rabbit_adapter import RabbitEventPublisher
from infrastructure.ingest.temporal_adapter import TemporalAdapter
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    async with AsyncExitStack() as stack:
        # 1. Infrastructure (Connections)
        redis = await stack.enter_async_context(
            from_url(settings.keydb_dsn, encoding="utf-8", decode_responses=True)
        )
        rabbit_conn = await stack.enter_async_context(
            await connect_robust(settings.rabbit_dsn)
        )
        channel = await stack.enter_async_context(
            await rabbit_conn.channel()
        )
        exchange = await channel.get_exchange('ingest', ensure=False)  # Assuming exchange exists

        temporal_client = await Client.connect(settings.temporal_dsn)

        # 2. Adapters
        status_store = KeyDbStatusStore(redis)
        publisher = RabbitEventPublisher(exchange)
        temporal_gateway = TemporalAdapter(temporal_client)

        # 3. Use Cases
        ingest_uc = StartIngestUseCase(temporal_gateway, status_store, publisher)

        # 4. Interface Layer (Consumers)
        queue = await channel.declare_queue('ingest.start', durable=True)

        async def process_message(message: IncomingMessage):
            async with message.process():
                try:
                    payload = IngestStartMessageDTO.model_validate_json(message.body)
                    command = to_start_command(payload)
                    logger.info(f"Starting ingest workflow {command.workflow_id}")
                    await ingest_uc.execute(command)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}", exc_info=True)
                    # Here we might want to publish a failure event or dead-letter the message

        await queue.consume(process_message)

        logger.info("Worker started. Waiting for messages...")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped.")
