import asyncio
import logging
import json
from contextlib import AsyncExitStack

from aio_pika import connect_robust, IncomingMessage, ExchangeType
from redis.asyncio import from_url
from temporalio.client import Client

from application.ingest.use_case import StartIngestUseCase
from application.new_pipeline.use_case import StartNewPipelineUseCase
from env_vars import settings
from infrastructure.ingest.keydb_adapter import KeyDbStatusStore
from infrastructure.ingest.rabbit_adapter import RabbitEventPublisher
from infrastructure.ingest.temporal_adapter import TemporalAdapter
from infrastructure.new_pipeline.keydb_adapter import NewPipelineKeyDbAdapter
from infrastructure.new_pipeline.rabbit_adapter import NewPipelineRabbitAdapter
from infrastructure.new_pipeline.temporal_adapter import NewPipelineTemporalAdapter
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.new_pipeline.dto import StartPipelineDTO, TriggerPayloadDTO, map_trigger_to_start_dto
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
        ingest_exchange = await channel.get_exchange('ingest', ensure=False)
        pipeline_exchange = await channel.declare_exchange('new_pipeline_events', ExchangeType.TOPIC)

        temporal_client = await Client.connect(settings.temporal_dsn)

        # 2. Adapters & Use Cases - Ingest
        ingest_status_store = KeyDbStatusStore(redis)
        ingest_publisher = RabbitEventPublisher(ingest_exchange)
        ingest_temporal = TemporalAdapter(temporal_client)
        ingest_uc = StartIngestUseCase(ingest_temporal, ingest_status_store, ingest_publisher)

        # 2.1 Adapters & Use Cases - New Pipeline
        pipeline_status_store = NewPipelineKeyDbAdapter(redis)
        pipeline_publisher = NewPipelineRabbitAdapter(pipeline_exchange)
        pipeline_temporal = NewPipelineTemporalAdapter(temporal_client)
        pipeline_uc = StartNewPipelineUseCase(pipeline_temporal, pipeline_status_store, pipeline_publisher)

        # 3. Consumers

        # 3.1 Ingest Queue
        ingest_queue = await channel.declare_queue('ingest.start', durable=True)

        async def process_ingest(message: IncomingMessage):
            async with message.process():
                try:
                    payload = IngestStartMessageDTO.model_validate_json(message.body)
                    command = to_start_command(payload)
                    logger.info(f"Starting ingest workflow {command.workflow_id}")
                    await ingest_uc.execute(command)
                except Exception as e:
                    logger.error(f"Failed to process ingest: {e}", exc_info=True)

        await ingest_queue.consume(process_ingest)

        # 3.2 New Pipeline Queue
        pipeline_queue = await channel.declare_queue('new_pipeline.start', durable=True)
        await pipeline_queue.bind(pipeline_exchange, routing_key="pipeline.start")

        async def process_pipeline(message: IncomingMessage):
            async with message.process():
                try:
                    payload = json.loads(message.body)
                    # Support both formats:
                    # 1. New TriggerPayloadDTO (from external system)
                    # 2. Legacy StartPipelineDTO (direct invocation)
                    try:
                        trigger = TriggerPayloadDTO(**payload)
                        logger.info(f"Received trigger payload for workflow {trigger.workflow_id}")
                        dto = map_trigger_to_start_dto(trigger)
                    except Exception:
                         # Fallback or assume direct DTO
                        dto = StartPipelineDTO(**payload)

                    logger.info(f"Starting new pipeline for company {dto.company_id}")
                    await pipeline_uc.execute(dto)
                except Exception as e:
                    logger.error(f"Failed to process new pipeline: {e}", exc_info=True)

        await pipeline_queue.consume(process_pipeline)

        logger.info("Worker started. Waiting for messages...")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped.")
