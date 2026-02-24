import asyncio
import logging
from typing import Any, Dict

from temporalio import activity
from redis.asyncio import from_url
from aio_pika import connect_robust, ExchangeType

from env_vars import settings
from infrastructure.new_pipeline.rabbit_adapter import NewPipelineRabbitAdapter
from infrastructure.new_pipeline.keydb_adapter import NewPipelineKeyDbAdapter

logger = logging.getLogger(__name__)

@activity.defn
async def update_pipeline_status(
    workflow_id: str,
    status: str,
    details: Dict[str, Any] = None,
) -> None:
    """
    Updates the status of the new architecture pipeline in KeyDB and publishes an event to RabbitMQ.
    """
    async def _run():
        try:
            # Re-initialize adapters within the activity worker context
            redis = await from_url(settings.keydb_dsn, encoding="utf-8", decode_responses=True)
            try:
                keydb_adapter = NewPipelineKeyDbAdapter(redis)
                await keydb_adapter.set_status(workflow_id, status, details)
            finally:
                await redis.close()

            connection = await connect_robust(settings.rabbit_dsn)
            async with connection:
                channel = await connection.channel()
                # Use the exchange declared in main.py or ensure it exists
                # We can't use get_exchange with ensure=False safely if it might not exist yet in worker context
                # So we declare it to be safe (idempotent)
                exchange = await channel.declare_exchange("new_pipeline_events", ExchangeType.TOPIC)

                rabbit_adapter = NewPipelineRabbitAdapter(exchange)
                await rabbit_adapter.publish_status(workflow_id, status, details)

            logger.info(f"Updated pipeline status: {workflow_id} -> {status}")

        except Exception as e:
            logger.error(f"Failed to update pipeline status for {workflow_id}: {e}", exc_info=True)
            # We allow failure here so Temporal retries
            raise e

    await _run()
