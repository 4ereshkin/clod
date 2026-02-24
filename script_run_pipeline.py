import asyncio
import json
import logging
import uuid
from aio_pika import connect_robust, Message, ExchangeType
from env_vars import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting simulation...")

    # Connect to RabbitMQ
    connection = await connect_robust(settings.rabbit_dsn)
    channel = await connection.channel()

    # Declare Exchange
    exchange = await channel.declare_exchange('new_pipeline_events', ExchangeType.TOPIC)

    # Create Response Queue
    queue = await channel.declare_queue(exclusive=True)
    await queue.bind(exchange, routing_key="pipeline.status.#")

    # Start Listening for Responses
    async def on_message(message: Message):
        async with message.process():
            try:
                body = json.loads(message.body)
                status = body.get("status")
                logger.info(f"Received update: {status} - {body}")

                if status == "DONE" or status == "FAILED":
                    logger.info("Pipeline finished.")
                    # In a real CLI, we might exit here, but for simulation let's just log
            except Exception as e:
                logger.error(f"Error decoding message: {e}")

    await queue.consume(on_message)

    # Prepare Start Payload
    payload = {
        "company_id": "test_company",
        "dataset_name": f"dataset_{uuid.uuid4().hex[:8]}",
        "target_crs_id": "EPSG:32641",
        "scans": [
            {
                "artifacts": [
                    {"kind": "raw.point_cloud", "local_file_path": "/tmp/test.laz"}
                ],
                "scan_meta": {"project": "simulation"}
            }
        ],
        "run_old_cluster": True
    }

    # Publish Start Message
    routing_key = "pipeline.start"
    logger.info(f"Publishing start message to '{routing_key}'...")

    await exchange.publish(
        Message(
            body=json.dumps(payload).encode(),
            content_type="application/json"
        ),
        routing_key=routing_key
    )

    logger.info("Message published. Listening for updates (Ctrl+C to exit)...")
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Simulation stopped.")
