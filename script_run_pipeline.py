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
            except Exception as e:
                logger.error(f"Error decoding message: {e}")

    await queue.consume(on_message)

    # User provided path
    test_file_path = r"D:\1_prod\data\user_data\НПС Крутое\wg\processed_clouds\small.laz"

    # Prepare Start Payload with new structure
    workflow_id = str(uuid.uuid4())
    payload = {
      "workflow_id": workflow_id,
      "scenario": "ingest",
      "version":
      {
        "message_version": "0",
        "pipeline_version": "1"
      },
      "dataset":
      {
        "scan1":
          {
            "point_cloud": {
                "1": {
                  "s3_key": test_file_path,
                  "etag": "mock_etag_123"
                }
            },
            "control_point": {},
            "trajectory": {}
          }
      },
      # Extra fields needed for mapping logic in main.py
      "company_id": "test_company",
      "dataset_name": f"dataset_{workflow_id[:8]}",
      "target_crs_id": "EPSG:32641",
      "run_old_cluster": True
    }

    # Publish Start Message
    routing_key = "pipeline.start"
    logger.info(f"Publishing start message to '{routing_key}'...")
    logger.info(f"Payload: {json.dumps(payload, indent=2)}")

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
