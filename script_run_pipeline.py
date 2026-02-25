import asyncio
import json
import logging
import uuid
from aio_pika import connect_robust, Message, ExchangeType
from env_vars import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = r"D:\1_prod\data\user_data\НПС Крутое"

# CRS исходников: CGCS2000 / Gauss-Kruger zone 11 (WKT2)
SOURCE_CRS_WKT = (
    'PROJCS["CGCS2000 / Gauss-Kruger zone 11",'
    'GEOGCS["CGCS 2000",DATUM["China_2000",SPHEROID["CGCS2000",6378137,298.257222101]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],'
    'PROJECTION["Transverse_Mercator"],'
    'PARAMETER["latitude_of_origin",0],'
    'PARAMETER["central_meridian",63],'
    'PARAMETER["scale_factor",1],'
    'PARAMETER["false_easting",500000],'
    'PARAMETER["false_northing",0],'
    'UNIT["metre",1]]'
)

# Минимальный набор для теста (filter_map = облегчённые облака)
SCANS = {
    "scan1": ("1", "t100pro_2025-04-28-08-36-08_filter_map.laz"),
    "scan2": ("2", "t100pro_2025-04-28-08-47-00_filter_map.laz"),
}

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

    # Build dataset from real scan directories
    import os
    dataset = {}
    for scan_name, (folder, laz_file) in SCANS.items():
        scan_dir = os.path.join(BASE_DIR, folder)
        dataset[scan_name] = {
            "point_cloud": {
                "1": {
                    "s3_key": os.path.join(scan_dir, laz_file),
                    "etag": "mock"
                }
            },
            "trajectory": {
                "1": {
                    "s3_key": os.path.join(scan_dir, "gpspath.txt"),
                    "etag": "mock"
                }
            },
            "control_point": {
                "1": {
                    "s3_key": os.path.join(scan_dir, "ControlPoint.txt"),
                    "etag": "mock"
                }
            },
        }

    # Prepare Start Payload
    workflow_id = str(uuid.uuid4())
    payload = {
        "workflow_id": workflow_id,
        "scenario": "ingest",
        "version": {
            "message_version": "0",
            "pipeline_version": "1"
        },
        "dataset": dataset,
        # Extra fields needed for mapping logic in main.py
        "company_id": "test_company",
        "dataset_name": "nps_krutoe_utm41",
        "target_crs_id": "EPSG:32641",  # UTM zone 41N — куда репроецируем
        "run_old_cluster": True
    }

    # Publish Start Message
    routing_key = "pipeline.start"
    logger.info(f"Publishing start message to '{routing_key}'...")
    logger.info(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")

    await exchange.publish(
        Message(
            body=json.dumps(payload, ensure_ascii=False).encode(),
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
