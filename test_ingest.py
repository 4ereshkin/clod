import asyncio
import json
import logging
import uuid
import aio_pika

# Конфигурация по умолчанию для RabbitMQ
RABBIT_DSN = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "ingest.start"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def send_test_message():
    # Генерируем тестовые данные
    workflow_id = str(uuid.uuid4())

    # Структура соответствует IngestStartMessageDTO из interfaces/ingest/dto.py
    payload = {
        "workflow_id": workflow_id,
        "scenario": "ingest",
        "version": {
            "message_version": "0",
            "pipeline_version": "1"
        },
        "dataset": {
            "scan_001": {
                "point_cloud": {
                    "part_1": {
                        "s3_key": "raw/scan_001_part1.laz",
                        "etag": "fake_etag_part1",
                        "crs": {} # Передаем кастомную CRS
                    },
                    "part_2": {
                        "s3_key": "raw/scan_001_part2.laz",
                        "etag": "fake_etag_part2"
                        # Без CRS, воркфлоу должен подставить дефолтный EPSG:4326
                    }
                },
                "trajectory": {
                    "main_traj": {
                        "s3_key": "raw/scan_001_trajectory.txt",
                        "etag": "fake_etag_traj",
                        "crs": {}
                    }
                },
                "control_point": {}
            },
            "scan_002": {
                "point_cloud": {
                    "part_1": {
                        "s3_key": "raw/scan_002_part1.laz",
                        "etag": "fake_etag_scan2_part1",
                        "crs": {}
                    }
                },
                "trajectory": {},
                "control_point": {}
            }
        }
    }

    try:
        # Подключаемся к RabbitMQ
        connection = await aio_pika.connect_robust(RABBIT_DSN)
        async with connection:
            channel = await connection.channel()

            # Убеждаемся, что очередь существует
            await channel.declare_queue(QUEUE_NAME, durable=True)

            # Отправляем сообщение
            message_body = json.dumps(payload).encode()
            message = aio_pika.Message(
                body=message_body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await channel.default_exchange.publish(
                message,
                routing_key=QUEUE_NAME,
            )

            logger.info(f"✅ Successfully sent test ingest message to '{QUEUE_NAME}'.")
            logger.info(f"Workflow ID: {workflow_id}")
            logger.info(f"Payload: \n{json.dumps(payload, indent=2)}")

    except Exception as e:
        logger.error(f"❌ Failed to send message: {e}")


if __name__ == "__main__":
    asyncio.run(send_test_message())