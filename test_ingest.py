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
            "message_version": "1",
            "pipeline_version": "1"
        },
        "dataset": {
            "scan_001": {
                "point_cloud": {
                    "part_1": {
                        "s3_key": "raw/scan_001.laz",
                        "etag": "fake_etag_123"
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

            logger.info(f"✅ Successfully sent test ingest message to '{QUEUE_NAME}'. Workflow ID: {workflow_id}")

    except Exception as e:
        logger.error(f"❌ Failed to send message: {e}")


if __name__ == "__main__":
    asyncio.run(send_test_message())