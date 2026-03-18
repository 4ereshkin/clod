import json
import uuid

import aio_pika
import pytest

QUEUE_NAME = "ingest.start"


@pytest.mark.integration
async def test_send_ingest_message(rabbit_connection):
    workflow_id = str(uuid.uuid4())

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
                        "crs": {}
                    }
                },
                "trajectory": {},
                "control_point": {}
            }
        }
    }

    channel = await rabbit_connection.channel()
    await channel.declare_queue(QUEUE_NAME, durable=True)

    message = aio_pika.Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        correlation_id=workflow_id,
    )

    await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
