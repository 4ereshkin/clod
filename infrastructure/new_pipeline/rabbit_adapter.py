import asyncio
import json
from typing import Any, Dict
from aio_pika import Exchange, Message
from application.new_pipeline.interfaces import NewPipelineEventPublisher

class NewPipelineRabbitAdapter(NewPipelineEventPublisher):
    def __init__(self, exchange: Exchange):
        self.exchange = exchange

    async def publish_status(self, workflow_id: str, status: str, details: Dict[str, Any] = None):
        payload = {
            "workflow_id": workflow_id,
            "status": status,
            "details": details or {}
        }
        body = json.dumps(payload).encode("utf-8")

        await self.exchange.publish(
            Message(body, content_type="application/json"),
            routing_key=f"pipeline.status.{status}"
        )
