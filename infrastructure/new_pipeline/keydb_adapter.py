from typing import Any, Dict
from redis.asyncio import Redis
import json

class NewPipelineKeyDbAdapter:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def set_status(self, workflow_id: str, status: str, result: Dict[str, Any] = None):
        key = f"pipeline:status:{workflow_id}"
        await self.redis.hset(key, mapping={"status": status, "result": json.dumps(result or {})})

    async def get_status(self, workflow_id: str) -> Dict[str, Any]:
        key = f"pipeline:status:{workflow_id}"
        return await self.redis.hgetall(key)
