import json
from typing import Any
from redis.asyncio import Redis


class KeyDbStatusStore:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client
        self.ttl = 86400


    async def set_status(self, *, workflow_id: str,
                         status: str, payload: dict[str, Any]) -> None:
        key = f'ingest:status:{workflow_id}'

        data = {
            'status': status,
            'payload': payload
        }

        await self.redis.set(key, json.dumps(data), ex=self.ttl)