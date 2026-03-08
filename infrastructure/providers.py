import os
from typing import Any, AsyncGenerator

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from aio_pika.pool import Pool
from dishka import Provider, Scope, provide, alias
from aioredis import Redis, from_url
from temporalio.client import Client

from application.common.interfaces import TemporalGateway, StatusStore, EventPublisher
from application.common.use_case import StartUseCase
from env_vars import settings
from infrastructure.ingest.keydb_adapter import KeyDbStatusStore
from infrastructure.ingest.rabbit_adapter import RabbitEventPublisher
from infrastructure.ingest.temporal_adapter import TemporalAdapter

# Добавленные импорты для Activities и S3:
from infrastructure.infrastructure_config import S3Config
from infrastructure.s3 import S3Client
from point_cloud.activities.ingest_activities_v1 import IngestActivitiesV1


class InfrastructureProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_redis_client(self) -> AsyncGenerator[Redis, Any]:
        client = from_url(settings.keydb_dsn, encoding="utf-8", decode_responses=True)
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    async def get_rabbit_connection(self) -> AsyncGenerator[AbstractRobustConnection, Any]:
        connection = await aio_pika.connect_robust(settings.rabbit_dsn)
        yield connection
        await connection.close()

    @provide(scope=Scope.APP)
    def get_channel_pool(self, connection: AbstractRobustConnection) -> Pool[AbstractChannel]:
        async def get_channel() -> AbstractChannel:
            return await connection.channel()

        return Pool(get_channel, max_size=10)

    @provide(scope=Scope.APP)
    async def get_temporal_client(self) -> Client:
        return await Client.connect(settings.temporal_dsn)

    # Добавлен провайдер для S3Client
    @provide(scope=Scope.APP)
    def get_s3_client(self) -> S3Client:
        s3_config = S3Config(**os.environ)
        return S3Client(s3_config)

    # --- INGEST ---

    @provide(scope=Scope.APP)
    def get_keydb_store(self, redis: Redis) -> KeyDbStatusStore:
        return KeyDbStatusStore(redis_client=redis)

    @provide(scope=Scope.APP)
    def get_rabbit_publisher(self, channel_pool: Pool[AbstractChannel]) -> RabbitEventPublisher:
        return RabbitEventPublisher(channel_pool=channel_pool)

    @provide(scope=Scope.APP)
    def get_temporal_adapter(self, client: Client) -> TemporalAdapter:
        return TemporalAdapter(client)


class ApplicationProvider(Provider):
    status_store = alias(source=KeyDbStatusStore, provides=StatusStore)
    event_publisher = alias(source=RabbitEventPublisher, provides=EventPublisher)
    temporal_gateway = alias(source=TemporalAdapter, provides=TemporalGateway)

    start_use_case = provide(StartUseCase, scope=Scope.APP)

    ingest_activities_v1 = provide(IngestActivitiesV1, scope=Scope.APP)