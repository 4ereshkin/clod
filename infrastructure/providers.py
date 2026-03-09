import os
import asyncio
from typing import Any, AsyncGenerator, Callable

from legacy_env_vars import settings, Settings # Импортируем и класс, и сам объект

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from aio_pika.pool import Pool
from dishka import Provider, Scope, provide, alias
from aioredis import Redis, from_url
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.hub.base_hub_connection import BaseHubConnection
from temporalio.client import Client

from application.common.interfaces import TemporalGateway, StatusStore, EventPublisher
from application.common.use_case import StartUseCase
from infrastructure.common.signalr import SignalREventPublisher
from infrastructure.ingest.keydb_adapter import KeyDbStatusStore
from infrastructure.ingest.rabbit_adapter import RabbitEventPublisher
from infrastructure.ingest.temporal_adapter import TemporalAdapter
from infrastructure.infrastructure_config import S3Config
from infrastructure.s3 import S3Client
from point_cloud.activities.ingest_activities_v1 import IngestActivitiesV1


class InfrastructureProvider(Provider):
    @provide(scope=Scope.APP)
    def get_settings(self) -> Settings:
        return settings

    @provide(scope=Scope.APP)
    async def get_redis_client(self, config: Settings) -> AsyncGenerator[Redis, Any]:
        client = from_url(config.keydb_dsn, encoding="utf-8", decode_responses=True)
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    async def get_rabbit_connection(self, config: Settings) -> AsyncGenerator[AbstractRobustConnection, Any]:
        connection = await aio_pika.connect_robust(config.rabbit_dsn)
        yield connection
        await connection.close()

    @provide(scope=Scope.APP)
    def get_channel_pool(self, connection: AbstractRobustConnection) -> Pool[AbstractChannel]:
        async def get_channel() -> AbstractChannel:
            return await connection.channel()

        return Pool(get_channel, max_size=10)

    @provide(scope=Scope.APP)
    async def get_temporal_client(self, config: Settings) -> Client:
        return await Client.connect(config.temporal_dsn)

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
    def get_rabbit_publisher(self, channel_pool: Pool[AbstractChannel], config: Settings) -> RabbitEventPublisher:
        return RabbitEventPublisher(channel_pool=channel_pool)

    @provide(scope=Scope.APP)
    def get_signalr_publisher(self, client: BaseHubConnection) -> SignalREventPublisher:
        return SignalREventPublisher(client=client,
                                     status_method='RecieveIngestStatus',
                                     completed_method='RecieveIngestCompleted',
                                     failed_method='RecieveIngestFailed'
                                     )

    @provide(scope=Scope.APP)
    def get_signalr_connection(self, config: Settings) -> BaseHubConnection:
        hub_connection = HubConnectionBuilder().with_url(config.signalr_hub_url).build()
        hub_connection.start()
        return hub_connection


    @provide(scope=Scope.APP)
    async def get_event_publisher(self,
                                  rabbit: RabbitEventPublisher,
                                  signalr: SignalREventPublisher,
                                  config: Settings) -> EventPublisher:
        if config.event_transport == "signalr":
            return signalr
        return rabbit

    @provide(scope=Scope.APP)
    def get_temporal_adapter(self, client: Client) -> TemporalAdapter:
        return TemporalAdapter(client)


class ApplicationProvider(Provider):
    status_store = alias(source=KeyDbStatusStore, provides=StatusStore)
    temporal_gateway = alias(source=TemporalAdapter, provides=TemporalGateway)

    start_use_case = provide(StartUseCase, scope=Scope.APP)
    ingest_activities_v1 = provide(IngestActivitiesV1, scope=Scope.APP)