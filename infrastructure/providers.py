import logging
from typing import Any, AsyncGenerator

from application.common.config import AppSettings

logger = logging.getLogger(__name__)

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from aio_pika.pool import Pool
from dishka import Provider, Scope, provide, alias
from redis.asyncio import Redis, from_url
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.hub.base_hub_connection import BaseHubConnection
from temporalio.client import Client

from application.common.interfaces import TemporalGateway, StatusStore, EventPublisher
from application.common.use_case import StartUseCase
from infrastructure.common.signalr import SignalREventPublisher
from infrastructure.common.keydb import KeyDbStatusStore
from infrastructure.common.rabbit import RabbitEventPublisher
from infrastructure.common.temporal import TemporalAdapter


class InfrastructureProvider(Provider):
    @provide(scope=Scope.APP)
    def get_settings(self) -> AppSettings:
        return AppSettings()

    @provide(scope=Scope.APP)
    async def get_redis_client(self, config: AppSettings) -> AsyncGenerator[Redis, Any]:
        logger.info("Connecting to KeyDB")
        client = from_url(config.keydb.dsn, encoding="utf-8", decode_responses=True)
        await client.ping()
        logger.info("Connected to KeyDB")
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    async def get_rabbit_connection(self, config: AppSettings) -> AsyncGenerator[AbstractRobustConnection, Any]:
        logger.info("Connecting to RabbitMQ")
        connection = await aio_pika.connect_robust(config.rabbitmq.dsn)
        logger.info("Connected to RabbitMQ")
        yield connection
        await connection.close()

    @provide(scope=Scope.APP)
    def get_channel_pool(self, connection: AbstractRobustConnection) -> Pool[AbstractChannel]:
        async def get_channel() -> AbstractChannel:
            return await connection.channel()

        return Pool(get_channel, max_size=10)

    @provide(scope=Scope.APP)
    async def get_temporal_client(self, config: AppSettings) -> Client:
        logger.info("Connecting to Temporal")
        client = await Client.connect(config.temporal.dsn)
        logger.info("Connected to Temporal")
        return client

    # --- INGEST ---

    @provide(scope=Scope.APP)
    def get_keydb_store(self, redis: Redis) -> KeyDbStatusStore:
        return KeyDbStatusStore(redis_client=redis, prefix='pipeline')

    @provide(scope=Scope.APP)
    def get_rabbit_publisher(self, channel_pool: Pool[AbstractChannel], config: AppSettings) -> RabbitEventPublisher:
        return RabbitEventPublisher(channel_pool=channel_pool)

    @provide(scope=Scope.APP)
    def get_signalr_publisher(self, client: BaseHubConnection) -> SignalREventPublisher:
        return SignalREventPublisher(client=client)

    @provide(scope=Scope.APP)
    def get_signalr_connection(self, config: AppSettings) -> BaseHubConnection:
        hub_connection = HubConnectionBuilder().with_url(config.signalr_hub_url).build()
        return hub_connection


    @provide(scope=Scope.APP)
    async def get_event_publisher(self,
                                  rabbit: RabbitEventPublisher,
                                  signalr: SignalREventPublisher,
                                  config: AppSettings) -> EventPublisher:
        if config.event_transport == "signalr":
            signalr.client.start()
            return signalr
        return rabbit

    @provide(scope=Scope.APP)
    def get_temporal_adapter(self, client: Client) -> TemporalAdapter:
        return TemporalAdapter(client)


class ApplicationProvider(Provider):
    status_store = alias(source=KeyDbStatusStore, provides=StatusStore)
    temporal_gateway = alias(source=TemporalAdapter, provides=TemporalGateway)

    start_use_case = provide(StartUseCase, scope=Scope.APP)