import asyncio
import logging

from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from dishka import make_async_container

from application.ingest.use_case import StartIngestUseCase
from infrastructure.providers import InfrastructureProvider, ApplicationProvider
from interfaces.ingest.dto import IngestStartMessageDTO
from interfaces.ingest.mappers import to_start_command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # 1. Создаем DI Контейнер. Dishka сама инициализирует пулы и соединения при их первом запросе
    container = make_async_container(
        InfrastructureProvider(),
        ApplicationProvider()
    )

    try:
        # 2. Достаем готовый Use Case из контейнера.
        # Dishka сама найдет Redis, RabbitMQ, Temporal, соберет адаптеры и передаст их в Use Case!
        ingest_uc = await container.get(StartIngestUseCase)

        # 3. Нам нужно подключение к RabbitMQ только для того, чтобы слушать очередь (Consumer)
        rabbit_conn = await container.get(AbstractRobustConnection)
        channel: AbstractChannel = await rabbit_conn.channel()

        # 4. Настраиваем Consumer
        ingest_queue = await channel.declare_queue('ingest.start', durable=True)

        async def process_ingest(message: IncomingMessage):
            async with message.process():
                try:
                    payload = IngestStartMessageDTO.model_validate_json(message.body)
                    command = to_start_command(payload)
                    logger.info(f"Starting ingest workflow {command.workflow_id}")

                    # Запускаем наш Use Case!
                    await ingest_uc.execute(command)

                except Exception as e:
                    logger.error(f"Failed to process ingest: {e}", exc_info=True)

        # Начинаем слушать очередь
        await ingest_queue.consume(process_ingest)

        logger.info("Worker started. Waiting for messages...")

        # Бесконечный цикл, пока не придет сигнал остановки (Ctrl+C)
        await asyncio.Future()

    finally:
        # 5. При остановке приложения Dishka КОРРЕКТНО закроет все соединения
        # (вызовет aclose() у Redis, close() у RabbitMQ, закроет пулы и т.д.)
        logger.info("Closing container and connections...")
        await container.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped.")