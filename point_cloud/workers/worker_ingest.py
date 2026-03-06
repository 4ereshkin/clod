import asyncio
import logging
from dotenv import load_dotenv

# ВАЖНО: Загружаем переменные окружения ДО остальных импортов
load_dotenv()

from temporalio.worker import Worker
from temporalio.client import Client
from dishka import make_async_container

# Импортируем DI-провайдеры
from infrastructure.providers import InfrastructureProvider, ApplicationProvider
from point_cloud.activities.ingest_activities_v1 import IngestActivitiesV1
# Импортируем сам Workflow
from point_cloud.workflows.ingest import IngestWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # 1. Создаем DI-контейнер для воркера (такой же, как в main.py!)
    container = make_async_container(
        InfrastructureProvider(),
        ApplicationProvider()
    )

    try:
        # 2. Получаем готового клиента Temporal из DI
        temporal_client = await container.get(Client)

        # 3. Получаем собранные Activities (с инжектированными пулами/подключениями)
        activities = await container.get(IngestActivitiesV1)

        # 4. Настраиваем Worker
        # Имя Task Queue должно совпадать с тем, что ты передаешь в UseCase
        task_queue = "ingest-task-queue"

        worker = Worker(
            temporal_client,
            task_queue=task_queue,
            workflows=[IngestWorkflow],
            # Внимание: передаем методы экземпляра класса Activities!
            activities=[
                activities.download_s3_object,
                activities.upload_s3_object,
                activities.compute_point_cloud_stats,
                activities.save_dict_to_json,
                activities.reproject_to_copc,
            ],
        )

        logger.info(f"Ingest Temporal Worker started on queue '{task_queue}'...")
        # 5. Запускаем воркер (он будет крутиться вечно)
        await worker.run()

    finally:
        # 6. При остановке воркера (Ctrl+C), Dishka корректно закроет соединения
        logger.info("Worker stopped. Closing infrastructure connections...")
        await container.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass