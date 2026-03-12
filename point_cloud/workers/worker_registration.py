import asyncio
import logging
from dotenv import load_dotenv

load_dotenv()

import concurrent.futures
from temporalio.worker import Worker
from temporalio.client import Client
from dishka import make_async_container

from infrastructure.providers import InfrastructureProvider, ApplicationProvider
# Импортируем наш новый класс активностей!
from point_cloud.activities.registration_activities import RegistrationActivitiesV1
# Импортируем наш новый воркфлоу!
from point_cloud.workflows.registration import RegistrationWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():

    container = make_async_container(
        InfrastructureProvider(),
        ApplicationProvider()
    )

    try:
        temporal_client = await container.get(Client)

        activities = await container.get(RegistrationActivitiesV1)

        task_queue = "registration-queue"

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as activity_executor:
            worker = Worker(
                temporal_client,
                task_queue=task_queue,
                workflows=[RegistrationWorkflow],  # Наш Воркфлоу
                activities=[
                    activities.prepare_scan_for_registration,
                    activities.propose_edges,
                    activities.register_pair,
                    activities.solve_pose_graph,
                    activities.download_scan,
                    activities.upload_s3_object,
                    activities.save_dict_to_json,
                    activities.publish_status_activity,
                    activities.publish_completed_activity
                ],
                activity_executor=activity_executor,
            )

            logger.info(f"Registration Temporal Worker started on queue '{task_queue}'...")
            await worker.run()

    finally:
        logger.info("Worker stopped. Closing infrastructure connections...")
        await container.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass