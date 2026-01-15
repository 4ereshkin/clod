import asyncio
import os
from temporalio.client import Client
from point_cloud.workflows.cluster_workflow import ClusterPipeline, ClusterPipelineParams

async def main():
    os.environ["WORKFLOW_VERSION"] = "MVP-plus"  # обязательно
    # os.environ["SCHEMA_VERSION"] = "1.1.0"  # опционально

    client = await Client.connect("localhost:7233")

    params = ClusterPipelineParams(
        dataset_version_id="01KF1G0T7KW7MZPR0RY6K579W3"
    )

    handle = await client.start_workflow(
        ClusterPipeline.__temporal_workflow_definition.name,
        params,
        id=f"cluster-{params.dataset_version_id}",
        task_queue="point-cloud-task-queue",
    )

    print("STARTED:", handle.id)
    result = await handle.result()
    print("RESULT:", result)

asyncio.run(main())
