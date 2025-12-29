import asyncio
from temporalio.client import Client
from point_cloud.temporal.workflows.registration_solver_workflow import RegistrationSolverParams
import yaml

with open(r"config.yaml", "r") as f:
    VERSION = yaml.safe_load(f.read())["VERSION_INFO"]["WORKFLOW_VERSION"]

async def main():
    client = await Client.connect("localhost:7233")
    dataset_version_id = '01KDMCEKVSRT2ZA8CQR4RQBQ76'
    company_id = "ArsBazh"

    params = RegistrationSolverParams(
        company_id=company_id,
        dataset_version_id=dataset_version_id,
        schema_version="1.1.0",
        force=True,
    )

    wf_id = f"reg-{dataset_version_id}"
    handle = await client.start_workflow(
        f"{VERSION}-registration-solver",
        params,
        id=wf_id,
        task_queue="point-cloud-task-queue",
    )
    print("STARTED:", handle.id)
    print(await handle.result())

asyncio.run(main())
