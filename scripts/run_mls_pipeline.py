import asyncio
import time
from pathlib import Path

from temporalio.client import Client

import sys
from pathlib import Path

# add project root to PYTHONPATH
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from point_cloud.temporal.workflows.mls_new import MlsPipelineWorkflow, MlsPipelineParams

async def main():
    company = "MaxCher"
    dataset_name = "mirea"
    dataset_crs_id = "CGCS2000"
    target_srs = "EPSG:4326"
    schema_version = "1.1.0"

    cloud = Path(r"data\user_data\НПС Крутое\1\t100pro_2025-04-28-08-36-08_filter_map.laz").absolute()
    path  = Path(r"data\user_data\НПС Крутое\1\path.txt").absolute()
    cp    = Path(r"data\user_data\НПС Крутое\1\ControlPoint.txt").absolute()

    artifacts = [
        {"kind": "raw.point_cloud", "local_file_path": str(cloud)},
        {"kind": "raw.trajectory",  "local_file_path": str(path)},
        {"kind": "raw.control_point","local_file_path": str(cp)},
    ]

    client = await Client.connect("localhost:7233")
    wf_id = f"mls-{company}-{dataset_name}-{int(time.time())}"

    params = MlsPipelineParams(
        company_id=company,
        dataset_name=dataset_name,
        bump_version=False,          # для нескольких сканов в одной версии
        dataset_crs_id=dataset_crs_id,
        target_srs=target_srs,
        schema_version=schema_version,
        force=False,
        artifacts=artifacts,
    )

    handle = await client.start_workflow(
        f"{MlsPipelineWorkflow.__temporal_workflow_definition.name}",
        params,
        id=wf_id,
        task_queue="point-cloud-task-queue",
    )

    print("STARTED:", wf_id)
    res = await handle.result()
    print("RESULT:", res)

if __name__ == "__main__":
    asyncio.run(main())
