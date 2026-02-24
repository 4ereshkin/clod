import pytest
from unittest.mock import Mock, MagicMock
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from point_cloud.workflows.new_architecture_workflow import NewArchitectureWorkflow, NewArchitectureParams, NewArchitectureScan
from point_cloud.workflows.smart_ingest_workflow import SmartIngestWorkflow

# Mock activities
@activity.defn(name="ensure_company")
async def ensure_company_mock(*args): pass

@activity.defn(name="ensure_crs")
async def ensure_crs_mock(*args): pass

@activity.defn(name="ensure_dataset")
async def ensure_dataset_mock(*args): return "dataset_id_1"

@activity.defn(name="ensure_dataset_version")
async def ensure_dataset_version_mock(*args): return {"id": "dv_id_1"}

@activity.defn(name="create_scan")
async def create_scan_mock(*args): return "scan_id_1"

@activity.defn(name="update_scan_meta")
async def update_scan_meta_mock(*args): pass

@activity.defn(name="upload_raw_artifact")
async def upload_raw_artifact_mock(*args): return {"key": "uploaded_key"}

@activity.defn(name="create_ingest_run")
async def create_ingest_run_mock(*args): return 123

@activity.defn(name="process_ingest_run")
async def process_ingest_run_mock(*args): return {"manifest_key": "mkey", "manifest_bucket": "mbucket"}

@activity.defn(name="point_cloud_meta")
async def point_cloud_meta_mock(*args):
    return {"srs": {"wkt": "EPSG:32641"}}

@activity.defn(name="read_cloud_hexbin")
async def read_cloud_hexbin_mock(*args): return {}

@activity.defn(name="extract_hexbin_fields")
async def extract_hexbin_fields_mock(*args): return {}

@activity.defn(name="upload_hexbin")
async def upload_hexbin_mock(*args): return {}

@activity.defn(name="upload_profiling_manifest")
async def upload_profiling_manifest_mock(*args): return {}

@activity.defn(name="count_scans_in_dataset_version")
async def count_scans_in_dataset_version_mock(*args): return 10

@activity.defn(name="update_ingest_manifest_with_logic")
async def update_ingest_manifest_with_logic_mock(*args): pass

@activity.defn(name="reproject_scan_to_target_crs")
async def reproject_scan_to_target_crs_mock(*args): return {}

@activity.defn(name="propose_registration_edges_for_dataset")
async def propose_registration_edges_for_dataset_mock(*args): return {}

@activity.defn(name="cluster_scan_custom")
async def cluster_scan_custom_mock(*args): return {}

@activity.defn(name="export_merged_laz")
async def export_merged_laz_mock(*args): return {}

# Child workflow mock for Download
from point_cloud.workflows.download_workflow import DownloadWorkflow
from temporalio import workflow

@workflow.defn(name="MVP-download")
class MockDownloadWorkflow:
    @workflow.run
    async def run(self, params):
        return {"raw.point_cloud": "/tmp/mock/cloud.laz"}

# Child workflow mock for ClusterPipeline
@workflow.defn(name="MVP-plus_cluster")
class MockClusterPipeline:
    @workflow.run
    async def run(self, params):
        return {}


@pytest.mark.asyncio
async def test_new_architecture_workflow():
    async with await WorkflowEnvironment.start_local() as env:
        async with env.worker_task_queue("point-cloud-task-queue") as worker:
            worker.workflows = [
                NewArchitectureWorkflow,
                SmartIngestWorkflow,
                MockDownloadWorkflow,
                MockClusterPipeline
            ]
            worker.activities = [
                ensure_company_mock, ensure_crs_mock, ensure_dataset_mock, ensure_dataset_version_mock,
                create_scan_mock, update_scan_meta_mock, upload_raw_artifact_mock, create_ingest_run_mock,
                process_ingest_run_mock, point_cloud_meta_mock, read_cloud_hexbin_mock,
                extract_hexbin_fields_mock, upload_hexbin_mock, upload_profiling_manifest_mock,
                count_scans_in_dataset_version_mock, update_ingest_manifest_with_logic_mock,
                reproject_scan_to_target_crs_mock, propose_registration_edges_for_dataset_mock,
                cluster_scan_custom_mock, export_merged_laz_mock
            ]

            # Start workflow
            params = NewArchitectureParams(
                company_id="test_company",
                dataset_name="test_dataset",
                target_crs_id="EPSG:32641",
                scans=[
                    NewArchitectureScan(
                        artifacts=[{"kind": "raw.point_cloud", "local_file_path": "test.laz"}]
                    )
                ],
                run_old_cluster=True
            )

            client = env.client
            handle = await client.start_workflow(
                NewArchitectureWorkflow.run,
                params,
                id="test-workflow-id",
                task_queue="point-cloud-task-queue",
            )

            result = await handle.result()

            assert "ingest" in result
            assert "reproject" in result
            assert "icp" in result
            assert "new_cluster" in result
            assert "old_cluster" in result
            assert "publish" in result

            # Check logic passed correctly
            ingest_logic = result["ingest"][0]["logic"]
            assert ingest_logic["georeference"] == "YES" # mock count=10
            assert ingest_logic["reproject"] == "YES"
            assert ingest_logic["target_crs_id"] == "EPSG:32641"
