import asyncio
from temporalio.client import Client
from interfaces.new_pipeline.dto import StartPipelineDTO
from point_cloud.workflows.new_architecture_workflow import NewArchitectureWorkflow, NewArchitectureParams, NewArchitectureScan

class NewPipelineTemporalAdapter:
    def __init__(self, client: Client):
        self.client = client

    async def start_pipeline(self, dto: StartPipelineDTO) -> str:
        workflow_id = f"new-pipeline-{dto.company_id}-{dto.dataset_name}"

        # Convert Pydantic DTO to NewArchitectureParams (dataclass)
        # This handles the serialization issue by passing a known type to Temporal
        # or we could pass a dict if the workflow signature accepted it, but dataclass is better typed.

        scans = []
        for scan_dto in dto.scans:
            scans.append(NewArchitectureScan(
                artifacts=scan_dto.artifacts,
                scan_meta=scan_dto.scan_meta
            ))

        params = NewArchitectureParams(
            company_id=dto.company_id,
            dataset_name=dto.dataset_name,
            target_crs_id=dto.target_crs_id,
            scans=scans,
            schema_version=dto.schema_version,
            force=dto.force,
            run_old_cluster=dto.run_old_cluster
        )

        # Start workflow async (fire and forget from client's perspective)
        await self.client.start_workflow(
            NewArchitectureWorkflow.run,
            params,
            id=workflow_id,
            task_queue="point-cloud-task-queue",
            search_attributes=None
        )
        return workflow_id
