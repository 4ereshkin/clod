from temporalio import workflow
from shared.workflows.catalog import INGEST_V1


@workflow.defn(name=INGEST_V1.workflow_name)
class IngestWorkflow:
    pass