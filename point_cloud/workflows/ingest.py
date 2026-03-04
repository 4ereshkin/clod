from temporalio import workflow
from shared.workflows.catalog import INGEST_V1

from typing import Any, Dict, List, Optional


@workflow.defn(name=INGEST_V1.workflow_name)
class IngestWorkflow:
    def __init__(self):
        self._stage: str = 'Initializing'
        self._errors: Dict[str, str] = {}
