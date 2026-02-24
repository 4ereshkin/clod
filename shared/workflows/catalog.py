from __future__ import annotations

from dataclasses import dataclass
# bayonda

@dataclass(frozen=True)
class WorkflowBinding:
    workflow_name: str
    task_queue: str
    query_name: str


INGEST_V1 = WorkflowBinding(
    workflow_name='Ingest-1',
    task_queue='ingest-queue',
    query_name='progress',
)