from time import time

from dataclasses import dataclass, field
from typing import Any
from application.ingest.status import WorkflowStatus, ErrorCode


@dataclass(frozen=True)
class StartIngestObjectRef:
    s3_key: str
    etag: str


@dataclass(frozen=True)
class StartIngestScanPayload:
    point_cloud: dict[str, StartIngestObjectRef] = field(default_factory=dict)
    trajectory: dict[str, StartIngestObjectRef] = field(default_factory=dict)
    control_point: dict[str, StartIngestObjectRef] = field(default_factory=dict)


@dataclass(frozen=True)
class StartIngestCommand:
    workflow_id: str
    scenario: str
    message_version: str
    pipeline_version: str
    dataset: dict[str, StartIngestScanPayload]


@dataclass(frozen=True)
class ScenarioSpec:
    workflow_name: str
    task_queue: str
    query_name: str = 'progress'


@dataclass(frozen=True)
class ResultObjectDTO:
    kind: str
    s3_key: str
    etag: str | None = None


@dataclass(frozen=True)
class ScenarioResult:
    workflow_id: str
    scenario: str
    status: WorkflowStatus
    outputs: list[ResultObjectDTO] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: float = time()