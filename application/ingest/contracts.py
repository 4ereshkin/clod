from time import time
from typing import Any, Literal

from pydantic import BaseModel, Field

from application.common.contracts import (
    WorkflowStatus, ResultObject
)


class StartIngestObjectRef(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)


class StartIngestScanPayload(BaseModel):
    point_cloud: dict[str, StartIngestObjectRef] = Field(default_factory=dict)
    trajectory: dict[str, StartIngestObjectRef] = Field(default_factory=dict)
    control_point: dict[str, StartIngestObjectRef] = Field(default_factory=dict)


class StartIngestCommand(BaseModel):
    workflow_id: str
    scenario: Literal['ingest']
    message_version: str
    pipeline_version: str
    dataset: dict[str, StartIngestScanPayload]


class ScenarioSpec(BaseModel):
    workflow_name: str
    task_queue: str
    query_name: str = 'progress'


class ScenarioResult(BaseModel):
    workflow_id: str
    scenario: str
    status: WorkflowStatus
    outputs: list[ResultObject] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=time)
