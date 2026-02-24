from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, Field

from application.common.contracts import (
    WorkflowStatus, ResultObject
)


class IngestObjectRefDTO(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)

class ScanPayloadDTO(BaseModel):
    point_cloud: dict[str, IngestObjectRefDTO] = Field(default_factory=dict)
    trajectory: dict[str, IngestObjectRefDTO] = Field(default_factory=dict)
    control_point: dict[str, IngestObjectRefDTO] = Field(default_factory=dict)

class VersionDTO(BaseModel):
    message_version: str = Field(min_length=1)
    pipeline_version: str = Field(min_length=1)

class IngestStartMessageDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str = Field(min_length=1)
    version: VersionDTO
    dataset: dict[str, ScanPayloadDTO]

class ResultObjectDTO(BaseModel):
    kind: str
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)

class WorkflowCompletedDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus = WorkflowStatus.COMPLETED
    outputs: list[ResultObjectDTO]

class WorkflowFailedDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus = WorkflowStatus.FAILED
    error_code: Any
    error_message: str
    retryable: bool
    failed_at: float

class StatusEventDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus
    timestamp: float
    details: dict[str, Any] = Field(default_factory=dict)
