from time import time

from typing import Literal, Any
from pydantic import BaseModel, Field, model_validator
from application.ingest.status import WorkflowStatus, ErrorCode


class ResultObjectDTO(BaseModel):
    kind: str
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)


class StatusEventDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus
    timestamp: float = Field(default_factory=time)
    details: dict[str, Any] = Field(default_factory=dict)


class VersionDTO(BaseModel):
    message_version: Literal['0']
    pipeline_version: Literal['1']


class S3ObjectRefDTO(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)


class ScanPayloadDTO(BaseModel):
    control_point: dict[str, S3ObjectRefDTO] = Field(default_factory=dict)
    trajectory: dict[str, S3ObjectRefDTO] = Field(default_factory=dict)
    point_cloud: dict[str, S3ObjectRefDTO]


class IngestStartMessageDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: Literal['ingest']
    version: VersionDTO
    dataset: dict[str, ScanPayloadDTO]


class WorkflowCompletedDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: Literal[WorkflowStatus.COMPLETED] = WorkflowStatus.COMPLETED
    outputs: list[ResultObjectDTO]


class WorkflowFailedDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: Literal[WorkflowStatus.FAILED]
    error_code: ErrorCode
    error_message: str
    retryable: bool
    failed_at: float = Field(default_factory=time)