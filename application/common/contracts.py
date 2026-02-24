from enum import Enum
from time import time
from typing import Any

from pydantic import BaseModel, Field


class WorkflowStatus(Enum):
    RECEIVED = 'RECEIVED'
    VALIDATED = 'VALIDATED'
    RESOLVED_SCENARIO = 'RESOLVED_SCENARIO'
    STARTING ='STARTING'
    RUNNING ='RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    RETRYING = 'RETRYING'


class ErrorCode(Enum):
    VALIDATION_ERROR = 'VALIDATION_ERROR'
    TEMPORAL_START_ERROR = 'TEMPORAL_START_ERROR'
    TEMPORAL_EXECUTION_ERROR = 'TEMPORAL_EXECUTION_ERROR'


class StatusEvent(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus
    timestamp: float = Field(default_factory=time)
    details: dict[str, Any] = Field(default_factory=dict)


class FailedEvent(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str
    status: WorkflowStatus = WorkflowStatus.FAILED
    error_code: ErrorCode
    error_message: str
    retryable: bool
    failed_at: float = Field(default_factory=time)


class ResultObject(BaseModel):
    kind: str
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)
