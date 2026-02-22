from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class VersionDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_version: str = Field(min_length=1)
    pipeline_version: str = Field(min_length=1)


class S3ObjectRefDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)


class ScanPayloadDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    control_point: dict[str, S3ObjectRefDTO] = Field(default_factory=dict)
    trajectory: dict[str, S3ObjectRefDTO] = Field(default_factory=dict)
    point_cloud: dict[str, S3ObjectRefDTO]


class IngestStartMessageDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str = Field(min_length=1)
    scenario: str = Field(min_length=1)
    version: VersionDTO
    dataset: dict[str, ScanPayloadDTO]


class StatusEventDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    scenario: str
    status: str
    details: dict[str, Any] = Field(default_factory=dict)


class ResultObjectDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str
    s3_key: str
    etag: str | None = None


class WorkflowCompletedEventDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    scenario: str
    status: Literal["COMPLETED"] = "COMPLETED"
    outputs: list[ResultObjectDTO] = Field(default_factory=list)


class WorkflowFailedEventDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    scenario: str
    status: Literal["FAILED"] = "FAILED"
    error_code: str
    error_message: str
    retryable: bool = False
