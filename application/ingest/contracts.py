from typing import Any, Literal

from pydantic import BaseModel, Field

from application.common.contracts import BaseStartCommand


class StartIngestObjectRef(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)
    crs: dict[str, Any] | None = Field(default=None)


class StartIngestScanPayload(BaseModel):
    point_cloud: dict[str, StartIngestObjectRef] = Field(default_factory=dict)
    trajectory: dict[str, StartIngestObjectRef] = Field(default_factory=dict)
    control_point: dict[str, StartIngestObjectRef] = Field(default_factory=dict)


class StartIngestCommand(BaseStartCommand):
    scenario: Literal['ingest']
    dataset: dict[str, StartIngestScanPayload]
