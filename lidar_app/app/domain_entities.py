from __future__ import annotations

import uuid
from typing import Literal, Optional

from pydantic import BaseModel, Field


class Point(BaseModel):
    x: float
    y: float
    z: float
    crs_projjson: str


class ControlPoint(BaseModel):
    local_point: Point
    reference_point: Point


class PathFile(BaseModel):
    path: str
    etag: str
    crs_projjson: str
    columns: Optional[list[str]] = None
    provider: Literal['s3', 'fs']


class PointCloud(BaseModel):
    path: str
    etag: str
    crs_projjson: str
    provider: Literal['s3', 'fs']


class Scan(BaseModel):
    scan_uuid: uuid.UUID = Field(default_factory=uuid.uuid4)
    cp: ControlPoint
    trajectory: PathFile
    cloud: PointCloud


class Dataset(BaseModel):
    dataset_uuid: uuid.UUID = Field(default_factory=uuid.uuid4)
    path: str
    provider: Literal['s3', 'fs']
    scans: list[Scan] = Field(default_factory=list)
