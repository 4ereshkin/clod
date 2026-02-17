from typing import Optional, Literal, List, Any
from pydantic import BaseModel, Field

import uuid


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

    columns: Optional[List[str]] = None
    provider: Literal['s3', 'fs'] # извлекаем файл по path или из S3, или из файловой системы

class PointCloud(BaseModel):
    path: str
    etag: str
    crs_projjson: str

    provider: Literal['s3', 'fs'] # извлекаем файл по path или из S3, или из файловой системы

class Scan(BaseModel):
    scan_uuid: uuid.UUID = Field(default_factory=uuid.uuid4)
    cp: ControlPoint
    trajectory: PathFile
    cloud: PointCloud

class Dataset(BaseModel):
    dataset_uuid: uuid.UUID = Field(default_factory=uuid.uuid4)
    scans: List[Scan] = Field(default_factory=list)
