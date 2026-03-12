from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class RegObjectRefDTO(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)

class RegScanPayloadDTO(BaseModel):
    point_cloud: RegObjectRefDTO
    trajectory: Optional[RegObjectRefDTO] = None

class RegParamsDTO(BaseModel):
    crop_radius_m: float = 40.0
    global_voxel_m: float = 1.0
    cascade_voxels_m: tuple[float, float, float] = (1.0, 0.3, 0.1)
    cascade_max_corr_multipliers: tuple[float, float, float] = (3.0, 2.0, 1.5)
    min_fitness: float = 0.2

class VersionDTO(BaseModel):
    message_version: str = Field(min_length=1)
    pipeline_version: str = Field(min_length=1)

class RegistrationStartMessageDTO(BaseModel):
    workflow_id: str = Field(min_length=1)
    scenario: str = Field(min_length=1)
    version: VersionDTO
    dataset: dict[str, RegScanPayloadDTO]
    params: Optional[RegParamsDTO] = Field(default_factory=RegParamsDTO)