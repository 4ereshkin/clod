from typing import Literal, Dict, Any, List
from pydantic import BaseModel, Field
from application.common.contracts import BaseStartCommand

class RegistrationObjectRef(BaseModel):
    s3_key: str = Field(min_length=1)
    etag: str = Field(min_length=1)


class RegistrationScanPayload(BaseModel):
    point_cloud: RegistrationObjectRef
    trajectory: RegistrationObjectRef | None = None


class RegistrationParams(BaseModel):
    # Общие
    crop_radius_m: float = 40.0
    global_voxel_m: float = 1.0

    # FPFH и RANSAC (Global)
    fpfh_max_nn: int = 100
    ransac_max_iterations: int = 4000000
    ransac_confidence: float = 0.999
    ransac_edge_length_threshold: float = 0.9

    # Cascade ICP
    cascade_voxels_m: tuple[float, float, float] = (1.0, 0.3, 0.1)
    cascade_max_corr_multipliers: tuple[float, float, float] = (3.0, 2.0, 1.5)
    icp_max_iterations: int = 50
    min_fitness: float = 0.2


class StartRegistrationCommand(BaseStartCommand):
    scenario: Literal['registration']
    dataset: dict[str, RegistrationScanPayload]
    params: RegistrationParams = Field(default_factory=RegistrationParams)