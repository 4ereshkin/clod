from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import lidar_app.app

@dataclass
class IngestWorkflowParams:



    dataset_id:
    crs_id: Optional[str] = None
    crs_epsg: Optional[int] = None
    crs_name: Optional[str] = None
    crs_zone_degree: int = 0
    crs_units: str = "m"
    crs_axis_order: str = "x_east,y_north,z_up"
    schema_version: str = "1.1.0"
    force: bool = False
    artifacts: Optional[List[Dict[str, str]]] = None
    scan_meta: Optional[Dict[str, Any]] = None