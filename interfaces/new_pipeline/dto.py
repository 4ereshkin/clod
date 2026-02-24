from typing import List, Optional, Dict
from pydantic import BaseModel, Field

class PipelineScanDTO(BaseModel):
    artifacts: List[Dict[str, str]]
    scan_meta: Optional[Dict[str, Any]] = None

class StartPipelineDTO(BaseModel):
    company_id: str
    dataset_name: str
    target_crs_id: str
    scans: List[PipelineScanDTO]
    schema_version: str = "1.1.0"
    force: bool = False
    run_old_cluster: bool = True
