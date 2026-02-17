from __future__ import annotations

from typing import Dict

from .core import normalize_crs_spec
from .ingest_contract import WorkflowIngestPayload
from .ingest_request import IngestRequest
from .msk_presets import MSKRegionPreset


def normalize_to_workflow(
    req: IngestRequest,
    *,
    msk_presets: Dict[int, MSKRegionPreset],
) -> WorkflowIngestPayload:
    normalized = normalize_crs_spec(req.crs, msk_presets=msk_presets)
    return WorkflowIngestPayload(
        payload_version='v1',
        company=req.company,
        department=req.department,
        employee=req.employee,
        plan=req.plan,
        authority=req.authority,
        **normalized.__dict__,
    )
