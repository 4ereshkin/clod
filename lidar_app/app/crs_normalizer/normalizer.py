from __future__ import annotations

from typing import Dict

from .core import normalize_crs_spec
from .models import CRSNormalizeRequest, CRSNormalizeResult
from .msk_presets import MSKRegionPreset


def normalize_crs(req: CRSNormalizeRequest, *, msk_presets: Dict[int, MSKRegionPreset]) -> CRSNormalizeResult:
    normalized = normalize_crs_spec(req.crs, msk_presets=msk_presets)
    return CRSNormalizeResult(payload_version='v1', **normalized.__dict__)
