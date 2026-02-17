from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from .models import CRSNormalizeRequest, CRSNormalizeResult
from .msk_presets import MSKRegionPreset, load_msk_presets_yaml
from .normalizer import normalize_crs


@lru_cache(maxsize=1)
def _get_msk_presets(path: str) -> Dict[int, MSKRegionPreset]:
    return load_msk_presets_yaml(path)


def handle_crs_normalization(raw: Dict[str, Any], *, msk_presets_path: str | None = None) -> CRSNormalizeResult:
    req = CRSNormalizeRequest.model_validate(raw)
    if msk_presets_path is None:
        msk_presets_path = str(Path(__file__).with_name('MSK_PRESETS.yaml'))
    presets = _get_msk_presets(msk_presets_path)
    return normalize_crs(req, msk_presets=presets)
