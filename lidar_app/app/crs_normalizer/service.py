from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from .models_v1 import CRSNormalizeRequestV1, CRSNormalizeResultV1
from .msk_presets import MSKRegionPreset, load_msk_presets_yaml
from .normalizer_v1 import normalize_crs_v1


@lru_cache(maxsize=1)
def _get_msk_presets(path: str) -> Dict[int, MSKRegionPreset]:
    return load_msk_presets_yaml(path)


def handle_crs_normalization(raw: Dict[str, Any], *, msk_presets_path: str | None = None) -> CRSNormalizeResultV1:
    req = CRSNormalizeRequestV1.model_validate(raw)
    if msk_presets_path is None:
        msk_presets_path = str(Path(__file__).with_name('MSK_PRESETS.yaml'))
    presets = _get_msk_presets(msk_presets_path)
    return normalize_crs_v1(req, msk_presets=presets)
