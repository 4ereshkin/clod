"""CRS normalization core: transport-agnostic and atomic per CRS object."""

from .models_v1 import CRSNormalizeRequestV1, CRSNormalizeResultV1
from .normalizer_v1 import normalize_crs_v1
from .service import handle_crs_normalization


def normalize_raw_to_projjson(raw: dict, *, msk_presets_path: str | None = None) -> str | None:
    result = handle_crs_normalization(raw, msk_presets_path=msk_presets_path)
    return result.built_crs_projjson


__all__ = [
    'CRSNormalizeRequestV1',
    'CRSNormalizeResultV1',
    'normalize_crs_v1',
    'handle_crs_normalization',
    'normalize_raw_to_projjson',
]
