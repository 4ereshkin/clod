"""Atomic CRS normalization package."""

from .ingest_request import CRSNormalizeRequestV1
from .ingest_contract_v1 import NormalizedCRSPayloadV1
from .handler import handle_crs_normalization
from .normalize_to_workflow_v1 import normalize_crs_v1


def normalize_raw_to_projjson(raw: dict, *, msk_presets_path: str | None = None) -> str | None:
    """Normalize raw CRS request and return built CRS as PROJJSON string."""
    payload = handle_crs_normalization(raw, msk_presets_path=msk_presets_path)
    return payload.built_crs_projjson
