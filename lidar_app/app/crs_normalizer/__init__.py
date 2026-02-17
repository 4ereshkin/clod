"""CRS normalization utilities extracted from `test_ideas/new_ingest`.

This package provides a stable import path for production code.
"""

from .ingest_request import IngestRequest
from .ingest_contract_v1 import WorkflowIngestPayloadV1
from .handler import handle_ingest_request
from .normalize_to_workflow_v1 import normalize_to_workflow_v1


def normalize_raw_to_projjson(raw: dict, *, msk_presets_path: str = "MSK_PRESETS.yaml") -> str | None:
    """Normalize raw ingest request and return built CRS as PROJJSON string."""
    payload = handle_ingest_request(raw, msk_presets_path=msk_presets_path)
    return payload.built_crs_projjson
