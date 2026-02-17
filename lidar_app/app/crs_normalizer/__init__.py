"""CRS normalization utilities for ingest and workflow payload preparation."""

from .handler import handle_ingest_request
from .ingest_contract import WorkflowIngestPayload
from .ingest_request import IngestRequest
from .normalize_to_workflow import normalize_to_workflow


def normalize_raw_to_projjson(raw: dict, *, msk_presets_path: str = "MSK_PRESETS.yaml") -> str | None:
    """Normalize raw ingest request and return built CRS as PROJJSON string."""
    payload = handle_ingest_request(raw, msk_presets_path=msk_presets_path)
    return payload.built_crs_projjson


__all__ = [
    "IngestRequest",
    "WorkflowIngestPayload",
    "handle_ingest_request",
    "normalize_to_workflow",
    "normalize_raw_to_projjson",
]
