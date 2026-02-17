from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from .ingest_contract import WorkflowIngestPayload
from .ingest_request import IngestRequest
from .msk_presets import MSKRegionPreset, load_msk_presets_yaml
from .normalize_to_workflow import normalize_to_workflow


@lru_cache(maxsize=1)
def _get_msk_presets(path: str) -> Dict[int, MSKRegionPreset]:
    """
    Загружаем YAML один раз на процесс.
    Важно: вызывать ВНЕ Temporal workflow (на стороне handler / worker / api).
    """
    return load_msk_presets_yaml(path)


def handle_ingest_request(
    raw: Dict[str, Any],
    *,
    msk_presets_path: str | None = None,
) -> WorkflowIngestPayload:
    """
    Канонический вход в систему. Неважно, откуда пришёл raw (REST/gRPC/CLI/Nexus).
    Возвращает строго нормализованный payload для передачи в Temporal workflow.
    """
    req = IngestRequest.model_validate(raw)
    if msk_presets_path is None:
        msk_presets_path = str(Path(__file__).with_name("MSK_PRESETS.yaml"))
    presets = _get_msk_presets(msk_presets_path)
    return normalize_to_workflow(req, msk_presets=presets)


def start_workflow_placeholder(payload: WorkflowIngestPayload) -> str:
    """Заглушка для будущего запуска Temporal workflow."""
    return f"workflow-placeholder:{payload.company}:{payload.employee}"
