# handler.py
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

from .ingest_request import IngestRequest
from .ingest_contract_v1 import WorkflowIngestPayloadV1
from .normalize_to_workflow_v1 import normalize_to_workflow_v1
from .msk_presets import load_msk_presets_yaml, MSKRegionPreset


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
) -> WorkflowIngestPayloadV1:
    """
    Канонический вход в систему. Неважно, откуда пришёл raw (REST/gRPC/CLI/Nexus).
    Возвращает строго нормализованный payload для передачи в Temporal workflow.
    """
    req = IngestRequest.model_validate(raw)
    if msk_presets_path is None:
        msk_presets_path = str(Path(__file__).with_name("MSK_PRESETS.yaml"))
    presets = _get_msk_presets(msk_presets_path)
    payload = normalize_to_workflow_v1(req, msk_presets=presets)
    return payload


# Ниже — опциональный пример “интеграционного” вызова.
# Здесь не делаем реальный Temporal start, потому что у тебя транспорт не определён.
def start_workflow_placeholder(payload: WorkflowIngestPayloadV1) -> str:
    """
    Заглушка: здесь потом будет Temporal start (или activity submit, или RPC в обработчик облака).
    Сейчас возвращаем идентификатор для отладки.
    """
    # Например: workflow_id = f"ingest:{payload.company}:{payload.employee}:{uuid4()}"
    return f"workflow-placeholder:{payload.company}:{payload.employee}"
