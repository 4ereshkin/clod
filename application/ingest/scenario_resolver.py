from __future__ import annotations

from application.ingest.contracts import ScenarioSpec
from shared.workflows.catalog import *

REGISTRY: dict[tuple[str, str], ScenarioSpec] = {
    ('ingest', '1'): ScenarioSpec(
        workflow_name=INGEST_V1.workflow_name,
        task_queue=INGEST_V1.task_queue,
        query_name=INGEST_V1.query_name,
    )
}

def resolve_scenario(*, scenario: str,
                     pipeline_version: str) -> ScenarioSpec:
    key = (scenario.strip().lower(), pipeline_version.strip())

    if key not in REGISTRY:
        raise ValueError(f'Неподдерживаемый сценарий: {scenario!r} версии {pipeline_version!r} не найден')

    return REGISTRY[key]