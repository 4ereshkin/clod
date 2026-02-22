from __future__ import annotations

from application.ingest.contracts import ScenarioSpec


def resolve_scenario(*, scenario: str,
                     pipeline_version: str) -> ScenarioSpec:
    key = (scenario.strip().lower(), pipeline_version.strip())

    registry: dict[tuple[str, str], ScenarioSpec] = {
        ('ingest', '1'): ScenarioSpec(
            workflow_name='ingest-1',
            task_queue='point_cloud',
            query_name='progress'
        ),
    }

    if key not in registry:
        raise ValueError(f'Неподдерживаемый сценарий: {scenario!r} версии {pipeline_version!r} не найден')

    return registry[key]