from __future__ import annotations

from application.ingest.contracts import ScenarioSpec


def resolve_scenario(*, scenario: str, pipeline_version: str) -> ScenarioSpec:
    key = (scenario.strip().lower(), pipeline_version.strip())

    registry: dict[tuple[str, str], ScenarioSpec] = {
        ("ingest", "1"): ScenarioSpec(
            workflow_name="1-ingest",
            task_queue="point-cloud",
            query_name="progress",
        ),
    }

    if key not in registry:
        raise ValueError(f"Unsupported scenario={scenario!r} pipeline_version={pipeline_version!r}")

    return registry[key]
