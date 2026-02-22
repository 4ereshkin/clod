from __future__ import annotations

from typing import Any

from application.ingest.contracts import (
    ResultObjectDTO as AppResultObjectDTO,
    ScenarioResult,
    StartIngestCommand,
    StartIngestObjectRef,
    StartIngestScanPayload,
    WorkflowStatus,
)
from interfaces.ingest.dto import (
    IngestStartMessageDTO,
    ResultObjectDTO,
    StatusEventDTO,
    WorkflowCompletedEventDTO,
    WorkflowFailedEventDTO,
)


def to_start_command(message: IngestStartMessageDTO) -> StartIngestCommand:
    dataset = {
        scan_id: StartIngestScanPayload(
            point_cloud={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in scan.point_cloud.items()},
            trajectory={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in scan.trajectory.items()},
            control_point={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in scan.control_point.items()},
        )
        for scan_id, scan in message.dataset.items()
    }

    return StartIngestCommand(
        workflow_id=message.workflow_id,
        scenario=message.scenario,
        message_version=message.version.message_version,
        pipeline_version=message.version.pipeline_version,
        dataset=dataset,
    )


def to_status_event(*, workflow_id: str, scenario: str, status: WorkflowStatus, details: dict[str, Any] | None = None) -> StatusEventDTO:
    return StatusEventDTO(
        workflow_id=workflow_id,
        scenario=scenario,
        status=status.value,
        details=details or {},
    )


def to_completed_event(result: ScenarioResult) -> WorkflowCompletedEventDTO:
    outputs = [ResultObjectDTO(kind=item.kind, s3_key=item.s3_key, etag=item.etag) for item in result.outputs]
    return WorkflowCompletedEventDTO(
        workflow_id=result.workflow_id,
        scenario=result.scenario,
        outputs=outputs,
    )


def to_failed_event(
    *,
    workflow_id: str,
    scenario: str,
    error_code: str,
    error_message: str,
    retryable: bool,
) -> WorkflowFailedEventDTO:
    return WorkflowFailedEventDTO(
        workflow_id=workflow_id,
        scenario=scenario,
        error_code=error_code,
        error_message=error_message,
        retryable=retryable,
    )


def to_result_objects(raw_outputs: list[dict[str, Any]]) -> list[AppResultObjectDTO]:
    return [
        AppResultObjectDTO(kind=str(item["kind"]), s3_key=str(item["s3_key"]), etag=item.get("etag"))
        for item in raw_outputs
    ]
