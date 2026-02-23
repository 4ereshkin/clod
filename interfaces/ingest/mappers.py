from __future__ import annotations

import time
from typing import Any

from application.ingest.contracts import (
    ScenarioResult,
    StartIngestCommand,
    StartIngestObjectRef,
    StartIngestScanPayload,
    StatusEvent,
    FailedEvent
)
from application.ingest.status import (
    WorkflowStatus,
)
from interfaces.ingest.dto import (
    IngestStartMessageDTO,
    ResultObjectDTO,
    WorkflowCompletedDTO,
    WorkflowFailedDTO,
    StatusEventDTO
)

def to_start_command(message: IngestStartMessageDTO) -> StartIngestCommand:
    dataset = {
        scan_id: StartIngestScanPayload(
            point_cloud={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in scan.point_cloud.items()},
            trajectory={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in scan.trajectory.items()},
            control_point={k: StartIngestObjectRef(s3_key=v.s3_key, etag=v.etag) for k, v in
                           scan.control_point.items()},
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

def to_status_dto(event: StatusEvent) -> StatusEventDTO:
    return StatusEventDTO(
        workflow_id=event.workflow_id,
        scenario=event.scenario,
        status=event.status,
        timestamp=event.timestamp,
        details=event.details,
    )

def to_completed_event(result: ScenarioResult) -> WorkflowCompletedDTO:
    outputs = [ResultObjectDTO(kind=item.kind, s3_key=item.s3_key, etag=item.etag) for item in result.outputs]
    return WorkflowCompletedDTO(
        workflow_id=result.workflow_id,
        scenario=result.scenario,
        outputs=outputs,
    )

def to_failed_event(event: FailedEvent) -> WorkflowFailedDTO:
    return WorkflowFailedDTO(
        workflow_id=event.workflow_id,
        scenario=event.scenario,
        status=WorkflowStatus.FAILED,
        error_code=event.error_code,
        error_message=event.error_message,
        retryable=event.retryable,
        failed_at=event.failed_at,
    )
