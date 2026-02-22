from __future__ import annotations

import time
from typing import Any

from application.ingest.contracts import (
    ScenarioResult,
    StartIngestCommand,
    StartIngestObjectRef,
    StartIngestScanPayload
)
from application.ingest.status import (
    WorkflowStatus,
)
from interfaces.ingest.dto import (
    IngestStartMessageDTO,
    ResultObjectDTO,
    WorkflowCompletedDTO,
    WorkflowFailedDTO,
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

def to_completed_event(result: ScenarioResult) -> WorkflowCompletedDTO:
    outputs = [ResultObjectDTO(kind=item.kind, s3_key=item.s3_key, etag=item.etag) for item in result.outputs]
    return WorkflowCompletedDTO(
        workflow_id=result.workflow_id,
        scenario=result.scenario,
        outputs=outputs,
    )

def to_failed_event(*, workflow_id: str,
                    scenario: str,
                    error_code: str,
                    error_message: str,
                    retryable: bool
                    ) -> WorkflowFailedDTO:
    return WorkflowFailedDTO(
        workflow_id=workflow_id,
        scenario=scenario,
        status=WorkflowStatus.FAILED,
        error_code=error_code,
        error_message=error_message,
        retryable=retryable,
        failed_at=time.time()
    )
