from __future__ import annotations

import time
from typing import Any

from application.ingest.contracts import (
    ResultObject,
    StatusEvent
)
from application.ingest.status import (
    WorkflowStatus,
)


def to_status_event(*, workflow_id: str, scenario: str,
                    status: WorkflowStatus,
                    details: dict[str, Any] | None = None) -> StatusEvent:
    return StatusEvent(
        workflow_id=workflow_id,
        scenario=scenario,
        status=status,
        details=details or {},
        timestamp=time.time()
    )

def to_result_objects(raw_outputs: list[dict[str, Any]]) -> list[ResultObject]:
    return [
        ResultObject(kind=str(item.get('kind')),
                     s3_key=str(item.get('s3_key')),
                     etag=str(item.get('etag')))
        for item in raw_outputs
    ]
