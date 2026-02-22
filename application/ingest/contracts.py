from time import time

from typing import Literal, Any
from pydantic import BaseModel, Field
from application.ingest.status import WorkflowStatus, ErrorCode


class StartIngestCommand(BaseModel):
    