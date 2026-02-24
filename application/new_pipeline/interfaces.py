from typing import Protocol, Any, Dict
from interfaces.new_pipeline.dto import StartPipelineDTO

class NewPipelineTemporalGateway(Protocol):
    async def start_pipeline(self, dto: StartPipelineDTO) -> str:
        """Starts the new architecture pipeline and returns the workflow ID."""
        ...

class NewPipelineEventPublisher(Protocol):
    async def publish_status(self, workflow_id: str, status: str, details: Dict[str, Any] = None):
        ...

class NewPipelineStatusStore(Protocol):
    async def set_status(self, workflow_id: str, status: str, result: Dict[str, Any] = None):
        ...
    async def get_status(self, workflow_id: str) -> Dict[str, Any]:
        ...
