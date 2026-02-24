from typing import Any, Protocol


class TemporalGateway(Protocol):
    async def start_workflow(self, *, workflow_name: str,
                             workflow_id: str, task_queue: str,
                             payload: dict[str, Any]) -> None:
        ...
    async def query_workflow(self, *, workflow_id: str,
                             query_name: str) -> dict[str, Any]:
        ...
    async def wait_result(self, *, workflow_id: str,) -> dict[str, Any]:
        ...


class StatusStore(Protocol):
    async def set_status(self, *, workflow_id: str, status: str,
                         payload: dict[str, Any]) -> None:
        ...


class EventPublisher(Protocol):
    async def publish_status(self, event: Any) -> None:
        ...
    async def publish_completed(self, event: Any) -> None:
        ...
    async def publish_failed(self, event: Any) -> None:
        ...
