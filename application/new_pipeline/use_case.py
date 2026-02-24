import asyncio
import logging
from typing import Dict, Any

from interfaces.new_pipeline.dto import StartPipelineDTO
from application.new_pipeline.interfaces import (
    NewPipelineTemporalGateway,
    NewPipelineEventPublisher,
    NewPipelineStatusStore
)

logger = logging.getLogger(__name__)

class StartNewPipelineUseCase:
    def __init__(
        self,
        temporal: NewPipelineTemporalGateway,
        store: NewPipelineStatusStore,
        publisher: NewPipelineEventPublisher
    ):
        self.temporal = temporal
        self.store = store
        self.publisher = publisher

    async def execute(self, dto: StartPipelineDTO):
        workflow_id = await self.temporal.start_pipeline(dto)

        await self.store.set_status(workflow_id, "STARTED")
        await self.publisher.publish_status(workflow_id, "STARTED", {"company_id": dto.company_id})

        logger.info(f"Pipeline started: {workflow_id}")
        return workflow_id
