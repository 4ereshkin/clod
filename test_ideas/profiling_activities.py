from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, Dict, List
from pathlib import Path

import json
import pdal

from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn
async def point_cloud_meta(
        point_cloud_file: str,
        geojson_dst: str) -> Dict[str, Any]:

    def _run():
        output_path = Path(geojson_dst)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_json = [
            {
                "type": "readers.las",
             "filename": fr"{point_cloud_file}"
            },
            {
                "type": "filters.info"
            },
            {
                "type": "filters.stats"
            },
            {
                "type": "filters.hexbin",
             "density": str(output_path),
            },
        ]
        try:
            pipeline_spec = json.dumps(pipeline_json)
        except Exception as exc:
            raise ApplicationError(f"Failed to serialize PDAL pipeline:: \n{exc}")

        pipeline = pdal.Pipeline(pipeline_spec  )
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to execute PDAL pipeline: \n{exc}")

        raw_metadata = pipeline.metadata['metadata']

        try:
            metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        except Exception as exc:
            raise ApplicationError(f"Failed to decode PDAL metadata: \n{exc}")

        return metadata.get('metadata', metadata)

    activity.heartbeat(
        {'stage':'point_cloud_meta',
         'file':point_cloud_file,
         '.geojson from filters.hexbin':geojson_dst})
    return await asyncio.to_thread(_run)


@activity.defn
async def read_cloud_hexbin(geojson_dst: str,) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        geojson_path = Path(geojson_dst)
        if not geojson_path.exists():
            raise ApplicationError(f"Hexbin GeoJSON not found: {geojson_path}")

        try:
            return json.loads(geojson_path.read_text(encoding='utf-8'))
        except Exception as exc:
            raise ApplicationError(f"Failed to read hexbin GeoJSON: {geojson_path}")

    activity.heartbeat({
        "stage":"read_cloud_hexbin",
        "geojson_dst":geojson_dst,
    })
    return await asyncio.to_thread(_run)

@activity.defn
async def aggregate_metadata(clouds_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
